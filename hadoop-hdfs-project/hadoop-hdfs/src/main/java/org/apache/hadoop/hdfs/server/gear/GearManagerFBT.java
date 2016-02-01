/**
 *
 */
package org.apache.hadoop.hdfs.server.gear;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenodeFBT.FBTDirectory;
import org.apache.hadoop.hdfs.server.namenodeFBT.FBTModifyVisitor;
import org.apache.hadoop.hdfs.server.namenodeFBT.FBTTransferedDirectoryVisitor;
import org.apache.hadoop.hdfs.server.namenodeFBT.NameNodeFBT;
import org.apache.hadoop.hdfs.server.namenodeFBT.NameNodeFBTProcessor;
import org.apache.hadoop.hdfs.server.namenodeFBT.utils.StringUtility;
import org.apache.hadoop.hdfs.server.protocol.GearProtocol;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;

/**
 * @author hanhlh
 *
 */
public class GearManagerFBT extends GearManager{


	public GearManagerFBT(String gearConfig, String namenodeType) {
		super(gearConfig, namenodeType);
		StringUtility.debugSpace("GearManagerFBT called");
	}

	@Override
	public boolean modifyStructure(int oldGear, int newGear) {
		StringUtility.debugSpace("GearManagerFBT.modifyStructure oldGear, "+oldGear+
									", newGear, "+ newGear);
		boolean results = false;
		ArrayList<String> activeNodesNewGear = getActivateNodes(newGear);
		String localHostName=NameNode.getNameNodeAddress().getHostName();
		if (activeNodesNewGear.contains(localHostName)) {
			modifyStructureAtNonDeactivatedNodes(localHostName,
										activeNodesNewGear, oldGear, newGear);
		} else if (!activeNodesNewGear.contains(localHostName)) {
			modifyStructureFromDeactivatedNodes(localHostName, oldGear, newGear);
		}
		results=true;
		StringUtility.debugSpace("GearManagerFBT.modifyStructure end and namespace is ready to access");
		NameNodeFBT.setDelay(false);
		return results;
	}

	public boolean modifyStructureAtNonDeactivatedNodes(String hostname,
												ArrayList<String> activeNodesNewGear,
												int oldGear, int newGear) {
		StringUtility.debugSpace("GearManagerGBT.modifyStructureAtNonDeactivatedNodes "+hostname+
								", "+oldGear+
								", "+newGear);
		FBTDirectory modifiedDirectory = (FBTDirectory) NameNodeFBT.getEndPointFBTMapping().get(hostname);
		FBTModifyVisitor visitor = new FBTModifyVisitor(modifiedDirectory,
													oldGear, newGear, backUpMapping);
		visitor.setActivateNodes(activeNodesNewGear);
		visitor.run();
		return true;
	}

	public boolean modifyStructureFromDeactivatedNodes(String hostname,
												int oldGear, int newGear) {
		StringUtility.debugSpace("GearManagerGBT.modifyStructureAtDeactivatedNodes "+hostname+
								", "+oldGear+
								", "+newGear);
		FBTDirectory modifiedDirectory = (FBTDirectory) NameNodeFBT.getEndPointFBTMapping().get(hostname);
		FBTModifyVisitor visitor = new FBTModifyVisitor(modifiedDirectory,
													oldGear, newGear, backUpMapping, true);
		visitor.run();
		return true;
	}
	public int catchSetCurrentGear(int gear) {
		StringUtility.debugSpace("GearManagerFBT.catchSetCurrentGear");
		modifyStructure(getCurrentGear(), gear);
		_currentGear = gear;
		return gear;
	}

	@Override
	/**
	 * transferBlocksCommandIssueMode sequential or batch
	 * transferBlockMode 			  sequential or batch
	 * */
	public boolean transferDeferredNamespace(String targetMachine,
							String transferBlocksCommandIssueMode,
							String transferBlockMode) {
		StringUtility.debugSpace("NameNodeFBT.Namespace to "+targetMachine +", "+
								transferBlocksCommandIssueMode+", "+
								transferBlockMode);
		Date start = new Date();
		Configuration conf = new Configuration();
		boolean result = false;
		result = transferDeferredDirectory(targetMachine, targetMachine, conf);
		NameNode.LOG.info("NameNodeFBT.transferNamespace to "+targetMachine+","+
							(new Date().getTime()-start.getTime())/1000.0);
		return result;
	}
	public boolean transferDeferredNamespace(String targetMachine,
			String transferBlocksCommandIssueMode,
			String transferBlockMode,
			int currentGear,
			int nextGear) {
		StringUtility.debugSpace("NameNodeFBT.Namespace to "+targetMachine +", "+
						transferBlocksCommandIssueMode+", "+
						transferBlockMode+","+
						currentGear+","+
						nextGear);
		Date start = new Date();
		Configuration conf = new Configuration();
		boolean result = false;
		result = transferDeferredDirectory(targetMachine, targetMachine, conf, 
								currentGear, 
								nextGear);
		//modify Structure here
		//if down gear: modify @ nodes will deactivate; modify@nodes will receive the namespace for deactivated nodes
		//if up gear: modify @ nodes will reactivated; modify@nodes send the namespace to reactivated nodes
		//reconfig the delay parameter here
		if (currentGear > nextGear) { //downGear
			//modify @ deactivate nodes
			//ArrayList<String> activeNodesNewGear = getActivateNodes(nextGear);
			String localHostName=NameNode.getNameNodeAddress().getHostName();
			//if (!activeNodesNewGear.contains(localHostName)) {
				result = result && modifyStructureFromDeactivatedNodes(localHostName, currentGear, nextGear);
			//}
		} else if (currentGear < nextGear) { //upGear
			//modify @ nondeactivate nodes
			ArrayList<String> activeNodesNewGear = getActivateNodes(nextGear);
			String localHostName=NameNode.getNameNodeAddress().getHostName();
			//if (activeNodesNewGear.contains(localHostName)) {
				result = result && modifyStructureAtNonDeactivatedNodes(localHostName,
											activeNodesNewGear, currentGear, nextGear);
			//}
		}
		NameNodeFBT.setDelay(false);
		
		NameNode.LOG.info("NameNodeFBT.transferNamespace to "+targetMachine+","+
					(new Date().getTime()-start.getTime())/1000.0);
		return result;
	}

	public boolean transferDeferredNamespaces(String[] targetMachines,
			String transferBlocksCommandIssueMode,
			String transferBlockMode,
			int currentGear,
			int nextGear) {
		StringUtility.debugSpace("GearManagerFBT.transferDefferedNamespaces to "+targetMachines.toString() +", "+
						transferBlocksCommandIssueMode+", "+
						transferBlockMode+","+
						currentGear+","+
						nextGear);
		Date start = new Date();
		Configuration conf = new Configuration();
		boolean result = false;
		for (String targetMachine:targetMachines) {
			result = transferDeferredDirectory(targetMachine, targetMachine, conf, 
								currentGear, 
								nextGear);
		}
		//modify Structure here
		//if down gear: modify @ nodes will deactivate; modify@nodes will receive the namespace for deactivated nodes
		//if up gear: modify @ nodes will reactivated; modify@nodes send the namespace to reactivated nodes
		//reconfig the delay parameter here
		if (currentGear > nextGear) { //downGear
			//modify @ deactivate nodes
			//ArrayList<String> activeNodesNewGear = getActivateNodes(nextGear);
			String localHostName=NameNode.getNameNodeAddress().getHostName();
			//if (!activeNodesNewGear.contains(localHostName)) {
				result = result && modifyStructureFromDeactivatedNodes(localHostName, currentGear, nextGear);
			//}
		} else if (currentGear < nextGear) { //upGear
			//modify @ nondeactivate nodes
			ArrayList<String> activeNodesNewGear = getActivateNodes(nextGear);
			String localHostName=NameNode.getNameNodeAddress().getHostName();
			//if (activeNodesNewGear.contains(localHostName)) {
				result = result && modifyStructureAtNonDeactivatedNodes(localHostName,
											activeNodesNewGear, currentGear, nextGear);
			//}
		}
		NameNodeFBT.setDelay(false);
		
		NameNode.LOG.info("NameNodeFBT.transferNamespace to "+targetMachines.toString()+","+
					(new Date().getTime()-start.getTime())/1000.0);
		return result;
	}

	@Override
	public boolean transferDeferredNamespace(
					String targetMachine, String owner) {
		StringUtility.debugSpace("GearManagerFBT.transferDeferredNamespace to "+targetMachine);
		return transferDeferredDirectory(targetMachine, owner, new Configuration());
	}
	public boolean transferDeferredNamespace(
					String targetMachine, String owner, int currentGear, int nextGear) {
		StringUtility.debugSpace("GearManagerFBT.transferDeferredNamespace to "+targetMachine);
		return transferDeferredDirectory(targetMachine, owner, new Configuration(),
										currentGear, nextGear);
	}
	@Override
	/**
	 * Transfer deferred directory to original node (up gear)
	 * */
	public boolean transferDeferredDirectory(String targetMachine, String owner, Configuration conf)
						{
		StringUtility.debugSpace("GearManagerFBT.transferDifferenceDirectory to "+targetMachine+
									", owner "+owner);
		//Date xferDirectoryStart = new Date();
		FBTDirectory directory = NameNodeFBT.getEndPointFBTMapping().get(NameNode.
											getNameNodeAddress().getHostName());
		//FBTDirectory directory =
	    //       (FBTDirectory) NameNodeFBTProcessor.lookup("/directory.".concat(targetMachine));

		//directory.blocksMap = _endPointFBTMapping.get(serverAddress.getHostName()).blocksMap;
		FBTTransferedDirectoryVisitor visitor =
			new FBTTransferedDirectoryVisitor(directory, targetMachine, owner);
		visitor.run();
		return true;
	}
	
	public boolean transferDeferredDirectory(String targetMachine, String owner, Configuration conf,
						int currentGear, int nextGear)
	{
		StringUtility.debugSpace("GearManagerFBT.transferDifferenceDirectory to "+targetMachine+
				", owner "+owner+
				", currentGear "+currentGear+
				", nextGear "+nextGear);
			//Date xferDirectoryStart = new Date();
			FBTDirectory directory = NameNodeFBT.getEndPointFBTMapping().get(NameNode.
									getNameNodeAddress().getHostName());
			//FBTDirectory directory =
			//       (FBTDirectory) NameNodeFBTProcessor.lookup("/directory.".concat(targetMachine));
			
			//directory.blocksMap = _endPointFBTMapping.get(serverAddress.getHostName()).blocksMap;
			FBTTransferedDirectoryVisitor visitor =
			new FBTTransferedDirectoryVisitor(directory, targetMachine, owner, currentGear, nextGear);			
			visitor.run();
			return true;
	}
		
	public boolean getTransferedDirectoryHandlerDownGear
						(FBTDirectory directory) {
		boolean result= true;
		StringUtility.debugSpace("NameNodeFBT.getTransferedDirectoryHandlerDownGear");
		try {
			int pre_Gear = NameNodeFBT.gearManager.getPreviousGear();
			int cur_Gear = NameNodeFBT.gearManager.getCurrentGear();
			System.out.println("Modify FBT structure Gear "+pre_Gear+" to Gear "+cur_Gear);			
			NameNodeFBT.getEndPointFBTMapping().
						get(NameNode.getNameNodeAddress().getHostName()).modify(
												directory,
												pre_Gear,
												cur_Gear,
												NameNodeFBT.gearManager.getBackUpMapping()
												);
			directory.modify(directory,
							pre_Gear,
							cur_Gear,
							NameNodeFBT.gearManager.getBackUpMapping());
		} catch (UnknownHostException e) {
			e.printStackTrace();
			result = false;
		}
		NameNodeFBT.getEndPointFBTMapping().put(directory.getOwner(), directory);
		NameNodeFBTProcessor.bind("/directory.".concat(
				directory.getOwner()), directory);
		return result;

	}

	/**
	 * First, send transfer deferred namespace command to going-to-be-deactivated nodes
	 * Second, send block map to going-to-be-deactivated nodes
	 * Third, send modify structure command to going-to-be-activated nodes
	 * */
	public int setCurrentGear(int newGear) {
		StringUtility.debugSpace("GearManagerFBT.setCurrentGear "+newGear);
		boolean downGear = (_currentGear>newGear)?true:false;
		if (downGear) {
			setDownGear(true);
			setUpGear(false);
		} else {
			setDownGear(false);
			setUpGear(true);
		}
		if (getDownGear()) {
			//send transfer namespace command to other going-to-deactivated nodes
			String hostName = NameNode.getNameNodeAddress().getHostName();
			for(Iterator<Map.Entry<Integer, InetSocketAddress>> i = nnRPCAddrs
					.entrySet().iterator(); i.hasNext();) {
				InetSocketAddress isa = i.next().getValue();
				if(!getActivateNodes(newGear).contains(isa.getHostName())) {
					try {
						gearProtocol = (GearProtocol) RPC.waitForProxy(
								GearProtocol.class, GearProtocol.versionID,
								isa, new Configuration());
						String[] locations = backUpMapping.get(isa.getHostName());
						String targetMachine = locations[newGear-1]; //newGear=2
						System.out.println("GearManagerFBT.setCurrentGear.downGear send "
								+ "deffered namespace to "+
								targetMachine+ " from "+ isa.getHostName());
						boolean finished = gearProtocol.catchTransferDefferedNamespace(
											targetMachine, isa.getHostName(), _currentGear, newGear);
						if (finished)
							continue;
					} catch (IOException e) {
						// TODO �����������catch ������
						e.printStackTrace();
					}

				}
			}

		}

		StringUtility.debugSpace("finished transfered namespace");
/*
		if (!_namenodeType.equals("default")) {
			for (Iterator<Map.Entry<Integer, InetSocketAddress>> i = nnRPCAddrs
					.entrySet().iterator(); i.hasNext();) {
				InetSocketAddress a = i.next().getValue();
				if (getActivateNodes(newGear).contains(a.getHostName()) &&
						(!a.getHostName().equals(NameNode.getNameNodeAddress().getHostName()))) {
					try {
						gearProtocol = (GearProtocol) RPC.waitForProxy(
								GearProtocol.class, GearProtocol.versionID,
								a, new Configuration());
						System.out.println("connect to " +a.toString());
						boolean finished = false;
						while (!finished) {
							finished = gearProtocol.catchModifyStructure(_currentGear, newGear);
						}
					} catch (IOException e) {
						// TODO �����������catch ������
						e.printStackTrace();
					}

				}
			}
			modifyStructure(_currentGear, newGear);
		}
*/		_currentGear = newGear;
		StringUtility.debugSpace("finished modify structure");
		return newGear;
	}


	public boolean catchTransferDefferedNamespace(String targetMachine,
			String owner) {
		return transferDeferredNamespace(targetMachine, owner);
	}
	public boolean catchTransferDefferedNamespace(String targetMachine,
			String owner, int currentGear, int  nextGear) {
		return transferDeferredNamespace(targetMachine, owner, currentGear, nextGear);
	}

	public boolean catchModifyStructure(int oldGear, int newGear) {
		modifyStructure(oldGear, newGear);
		_currentGear = newGear;
		return true;
	}

	@Override
	/*
	 * Set namespace at all nodes delay until transferNamespace and modifyStructure finished
	 * Only performed at one of the CoveringSet Node which is the master node.
	 * Here is edn32. 
	 */
	public boolean setNameSpaceAccessDelay() {
		for(Iterator<Map.Entry<Integer, InetSocketAddress>> i = nnRPCAddrs
				.entrySet().iterator(); i.hasNext();) {
			InetSocketAddress isa = i.next().getValue();
			if (!NameNode.getNameNodeAddress().getHostName().equals(isa.getHostName())) {
				try {
					gearProtocol = (GearProtocol) RPC.waitForProxy(
								GearProtocol.class, GearProtocol.versionID,
								isa, new Configuration());
					System.out.println("GearManagerFBT.setNameSpaceAccessDelay send to "+
								isa.getHostName() +
								" from "+ 
								NameNode.getNameNodeAddress().getHostName());
					boolean finished = gearProtocol.catchSetNameSpaceAccessDelay();
					if (finished)
						continue;
				} catch (IOException e) {
					// TODO �����������catch ������
					e.printStackTrace();
					return false;
				}
			}
		}
		NameNodeFBT.setDelay(true);
		return true;
	}


	@Override
	public boolean catchSetNameSpaceAccessDelay() {
		StringUtility.debugSpace("GearManagerFBT.catchSetNameSpaceAccessDelay");
		return NameNodeFBT.setDelay(true);
	}

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }


}
