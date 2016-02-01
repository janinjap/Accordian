/**
 *
 */
package org.apache.hadoop.hdfs.server.gear;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenodeFBT.FBTDirectory;
import org.apache.hadoop.hdfs.server.namenodeFBT.NameNodeFBT;
import org.apache.hadoop.hdfs.server.namenodeFBT.utils.StringUtility;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.GearProtocol;
import org.apache.hadoop.hdfs.server.protocol.NNClusterProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.TransferMetadataProtocol;
import org.apache.hadoop.hdfs.server.protocol.WOLProtocol;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;

/**
 * @author hanhlh
 *
 */
public abstract class GearManager implements GearProtocol{

	public long getProtocolVersion(String protocol, long clientVersion)
			throws IOException {
		//System.out.println(GearProtocol.class.getName());
		if (protocol.equals(GearProtocol.class.getName())) {
			return GearProtocol.versionID;
		} else {
		      throw new IOException("Unknown protocol to gear manager: " + protocol);
		}
	}
	public static final int HIGH_GEAR=3;
	public static final int MEDIUM_GEAR=2;
	public static final int LOW_GEAR=1;

	protected int _currentGear;
	protected int _previousGear;
	protected boolean _upGear;
	protected boolean _downGear;

	protected String _namenodeType;

	/**
	 * 3: High Gear
	 * 2: Medium Gear
	 * 1: Low Gear
	 * */

	private Map<Integer, ArrayList<String>> gearActivateNodeMap;

	protected ConcurrentHashMap<String, String[]> backUpMapping;

	protected ArrayList<String> containedFBT;

	protected ConcurrentHashMap<Integer, InetSocketAddress> nnRPCAddrs;
	

	public GearManager(String gearConfig, String namenodeType) {

		Configuration conf = new Configuration();

		Configuration.addDefaultResource(gearConfig);
		_currentGear = conf.getInt("gear.numberOfGear",1);
		_previousGear = 0;
		_namenodeType = namenodeType;
		_upGear = false;
		_downGear = false;
		gearActivateNodeMap = new HashMap<Integer, ArrayList<String>>();
		int numOfGear = conf.getInt("gear.numberOfGear", 1);
		 if (numOfGear<1) {
			  System.err.print("wrong numOfGear");
		  }

		 for (int i=1; i<=numOfGear; i++) {
			  String[] activateNodesStr = conf.getStrings("gear.Gear["+i+"]");
			  ArrayList<String> activateNodesL = new ArrayList<String>();
			  for (String activateNode : activateNodesStr) {
				  activateNodesL.add(activateNode);
			  }
			  gearActivateNodeMap.put(i, activateNodesL);
		  }
		  System.out.println("gearActivateNode: "+gearActivateNodeMap.size());
		  System.out.println("currentGear: "+_currentGear);
	}


	@Override
	public int getCurrentGear() {
		return _currentGear;
	}
	public int setCurrentGearLocal(int nextGear) {
		_currentGear = nextGear;
		return  _currentGear;
	}
	public int getPreviousGear() {
		return _previousGear;
	}
	public int setPreviousGearLocal(int previousGear) {
		_previousGear = previousGear;
		return _previousGear;
	}
	protected GearProtocol gearProtocol;
	@Override
	public int setCurrentGear(int gear) {
		if (!_namenodeType.equals("default")) {
			for (Iterator<Map.Entry<Integer, InetSocketAddress>> i = nnRPCAddrs
					.entrySet().iterator(); i.hasNext();) {
				InetSocketAddress a = i.next().getValue();
				if (!a.equals(NameNode.getNameNodeAddress())) {
					try {
						gearProtocol = (GearProtocol) RPC.waitForProxy(
								GearProtocol.class, GearProtocol.versionID,
								a, new Configuration());
						System.out.println("connect to " +a.toString());
						gearProtocol.catchSetCurrentGear(gear);
					} catch (IOException e) {
						// TODO �����������catch ������
						e.printStackTrace();
					}

				}
			}
			modifyStructure(_currentGear, gear);
		}
		_previousGear = _currentGear;
		_currentGear = gear;
		if (_currentGear < gear) {
			_upGear = true;
			_downGear = false;
		} else if (_currentGear > gear) {
			_upGear = false;
			_downGear = true;
		}
			
		return _currentGear;
	}
	public boolean getUpGear() {
		return _upGear;
	}
	public boolean getDownGear() {
		return _downGear;
	}
	public void setUpGear(boolean upGear) {
		_upGear=upGear;
	}
	public void setDownGear(boolean downGear) {
		_downGear=downGear;
	}

	@Override
	public int upGear() {
		return _currentGear++;
	}

	@Override
	public int downGear() {
		return _currentGear--;
	}

	public ArrayList<String> getActivateNodes(int gear) {
		return gearActivateNodeMap.get(gear);
	}
	public Map<Integer, ArrayList<String>> getActivateNodesMap() {
		return gearActivateNodeMap;
	}

	public void setContainNamespace(ArrayList<String> containFBTs) {
		this.containedFBT = containFBTs;
	}

	public void setNNRPCAddrs (ConcurrentHashMap<Integer, InetSocketAddress>
												nnRPCAddrs) {
		this.nnRPCAddrs = nnRPCAddrs;
	}
	public ConcurrentHashMap<Integer, InetSocketAddress> getNNRPCAddrs() {
		return this.nnRPCAddrs;
	}
	@Override
	public boolean modifyStructure(int oldGear, int newGear) {
		// TODO �������������純�����鴻���
		return false;
	}

	public void setEndPointFBTMapping(
			TreeMap<String, FBTDirectory> _endPointFBTMapping) {
		StringUtility.debugSpace("GearManager.setEndPointMapping");
	}

	public void setEndPointNamesystemMapping(
			Map<String, FSNamesystem> _endPointFSNamesystemMapping) {
		// TODO �������������純�����鴻���

	}

	public void setBackUpMapping(ConcurrentHashMap<String, String[]>
											backUpMapping) {
		this.backUpMapping = backUpMapping;
	}

	public ConcurrentHashMap<String, String[]> getBackUpMapping() {
		return this.backUpMapping;
	}
	@Override
	public int catchSetCurrentGear(int gear) {
		StringUtility.debugSpace("GearManager.catchSetCurrentGear");
		modifyStructure(_currentGear, gear);
		_currentGear=gear;
		return _currentGear;
	}

	@Override
	public boolean transferDeferredNamespace(String targetMachine, String owner) {
		// TODO �������������純�����鴻���
		return false;
	}


	@Override
	public boolean transferDeferredDirectory(String targetMachine,
			String owner, Configuration conf) {
		// TODO �������������純�����鴻���
		return false;
	}


	@Override
	public boolean transferDeferredNamespace(String targetMachine,
			String transferBlocksCommandIssueMode, String transferBlockMode) {
		// TODO �������������純�����鴻���
		return false;
	}
	public boolean transferDeferredNamespace(String targetMachine,
			String transferBlocksCommandIssueMode, String transferBlockMode,
			int currentGear, int nextGear) {
		// TODO �������������純�����鴻���
		return false;
	}


	public boolean transferDeferredNamespaces(String[] targetMachines,
			String transferBlocksCommandIssueMode, String transferBlockMode,
			int currentGear, int nextGear) {
		// TODO �������������純�����鴻���
		StringUtility.debugSpace("GearManager.transferDefferedNamespace to "+targetMachines.toString() +", "+
				transferBlocksCommandIssueMode+", "+
				transferBlockMode+","+
				currentGear+","+
				nextGear);

		return false;
	}

	@Override
	public boolean transferDeferredDirectory(String targetMachine,
			Configuration conf, String transferBlockMode) {
		// TODO �������������純�����鴻���
		return false;
	}


	@Override
	public boolean catchTransferDefferedNamespace(String targetMachine,
			String owner) {
		// TODO �������������純�����鴻���
		return false;
	}


	@Override
	public boolean catchModifyStructure(int oldGear, int newGear) {
		// TODO �������������純�����鴻���
		return false;
	}


	public boolean catchTransferDefferedNamespace(String targetMachine,
			String owner, boolean _downGear, boolean _upGear) {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public boolean setNameSpaceAccessDelay() {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public boolean catchSetNameSpaceAccessDelay() {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public boolean catchTransferDefferedNamespace(String targetMachine,
			String owner, int currentGear, int nextGear) {
		// TODO Auto-generated method stub
		return false;
	}


}
