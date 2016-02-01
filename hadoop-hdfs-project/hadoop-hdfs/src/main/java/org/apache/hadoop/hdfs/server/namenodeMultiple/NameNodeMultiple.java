/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeMultiple;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.Forwards;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.TargetNamespace;
import org.apache.hadoop.hdfs.server.namenode.WriteOffLoading;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.CompleteFileStatus;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.writeOffLoading.WriteOffLoadingCommand;
import org.apache.hadoop.hdfs.server.namenodeFBT.NameNodeFBT;
import org.apache.hadoop.hdfs.server.namenodeFBT.msg.MessageException;
import org.apache.hadoop.hdfs.server.namenodeFBT.service.ServiceException;
import org.apache.hadoop.hdfs.server.namenodeFBT.utils.StringUtility;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NNClusterProtocol;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
//import org.apache.hadoop.security.authorize.ConfiguredPolicy; //janin 
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * @author hanhlh
 *
 */
public class NameNodeMultiple extends NameNode {

	private Map<Integer, String> _nameNodeIDMapping;

	public static int currentGear = 1;

	public NameNodeMultiple(Configuration conf) throws IOException {
		super(conf);
		//initialize(conf);
	}


/*	private void initialize(Configuration conf) throws IOException {
		  NameNode.LOG.info("NameNode.initialze()");
	    InetSocketAddress socAddr = NameNode.getAddress(conf);
	    int handlerCount = conf.getInt("dfs.namenode.handler.count", 10);

	    // set service-level authorization security policy
	    if (serviceAuthEnabled =
	          conf.getBoolean(
	            ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, false)) {
	      PolicyProvider policyProvider =
	        (PolicyProvider)(ReflectionUtils.newInstance(
	            conf.getClass(PolicyProvider.POLICY_PROVIDER_CONFIG,
	                HDFSPolicyProvider.class, PolicyProvider.class),
	            conf));
	      SecurityUtil.setPolicy(new ConfiguredPolicy(conf, policyProvider));
	    }


	    // create rpc server
	    this.server = RPC.getServer(this,
	    							socAddr.getHostName(),
	    							socAddr.getPort(),
	                                handlerCount,
	                                false,
	                                conf);

	    // The rpc-server port can be ephemeral... ensure we have the correct info
	    this.serverAddress = this.server.getListenerAddress();
	    FileSystem.setDefaultUri(conf, getUri(serverAddress));
	    LOG.info("Namenode up at: " + this.serverAddress);

	    myMetrics = new NameNodeMetrics(conf, this);


	    //initialize namenodeIDMapping
	    //setNameNodeIDMapping(conf);

	    //namesystem = new FSNamesystem(this, conf, serverAddress.getHostName());
	    namesystem = new FSNamesystem(this, conf);
		this.namenodeID = getNamenodeID(conf);
		namesystem.setNamenodeID(namenodeID);
		setStartAndEnd(conf);
		// end of appended//

	    //startHttpServer(conf); TODO make it done
	    this.server.start();  //start RPC server

	 // appended
		nnRPCAddrs = new ConcurrentHashMap<Integer, InetSocketAddress>();
		InetSocketAddress leadAddr = getLeaderAddress(conf);
		if (leadAddr.equals(serverAddress))
			nnRPCAddrs.put(new Integer(namenodeID), leadAddr);
		else {
			nnRPCAddrs.put(new Integer(namenodeID), serverAddress);
			nnNamenode = (NNClusterProtocol) RPC.waitForProxy(
					NNClusterProtocol.class, NNClusterProtocol.versionID,
					leadAddr, conf);
			String host = serverAddress.getHostName();
			int port = serverAddress.getPort();
			int id = namenodeID;
			nnNamenode.namenodeRegistration(host, port, id);
		}
		// end of appended
	    startTrashEmptier(conf);
	  }

	  public LocatedBlocks   getBlockLocations(String src,
              long offset,
              long length) throws IOException {
		myMetrics.numGetBlockLocations.inc();
		//NameNode.LOG.info("NameNode.getBlockLocations "+src+"at "+ this.namenodeID);
		Integer getAt = selection(src);
		if(nnRPCAddrs.get(getAt) == null) {
			end = "END";
			//getAt = responsibleNamespace.getNamenodeID();
		}
		if (getAt.intValue() == this.namenodeID) {
			System.out.println("getBlockLocations "+src+" locally at "+this.namenodeID);

			return namesystem.getBlockLocations(src, offset, length);
			} else {
					ClientProtocol name = (ClientProtocol) RPC.waitForProxy(
					ClientProtocol.class, ClientProtocol.versionID, nnRPCAddrs.get(getAt),
					new Configuration());
					System.out.println("forward getBlockLocations "+src+" to "+getAt.intValue());
					return name.getBlockLocations(src, offset, length);
			}
	  }


	  public
	  //void
	  boolean
	  create(String src,
	                     FsPermission masked,
	                             String clientName,
	                             boolean overwrite,
	                             short replication,
	                             long blockSize
	                             ) throws IOException {
		  boolean success = false; //;append

		  Integer createAt = selection(src);
		if(nnRPCAddrs.get(createAt) == null) {
				end = "END";
				//createAt = tn.getNamenodeID();
			}
			if (createAt.intValue() == this.namenodeID) {
				System.out.println("createLocal "+src+" at "+createAt);
				String clientMachine = getClientMachine();
				if (stateChangeLog.isDebugEnabled()) {
					stateChangeLog.debug("*DIR* NameNode.create: file "
							+src+" for "+clientName+" at "+clientMachine);
				}
				if (!checkPathLength(src)) {
					throw new IOException("create: Pathname too long.  Limit "
		                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
				}
				namesystem.startFile(src,
			    					new PermissionStatus(
			    							UserGroupInformation.getCurrentUGI().getUserName(),
			    							null, masked),
			    					clientName, clientMachine, overwrite, replication, blockSize);
			    myMetrics.numFilesCreated.inc();
			    myMetrics.numCreateFileOps.inc();
			    success = true;

			} else {
					ClientProtocol namenode = (ClientProtocol) RPC.waitForProxy(
							ClientProtocol.class, ClientProtocol.versionID,
							nnRPCAddrs.get(createAt), new Configuration());
					NameNode.LOG.info("forward from "+this.namenodeID+" create "+src+" to "+createAt);
					success =
					namenode.create(src, masked, clientName, overwrite, replication, blockSize);
			}

		return success;
	  }


	  public LocatedBlock addBlock(String src, String clientName,
				DatanodeInfo[] excludedNodes) throws IOException {
		  LocatedBlock lb = null;
			// appended(if~) //
		  Integer addAt = selection(src);
			if (addAt.intValue() == namenodeID) {
				List<Node> excludedNodeList = null;
				if (excludedNodes != null) {
					// We must copy here, since this list gets modified later on
					// in ReplicationTargetChooser
					excludedNodeList = new ArrayList<Node>(
							Arrays.<Node> asList(excludedNodes));
				}

				stateChangeLog.debug("*BLOCK* NameNode.addBlock: file " + src
						+ " for " + clientName);
				LocatedBlock locatedBlock =
					namesystem.getAdditionalBlock(src,
								clientName, excludedNodeList);
				if (locatedBlock != null)
					lb = locatedBlock;
					//myMetrics.incrNumAddBlockOps();
				// forwardAddBlock(src, clientName, excludedNodes);

			} else {
				ClientProtocol namenode = (ClientProtocol) RPC.waitForProxy(
						ClientProtocol.class, ClientProtocol.versionID,
						nnRPCAddrs.get(new Integer(addAt)), new Configuration());
				NameNode.LOG.info("forward addBlock from "+namenodeID+ "to "+addAt);
				lb = namenode.addBlock(src, clientName, excludedNodes);
			}
			return lb;
		}

	  *//**
	   * The client needs to give up on the block.
	   *//*
	  public void abandonBlock(Block b, String src, String holder
	      ) throws IOException {
		// appended(if~) //
		  Integer abandonAt = selection(src);
			if (abandonAt.intValue() == namenodeID) {
				stateChangeLog.debug("*BLOCK* NameNode.abandonBlock: " + b
						+ " of file " + src);
				if (namesystem.abandonBlock(b, src, holder)) {
					throw new IOException("Cannot abandon block during write to "
							+ src);
				}

			} else {
				ClientProtocol namenode = (ClientProtocol) RPC.waitForProxy(
						ClientProtocol.class, ClientProtocol.versionID,
						nnRPCAddrs.get(abandonAt), new Configuration());
				namenode.abandonBlock(b, src, holder);
			}
	  }

	  *//** {@inheritDoc}
	 * @throws MessageException *//*
	  public boolean complete(String src, String clientName) throws IOException {
		// appended //
		  Integer completeAt = selection(src);
			if (completeAt.intValue() == this.namenodeID) {
				NameNode.LOG.info("complete locally "+src+" at "+completeAt);
				stateChangeLog.debug("*DIR* NameNode.complete: " + src + " for "
						+ clientName);
				CompleteFileStatus returnCode =
					namesystem.completeFile(src, clientName);
				// forwardComplete(src, clientName);
				if (returnCode == CompleteFileStatus.STILL_WAITING) {
					return false;
				} else if (returnCode == CompleteFileStatus.COMPLETE_SUCCESS) {
					return true;
				} else {
					throw new IOException("Could not complete write to file " + src
							+ " by " + clientName);
				}
			} else {
				ClientProtocol namenode = (ClientProtocol) RPC.waitForProxy(
						ClientProtocol.class, ClientProtocol.versionID,
						nnRPCAddrs.get(completeAt), new Configuration());
				return namenode.complete(src, clientName);
			}
	  }


	  public boolean mkdirs(String src, FsPermission masked) throws IOException {
		  Integer mkdirAt = selection(src);
		  NameNode.LOG.info("NameNode.mkdirs at "+mkdirAt);
		  if (nnRPCAddrs.get(mkdirAt) == null) {
			  end = "END";
			  //mkdirAt = tn.getNamenodeID();
		  }
		  if (mkdirAt.intValue() == namenodeID) {
			  stateChangeLog.debug("*DIR* NameNode.mkdirs: " + src);
			  if (!checkPathLength(src)) {
				  throw new IOException("mkdirs: Pathname too long.  Limit "
	                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
			  }
			  return namesystem.mkdirs(src,
			  					new PermissionStatus(UserGroupInformation.getCurrentUGI().getUserName(),
							  null, masked));
		  } else {
			  ClientProtocol name = (ClientProtocol) RPC.waitForProxy(
						ClientProtocol.class, ClientProtocol.versionID, nnRPCAddrs.get(mkdirAt),
						new Configuration());
			  return name.mkdirs(src, masked);
		  }
	  }


	  private int selection(String src) {
		  int nodes = nnEnds.keySet().size();
			int i = namenodeID;

			start = nnEnds.get(namenodeID)[0];
			end = nnEnds.get(namenodeID)[1];
			if(src == null)
				return (Integer) null;
			if(src.compareTo(start)<= 0) {
				for(i-- ;i > 0;i--) {
					if(src.compareTo(nnEnds.get(new Integer(i))[1]) > 0) {
						return (new Integer(i + 1));
					}
				}
				return (new Integer(1));
			} else if(src.compareTo(end) > 0) {
				for(i++;i < nodes;i++) {
					if(src.compareTo(nnEnds.get(new Integer(i))[1]) <= 0) {
						return (new Integer(i));
					}
				}
				return (new Integer(nodes));
			} else {
				return (new Integer(i));
			}

	  }

	*//**
	 * DatanodeProtocol
	 * *//*
		public DatanodeRegistration register(DatanodeRegistration nodeReg
        ) throws IOException {
			verifyVersion(nodeReg.getVersion());
			namesystem.registerDatanode(nodeReg);
			forwardRegister(nodeReg);
			return nodeReg;
		}


		public synchronized void forwardRegister(DatanodeRegistration registration)
		throws IOException {
			for (Iterator<Map.Entry<Integer, InetSocketAddress>> i = nnRPCAddrs
					.entrySet().iterator(); i.hasNext();) {
				InetSocketAddress a = i.next().getValue();
				if (!a.equals(serverAddress)) {
					nnNamenode = (NNClusterProtocol) RPC.waitForProxy(
							NNClusterProtocol.class, NNClusterProtocol.versionID,
							a, new Configuration());
					NameNode.LOG.info("foreward datanode registration to "+a);
					nnNamenode.catchRegister(registration);
				}
			}
		}

		public DatanodeCommand[] sendHeartbeat(DatanodeRegistration nodeReg,
                long capacity,
                long dfsUsed,
                long remaining,
                int xmitsInProgress,
                int xceiverCount) throws IOException {
			// verifyRequest(nodeReg);
			//NameNode.LOG.info("NameNode.sendHeartbeat()");
			forwardHeartBeat(nodeReg, capacity, dfsUsed, remaining,
					xmitsInProgress, xceiverCount);
			return namesystem.handleHeartbeat(nodeReg, capacity, dfsUsed,
					remaining, xceiverCount, xmitsInProgress);
}
		public void forwardHeartBeat(DatanodeRegistration nodeReg, long capacity,
				long dfsUsed, long remaining, int xmitsInProgress, int xceiverCount)
				throws IOException {
			Configuration conf = new Configuration();
			Forwards[] f = new Forwards[nnRPCAddrs.size()];
			for(int i = 1;i <= nnRPCAddrs.size();i++) {
				if(!(i == namenodeID)) {
					nnNamenode = (NNClusterProtocol) RPC.waitForProxy(NNClusterProtocol.class, NNClusterProtocol.versionID,  nnRPCAddrs.get(new Integer(i)), conf);
					f[i-1] = new Forwards(nodeReg, capacity, dfsUsed, remaining, xmitsInProgress, xceiverCount, nnNamenode);
					f[i-1].start();
				}
			}
		}

		public void catchHeartBeat(DatanodeRegistration nodeReg, long capacity,
				long dfsUsed, long remaining, int xmitsInProgress, int xceiverCount)
				throws IOException {
			// verifyRequest(nodeReg);
			namesystem.handleHeartbeat(nodeReg, capacity, dfsUsed, remaining,
					xceiverCount, xmitsInProgress);
		}
		public void catchRegister(DatanodeRegistration registration)
		throws IOException {
			NameNode.LOG.info("catchRegister "+registration);
			NameNode.LOG.info("registration name "+registration.name);
			NameNode.LOG.info("registration getName "+registration.getName());
			verifyVersion(registration.getVersion());
			namesystem.registerDatanode(registration);
		}

		//targetNode = edn13

		@Override
		public boolean resetOffloading() throws IOException {
			return resetOffloading(WriteOffLoading.writeOffLoadingFile.concat(
					"."+serverAddress.getHostName()));
		}


		private synchronized boolean resetOffloading(String logFile) throws IOException {
			NameNode.LOG.info("NameNodeFBT.resetOffloading");
			Date start= new Date();
			boolean result = false;
			File file = null;
			FileInputStream fis = null;
			DataInputStream dis = null;
			BufferedReader br = null;
			String line = null;
			//PrintWriter pw = null;
			//NameNode.LOG.info("FSNamesystem.reOffloading offset "+getOffset());
			try {
				file = new File(logFile);
				if (!file.exists())
					if (!file.canRead()) System.out.println("cannot open" +
							logFile);

				fis = new FileInputStream(file);
				dis = new DataInputStream(fis);
				br = new BufferedReader(new InputStreamReader(dis));
				int lineCount=0;
				while ((line = br.readLine())!=null) {
					String[] record = line.split(",");
					String src = record[2];
					String wolDst = record[3];  //IPAddress 192.168.0.114
					String dst = record[4];
					Date startSearchBlock = new Date();
					Block[] block = namesystem.getINodeFile(src).getBlocks();
					NameNode.LOG.info("searchBlock,"+ (new Date().getTime()-
							startSearchBlock.getTime())/1000.0);
					ArrayList<Block[]> blocks = new ArrayList<Block[]>();
					blocks.add(block);
					lineCount++;
					writeOffLoadingCommandHandler(new WriteOffLoadingCommand(
							blocks, dst, dst));
				}

				dis.close();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
			result = true;

			NameNode.LOG.info("NameNodeFBT.resetOffLoading,"+(new Date().getTime()-start.getTime())/1000.0);
			return result;
		}

		public
		//synchronized
		void blockReceived(DatanodeRegistration nodeReg,
		                            Block blocks[],
		                            String delHints[]) throws IOException {
		    verifyRequest(nodeReg);
		    stateChangeLog.debug("*BLOCK* NameNode.blockReceived: "
		                         +"from "+nodeReg.getName()+" "+blocks.length+" blocks.");
		    forwardBlockReceived(nodeReg, blocks, delHints);
		    for (int i = 0; i < blocks.length; i++) {
		      namesystem.blockReceived(nodeReg, blocks[i], delHints[i]);
		    }


		    NameNode.stateChangeLog.info(String.format("%s%d",
					"blockReceived,",
					getBlockReceiveCount(blocks.length)));
		  }

		public void forwardBlockReceived(DatanodeRegistration nodeReg,
				Block[] blocks, String[] delHints) throws IOException {
			Configuration conf = new Configuration();
			Forwards[] f = new Forwards[nnRPCAddrs.size()];
			for(int i = 1;i <= nnRPCAddrs.size();i++) {
				if(!(i == namenodeID)) {
					nnNamenode = (NNClusterProtocol) RPC.waitForProxy(NNClusterProtocol.class, NNClusterProtocol.versionID,  nnRPCAddrs.get(new Integer(i)), conf);
					f[i-1] = new Forwards(nodeReg, blocks, delHints, nnNamenode);
					f[i-1].start();
				}
			}
		}
		public void catchBlockReceived(DatanodeRegistration nodeReg,
				Block[] blocks, String[] delHints) throws IOException {
			// verifyRequest(nodeReg);
			stateChangeLog.debug("*BLOCK* NameNode.blockReceived: " + "from "
					+ nodeReg.getName() + " " + blocks.length + " blocks.");
			for (int i = 0; i < blocks.length; i++) {
				namesystem.blockReceived(nodeReg, blocks[i], delHints[i]);
			}
		}

		synchronized int getBlockReceiveCount(int length) {
			  return _blockReceivedCount.addAndGet(length);
		  }

*/	}
