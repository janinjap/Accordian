/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeStaticPartition;

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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.gear.GearManagerStaticPartition;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.Forwards;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.TargetNamespace;
import org.apache.hadoop.hdfs.server.namenode.WriteOffLoading;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.CompleteFileStatus;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.writeOffLoading.WriteOffLoadingCommand;
import org.apache.hadoop.hdfs.server.namenodeFBT.FBTDirectory;
import org.apache.hadoop.hdfs.server.namenodeFBT.NameNodeFBT;
import org.apache.hadoop.hdfs.server.namenodeFBT.NameNodeFBTProcessor;
import org.apache.hadoop.hdfs.server.namenodeFBT.msg.MessageException;
import org.apache.hadoop.hdfs.server.namenodeFBT.msg.PooledWOLReceiver;
import org.apache.hadoop.hdfs.server.namenodeFBT.msg.WOLReceiver;
import org.apache.hadoop.hdfs.server.namenodeFBT.msg.WOLSender;
import org.apache.hadoop.hdfs.server.namenodeFBT.service.ServiceException;
import org.apache.hadoop.hdfs.server.namenodeFBT.utils.StringUtility;
import org.apache.hadoop.hdfs.server.namenodeFBT.utils.ThreadPool;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NNClusterProtocol;
import org.apache.hadoop.hdfs.server.protocol.TransferMetadataProtocol;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ConfiguredPolicy;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * @author hanhlh
 *
 */
public class NameNodeStaticPartition extends NameNode implements
				NNClusterProtocol,
				TransferMetadataProtocol{

	public NameNodeStaticPartition(Configuration conf) throws IOException {
		super(conf);
		try {
			initialize(conf);
		} catch (Exception e) {
			// TODO 鐃緒申動鐃緒申鐃緒申鐃緒申鐃曙た catch 鐃瞬ワ申奪鐃�
			e.printStackTrace();
		}
	}


	private Map<String, FSNamesystem> _endPointFSNamesystemMapping;

	private Map<Integer, String> _nameNodeIDMapping;

	public static int currentGear = 2;

	private PooledWOLReceiver wolReceiver;
	private Thread WOLReceiverThread;
	private static WOLSender wolSender;
	private Thread WOLSenderThread;

	// List of Namenode RPC address
	protected Map<Integer, InetSocketAddress> nnRPCAddrs;
	protected Map<Integer, String[]> nnEnds;

	// NameNode ID
	protected int namenodeID;
	protected InetSocketAddress left = null;
	protected InetSocketAddress right = null;

	protected String start;
	protected String end;


	private void initialize(Configuration conf) throws IOException {
		  NameNode.LOG.info("NameNodeStaticPartition.initialize()");
	    InetSocketAddress socAddr = NameNode.getAddress(conf);
	    int handlerCount = conf.getInt("dfs.namenode.handler.count", 50);
	    //NameNode.LOG.info("handlerCount,"+handlerCount);
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
	    setNameNodeIDMapping(conf);

	    //backup namespace
		_endPointFSNamesystemMapping = new TreeMap<String, FSNamesystem>();
		ArrayList<String> containedFBTLists = new ArrayList<String>();
		containedFBTLists.add(serverAddress.getHostName());
		System.out.println("containedFSNamesystems: "+
				Arrays.toString(containedFBTLists.toArray()));
		namesystem = new FSNamesystem(this, conf, serverAddress.getHostName());
		_endPointFSNamesystemMapping.put
				(serverAddress.getHostName(), namesystem);

		String[] backUpDirectories = conf.getStrings("fbt.backUpNode", "null");
		System.out.println("backupDirectories "+Arrays.toString(backUpDirectories));
		for (String backUpDir:backUpDirectories) {
			System.out.println("backUpDir "+backUpDir);
			if (!backUpDir.equals("null")) {
				FSNamesystem subNamesystem = new FSNamesystem(this, conf, backUpDir);
					_endPointFSNamesystemMapping.put(backUpDir, subNamesystem);
				containedFBTLists.add(backUpDir);
			}
		}
		System.out.println("containedFBTDirectories: "+Arrays.toString(containedFBTLists.toArray()));
		gearManager = new GearManagerStaticPartition("conf/gearAccordion.xml");
		gearManager.setContainNamespace(containedFBTLists);
		gearManager.setEndPointNamesystemMapping(_endPointFSNamesystemMapping);
		//end
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
		initWOLReceiver();
	    startTrashEmptier(conf);
	  }

	public void setStartAndEnd(Configuration conf) {
	  int namenodeNumber = conf.getInt("dfs.namenodeNumber", 1);
	//appended

		// TODO Auto-generated method stub by StringUtility
	  nnEnds = new HashMap<Integer, String[]>();
	  for (int nameID = 1; nameID <= namenodeNumber; nameID++) {
		  String[] range= new String[2];
		  start = StringUtility.generateRange(nameID,
			  		namenodeNumber,
			  		FSNamesystem.experimentDir,
			  		FSNamesystem.low,
			  		FSNamesystem.high)[0];
		  NameNode.LOG.info("start "+start);
		  range[0] = start;

		  end = StringUtility.generateRange(nameID,
				  		namenodeNumber,
				  		FSNamesystem.experimentDir,
				  		FSNamesystem.low,
				  		FSNamesystem.high)[1];
		  NameNode.LOG.info("end "+end);
		  range[1]=end;
		  nnEnds.put(new Integer(nameID), range);
	  }
	  NameNode.LOG.info("nnEnds value "+nnEnds.toString());
    }


	  /**
	   * Should be (id, hostname), ex: (1, edn13)
	   * */
	  private void setNameNodeIDMapping(Configuration conf) {
		  StringUtility.debugSpace("NameNode.setNameNodeIDMapping");
		  String[] fsNamespaces = conf.getStrings("fs.namenodeFBTs");
		  _nameNodeIDMapping = new TreeMap<Integer, String>();
		  int i =1;
		  for (String fsNamespace:fsNamespaces) {
			  _nameNodeIDMapping.put(i, fsNamespace);
			  i++;
		  }
		  //System.out.println("nameNodeIDMapping: "+_nameNodeIDMapping.toString());
	  }


	  public LocatedBlocks   getBlockLocations(String src,
              long offset,
              long length) throws IOException {
		myMetrics.numGetBlockLocations.inc();
		NameNode.LOG.info("NameNodeSP.getBlockLocations "+src+"at "+ this.namenodeID);
		TargetNamespace responsibleNamespace = selection(src);
		Integer getAt = responsibleNamespace.getNamenodeID();
		if(nnRPCAddrs.get(getAt) == null) {
			end = "END";
			getAt = responsibleNamespace.getNamenodeID();
		}
		if (getAt == this.namenodeID) {
			System.out.println("getBlockLocations "+src+" locally at "+this.namenodeID);

			return _endPointFSNamesystemMapping.get(responsibleNamespace.getNamespaceOwner())
						.getBlockLocations(src, offset, length);
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

		  TargetNamespace tn = selection(src);
		  Integer createAt = tn.getNamenodeID();
		if(nnRPCAddrs.get(createAt) == null) {
				end = "END";
				createAt = tn.getNamenodeID();
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
				this._endPointFSNamesystemMapping.get(tn.getNamespaceOwner()).
							startFile(src,
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
					return success;
			}

		return success;
	  }


	  public LocatedBlock addBlock(String src, String clientName,
				DatanodeInfo[] excludedNodes) throws IOException {
		  //NameNode.LOG.info("addBlock "+src);
		  LocatedBlock lb = null;
			// appended(if~) //
		  TargetNamespace tn =  selection(src);
		  Integer addAt = tn.getNamenodeID();
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
					this._endPointFSNamesystemMapping.get(tn.getNamespaceOwner()).
						getAdditionalBlock(src,
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

	  /**
	   * The client needs to give up on the block.
	   */
	  public void abandonBlock(Block b, String src, String holder
	      ) throws IOException {
		// appended(if~) //
		  TargetNamespace tn = selection(src);
		  Integer abandonAt = tn.getNamenodeID();
			if (abandonAt.intValue() == namenodeID) {
				stateChangeLog.debug("*BLOCK* NameNode.abandonBlock: " + b
						+ " of file " + src);
				if (!this._endPointFSNamesystemMapping.get(tn.getNamespaceOwner())
									.abandonBlock(b, src, holder)) {
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

	  /** {@inheritDoc}
	 * @throws MessageException */
	  public boolean complete(String src, String clientName) throws IOException {
		// appended //
		  TargetNamespace tn = selection(src);
		  Integer completeAt = tn.getNamenodeID();
			if (completeAt.intValue() == this.namenodeID) {
				NameNode.LOG.info("complete locally "+src+" at "+completeAt);
				stateChangeLog.debug("*DIR* NameNode.complete: " + src + " for "
						+ clientName);
				CompleteFileStatus returnCode =
					this._endPointFSNamesystemMapping.get(tn.getNamespaceOwner())
								.completeFile(src, clientName);
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
		  TargetNamespace tn = selection(src);
		  Integer mkdirAt = tn.getNamenodeID();
		  NameNode.LOG.info("NameNode.mkdirs at "+mkdirAt);
		  if (nnRPCAddrs.get(mkdirAt) == null) {
			  end = "END";
			  mkdirAt = tn.getNamenodeID();
		  }
		  if (mkdirAt.intValue() == namenodeID) {
			  stateChangeLog.debug("*DIR* NameNode.mkdirs: " + src);
			  if (!checkPathLength(src)) {
				  throw new IOException("mkdirs: Pathname too long.  Limit "
	                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
			  }
			  return this._endPointFSNamesystemMapping.get(tn.getNamespaceOwner())
			  			.mkdirs(src,
			  					new PermissionStatus(UserGroupInformation.getCurrentUGI().getUserName(),
							  null, masked));
		  } else {
			  ClientProtocol name = (ClientProtocol) RPC.waitForProxy(
						ClientProtocol.class, ClientProtocol.versionID, nnRPCAddrs.get(mkdirAt),
						new Configuration());
			  return name.mkdirs(src, masked);
		  }
	  }

	  /**
	   */
	  public FileStatus[] getListing(String src) throws IOException {
		  StringUtility.debugSpace("NameNodeSP.getListing, "+src);
		  /*if (src.equals("/user/hanhlh/input")) {
			  ArrayList<FileStatus> tempResults = new ArrayList<FileStatus>();
			  FileStatus[] listAtThis = namesystem.getListing(src);
			  tempResults.addAll((Arrays.asList(listAtThis)));

			  for (InetSocketAddress isa : nnRPCAddrs.values()) {
				  if (isa!=serverAddress) {
					  ClientProtocol nn = (ClientProtocol) RPC.waitForProxy(
								ClientProtocol.class, ClientProtocol.versionID,
								isa,
								new Configuration());
					  FileStatus[]
				  }
			  }
		  }
*/
		  TargetNamespace tn = selection(src);
		  Integer getListingAt = tn.getNamenodeID();
		  if (getListingAt == namenodeID) {
			  FileStatus[] files = this._endPointFSNamesystemMapping.get(tn.getNamespaceOwner()).
			  					getListing(src);
			  if (files != null) {
				  myMetrics.numGetListingOps.inc();
			  }
			  return files;
		  } else {
			  ClientProtocol name = (ClientProtocol) RPC.waitForProxy(
						ClientProtocol.class, ClientProtocol.versionID,
						nnRPCAddrs.get(getListingAt),
						new Configuration());
			  return name.getListing(src);
		  }
	  }

	  public FileStatus getFileInfo(String src)  throws IOException {
		  TargetNamespace tn = selection(src);
		  Integer getAt = tn.getNamenodeID();
		  if (getAt == namenodeID) {
			  FileStatus fileInfo = this._endPointFSNamesystemMapping.get(tn.getNamespaceOwner()).
			  				getFileInfo(src);
			  myMetrics.numFileInfoOps.inc();
			  return fileInfo;
		  } else {
			  ClientProtocol name = (ClientProtocol) RPC.waitForProxy(
						ClientProtocol.class, ClientProtocol.versionID,
						nnRPCAddrs.get(getAt),
						new Configuration());
			  return name.getFileInfo(src);
		  }

		  }
	  public boolean rename(String src, String dst) throws IOException {
		  stateChangeLog.debug("*DIR* NameNode.rename: " + src + " to " + dst);
		  stateChangeLog.info("*DIR* NameNode.rename: " + src + " to " + dst);
		  /*if (!checkPathLength(dst)) {
			  throw new IOException("rename: Pathname too long.  Limit "
	                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
		  }
		  TargetNamespace tn = selection(src);
		  int renameAt = tn.getNamenodeID();
		  if (renameAt==namenodeID) {
			  boolean ret = this._endPointFSNamesystemMapping.get(tn.getNamespaceOwner()).
			  				renameTo(src, dst);

			  if (ret) {
				  myMetrics.numFilesRenamed.inc();
		  		}
			  return ret;
		  } else {
			  ClientProtocol name = (ClientProtocol) RPC.waitForProxy(
						ClientProtocol.class, ClientProtocol.versionID,
						nnRPCAddrs.get(renameAt),
						new Configuration());
			  return name.rename(src, dst);
		  }
*/
		  return true;
		  }
	  public boolean delete(String src) throws IOException {
		  return true;
		  //return delete(src, true);
		}

		public boolean delete(String src, boolean recursive) throws IOException {
			if (stateChangeLog.isDebugEnabled()) {
			      stateChangeLog.debug("*DIR* Namenode.delete: src=" + src
			          + ", recursive=" + recursive);
		    }
			TargetNamespace tn = selection(src);
			int deleteAt = tn.getNamenodeID();

			if (deleteAt == namenodeID) {

				  boolean ret = this._endPointFSNamesystemMapping.get(tn.getNamespaceOwner()).
				  					delete(src, recursive);
				  if (ret)
				  	myMetrics.numDeleteFileOps.inc();
				  return ret;
			  } else {
				  ClientProtocol name = (ClientProtocol) RPC.waitForProxy(
							ClientProtocol.class, ClientProtocol.versionID,
							nnRPCAddrs.get(deleteAt),
							new Configuration());
				  return name.delete(src);
			  }
		}

  	  private TargetNamespace selection(String src) throws IOException {
		int nodes = nnEnds.keySet().size();
		int i = namenodeID;
		start = nnEnds.get(namenodeID)[0];
		end = nnEnds.get(namenodeID)[1];
		NameNode.LOG.info("start, end: "+start+", "
											+end);
		if(src == null)
			return null;
		if(src.compareTo(start)<= 0) {
			for(i-- ;i > 0;i--) {
				if(src.compareTo(nnEnds.get(new Integer(i))[1]) > 0) {
					return gearHandling(new Integer(i + 1));
				}
			}
			return gearHandling(new Integer(1));
		} else if(src.compareTo(end) > 0) {
			for(i++;i < nodes;i++) {
				if(src.compareTo(nnEnds.get(new Integer(i))[1]) <= 0) {
					return gearHandling(new Integer(i));
				}
			}
			return gearHandling(new Integer(nodes));
		} else {
			return gearHandling(new Integer(i));
		}
	}

		private TargetNamespace gearHandling(int nodeID) {
			Integer targetID = nodeID;
			String target = _nameNodeIDMapping.get(nodeID);

			return new TargetNamespace(nodeID, serverAddress.getHostName());
			/*NameNode.LOG.info("target,"+target);
			NameNode.LOG.info("backUpMapping,"+
					namesystem._backUpMapping.toString());
			//check appropriate target with gear information
			if (!namesystem.getGearActivateNodes().get(
									NameNode.gearManager.getCurrentGear()).
									contains(target)) {
				String backUpTarget = namesystem._backUpMapping.get(target)[0];
				for (Iterator<Map.Entry<Integer, String>>
								iter = _nameNodeIDMapping.entrySet().iterator();iter.hasNext();) {
					Map.Entry<Integer, String> e = iter.next();
					if (backUpTarget.equals(e.getValue())) {
						targetID = e.getKey();
						return new TargetNamespace(targetID, backUpTarget);

					}
				}
			}

			return new TargetNamespace(targetID, serverAddress.getHostName());
*/		}


	/**
	 * DatanodeProtocol
	 * */
		public DatanodeRegistration register(DatanodeRegistration nodeReg
        ) throws IOException {
			verifyVersion(nodeReg.getVersion());
			namesystem.registerDatanode(nodeReg);
			forwardRegister(nodeReg);
			return nodeReg;
		}


		@Override
		public boolean transferNamespace(String targetMachine,
						String transferBlocksCommandIssueMode,
						String transferBlockMode) throws IOException, ClassNotFoundException {
			NameNode.LOG.info("NameNodeStaticPartition.transferNamespace to "+targetMachine);
			Date start = new Date();
			Configuration conf = new Configuration();
			boolean result = false;
			result = transferDirectory(targetMachine, conf,
					transferBlocksCommandIssueMode, transferBlockMode);
			NameNode.LOG.info("NameNodeStaticPartition.transferNamespace to "+targetMachine+","+
						(new Date().getTime()-start.getTime())/1000.0);
			return result;
		  }

		public boolean transferNamespace(String targetMachine,
				String transferBlocksCommandIssueMode,
				String transferBlockMode,
				int currentGear,
				int nextGear) throws IOException, ClassNotFoundException {
			NameNode.LOG.info("NameNodeStaticPartition.transferNamespace to "+targetMachine);
			Date start = new Date();
			Configuration conf = new Configuration();
			boolean result = false;
			result = transferDirectory(targetMachine, conf,
							transferBlocksCommandIssueMode, transferBlockMode,
							currentGear, nextGear);
			NameNode.LOG.info("NameNodeStaticPartition.transferNamespace to "+targetMachine+","+
				(new Date().getTime()-start.getTime())/1000.0);
			return result;
  }

		//targetNode = edn13
		public boolean transferDirectory(String targetMachine, Configuration conf,
						String transferBlocksCommandIssueMode,
						String transferBlockMode)
						throws IOException, ClassNotFoundException {
			boolean result = false;
			NameNode.LOG.info("NameNodeStaticPartition.transferDirectory to "+targetMachine);
			Date xferDirectoryStart = new Date();
			FSNamesystem subNamesystem = _endPointFSNamesystemMapping.
									get(targetMachine);
			subNamesystem.blocksMap = _endPointFSNamesystemMapping.get(
					serverAddress.getHostName()).blocksMap;
			InetSocketAddress isa = getAddress(targetMachine);
			Socket socket = new Socket(isa.getAddress(), NameNodeFBT.FBT_DEFAULT_PORT);
			//System.out.println("socket: "+socket);
			socket.setTcpNoDelay(true);
	        socket.setKeepAlive(true);

	        ObjectOutputStream _oos= new ObjectOutputStream(
	                new BufferedOutputStream(socket.getOutputStream()));
	        _oos.writeByte(WOLReceiver.OP_XFER_FSNAMESYSTEM);
	        _oos.writeObject(subNamesystem);
	        _oos.flush();
	        Date writeDirectorytoStream = new Date();
	        resetOffloading(transferBlocksCommandIssueMode, transferBlockMode);
	        Date resetOffloadingEnd = new Date();
	        Date modifyAfterXfer = new Date();
	        NameNode.LOG.info("\n"+"writeDirectoryToStream,"+(writeDirectorytoStream.getTime()-xferDirectoryStart.getTime())/1000.0
	        					+"\n"+"resetOffLoading,"+(resetOffloadingEnd.getTime()-writeDirectorytoStream.getTime())/1000.0
	        					+"\n"+"modifyAfterTransfer,"+(modifyAfterXfer.getTime()-resetOffloadingEnd.getTime())/1000.0
	        					);
	        result = true;
	        return result;
		}
		public boolean transferDirectory(String targetMachine, Configuration conf,
				String transferBlocksCommandIssueMode,
				String transferBlockMode,
				int currentGear,
				int nextGear)
				throws IOException, ClassNotFoundException {
	boolean result = false;
	NameNode.LOG.info("NameNodeStaticPartition.transferDirectory to "+targetMachine);
	Date xferDirectoryStart = new Date();
	FSNamesystem subNamesystem = _endPointFSNamesystemMapping.
							get(targetMachine);
	subNamesystem.blocksMap = _endPointFSNamesystemMapping.get(
			serverAddress.getHostName()).blocksMap;
	InetSocketAddress isa = getAddress(targetMachine);
	Socket socket = new Socket(isa.getAddress(), NameNodeFBT.FBT_DEFAULT_PORT);
	//System.out.println("socket: "+socket);
	socket.setTcpNoDelay(true);
    socket.setKeepAlive(true);

    ObjectOutputStream _oos= new ObjectOutputStream(
            new BufferedOutputStream(socket.getOutputStream()));
    _oos.writeByte(WOLReceiver.OP_XFER_FSNAMESYSTEM);
    _oos.writeObject(subNamesystem);
    _oos.flush();
    Date writeDirectorytoStream = new Date();
    resetOffloading(transferBlocksCommandIssueMode, transferBlockMode);
    Date resetOffloadingEnd = new Date();
    Date modifyAfterXfer = new Date();
    NameNode.LOG.info("\n"+"writeDirectoryToStream,"+(writeDirectorytoStream.getTime()-xferDirectoryStart.getTime())/1000.0
    					+"\n"+"resetOffLoading,"+(resetOffloadingEnd.getTime()-writeDirectorytoStream.getTime())/1000.0
    					+"\n"+"modifyAfterTransfer,"+(modifyAfterXfer.getTime()-resetOffloadingEnd.getTime())/1000.0
    					);
    result = true;
    return result;
}



		public boolean resetOffloading(String transferBlocksCommandIssueMode,
				String transferBlockMode) throws IOException {
			if (transferBlocksCommandIssueMode.equals("sequential")) {
				return resetOffloadingSequentially(WriteOffLoading.writeOffLoadingFile.concat(
						"."+serverAddress.getHostName()), transferBlockMode);
			} else if (transferBlocksCommandIssueMode.equals("batch")) {
				return resetOffloadingBatchTransfer(WriteOffLoading.writeOffLoadingFile.concat(
						"."+serverAddress.getHostName()),
						transferBlockMode);
			}
			return false;
		}


		  	private boolean resetOffloadingSequentially(String logFile,
		  						String transferBlockMode) throws IOException {
		  		NameNode.LOG.info("NameNodeStaticPartition.resetOffloading");
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
		  							blocks, dst, dst),
		  							transferBlockMode);
		  				}

		  				dis.close();
		  			} catch (IOException e) {
		  				e.printStackTrace();
		  			} catch (Exception e) {
		  				e.printStackTrace();
		  			}
		  			/*boolean backUp = copy(WriteOffLoading.writeOffLoadingFile,
		  									WriteOffLoading.writeOffLoadingBackUp.concat(
		  											String.format("%s.%d.log", "WriteOffLoading",
		  							new Date().getTime())));
		  			if (backUp) {
		  				deleteLogFile(WriteOffLoading.writeOffLoadingFile);
		  			}*/
		  			//modifyAfterTransfer(targetMachine);
		  			result = true;

		  			NameNode.LOG.info("NameNodeFBT.resetOffLoading,"+(new Date().getTime()-start.getTime())/1000.0);
		  			return result;
		  	}

		  	private boolean resetOffloadingBatchTransfer(String logFile,
		  			String transferBlockMode) throws IOException {
				NameNode.LOG.info("NameNodeFBT.resetOffloadingBatchTransfer");
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
						String src = null;
						String wolDst = null;
						String dst = null;
						ArrayList<Block[]> blocks = new ArrayList<Block[]>();
						while ((line = br.readLine())!=null) {
							String[] record = line.split(",");
							src = record[2];
							wolDst = record[3];  //IPAddress 192.168.0.114
							dst = record[4];
							Date startSearchBlock = new Date();
							Block[] block = namesystem.getINodeFile(src).getBlocks();
							NameNode.LOG.info("searchBlock,"+ (new Date().getTime()-startSearchBlock.getTime())/1000.0);
							blocks.add(block);
							lineCount++;

						}
						dis.close();
						writeOffLoadingCommandHandler(new WriteOffLoadingCommand(blocks, dst, wolDst),
											transferBlockMode);
					} catch (IOException e) {
						e.printStackTrace();
					} catch (Exception e) {
						e.printStackTrace();
					}
					result = true;

					NameNode.LOG.info("NameNodeStaticPartition.resetOffLoading,"+(new Date().getTime()-start.getTime())/1000.0);
					return result;
			}

		  	public void writeOffLoadingCommandHandler(WriteOffLoadingCommand command,
		  					String transferBlockMode) {
		  		NameNode.LOG.info("NameNodeStaticPartition.writeOffLoadingCommandHandler");
		  		Date start=new Date();
		  		DatanodeDescriptor WOLSrc = ((DatanodeDescriptor) namesystem.getClusterMap().getNode(
						"/default-rack/"+serverAddress.getAddress().getHostAddress()+
						":50010"));

		  		String dstStr = normalizedDstHostName(command.getDestination());
		  		DatanodeDescriptor dst = ((DatanodeDescriptor)
		  							namesystem.getClusterMap().getNode(
		  							"/default-rack/192.168.0.1"+dstStr.substring(3, 5)+
		  							":50010"));
		  		if (transferBlockMode.equals("sequential")) {
					transferBlockSequentially(WOLSrc, dst, command);
				} else if (transferBlockMode.equals("batch")) {
					transferBlockBatch(WOLSrc, dst, command);
				} else {
					System.err.print("wrong transferBlockMode");
					System.exit(0);
				}
		  		//NameNode.LOG.info("WOLCHandler,"+(new Date().getTime()-start.getTime())/1000.0);
		  	}



		  	void transferBlockSequentially(DatanodeDescriptor WOLSrc, DatanodeDescriptor dst,
					WriteOffLoadingCommand command) {
				for (Block[]bs : command.getBlocks()) {
					for (Block b:bs) {
						WOLSrc.addBlockToBeReplicated(b, new DatanodeDescriptor[] {dst});
					}
				}

			}

			void transferBlockBatch(DatanodeDescriptor WOLSrc, DatanodeDescriptor dst,
										WriteOffLoadingCommand command) {
				DatanodeDescriptor targets[][] = new DatanodeDescriptor[1][1];
				targets[0][0] = dst;
				WOLSrc.addBlocksTobeReplicated(command.getBlocksArray(), targets);
			}

		  	public void getTransferedDirectoryHandler(FSNamesystem subNamesystem) throws UnknownHostException {
				NameNode.LOG.info("NameNodeStaticPartition.getTransferedDirectoryHandler");
				Date start = new Date();
				subNamesystem.setFSImage(namesystem.getFSImage());
				subNamesystem.hostsReader = namesystem.hostsReader;
				subNamesystem.dnsToSwitchMapping = namesystem.dnsToSwitchMapping;
				subNamesystem.pendingReplications = namesystem.pendingReplications;
				subNamesystem.setHeartbeat(namesystem.getHeartbeats());
				subNamesystem.setDatanodeMap(namesystem.getDatanodeMap());
				subNamesystem.setUpgradeManager(namesystem.getUpgradeManager());
				subNamesystem.setCorruptReplicasMap(namesystem.getCorruptReplicasMap());
				subNamesystem.setLeasManager(namesystem.getLeaseManager());
				subNamesystem.setRandom(namesystem.getRandom());
				subNamesystem.setNodeReplicationLimit(namesystem.getNodeReplicationLimit());
				getEndPointFSNamesystemMapping().put
							(serverAddress.getHostName(), subNamesystem);
				setDirectory(subNamesystem);
				NameNode.LOG.info("NameNodeStaticPartition.getTransferedDirectoryHandler,"+(new Date().getTime()-start.getTime())/1000.0);
			}


		  	public Map<String, FSNamesystem> getEndPointFSNamesystemMapping() {
				return this._endPointFSNamesystemMapping;
			}

		  	public void setDirectory(FSNamesystem directory) {
				namesystem = directory;
			}

		  	private void initWOLReceiver() throws IOException {
				wolReceiver = new PooledWOLReceiver(
									NameNodeFBT.FBT_XFERMETADATA_PORT);
				wolReceiver.setNamenodeStaticPartition(this);
				WOLReceiverThread = new Thread(wolReceiver, "Main WOLReceiver");
				WOLReceiverThread.setDaemon(true);
				WOLReceiverThread.start();


				wolSender = new WOLSender(serverAddress.getHostName());
				WOLSenderThread = new ThreadPool("Sender", wolSender, 1);
		        WOLSenderThread.setPriority(Thread.NORM_PRIORITY + 1);
		        WOLSenderThread.run();

			}

		  	@Override
			public void send(String src, INode inode, String toMachine) {

			}

			@Override
			public void receive(INode inode, String src, String fromMachine) {
				namesystem.addINode(inode, src, fromMachine);
			}

			public void receive(String src) {
				// TODO �������������純�����鴻���

			}

			  /** {@inheritDoc} */
			  public boolean setReplication(String src,
			                                short replication
			                                ) throws IOException {
				  int setAt = selection(src).getNamenodeID();
				  if (setAt == namenodeID) {
					  return namesystem.setReplication(src, replication);
				  } else {
					  ClientProtocol name = (ClientProtocol) RPC.waitForProxy(ClientProtocol.class,
							  ClientProtocol.versionID, nnRPCAddrs.get(setAt),
							  new Configuration());
					  return name.setReplication(src, replication);
				  }

			  }
			  /** {@inheritDoc} */
			  public void setPermission(String src, FsPermission permissions
			      ) throws IOException {
				  int setAt = selection(src).getNamenodeID();
				  if (setAt == namenodeID) {
					  namesystem.setPermission(src, permissions);
				  } else {
					  ClientProtocol name = (ClientProtocol) RPC.waitForProxy(ClientProtocol.class,
							  ClientProtocol.versionID, nnRPCAddrs.get(setAt),
							  new Configuration());
					  name.setPermission(src, permissions);
				  }
			  }

			  public void namenodeRegistration(String host, int port, int nID)
				throws IOException {
			InetSocketAddress addr = new InetSocketAddress(host, port);
			nnNamenode = (NNClusterProtocol) RPC.waitForProxy(
					NNClusterProtocol.class, NNClusterProtocol.versionID, addr,
					new Configuration());
			String h;
			int p;
			int ID;
			Map<Integer, InetSocketAddress> NNR = new HashMap<Integer, InetSocketAddress>(
					nnRPCAddrs);
			for (Iterator<Map.Entry<Integer, InetSocketAddress>> i = NNR.entrySet()
					.iterator(); i.hasNext();) {
				Map.Entry<Integer, InetSocketAddress> m = i.next();
				h = m.getValue().getHostName();
				p = m.getValue().getPort();
				ID = m.getKey();
				nnNamenode.catchAddr(h, p, ID);
			}
			for (Iterator<Map.Entry<Integer, InetSocketAddress>> i = NNR.entrySet()
					.iterator(); i.hasNext();) {
				Map.Entry<Integer, InetSocketAddress> m = i.next();
				InetSocketAddress a = m.getValue();
				if (!a.equals(serverAddress)) {
					nnNamenode = (NNClusterProtocol) RPC.waitForProxy(
							NNClusterProtocol.class, NNClusterProtocol.versionID,
							a, new Configuration());
					nnNamenode.catchAddr(host, port, nID);
				}
			}
			nnRPCAddrs.put(nID, addr);
			namesystem.dir.setNNAddrs(nnRPCAddrs);
			//namesystem.dir.rootDir.addINodeLocation(nID);

			if(nID == namenodeID + 1)
				right = addr;
			if(nID == namenodeID - 1)
				left = addr;
			System.out.println("[namenodeRegistration] NameNode " + nID);
		}

		public void catchAddr(String host, int port, int nID) throws IOException {
			InetSocketAddress addr = new InetSocketAddress(host, port);
			nnRPCAddrs.put(nID, addr);
			namesystem.dir.setNNAddrs(nnRPCAddrs);
			//namesystem.dir.rootDir.addINodeLocation(nID);
			if(nID == namenodeID + 1)
				right = addr;
			if(nID == namenodeID - 1)
				left = addr;
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

		public void forwardBlockReport(DatanodeRegistration nodeReg, long[] blocks)
				throws IOException {
			Configuration conf = new Configuration();
			Forwards[] f = new Forwards[nnRPCAddrs.size()];
			for(int i = 1;i <= nnRPCAddrs.size();i++) {
				if(!(i == namenodeID)) {
					nnNamenode = (NNClusterProtocol) RPC.waitForProxy(NNClusterProtocol.class, NNClusterProtocol.versionID,  nnRPCAddrs.get(new Integer(i)), conf);
					f[i-1] = new Forwards(nodeReg, blocks, nnNamenode, Forwards.BLOCKREPORT);
					f[i-1].start();
				}
			}
		}

		public void forwardBlockBeingWrittenReport(DatanodeRegistration nodeReg,
				long[] blocks) throws IOException {
			Configuration conf = new Configuration();
			Forwards[] f = new Forwards[nnRPCAddrs.size()];
			for(int i = 1;i <= nnRPCAddrs.size();i++) {
				if(!(i == namenodeID)) {
					nnNamenode = (NNClusterProtocol) RPC.waitForProxy(NNClusterProtocol.class, NNClusterProtocol.versionID,  nnRPCAddrs.get(new Integer(i)), conf);
					f[i-1] = new Forwards(nodeReg, blocks, nnNamenode, Forwards.BLOCKSBEINGWRITTENREPORT);
					f[i-1].start();
				}
			}
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

		public void forwardErrorReport(DatanodeRegistration nodeReg, int errorCode,
				String msg) throws IOException {
			Configuration conf = new Configuration();
			Forwards[] f = new Forwards[nnRPCAddrs.size()];
			for(int i = 1;i <= nnRPCAddrs.size();i++) {
				if(!(i == namenodeID)) {
					nnNamenode = (NNClusterProtocol) RPC.waitForProxy(NNClusterProtocol.class, NNClusterProtocol.versionID,  nnRPCAddrs.get(new Integer(i)), conf);
					f[i-1] = new Forwards(nodeReg, errorCode, msg, nnNamenode);
					f[i-1].start();
				}
			}
		}

		public void forwardReportBadBlocks(LocatedBlock[] blocks)
				throws IOException {
			for (Iterator<Map.Entry<Integer, InetSocketAddress>> i = nnRPCAddrs
					.entrySet().iterator(); i.hasNext();) {
				InetSocketAddress a = i.next().getValue();
				if (!a.equals(serverAddress)) {
					nnNamenode = (NNClusterProtocol) RPC.waitForProxy(
							NNClusterProtocol.class, NNClusterProtocol.versionID,
							a, new Configuration());
					nnNamenode.catchReportBadBlocks(blocks);
				}
			}
		}

		public void forwardNextGenerationStamp(Block block, boolean fromNN)
				throws IOException {
			for (Iterator<Map.Entry<Integer, InetSocketAddress>> i = nnRPCAddrs
					.entrySet().iterator(); i.hasNext();) {
				InetSocketAddress a = i.next().getValue();
				if (!a.equals(serverAddress)) {
					nnNamenode = (NNClusterProtocol) RPC.waitForProxy(
							NNClusterProtocol.class, NNClusterProtocol.versionID,
							a, new Configuration());
					nnNamenode.catchNextGenerationStamp(block, fromNN);
				}
			}
		}

		public void forwardCommitBlockSynchronization(Block block,
				long newgenerationstamp, long newlength, boolean closeFile,
				boolean deleteblock, DatanodeID[] newtargets) throws IOException {
			for (Iterator<Map.Entry<Integer, InetSocketAddress>> i = nnRPCAddrs
					.entrySet().iterator(); i.hasNext();) {
				InetSocketAddress a = i.next().getValue();
				if (!a.equals(serverAddress)) {
					nnNamenode = (NNClusterProtocol) RPC.waitForProxy(
							NNClusterProtocol.class, NNClusterProtocol.versionID,
							a, new Configuration());
					nnNamenode.catchCommitBlockSynchronization(block,
							newgenerationstamp, newlength, closeFile, deleteblock,
							newtargets);
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

		public void catchBlockReport(DatanodeRegistration nodeReg, long[] blocks)
				throws IOException {
			// verifyRequest(nodeReg);
			BlockListAsLongs blist = new BlockListAsLongs(blocks);
			stateChangeLog.debug("*BLOCK* NameNode.blockReport: " + "from "
					+ nodeReg.getName() + " " + blist.getNumberOfBlocks()
					+ " blocks");

			namesystem.processReport(nodeReg, blist);
		}

		public void catchBlockBeingWrittenReport(DatanodeRegistration nodeReg,
				long[] blocks) throws IOException {
			//TODO this module doesnt exist in 0.20.0
			// verifyRequest(nodeReg);
			BlockListAsLongs blist = new BlockListAsLongs(blocks);
			//namesystem.processBlocksBeingWrittenReport(nodeReg, blist);

			stateChangeLog
					.info("*BLOCK* NameNode.blocksBeingWrittenReport: " + "from "
							+ nodeReg.getName() + " " + blocks.length + " blocks");
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

		public void catchErrorReport(DatanodeRegistration nodeReg, int errorCode,
				String msg) throws IOException {
			// Log error message from datanode
			String dnName = (nodeReg == null ? "unknown DataNode" : nodeReg
					.getName());
			LOG.info("Error report from " + dnName + ": " + msg);
			if (errorCode == DatanodeProtocol.NOTIFY) {
				return;
			}
			// verifyRequest(nodeReg);
			if (errorCode == DatanodeProtocol.DISK_ERROR) {
				LOG.warn("Volume failed on " + dnName);
			} else if (errorCode == DatanodeProtocol.FATAL_DISK_ERROR) {
				namesystem.removeDatanode(nodeReg);
			}
		}

		public void catchReportBadBlocks(LocatedBlock[] blocks) throws IOException {
			stateChangeLog.info("*DIR* NameNode.reportBadBlocks");
			for (int i = 0; i < blocks.length; i++) {
				Block blk = blocks[i].getBlock();
				DatanodeInfo[] nodes = blocks[i].getLocations();
				for (int j = 0; j < nodes.length; j++) {
					DatanodeInfo dn = nodes[j];
					namesystem.markBlockAsCorrupt(blk, dn);
				}
			}
		}

		public void catchNextGenerationStamp(Block block, boolean fromNN)
				throws IOException {
			namesystem.nextGenerationStampForBlock(block, fromNN);
		}

		public void catchCommitBlockSynchronization(Block block,
				long newgenerationstamp, long newlength, boolean closeFile,
				boolean deleteblock, DatanodeID[] newtargets) throws IOException {
			namesystem.commitBlockSynchronization(block, newgenerationstamp,
					newlength, closeFile, deleteblock, newtargets);
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
					nnNamenode.catchRegister(registration);
				}
			}
		}

		public void catchRegister(DatanodeRegistration registration)
				throws IOException {
			verifyVersion(registration.getVersion());
			namesystem.registerDatanode(registration);
		}

		public void forwardCreate(String src, FsPermission masked,
				String clientName, boolean overwrite, short replication,
				long blockSize) throws IOException {

		}

		public void catchCreate(String src, FsPermission masked, String clientName,
				boolean overwrite, short replication, long blockSize)
				throws IOException {
			String clientMachine = getClientMachine();
			if (stateChangeLog.isDebugEnabled()) {
				stateChangeLog.debug("*DIR* NameNode.create: file " + src + " for "
						+ clientName + " at " + clientMachine);
			}
			if (!checkPathLength(src)) {
				throw new IOException("create: Pathname too long.  Limit "
						+ MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH
						+ " levels.");
			}
			namesystem.startFile(src, new PermissionStatus(UserGroupInformation
					//.getCurrentUser().getShortUserName()
					.getCurrentUGI().getUserName()
					,null, masked),
					clientName, clientMachine, overwrite, replication, blockSize);

			//myMetrics.incrNumFilesCreated();
			//myMetrics.incrNumCreateFileOps();
		}


		public void forwardDelete(String src, boolean recursive) throws IOException {

		}

		public void catchDelete(String src, boolean recursive) throws IOException {

		}

		// new configurations
		public InetSocketAddress getLeaderAddress(Configuration conf) {
			String la = conf.get("dfs.namenode.leader");
			return getAddress(la);
		}

		public int getNamenodeID(Configuration conf) {
			return conf.getInt("dfs.namenode.id", 0);
		}

		// processing INodes

		public INodeInfo getNodeFromOther(byte[][] components, int[] visited) {
			List<Integer> vit = new ArrayList<Integer>();
			for (int i = 0; i < visited.length; i++)
				vit.add(new Integer(visited[i]));
			INode target = namesystem.dir.getNodeFromOther(components, vit);
			if (target == null)
				return null;

			return target.getInfo();
		}

			public void setCopying(String src, int id, long atime, long mtime) {
				namesystem.dir.setCopying(src, id, atime, mtime);
			}

			public int getNumOfFiles() {

				return namesystem.numOfFiles;
			}

		public void setStart(String src) {
			start = src;
		}
		 //TODO: this module doesnt exist in 0.20.2
		/*public DirectoryListing getListingInternal(String src, byte[] startAfter)
				throws IOException {
			// TODO Auto-generated method stub
			DirectoryListing files = namesystem.getListing(src, startAfter);
			if(src == new Path(end).getParent().toString()) {
				nnNamenode = (NNClusterProtocol) RPC.waitForProxy(NNClusterProtocol.class, NNClusterProtocol.versionID, right, new Configuration());
				DirectoryListing filesAtRight = nnNamenode.getListingInternal(src, startAfter);
				HdfsFileStatus[] partial = new HdfsFileStatus[files.getPartialListing().length + filesAtRight.getPartialListing().length];
				HdfsFileStatus[] here = files.getPartialListing();
				HdfsFileStatus[] rightFiles = filesAtRight.getPartialListing();
				System.arraycopy(here, 0, partial, 0, here.length);
				System.arraycopy(rightFiles, 0, partial, here.length, rightFiles.length);
				int rem = files.getRemainingEntries() + filesAtRight.getRemainingEntries();
				files = new DirectoryListing(partial, rem);
			}
			myMetrics.incrNumGetListingOps();
			if (files != null) {
				myMetrics
						.incrNumFilesInGetListingOps(files.getPartialListing().length);
			}
			return files;
		}
*/
		// normalize(have not implement yet) //
		public void normalize() throws IOException {
			Configuration conf = new Configuration();
			nnNamenode = (NNClusterProtocol) RPC.waitForProxy(NNClusterProtocol.class, NNClusterProtocol.versionID, right, conf);
			int number = this.namesystem.numOfFiles - nnNamenode.getNumOfFiles();
			if(number > 0) {
				for(;number > 0 ;number--)
					sendToRight();
			}
			else if(number < -1) {
				for(;number < -1;number++)
					getFromRight();
			}
		}

		public void sendToRight() throws IOException {
			Configuration conf = new Configuration();
			Path srcPath = new Path(end);
			Path dstPath = new Path(end + "copy");
			FileSystem srcFS = srcPath.getFileSystem(conf);
			FileSystem dstFS = srcFS;
			FileUtil.copy(srcFS, srcPath, dstFS, dstPath, true, conf);
			ClientProtocol cp = (ClientProtocol) RPC.waitForProxy(ClientProtocol.class, ClientProtocol.versionID, right, conf);
			cp.rename(end + "copy", end);
			end = searchEnd();
			nnNamenode = (NNClusterProtocol) RPC.waitForProxy(NNClusterProtocol.class, NNClusterProtocol.versionID, right, conf);
			nnNamenode.setStart(end);
		}

		public void getFromRight() throws IOException{
			Configuration conf = new Configuration();
			nnNamenode = (NNClusterProtocol) RPC.waitForProxy(NNClusterProtocol.class, NNClusterProtocol.versionID, right, conf);
			end = nnNamenode.searchStart();
			nnNamenode.copyFromRight(end);
		}

		public String searchEnd() {
			return namesystem.dir.searchEnd();
		}

		public String searchStart() {
			start = namesystem.dir.searchStart();
			return start;
		}

		public void copyFromRight(String endPath) throws IOException {
			rename(endPath, endPath + "copy");
			Configuration conf = new Configuration();
			Path srcPath = new Path(endPath + "copy");
			Path dstPath = new Path(endPath);
			FileSystem srcFS = srcPath.getFileSystem(conf);
			FileSystem dstFS = srcFS;
			FileUtil.copy(srcFS, srcPath, dstFS, dstPath, true, conf);
		}

		public void forwardAddBlock(String src, String clientName) {
			// TODO 鐃緒申動鐃緒申鐃緒申鐃緒申鐃曙た鐃潤ソ鐃獣ド￥申鐃緒申鐃緒申鐃緒申

		}

		public void catchAddBlock(String src, String clientName) {
			// TODO 鐃緒申動鐃緒申鐃緒申鐃緒申鐃曙た鐃潤ソ鐃獣ド￥申鐃緒申鐃緒申鐃緒申

		}
	}
