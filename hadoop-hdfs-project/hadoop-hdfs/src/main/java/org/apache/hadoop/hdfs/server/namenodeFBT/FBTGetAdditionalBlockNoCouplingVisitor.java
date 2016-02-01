/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.ReplicationTargetChooser;
import org.apache.hadoop.hdfs.server.namenode.ReplicationTargetChooserAccordion;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenodeFBT.lock.Lock;
import org.apache.hadoop.hdfs.server.namenodeFBT.msg.MessageException;
import org.apache.hadoop.hdfs.server.namenodeFBT.rule.GetAdditionalBlockRequest;
import org.apache.hadoop.hdfs.server.namenodeFBT.rule.GetAdditionalBlockResponse;
import org.apache.hadoop.hdfs.server.namenodeFBT.utils.StringUtility;

/**
 * @author hanhlh
 *
 */
public class FBTGetAdditionalBlockNoCouplingVisitor extends FBTNodeVisitor {

	/**
     * 鐃叔ワ申鐃曙ク鐃夙リ検鐃緒申鐃緒申鐃緒申
     */
    private String _key;
    int _currentGear;
    private String _clientNode;

	public FBTGetAdditionalBlockNoCouplingVisitor(FBTDirectory directory) {
		super(directory);
	}
	// accessors //////////////////////////////////////////////////////////////

    public void setRequest(Request request) {
        setRequest((GetAdditionalBlockRequest) request);
    }

    public void setRequest(GetAdditionalBlockRequest request) {
        super.setRequest(request);
        _key = request.getKey();
        _currentGear = request.getCurrentGear();
        _clientNode = request.getClientNode();
    }

	public void run() {
		VPointer vp = _request.getTarget();

        if (vp == null) {
            /* 鐃暑ー鐃夙わ申鐃緒申鐃緒申鐃薯開誌申 */
            try {
				_directory.accept(this);
			} catch (NotReplicatedYetException e) {
				// TODO 鐃緒申動鐃緒申鐃緒申鐃緒申鐃曙た catch 鐃瞬ワ申奪鐃�
				e.printStackTrace();
			} catch (MessageException e) {
				// TODO 鐃緒申動鐃緒申鐃緒申鐃緒申鐃曙た catch 鐃瞬ワ申奪鐃�
				e.printStackTrace();
			} catch (IOException e) {
				// TODO 鐃緒申動鐃緒申鐃緒申鐃緒申鐃曙た catch 鐃瞬ワ申奪鐃�
				e.printStackTrace();
			}
        } else {
            /*
             * 他鐃緒申 PE 鐃緒申鐃緒申転鐃緒申鐃緒申鐃緒申討鐃緒申鐃緒申弋鐃塾常申鐃緒申 vp != null 鐃夙なわ申
             * vp 鐃叔誌申鐃所さ鐃曙た鐃塾￥申鐃宿わ申 Visitor 鐃緒申鐃熟わ申
             */
            Node node = (Node) _directory.getNode(vp);
			try {
				((org.apache.hadoop.hdfs.server.namenodeFBT.Node) node).accept(this, vp);
			} catch (NotReplicatedYetException e) {
				// TODO 鐃緒申動鐃緒申鐃緒申鐃緒申鐃曙た catch 鐃瞬ワ申奪鐃�
				e.printStackTrace();
			} catch (MessageException e) {
				// TODO 鐃緒申動鐃緒申鐃緒申鐃緒申鐃曙た catch 鐃瞬ワ申奪鐃�
				e.printStackTrace();
			} catch (IOException e) {
				// TODO 鐃緒申動鐃緒申鐃緒申鐃緒申鐃曙た catch 鐃瞬ワ申奪鐃�
				e.printStackTrace();
			}
		}


        while (_response == null) {
            ((FBTDirectory) _directory).incrementCorrectingCount();

            try {
				_directory.accept(this);
			} catch (NotReplicatedYetException e) {
				// TODO 鐃緒申動鐃緒申鐃緒申鐃緒申鐃曙た catch 鐃瞬ワ申奪鐃�
				e.printStackTrace();
			} catch (MessageException e) {
				// TODO 鐃緒申動鐃緒申鐃緒申鐃緒申鐃曙た catch 鐃瞬ワ申奪鐃�
				e.printStackTrace();
			} catch (IOException e) {
				// TODO 鐃緒申動鐃緒申鐃緒申鐃緒申鐃曙た catch 鐃瞬ワ申奪鐃�
				e.printStackTrace();
			}
        }

	}

	@Override
	public void visit(MetaNode meta, VPointer self) throws MessageException, NotReplicatedYetException, IOException {
		lock(self, Lock.IS);

        VPointer root = meta.getRootPointer();

        unlock(self);

        Node node = _directory.getNode(root);
        node.accept(this, root);

	}

	@Override
	public void visit(IndexNode index, VPointer self) throws MessageException, NotReplicatedYetException, IOException {
		lock(self, Lock.IS);
//      if (_parent != null) {
//          unlock(_parent);
//          _parent = null;
//      }

      if (!index.isInRange(_key) || index._deleteBit){
     // if (index.isDeleted()) {
          unlock(self);
      } else {
          /* 鐃緒申鐃塾￥申鐃宿での醐申鐃緒申鐃緒申鐃緒申鐃塾逸申鐃緒申 */
          int position = index.binaryLocate(_key);

          if (position < 0) {
              /* 鐃緒申鐃淳￥申鐃縦リー鐃祝ワ申鐃緒申肇蝓種申鐃渋醐申澆鐃緒申覆鐃�*/
              _response = new GetAdditionalBlockResponse(
            		  				(GetAdditionalBlockRequest) _request);
              endLock(self);
          } else {
              /* 鐃述ノ￥申鐃宿わ申悗鐃緒申櫂鐃緒申鐃�*/
              VPointer vp = index.getEntry(position);

              if (_directory.isLocal(vp)) {
                  unlock(self);
//                  _parent = self;

                  Node node = _directory.getNode(vp);
                  node.accept(this, vp);
              } else {
                  /*
                   * 鐃楯ワ申鐃緒申 vp 鐃塾指わ申鐃銃わ申鐃緒申痢鐃緒申匹聾鐃緒申澆鐃�PE 鐃祝はなわ申鐃緒申鐃潤，
                   * 適鐃緒申鐃緒申 PE 鐃緒申鐃緒申鐃薯しわ申鐃竣居申鐃重常申鐃�
                   */
                  endLock(self);

                  _request.setTarget(vp);
                  callRedirectionException(vp.getPointer().getPartitionID());
              }
          }
      }


	}

	@Override
	public void visit(LeafNode leaf, VPointer self)
			throws MessageException, IOException {
		//StringUtility.debugSpace("FBTGetAdditionalBlockVisitor visit leafnode "+leaf);
		lock(self, Lock.S);

      if (leaf.get_isDummy()) {
          VPointer vp = leaf.get_RightSideLink();
          endLock(self);
          _request.setTarget(vp);
          callRedirectionException(vp.getPointer().getPartitionID());
      } else {
          if (_key.compareTo(leaf.get_highKey()) >= 0 || leaf.get_deleteBit()) {
              unlock(self);
          } else {
              /* 鐃緒申鐃塾￥申鐃宿での醐申鐃緒申鐃緒申鐃緒申鐃塾逸申鐃緒申 */

        	  int position = leaf.binaryLocate(_key);
              if (position <= 0) {
                  // 鐃緒申鐃緒申鐃緒申存鐃淳わ申鐃淑わ申
                  endLock(self);
                  _response = new GetAdditionalBlockResponse(
                		  (GetAdditionalBlockRequest) _request);
              } else {
            	  long fileLength, blockSize;
            	  int replication;
            	  DatanodeDescriptor clientNode = null;
            	  Block newBlock = null;

            	  INodeFileUnderConstruction pendingFile =
              			(INodeFileUnderConstruction) leaf.getINode(position-1);
	            //
	            // If we fail this, bad things happen!
	            //
	            if (!checkFileProgress(pendingFile, false)) {
	              throw new NotReplicatedYetException("Not replicated yet:" + _key);
	            }
            	  fileLength = pendingFile.computeContentSummary().getLength();
            	  //System.out.println("fileLength "+fileLength);
            	  blockSize = pendingFile.getPreferredBlockSize();
            	  clientNode = pendingFile.getClientNode();
            	  replication = (int)pendingFile.getReplication();

            	  // choose targets for the new block tobe allocated.
            	  /*DatanodeDescriptor targets[] = _directory.replicator.chooseTarget(
            													replication,
                                                               clientNode,
                                                               null,
                                                               blockSize);*/
            	  //TODO get placement
            	  DatanodeDescriptor targets[] = null;
            	  /*//if (NameNode.gearManager.getCurrentGear() == 1) {
	            	  targets = new DatanodeDescriptor[1];
	            	  //System.out.println("clusterMap: "+_directory.getClusterMap());
	            	  String targetName = "/default-rack/"+_directory.getNameNodeAddress().getAddress().
	            	  					getHostAddress()+":50010";

	            	  //System.out.println("targetName: "+targetName);

	            	  DatanodeInfo targetNode = (DatanodeInfo) _directory.getClusterMap().getNode(targetName);

	            	  targets[0] = (DatanodeDescriptor) targetNode;

		            //} else if (NameNode.gearManager.getCurrentGear() == 2) {
					//	targets = new DatanodeDescriptor[2];
					//	String primary = "/default-rack/"+_directory.getNameNodeAddress().getAddress().
	  				//				getHostAddress()+":50010";
					//	targets[0] = (DatanodeDescriptor) _directory.getClusterMap().getNode(primary);
					//	String backup = _directory._backUpMapping.get(primary)[0];
					//	targets[1] = (DatanodeDescriptor) _directory.getClusterMap().getNode(backup);
					//	System.out.println("primary, backup: "+primary+", "+backup);
				//}
				 *
				 */
            	  if (FSNamesystem.replicator.getClass().toString().equals(
            	  		"class org.apache.hadoop.hdfs.server.namenode.ReplicationTargetChooser")) {
            	    	targets = FSNamesystem.replicator.chooseTarget(1, clientNode, null, blockSize);
            	  } else if (FSNamesystem.replicator.getClass().toString().equals(
            	  		"class org.apache.hadoop.hdfs.server.namenode.ReplicationTargetChooserAccordion")) {
            	    	targets = ((ReplicationTargetChooserAccordion)
            	    				FSNamesystem.replicator).chooseTarget(
            	    						_key,
            	    						leaf.getNodeIdentifier().getOwner(),
            	    						clientNode,
            	    						NameNode.gearManager.getCurrentGear(),
            	    						null, blockSize,
            	    						_directory.getClusterMap());
            	    }

            	  for (DatanodeDescriptor dd:targets) {
            		  System.out.println("target: "+dd.toString());
	            }
	            if (targets.length < _directory.minReplication) {
	            	throw new IOException("File " + _key + " could only be replicated to " +
                                targets.length + " nodes, instead of " +
                                _directory.minReplication);
	            }

	            // Allocate a new block and record it in the INode.
	            synchronized (this) {
	            	String[] parentPathNames = INode.getParentAndCurrentPathNames(_key);
	            	INode[] parentPathINodes = new INode[parentPathNames.length];
	            	/*for (int count=0; count<parentPathNames.length; count++) {
	            		parentPathINodes[count] = _directory.searchResponse(
            								parentPathNames[count]).getINode();
	            	}*/

	            	int inodesLen = parentPathINodes.length;
	            	//checkLease(src, clientName, pathINodes[inodesLen-1]);
	            	/*INodeFileUnderConstruction pendingIFile  = (INodeFileUnderConstruction)
                                               parentPathINodes[inodesLen - 1];*/

	            	//System.out.println("pendingFile "+pendingIFile.toString());
	            	//if (!_directory.checkFileProgress(pendingIFile, false)) {
	            	if (!_directory.checkFileProgress(pendingFile, false)) {
	            		throw new NotReplicatedYetException("Not replicated yet:" + _key);
	            	}

	            	// allocate new block record block locations in INode.
	            	newBlock = allocateBlock(_key, parentPathINodes, pendingFile);
	            	pendingFile.setTargets(targets);

	            	for (DatanodeDescriptor dn : targets) {
	            		dn.incBlocksScheduled();
	            	}

	            	// Create next block
	            	LocatedBlock lb = new LocatedBlock(newBlock, targets, fileLength);
	            	endLock(self);
	            	_response = new GetAdditionalBlockResponse(
                          (GetAdditionalBlockRequest) _request, self, _key, lb, _directory.getOwner());

	            }
              }
          }
      }

              //endLock(self);
	}
      //}
	//}
	/**
	* Allocate a block at the given pending filename
	*
	* @param src path to the file
	* @param inodes INode representing each of the components of src.
	*        <code>inodes[inodes.length-1]</code> is the INode for the file.
	*/
	private
	Block allocateBlock(String src, INode[] inodes, INodeFile inodeFile)
			throws	IOException {
		//StringUtility.debugSpace("FBTGetAdditionalBlockVisitor.allocateBlock for "+src);
		Block b = new Block(_directory.randBlockId.nextLong(), 0, 0);
		while(_directory.isValidBlock(b)) {
			b.setBlockId(_directory.randBlockId.nextLong());
		}
		b.setGenerationStamp(_directory.getGenerationStamp());
		b = addBlock(src, inodes, b, inodeFile);
		NameNode.stateChangeLog.info("BLOCK* FBTGetAdditionalBlock.allocateBlock: "
                         +src+ ". "+b);
		NameNode.LOG.info("BLOCK* FBTGetAdditionalBlock.allocateBlock: "
                +src+ ". "+b);
		return b;
	}
	/**
	* Add a block to the file. Returns a reference to the added block.
	*/
	Block addBlock(String path, INode[] inodes, Block block,
						INodeFile inodeFile) throws IOException {
		//waitForReady();
		//StringUtility.debugSpace("FBTGetAdditionalBlock.addBlock");
		//synchronized (rootDir) {
		//INodeFile fileINode = (INodeFile) inodes[inodes.length-1];
		INodeFile fileINode = inodeFile;
		//System.out.println("fileINode "+fileINode);
		// check quota limits and updated space consumed
		//updateCount(inodes, inodes.length-1, 0,
		//    fileNode.getPreferredBlockSize()*fileNode.getReplication(), true);

		// associate the new list of blocks with this file
		synchronized (_directory.blocksMap) {
			_directory.blocksMap.addINode(block, fileINode);
			BlockInfo blockInfo = _directory.blocksMap.getStoredBlock(block);
			fileINode.addBlock(blockInfo);
		}

		NameNode.stateChangeLog.info("DIR* FBTDirectory.addFile: "
		                            + path + " with " + block
		                            + " block is added to the in-memory "
		                            + "file system");
		//}
		return block;
	}

	/**
	   * Check that the indicated file's blocks are present and
	   * replicated.  If not, return false. If checkall is true, then check
	   * all blocks, otherwise check only penultimate block.
	   */
	  protected synchronized boolean checkFileProgress(INodeFile v, boolean checkall) {
	    if (checkall) {
	      //
	      // check all blocks of the file.
	      //
	      for (Block block: v.getBlocks()) {
	        if (_directory.blocksMap.numNodes(block) < _directory.minReplication) {
	          return false;
	        }
	      }
	    } else {
	      //
	      // check the penultimate block of this file
	      //
	      Block b = v.getPenultimateBlock();
	      if (b != null) {
	        if (_directory.blocksMap.numNodes(b) < _directory.minReplication) {
	          return false;
	        }
	      }
	    }
	    return true;
	  }
	@Override
	public void visit(PointerNode pointer, VPointer self) {
		// TODO 鐃緒申動鐃緒申鐃緒申鐃緒申鐃曙た鐃潤ソ鐃獣ド￥申鐃緒申鐃緒申鐃緒申

	}

	public String getClientNode() {
		return _clientNode;
	}
}
