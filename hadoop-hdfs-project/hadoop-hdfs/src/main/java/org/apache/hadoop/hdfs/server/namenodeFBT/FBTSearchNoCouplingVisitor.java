/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Date;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenodeFBT.lock.Lock;
import org.apache.hadoop.hdfs.server.namenodeFBT.msg.MessageException;
import org.apache.hadoop.hdfs.server.namenodeFBT.request.RequestFactory;
import org.apache.hadoop.hdfs.server.namenodeFBT.response.ResponseClassFactory;
import org.apache.hadoop.hdfs.server.namenodeFBT.rule.SearchRequest;
import org.apache.hadoop.hdfs.server.namenodeFBT.rule.SearchResponse;
import org.apache.hadoop.hdfs.server.namenodeFBT.utils.StringUtility;


/**
 * @author hanhlh
 *
 */
public class FBTSearchNoCouplingVisitor extends FBTNodeVisitor {

	/**
     * 鐃叔ワ申鐃曙ク鐃夙リ検鐃緒申鐃緒申鐃緒申
     */
    private String _key;
    // 	constructors ///////////////////////////////////////////////////////////

    public FBTSearchNoCouplingVisitor(FBTDirectory directory) {
        super(directory);
    }

    // accessors //////////////////////////////////////////////////////////////

    public void setRequest(Request request) {
        setRequest((SearchRequest) request);
    }

    public void setRequest(SearchRequest request) {
        super.setRequest(request);
        _key = request.getKey();
    }

	public void run() {
		Date start = new Date();
		VPointer vp = _request.getTarget();
        if (vp == null) {
            /* 鐃暑ー鐃夙わ申鐃緒申鐃緒申鐃薯開誌申 */
            try {
				_directory.accept(this);
			} catch (NotReplicatedYetException e) {
				e.printStackTrace();
			} catch (MessageException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
        } else {
            /*
             * 他鐃緒申 PE 鐃緒申鐃緒申転鐃緒申鐃緒申鐃緒申討鐃緒申鐃緒申弋鐃塾常申鐃緒申 vp != null 鐃夙なわ申
             * vp 鐃叔誌申鐃所さ鐃曙た鐃塾￥申鐃宿わ申 Visitor 鐃緒申鐃熟わ申
             */
            Node node = _directory.getNode(vp);
            try {
				node.accept(this, vp);
			} catch (MessageException e) {
				// TODO 鐃緒申動鐃緒申鐃緒申鐃緒申鐃曙た catch 鐃瞬ワ申奪鐃�
				e.printStackTrace();
			} catch (NotReplicatedYetException e) {
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
        //NameNode.LOG.info("searchVisitorCore,"+(new Date().getTime()-start.getTime())/1000.0);
	}

	@Override
	public void visit(MetaNode meta, VPointer self) throws MessageException, NotReplicatedYetException, IOException {
		StringUtility.debugSpace("FBTSearchNoCouplingVisitor.visit "+
				meta.getNameNodeID());
		lock(self, Lock.IS);
        VPointer root = meta.getRootPointer();
        System.out.println("root "+root);
        unlock(self);
        Node node = _directory.getNode(root);
        System.out.println("getNode "+node.getNodeID());
        node.accept(this, root);

	}

	@Override
	public void visit(IndexNode index, VPointer self) throws MessageException, NotReplicatedYetException, IOException {
		StringUtility.debugSpace("FBTSearchNoCouplingVisitor.visit "+index.getNameNodeID());
		index.key2String();
		lock(self, Lock.IS);
//      if (_parent != null) {
//          unlock(_parent);
//          _parent = null;
//      }

      if (!index.isInRange(_key) || index._deleteBit){
    	  //System.out.println(_key +" is not in range");
          unlock(self);
          //System.out.println("unlock IS");
      } else {
          /* 鐃緒申鐃塾￥申鐃宿での醐申鐃緒申鐃緒申鐃緒申鐃塾逸申鐃緒申 */
    	  //System.out.println(_key +" is in range");
          int position = index.binaryLocate(_key);
         // System.out.println("position "+position);

          if (position < 0) {
              /* 鐃緒申鐃淳￥申鐃縦リー鐃祝ワ申鐃緒申肇蝓種申鐃渋醐申澆鐃緒申覆鐃�*/
        	  //System.out.println("no entry");
              _response = new SearchResponse((SearchRequest) _request);
              endLock(self);
          } else {
              /* 鐃述ノ￥申鐃宿わ申悗鐃緒申櫂鐃緒申鐃�*/
              VPointer vp = index.getEntry(position);
              //System.out.println("vp "+vp);
              if (_directory.isLocal(vp)) { //local PE
            	  if (_directory.isLocalDirectory(vp)) { //local FBTDirectory
            		  //System.out.println(vp + "refers to node in local directory");
	                  unlock(self);
	//                  _parent = self;
	                  Node node = _directory.getNode(vp);
	                  node.accept(this, vp);
            	  } else { //fw to backup FBTDirectory
            		  //System.out.println(vp + " does not refer to node in local directory");
            		  unlock(self);
            		  FBTDirectory targetFBTDirectory =
            			  			(FBTDirectory) NameNodeFBTProcessor.lookup(
							"/directory."+vp.getPointer().getFBTOwner());
            		  Node node = targetFBTDirectory.getNode(vp);
            		  node.accept(this, vp);
            	  }
              } else { // not local PE
                  /*
                   * 鐃楯ワ申鐃緒申 vp 鐃塾指わ申鐃銃わ申鐃緒申痢鐃緒申匹聾鐃緒申澆鐃�PE 鐃祝はなわ申鐃緒申鐃潤，
                   * 適鐃緒申鐃緒申 PE 鐃緒申鐃緒申鐃薯しわ申鐃竣居申鐃重常申鐃�
                   */
                  endLock(self);
                  _request.setTarget(vp);
                  //System.out.println("partID "+vp.getPointer().getPartitionID());
                  callRedirectionException(vp.getPointer().
                		  					getPartitionID());
              }
          }
      }

	}


	@Override
	public void visit(LeafNode leaf, VPointer self) {
		StringUtility.debugSpace("FBTSearchNoCoupling visit "+leaf.getNameNodeID()+" for src: "+_key);
		lock(self, Lock.S);
//      if (_parent != null) {
//          unlock(_parent);
//          _parent = null;
//      }

      if (leaf.get_isDummy()) {
          VPointer vp = leaf.get_RightSideLink();
          endLock(self);
          _request.setTarget(vp);
          callRedirectionException(vp.getPointer().getPartitionID());
      } else {
    	  //System.out.println("key "+_key);
    	  //System.out.println("highkey "+leaf.get_highKey());
          if (_key.compareTo(leaf.get_highKey()) >= 0 || leaf.get_deleteBit()) {
              unlock(self);
          } else {
        	  System.out.println("keys: "+leaf.get_keys().toString());
        	  int position = leaf.binaryLocate(_key);
        	  System.out.println("position "+position);
              if (position <= 0) {
                  // 鐃緒申鐃緒申鐃緒申存鐃淳わ申鐃淑わ申
            	  //System.out.println("key "+_key+" doesnt exist");
                  endLock(self);
                  _response = new SearchResponse((SearchRequest) _request);
              } else {
                  INode inode = leaf.getINode(position - 1);
                  endLock(self);
                  _response = new SearchResponse(
                          (SearchRequest) _request, self, _key, inode);
              }

              endLock(self);

          }
      }

	}

	@Override
	public void visit(PointerNode pointer, VPointer self) {
		// TODO 鐃緒申動鐃緒申鐃緒申鐃緒申鐃曙た鐃潤ソ鐃獣ド￥申鐃緒申鐃緒申鐃緒申

	}

}
