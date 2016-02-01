/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.incopt;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenodeFBT.FBTDirectory;
import org.apache.hadoop.hdfs.server.namenodeFBT.FBTNodeVisitor;
import org.apache.hadoop.hdfs.server.namenodeFBT.IndexNode;
import org.apache.hadoop.hdfs.server.namenodeFBT.LeafNode;
import org.apache.hadoop.hdfs.server.namenodeFBT.MetaNode;
import org.apache.hadoop.hdfs.server.namenodeFBT.Node;
import org.apache.hadoop.hdfs.server.namenodeFBT.NodeVisitor;
import org.apache.hadoop.hdfs.server.namenodeFBT.PointerNode;
import org.apache.hadoop.hdfs.server.namenodeFBT.PointerSet;
import org.apache.hadoop.hdfs.server.namenodeFBT.Request;
import org.apache.hadoop.hdfs.server.namenodeFBT.Response;
import org.apache.hadoop.hdfs.server.namenodeFBT.VPointer;
import org.apache.hadoop.hdfs.server.namenodeFBT.lock.Lock;
import org.apache.hadoop.hdfs.server.namenodeFBT.msg.MessageException;
import org.apache.hadoop.hdfs.server.namenodeFBT.rule.FBTIncOptRequest;
import org.apache.hadoop.hdfs.server.namenodeFBT.utils.StringUtility;

/**
 * @author hanhlh
 *
 */
public abstract class FBTIncOptVisitor extends FBTNodeVisitor{


// instance attributes ////////////////////////////////////////////////////

    /**
     * �������륭����
     */
    protected String _key;

    /**
     * INC-OPT �ѿ� l
     */
    protected int _length;

    /**
     * INC-OPT �ѿ� h
     */
    protected int _height;

    /**
     * ���ߤΥΡ��ɤλҥΡ��ɤؤΥݥ���
     */
    protected VPointer _child;

    protected VPointer _parent;

	public FBTIncOptVisitor(FBTDirectory directory) {
		super(directory);
	}
	public void setRequest(FBTIncOptRequest request) {
        _key = request.getKey();
        _length = request.getLength();
        _height = request.getHeight();
        _child = request.getTarget();
        _parent = null;
    }


	public void run() {
		StringUtility.debugSpace("FBTIncOptVisitor.run");
		if (_child == null) {
	    	/* �롼�Ȥ������򳫻� */
	   		try {
				_directory.accept(this);
			} catch (NotReplicatedYetException e) {
				// TODO ��ư�������줿 catch �֥�å�
				e.printStackTrace();
			} catch (MessageException e) {
				// TODO ��ư�������줿 catch �֥�å�
				e.printStackTrace();
			} catch (IOException e) {
				// TODO ��ư�������줿 catch �֥�å�
				e.printStackTrace();
			}
		} else {
	    	/*
	    	 * ¾�� PE ����ž������Ƥ����׵�ξ��� _child != null �Ȥʤ�.
	    	 * _child �ǻ��ꤵ�줿�Ρ��ɤ� Visitor ���Ϥ�.
	    	 */
	    	Node node = _directory.getNode(_child);
	    	try {
				node.accept(this, _child);
			} catch (NotReplicatedYetException e) {
				// TODO ��ư�������줿 catch �֥�å�
				e.printStackTrace();
			} catch (MessageException e) {
				// TODO ��ư�������줿 catch �֥�å�
				e.printStackTrace();
			} catch (IOException e) {
				// TODO ��ư�������줿 catch �֥�å�
				e.printStackTrace();
			}
		}
	}

	@Override
	public void visit(MetaNode meta, VPointer self) throws NotReplicatedYetException, MessageException, IOException {
		StringUtility.debugSpace("FBTIncOptVisitor.visit "+meta);
		if (_height == _length) {
		    VPointer vPointer = meta.getPointerEntry();
			_child = meta.getRootPointer();

		    lock(vPointer, Lock.IX);

		    Node node = _directory.getNode(vPointer);
		    node.accept(this, vPointer);
		} else {
		    _height++;

			_child = meta.getRootPointer();

	        lock(_child, Lock.IX);

			/* _child �ǻ��ꤵ�줿�Ρ��ɤ� Visitor ���Ϥ� */
			Node node = _directory.getNode(_child);
			node.accept(this, _child);
		}
	}

	@Override
	public void visit(IndexNode index, VPointer self) throws NotReplicatedYetException, MessageException, IOException {
		StringUtility.debugSpace("FBTIncOptVisitor.visit "+index);
		int position = locate(index);

        if (_height == _length) {
            _child = index.getEntry(position);
            VPointer vPointer = index.getPointer(position);

		    lock(vPointer, Lock.IX);

//			unlock(self);
		    _parent = self;

            Node node = _directory.getNode(vPointer);
            node.accept(this, vPointer);
        } else if (index.isLeafIndex()) {
            if (index.size() > 0) {
                visitLeafIndexWithEntry(index, self, position);
            } else {
                visitLeafIndexWithoutEntry(self);
            }
        } else {
            goNextNode(index, self, position, Lock.IX);
        }
	}

	@Override
	public void visit(LeafNode leaf, VPointer self)
			throws MessageException, NotReplicatedYetException, IOException {
		StringUtility.debugSpace("FBTIncOptVisitor.visit "+leaf);
        int position = leaf.binaryLocate(_key);
        System.out.println("postition, "+position);
        if (position <= 0) {
            operateWhenSameKeyNotExist(leaf, self, position);
        } else {
            operateWhenSameKeyExist(leaf, self, position);
        }
	}

	@Override
	public void visit(PointerNode pointer, VPointer self) throws NotReplicatedYetException, MessageException, IOException {
		// TODO ��ư�������줿�᥽�åɡ�������
		PointerSet pointerSet = pointer.getEntry();

        /* �ҥڡ����򥰥?�Х���¾��å� */
        for (Iterator iter = pointerSet.iterator(); iter.hasNext();) {
            VPointer vPointer = (VPointer) iter.next();
            lock(vPointer, Lock.X);
        }
        ((FBTDirectory) _directory).incrementLockCount(pointerSet.size() - 1);

	    if (_parent != null) {
	        unlock(_parent);
		    _parent = null;
	    }
		unlock(self);

        Request request = generateModifyRequest(pointerSet);
        if (_directory.isLocal(_child)) {
            NodeVisitor visitor = generateNodeVisitor();
            visitor.setRequest(request);
            visitor.run();

            Response response = visitor.getResponse();

            /* �ҥڡ����Υ�å����� */
//            for (Iterator iter = pointerSet.iterator(); iter.hasNext();) {
//                VPointer vPointer = (VPointer) iter.next();
//                endLock(vPointer);
//            }
            endLockRequestConcurrently(pointerSet);

            handle(response);
        } else {
    	    /*
    	     * �ݥ��� vp �λؤ��Ƥ���Ρ��ɤϸ��ߤ� PE �ˤϤʤ����ᡤ
    	     * Ŭ���� PE �����򤷤��׵��ž��
    	     */
            try {
                Response response = callModifyRequest(request);

                /* �ҥڡ����Υ�å����� */
//                for (Iterator iter = pointerSet.iterator(); iter.hasNext();) {
//                    VPointer vPointer = (VPointer) iter.next();
//                    endLock(vPointer);
//                }
                endLockRequestConcurrently(pointerSet);

                judgeResponse(response);
            } catch (MessageException e) {
                e.printStackTrace();
            }
        }

	}

	protected void goNextNode(IndexNode index, VPointer self,
			int position, int lockMode) throws NotReplicatedYetException, MessageException, IOException {
		_height++;

		_child = index.getEntry(position);

		if (_directory.isLocal(_child)) {
		lock(_child, lockMode);
		unlock(self);

		try {
		Node node = _directory.getNode(_child);
		node.accept(this, _child);
		} catch (NullPointerException e) {
		e.printStackTrace();
		}
		} else {
		/*
		* �ݥ��� vp �λؤ��Ƥ���Ρ��ɤϸ��ߤ� PE �ˤϤʤ����ᡤ
		* Ŭ���� PE �����򤷤��׵��ž��
		*/
		lockRequest(_child, lockMode);
		endLock(self);

		callRedirect();
		}
		}

		protected void restart() throws NotReplicatedYetException, MessageException, IOException {
		_length = _height - 1;
		_height = 0;

		_directory.accept(this);
		}

	protected abstract void noEntry(VPointer self);

	protected void callRedirect() {
		FBTIncOptRequest request =
		(FBTIncOptRequest) _request;
		request.setTarget(_child);
		request.setLength(_length);
		request.setHeight(_height);
		callRedirectionException(_child.getPointer().getPartitionID());
	}

	protected abstract void operateWhenSameKeyExist(
	LeafNode leaf, VPointer self, int position) throws NotReplicatedYetException, MessageException, IOException;

	protected abstract void operateWhenSameKeyNotExist(
	LeafNode leaf, VPointer self, int position);

	protected abstract Response callModifyRequest(Request request)
							throws MessageException;

	protected abstract void judgeResponse(Response response);

	protected abstract Request generateModifyRequest(PointerSet pointerSet);

	protected abstract NodeVisitor generateNodeVisitor();

	protected abstract void handle(Response response) throws NotReplicatedYetException, MessageException, IOException;

	protected abstract int locate(IndexNode index);

	protected abstract void visitLeafIndexWithEntry(
			IndexNode index, VPointer self, int position) throws NotReplicatedYetException, MessageException, IOException;

	protected abstract void visitLeafIndexWithoutEntry(VPointer self);


}
