/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.blink;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenodeFBT.FBTDirectory;
import org.apache.hadoop.hdfs.server.namenodeFBT.FBTNodeVisitor;
import org.apache.hadoop.hdfs.server.namenodeFBT.Node;
import org.apache.hadoop.hdfs.server.namenodeFBT.Request;
import org.apache.hadoop.hdfs.server.namenodeFBT.VPointer;
import org.apache.hadoop.hdfs.server.namenodeFBT.msg.MessageException;
import org.apache.hadoop.hdfs.server.namenodeFBT.utils.StringUtility;

/**
 * @author hanhlh
 *
 */
public abstract class FBLTNodeVisitor extends FBTNodeVisitor {

	public FBLTNodeVisitor(FBTDirectory directory) {
		super(directory);
	}

	public void setRequest(Request request) {
        super.setRequest(request);
    }
	public void run() {
		StringUtility.debugSpace("FBLTNodeVisitor.run");
		// TODO ��ư�������줿�᥽�åɡ�������
		VPointer vp = _request.getTarget();

        if (vp == null) {
            /* �롼�Ȥ�������򳫻� */
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
             * ¾�� PE ����ž������Ƥ����׵�ξ��� vp != null �Ȥʤ�
             * vp �ǻ��ꤵ�줿�Ρ��ɤ� Visitor ���Ϥ�
             */
        	System.out.println("vp "+vp.toString());
            Node node = _directory.getNode(vp);
            try {
				node.accept(this, vp);
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
	public void lock(VPointer target, int mode, int h) {
//      super.lock(target, mode + 10 * h);
      lock(target, mode);
  }

  public void lock(VPointer target, int mode, int h, int c, int r) {
      lock(target, mode);
      _locker.incrementXCount(h, c, r);
  }

  protected FBTDirectory getDirectory() {
      return (FBTDirectory) _directory;
  }

}
