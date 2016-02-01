/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.lock;


import org.apache.hadoop.hdfs.server.namenodeFBT.Request;
import org.apache.hadoop.hdfs.server.namenodeFBT.VPointer;

/**
 * @author hanhlh
 *
 */
public final class UnlockRequest extends Request{

	// constructors ///////////////////////////////////////////////////////////

    /**
     * �Ρ��ɥ�å�����׵ᥪ�֥������Ȥ��������ޤ�.
     *
     * @param target �����׵�ν������ǽ�˹Ԥ��� node ��ؤ��ݥ���
	 */
	public UnlockRequest(VPointer target) {
		super(target);
	}


}
