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
public final class EndLockRequest extends Request{

	// constructors ///////////////////////////////////////////////////////////

    /**
	 *
	 */
	private static final long serialVersionUID = 1L;

	/**
     * �Ρ��ɥ�å������ Transaction-ID ������׵ᥪ�֥������Ȥ��������ޤ�.
     *
     * @param target �����׵�ν������ǽ�˹Ԥ��� node ��ؤ��ݥ���
     */
    public EndLockRequest(VPointer target) {
        super(target);
    }

}
