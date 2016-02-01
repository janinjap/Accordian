/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.lock;

import org.apache.hadoop.hdfs.server.namenodeFBT.Response;

/**
 * @author hanhlh
 *
 */
public final class EndLockResponse extends Response {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	/**
     * EndLockRequest ���Ф��뿷�����������֥������Ȥ��������ޤ�.
     * ������ȯ�������������Ȥʤ�
     * �׵ᥪ�֥������� (EndLockRequest) ��Ϳ����ɬ�פ�����ޤ�.
     *
     * @param request �����˵��������׵ᥪ�֥������� (EndLockRequest)
     */
    public EndLockResponse(EndLockRequest request) {
        super(request);
    }
}
