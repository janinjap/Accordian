/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.lock;


import org.apache.hadoop.hdfs.server.namenodeFBT.Response;

/**
 * @author hanhlh
 *
 */
public final class UnlockResponse extends Response {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	// Constructors ///////////////////////////////////////////////////////////

    /**
     * UnockRequest ���Ф��뿷�����������֥������Ȥ��������ޤ�.
     * ������ȯ�������������Ȥʤ�
     * �׵ᥪ�֥������� (UnlockRequest) ��Ϳ����ɬ�פ�����ޤ�.
     *
     * @param request �����˵��������׵ᥪ�֥������� (UnlockRequest)
     */
    public UnlockResponse(UnlockRequest request) {
        super(request);
    }
}
