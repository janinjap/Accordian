package org.apache.hadoop.hdfs.server.namenodeFBT.lock;


import org.apache.hadoop.hdfs.server.namenodeFBT.Response;

public final class LockResponse extends Response {

	// Constructors ///////////////////////////////////////////////////////////

    /**
	 *
	 */
	private static final long serialVersionUID = 1L;

	/**
     * LockRequest ���Ф��뿷�����������֥������Ȥ��������ޤ�.
     * ������ȯ�������������Ȥʤ�
     * �׵ᥪ�֥������� (LockRequest) ��Ϳ����ɬ�פ�����ޤ�.
     *
     * @param request �����˵��������׵ᥪ�֥������� (LockRequest)
     */
    public LockResponse(LockRequest request) {
        super(request);
    }

}
