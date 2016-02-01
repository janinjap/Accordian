/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.rule;

import org.apache.hadoop.hdfs.server.namenodeFBT.Response;

/**
 * @author hanhlh
 *
 */
public class DeleteResponse extends Response {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;


// instance attributes ///////////////////////////////////////////////

    /** B-Tree �ΥǥХå�����פ��Ǽ����Хåե� */
    private final boolean _isSuccess;

    // constructors //////////////////////////////////////////////////////

    /**
     * AbstractDeleteRequest ���Ф��뿷�����������֥������Ȥ��������ޤ�.
     * ������ȯ�������������Ȥʤ�
     * �׵ᥪ�֥������� (AbstractDeleteRequest) ��Ϳ����ɬ�פ�����ޤ�.
     *
     * @param request �����˵��������׵ᥪ�֥������� (AbstractDeleteRequest)
     * @param isSuccess ����������������ɤ���
     */
    public DeleteResponse(DeleteRequest request, boolean success){
    	super(request);
	_isSuccess = success;
    }

    public DeleteResponse(DeleteRequest request) {
        this(request, true);
    }

    // accessors /////////////////////////////////////////////////////////

    public boolean isSuccess() {
        return _isSuccess;
    }

}
