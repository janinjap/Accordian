/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.response;


import org.apache.hadoop.hdfs.server.namenodeFBT.Response;
import org.apache.hadoop.hdfs.server.namenodeFBT.request.AbstractMigrateRequest;

/**
 * @author hanhlh
 *
 */
public abstract class AbstractMigrateResponse extends Response {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	// instance attributes ////////////////////////////////////////////////////

    private final boolean _isSuccess;

    // constructors ///////////////////////////////////////////////////////////

    /**
     * AbstractMigrateRequest ���Ф��뿷�����������֥������Ȥ��������ޤ�.
     * ������ȯ�������������Ȥʤ�
     * �׵ᥪ�֥������� (AbstractMigrateRequest) ��Ϳ����ɬ�פ�����ޤ�.
     *
     * @param request �����˵��������׵ᥪ�֥������� (AbstractMigrateRequest)
     */
    public AbstractMigrateResponse(
            AbstractMigrateRequest request, boolean isSuccess) {
        super(request);
        _isSuccess = isSuccess;
    }

    public AbstractMigrateResponse(AbstractMigrateRequest request) {
        this(request, true);
    }

    // accessors //////////////////////////////////////////////////////////////

    public boolean isSuccess() {
        return _isSuccess;
    }

}
