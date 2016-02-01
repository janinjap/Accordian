/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.request;


import org.apache.hadoop.hdfs.server.namenodeFBT.FBTDirectory;
import org.apache.hadoop.hdfs.server.namenodeFBT.Request;
import org.apache.hadoop.hdfs.server.namenodeFBT.VPointer;

/**
 * @author hanhlh
 *
 */
public abstract class AbstractMigrateRequest extends Request{

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;


// instance attributes ////////////////////////////////////////////////////

    /**
     * �׵���Ф���������Ԥ��� Directory ��̾����
     * "/service/directory/" �ޤ��� "/service/directory/backup"��
     */
    protected final String _directoryName;

    /**
     * �ǡ������ư����������
     * true �ΤȤ��Ϻ��ǡ�false �ΤȤ��ϱ���
     */
    protected final boolean _side;

    // constructors ///////////////////////////////////////////////////////////

    /**
     * �������ޥ����졼������׵ᥪ�֥������Ȥ��������ޤ���
     *
     * @param directoryName �����׵�ν������Ԥ��� Directory ��̾��
     * @param side �ǡ������ư��������(true �ΤȤ��Ϻ��ǡ�false �ΤȤ��ϱ�)
     */
    public AbstractMigrateRequest(
            String directoryName, boolean side, VPointer target) {
        super(target);
        _directoryName = directoryName;
        _side = side;
    }

    /**
     * �������ޥ����졼������׵ᥪ�֥������Ȥ��������ޤ���
     *
     * @param directoryName �����׵�ν������Ԥ��� Directory ��̾��
     * @param side �ǡ������ư��������(true �ΤȤ��Ϻ��ǡ�false �ΤȤ��ϱ�)
     */
    public AbstractMigrateRequest(String directoryName, boolean side) {
        this(directoryName, side, null);
    }

    /**
     * �������ޥ����졼������׵ᥪ�֥������Ȥ��������ޤ���
     *
     * @param side �ǡ������ư��������(true �ΤȤ��Ϻ��ǡ�false �ΤȤ��ϱ�)
     */
    public AbstractMigrateRequest(boolean side) {
        this(FBTDirectory.DEFAULT_NAME, side);
    }

    /**
     * �������ޥ����졼������׵ᥪ�֥������Ȥ��������ޤ���
     */
    public AbstractMigrateRequest() {
        this(FBTDirectory.DEFAULT_NAME, true);
    }

    // accessors //////////////////////////////////////////////////////////////

    public String getDirectoryName() {
        return _directoryName;
    }

    public boolean getSide() {
        return _side;
    }

}
