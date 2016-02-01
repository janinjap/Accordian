/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.rule;

import org.apache.hadoop.hdfs.server.namenodeFBT.FBTDirectory;
import org.apache.hadoop.hdfs.server.namenodeFBT.Request;
import org.apache.hadoop.hdfs.server.namenodeFBT.VPointer;

/**
 * @author hanhlh
 *
 */
public class DeleteRequest extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;


	// instance attributes ///////////////////////////////////////////////

    /**
     * �׵���Ф���������Ԥ��� Directory ��̾��.
     * "/service/directory/" �ޤ��� "/service/directory/backup".
     */
    protected final String _directoryName;

    /**
     * ������륭����
     */
    protected final String _key;

    /**
     * �׵���Ф���������Ԥ���Ǿ�̤� node ��ؤ��ݥ���.
     * null �ΤȤ��� MetaNode ���������Ԥ���.
     */
    protected VPointer _target;

    // constructors //////////////////////////////////////////////////////

    /**
     * ����������׵ᥪ�֥������Ȥ��������ޤ�.
     *
     * @param directoryName �����׵�ν������Ԥ��� Directory ��̾��
     * @param key ������륭����
     * @param target �����׵�ν������ǽ�˹Ԥ��� node ��ؤ��ݥ���
     */
    public DeleteRequest(
            String directoryName, String key, VPointer target) {
        super();
        _directoryName = directoryName;
        _key = key;
        _target = target;
    }

    /**
     * ����������׵ᥪ�֥������Ȥ��������ޤ�.
     *
     * @param directoryName �����׵�ν������Ԥ��� Directory ��̾��
     * @param key ������륭����
     */
    public DeleteRequest(String directoryName, String key) {
        this(directoryName, key, null);
    }

    /**
     * ����������׵ᥪ�֥������Ȥ��������ޤ�.
     *
     * @param key ������륭����
     * @param target �����׵�ν������ǽ�˹Ԥ��� node ��ؤ��ݥ���
     */
    public DeleteRequest(VPointer target, String key) {
        this(FBTDirectory.DEFAULT_NAME, key, target);
    }

    /**
     * ����������׵ᥪ�֥������Ȥ��������ޤ�.
     *
     * @param key ������륭����
     */
    public DeleteRequest(String key) {
        this(FBTDirectory.DEFAULT_NAME, key, null);
    }

    /**
     * �����������׵ᥪ�֥������Ȥ��������ޤ�.
     */
    public DeleteRequest() {
        this(FBTDirectory.DEFAULT_NAME, null, null);
    }

    // accessors //////////////////////////////////////////////////////////////

    public String getDirectoryName() {
        return _directoryName;
    }

    public String getKey() {
        return _key;
    }

    public VPointer getTarget() {
        return _target;
    }

    public void setTarget(VPointer target) {
        _target = target;
    }

	// instance methods ///////////////////////////////////////////////////////

	public String toString() {
		StringBuffer buf = new StringBuffer(super.toString());
		buf.append("directoryName= ");
		buf.append(_directoryName);
		buf.append(", key= ");
		buf.append(_key);
		buf.append(", target= ");
		buf.append(_target);
		return buf.toString();
	}

}
