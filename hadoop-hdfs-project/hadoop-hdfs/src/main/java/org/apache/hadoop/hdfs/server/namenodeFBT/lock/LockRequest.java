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
public final class LockRequest extends Request {

// instance attributes ////////////////////////////////////////////////////

    /**
	 *
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * ��å��Υ⡼��(S��X�ʤ�)
	 */
	private final int _mode;

    // constructors ///////////////////////////////////////////////////////////

	/**
     * �������Ρ��ɥ�å��׵ᥪ�֥������Ȥ��������ޤ�.
     *
     * @param target �����׵�ν������ǽ�˹Ԥ��� node ��ؤ��ݥ���
	 * @param mode �׵᤹���å��Υ⡼��
	 */
	public LockRequest(VPointer target, int mode) {
		super(target);
		_mode = mode;
	}

	// accessors //////////////////////////////////////////////////////////////

	public int getMode() {
		return _mode;
	}

    // instance methods ///////////////////////////////////////////////////////

    public String toString() {
        StringBuffer buf = new StringBuffer(super.toString());

        buf.append(", mode= ");
        buf.append(_mode);

        return buf.toString();
    }

}
