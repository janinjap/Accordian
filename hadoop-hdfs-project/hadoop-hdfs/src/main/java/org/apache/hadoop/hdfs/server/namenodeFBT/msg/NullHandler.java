/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.msg;

/**
 * @author hanhlh
 *
 */
public class NullHandler implements MessageHandler {

	// class attributes ///////////////////////////////////////////////////////

    /** NullHandler ���֥������Ȥ�ͣ��Υ��󥹥��� */
    private static NullHandler _instance;

 // class methods //////////////////////////////////////////////////////////

    /**
     * <p>NullHandler ���֥������Ȥ�ͣ��Υ��󥹥��󥹤�������ޤ���</p>
     *
     * @pattern-name Singleton
     * @pattern-role Instance ���ڥ졼�����
     *
     */
    public static synchronized NullHandler getInstance() {
        if (_instance == null) {
            _instance = new NullHandler();
        }
        return _instance;
    }

    // constructors ///////////////////////////////////////////////////////////

    /**
     * <p>�ץ饤�١��ȥ��󥹥ȥ饯���Ǥ������Υ��饹�Υ��󥹥��󥹤�ľ��
     * �������뤳�Ȥ�ػߤ��Ƥ��ޤ���</p>
     */
    private NullHandler() {
        // NOP
    }


	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.msg.MessageHandler#handle(org.apache.hadoop.hdfs.server.namenodeFBT.msg.Message)
	 */
	public void handle(Message message) {
		// NOP
		System.out.println("NullHandler.handle");
	}

}
