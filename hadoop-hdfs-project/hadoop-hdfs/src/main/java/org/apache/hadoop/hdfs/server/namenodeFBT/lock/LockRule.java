/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.lock;

import org.apache.hadoop.hdfs.server.namenodeFBT.NameNodeFBTProcessor;
import org.apache.hadoop.hdfs.server.namenodeFBT.VPointer;
import org.apache.hadoop.hdfs.server.namenodeFBT.rule.AbstractRule;
import org.apache.hadoop.hdfs.server.namenodeFBT.rule.RuleEvent;
import org.apache.hadoop.hdfs.server.namenodeFBT.rule.RuleManager;

/**
 * @author hanhlh
 *
 */
public class LockRule extends AbstractRule{

// instance attributes ////////////////////////////////////////////////////

    /**
     * �׵ᤵ�줿 lock ��Ԥ������ Locker
     */
	private final Locker _locker;

	// Constructors ///////////////////////////////////////////////////////////

    /**
     * Node ���Ф����å������Ԥ� LockRule ���������ޤ�.
     *
     * @param manager ���Υ롼�����Ͽ���� RuleManager
     */
    public LockRule(RuleManager manager) {
        super(manager);
        _locker = (Locker) NameNodeFBTProcessor.lookup("/locker");
    }

    // interface AbstractRule /////////////////////////////////////////////////

    protected Class[] events() {
        return new Class[] { LockRequest.class };
    }

    protected void action(RuleEvent event) {
    	//System.out.println("Lock.rule action");
        LockRequest request = (LockRequest) event;

		String transactionID = request.getTransactionID();
		VPointer target = request.getTarget();
		int mode = request.getMode();
		//System.out.println("Lock target "+target);
		_locker.lock(transactionID, target, mode);

		_manager.dispatch(new LockResponse(request));
    }

}
