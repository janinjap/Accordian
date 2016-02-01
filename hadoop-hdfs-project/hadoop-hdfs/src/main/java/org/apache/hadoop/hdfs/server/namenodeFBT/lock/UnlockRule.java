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
public class UnlockRule extends AbstractRule {

// instance attributes ////////////////////////////////////////////////////

    /**
     * �׵ᤵ�줿 unlock ��Ԥ������ Locker
     */
	private final Locker _locker;

	// Constructors ///////////////////////////////////////////////////////////

    /**
     * Node ���Ф����å����������Ԥ� UnlockRule ���������ޤ�.
     *
     * @param manager ���Υ롼�����Ͽ���� RuleManager
     */
    public UnlockRule(RuleManager manager) {
        super(manager);
        _locker = (Locker) NameNodeFBTProcessor.lookup(Locker.NAME);
    }

    // interface AbstractRule /////////////////////////////////////////////////

    protected Class[] events() {
        return new Class[] { UnlockRequest.class };
    }

    protected void action(RuleEvent event) {
        UnlockRequest request = (UnlockRequest) event;

		String transactionID = request.getTransactionID();
		VPointer target = request.getTarget();

		_locker.unlock(transactionID, target);

		_manager.dispatch(new UnlockResponse(request));
    }

}

