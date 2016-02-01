/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.rule;

import java.util.Properties;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenodeFBT.ExceptionHandler;
import org.apache.hadoop.hdfs.server.namenodeFBT.service.ServiceException;
import org.apache.hadoop.hdfs.server.namenodeFBT.utils.GroupedThreadFactory;
import org.apache.hadoop.hdfs.server.namenodeFBT.utils.ThreadPoolExecutorObserver;

/**
 * @author hanhlh
 *
 */
public class ThreadedRuleManager extends SimpleRuleManager {

	// instance attributes ////////////////////////////////////////////////////

    /** ���٥�Ƚ����˻��Ѥ��륹��åɥס��� */
    private ThreadPoolExecutor _executor;

    private ThreadPoolExecutorObserver _observer;

    private Thread _observerThread;

    // constructors ///////////////////////////////////////////////////////////

    /**
     * <p>����������åɥס����Ǥ� RuleManager ��������ޤ����������
     * initialize �᥽�åɤǽ��������ɬ�פ�����ޤ���</p>
     */
    public ThreadedRuleManager() {
        super();
    }

    // interface Service //////////////////////////////////////////////////////

    /**
     * <p>���ꤵ�줿�ץ�ѥƥ��ˤ�äƥ���åɥס����� RuleManager ������
     * ���ޤ������ѤǤ��������ץ�ѥƥ��ϰʲ����̤�Ǥ���</p>
     *
     * <ul>
     *   <li>max-threads : ������ǽ�ʥ���åɿ��ξ����</li>
     * </ul>
     *
     * @param prop ������ץ�ѥƥ�
     * @throws ServiceException
     */

    public void initialize(Configuration conf) throws ServiceException {
    	super.initialize(conf);
        try {
            _executor = new ThreadPoolExecutor(0,
            									Integer.MAX_VALUE,
            									60L,
            									TimeUnit.SECONDS,
            									new SynchronousQueue<Runnable>(),
            									new GroupedThreadFactory("RuleManager"));
            _observer = new ThreadPoolExecutorObserver(_executor);
            _observerThread = new Thread(_observer, "RuleManagerObserver");
            _observerThread.setDaemon(true);
            _observerThread.start();

        } catch (Exception e) {
        	e.printStackTrace();
            throw new ServiceException(e);

        }
    }

    public void disable() {
        // TODO
    	NameNode.LOG.info("ThreadedRuleManager.disable()");
        _executor.shutdown();
    }

    // interface RuleManager //////////////////////////////////////////////////

    public void dispatch(RuleEvent event, ExceptionHandler exHandler) {
    	Rule[] rules = getRules(event);
        if (rules == null) {
            return;
        }

        for(int i=0; i<rules.length; i++){
        	DispatchTask dt = new DispatchTask(rules[i], event, exHandler);
            _executor.execute(dt);
        }
    }

    // inner classes //////////////////////////////////////////////////////////

    /**
     * <p>����åɥס���μ¹��Ԥ����塼����Ͽ���뤿��ˡ��롼��μ¹��׵��
     * Runnable ���󥿡��ե���������ĥ������Ȥ���ɽ���������饹�Ǥ���</p>
     *
     * $Id: ThreadedRuleManager.java,v 1.9.2.3 2005/12/02 06:55:58 yoshihara Exp $
     *
     * @author Hiroshi Kazato <kazato@de.cs.titech.ac.jp>
     * @version $Revision: 1.9.2.3 $
     */
    private class DispatchTask implements Runnable {

        // instance attributes ////////////////////////////////////////////////

        /** �¹Ԥ���롼�� */
        private final Rule _rule;

        /** �롼���Ŭ���оݤȤʤ륤�٥�� */
        private final RuleEvent _event;

        private final ExceptionHandler _exHandler;

        // constructors ///////////////////////////////////////////////////////

        /**
         * <p>�������롼��μ¹��׵� (������) ��������ޤ���</p>
         *
         * @param rule �¹Ԥ���롼��
         * @param event �롼���Ŭ���оݤȤʤ륤�٥��
         */
        public DispatchTask (Rule rule, RuleEvent event,
                ExceptionHandler exHandler) {
            _rule = rule;
            _event = event;
            _exHandler = exHandler;
        }

        // interface runnable /////////////////////////////////////////////////

        public void run() {
            try {
                _rule.dispatch(_event);
            } catch (Exception e) {
                //e.printStackTrace();
            	_exHandler.handleException(e);
            }
        }
    }

}
