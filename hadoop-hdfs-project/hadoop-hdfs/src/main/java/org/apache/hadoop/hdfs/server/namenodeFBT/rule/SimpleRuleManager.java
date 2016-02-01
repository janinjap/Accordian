/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.rule;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenodeFBT.ExceptionHandler;
import org.apache.hadoop.hdfs.server.namenodeFBT.service.ServiceException;

/**
 * @author hanhlh
 *
 */
public class SimpleRuleManager implements RuleManager {

	/**
     * {@link #dispatch(RuleEvent)}�ǻ��Ѥ���롢�ǥե���Ȥ��㳰�ϥ�ɥ�Ǥ���
     */
    private static final ExceptionHandler _nullHandler =
        new ExceptionHandler() {
        public void handleException(Exception e) {
            e.printStackTrace();
        }
    };

    /** �ȥ�󥶥������ID�ѤΥ��������ֹ�. */
    private static AtomicInteger _sequence = new AtomicInteger(0);

    /** �ȥ�󥶥������ID�ѤΥۥ���̾. */
    private static String _hostName;

    static {
        try {
            _hostName = InetAddress.getLocalHost().getHostName();
            _hostName = _hostName.split("\\.")[0];
        } catch (UnknownHostException e) {
        	NameNode.LOG.info("SimpleRuleManager.UnknownHostException exit");
        	System.out.println("SimpleRuleManager.UnknownHostException exit");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    // instance attributes ////////////////////////////////////////////////////

    /** ���٥�Ȥȥ롼����б��ط����ݻ�����ơ��֥� */
    private final ConcurrentMap<Class, Rule[]> _eventXruleMap;

    // constructors ///////////////////////////////////////////////////////////

    /**
     * <p>������ñ�쥹��å��Ǥ� RuleManager ��������ޤ����������
     * initialize �᥽�åɤǽ��������ɬ�פ�����ޤ���</p>
     */
    public SimpleRuleManager() {
        _eventXruleMap = new ConcurrentHashMap<Class, Rule[]>();
    }

    public void initialize(Configuration conf) throws ServiceException {
        _eventXruleMap.clear();
    }

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.rule.RuleManager#register(java.lang.Class, org.apache.hadoop.hdfs.server.namenodeFBT.rule.Rule)
	 */
	public synchronized void register(Class type, Rule rule) {
		Rule[] rules = _eventXruleMap.get(type);
        if (rules == null) {
            _eventXruleMap.put(type, new Rule[] { rule });
        } else {
            Rule[] temp = new Rule[rules.length + 1];
            System.arraycopy(rules, 0, temp, 0, rules.length);
            temp[rules.length] = rule;
            _eventXruleMap.put(type, temp);
        }
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.rule.RuleManager#unregister(java.lang.Class, org.apache.hadoop.hdfs.server.namenodeFBT.rule.Rule)
	 */
	public synchronized void unregister(Class type, Rule rule) {
		// TODO ��ư�������줿�᥽�åɡ�������
		Rule[] rules = _eventXruleMap.get(type);
        NameNode.LOG.debug("start class= " + type + ", rule= " + rule
                 + ", ruleCount= " + rules.length);
        if (rules != null) {
            List<Rule> list = new ArrayList<Rule>(Arrays.asList(rules));
            if (! list.remove(rule)) {
                return;
            }
            _eventXruleMap.put(type, list.toArray(new Rule[list.size()]));
        }
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.rule.RuleManager#flush()
	 */
	public void flush() {
		_eventXruleMap.clear();
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.rule.RuleManager#dispatch(org.apache.hadoop.hdfs.server.namenodeFBT.rule.RuleEvent)
	 */
	public void dispatch(RuleEvent event) {
		Rule[] rules;
		synchronized (_eventXruleMap) {
			rules = _eventXruleMap.get(event.getEventClass());
		}
        if (rules == null) {
            return;
        }

        for (int i = 0; i < rules.length; i++) {
            try {
                rules[i].dispatch(event);
            } catch (Exception e) {
            	e.printStackTrace();
                _nullHandler.handleException(e);
            }
        }
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.rule.RuleManager#dispatch(org.apache.hadoop.hdfs.server.namenodeFBT.rule.RuleEvent, org.apache.hadoop.hdfs.server.namenodeFBT.ExceptionHandler)
	 */
	public void dispatch(RuleEvent event, ExceptionHandler exHandler) {
		Rule[] rules = getRules(event);
        if (rules == null) {
            return;
        }

        for (int i = 0; i < rules.length; i++) {
            try {
                rules[i].dispatch(event);
            } catch (Exception e) {
            	e.printStackTrace();
                exHandler.handleException(e);
            }
        }
	}

	/**
     * �ͥåȥ����Τ��٤Ƥ� RuleManager �ˤ����ƥ�ˡ����� Transaction-ID
     * ʸ��������������֤��ޤ�.
     *
     * @return ��ˡ����� Transaction-ID
     */
    private String generateTransactionID() {
        return _sequence.incrementAndGet() + "@" + _hostName;
    }

	protected Rule[] getRules(RuleEvent event) {
        if (event.getTransactionID() == null) {
            event.setTransactionID(generateTransactionID());
        }
        return _eventXruleMap.get(event.getEventClass());
    }
}
