/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.rule;

import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;

/**
 * @author hanhlh
 *
 */
public abstract class AbstractRule implements Rule {


	// instance attributes ////////////////////////////////////////////////////

    /** ���Υ롼�����Ͽ���� RuleManager */
    protected final RuleManager _manager;

    protected static LinkedList<String> stack = new LinkedList<String>();
	protected int pop_lock;


	public void push(String obj){

		stack.addFirst(obj);
		}

		//get top
		public String peek(){

		//isnull?
			if(!isEmpty()){

			return stack.getFirst();

			}else{
			return null;
			}

		}

		//pop
		public String pop(){

		//is null
			if(!isEmpty()){

			return stack.removeFirst();

			}else{
			return null;
			}

		}

		//is null
		public boolean isEmpty(){

			return stack.isEmpty();
		}

		// constructors ///////////////////////////////////////////////////////////

	    /**
	     * <p>���Υ��饹���󶡤��륢���ץ�����Ѥ��ƿ����� Rule ���������ޤ���</p>
	     *
	     * @param manager ���Υ롼�����Ͽ���� RuleManager
	     */
	    public AbstractRule(RuleManager manager) {
	    	//super((Messenger) NameNodeFBTProcessor.lookup("/service/messenger"));
	        _manager = manager;
	        enable();
	    }


	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.rule.Rule#initialize(org.apache.hadoop.conf.Configuration)
	 */
	public void initialize(Configuration conf) {
		// TODO ��ư�������줿�᥽�åɡ�������

	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.rule.Rule#dispatch(org.apache.hadoop.hdfs.server.namenodeFBT.rule.RuleEvent)
	 */
	public void dispatch(RuleEvent event) {
		if (condition(event)) {
            action(event);
        }
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.rule.Rule#enable()
	 */
	public void enable() {
		Class[] events = events();
        for (int i = events.length - 1; i >= 0; i--) {
            _manager.register(events[i], this);
        }
	}

	// instance methods ///////////////////////////////////////////////////////

    /**
     * <p>���Υ롼�뤬�оݤȤ��Ƥ��륤�٥�ȥ����פ�������ޤ���</p>
     *
     * @return ���Τ������ RuleEvent ���饹������
     */
    protected abstract Class[] events();

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.rule.Rule#disable()
	 */
	public void disable() {
		Class[] events = events();
        for (int i = events.length - 1; i >= 0; i--) {
            _manager.unregister(events[i], this);
        }
	}
	/**
     * <p>���Υ᥽�åɤˤ� ECA �롼���ȯ�о���������ޤ������Ƚ�Ǥˤ�����
     * �����Ѥ�ȼ������Ԥ��٤��ǤϤ���ޤ��󡣤��Υ᥽�åɤ򥪡��С��饤��
     * ������ϡ��᥽�åɤ���� AutoDisk ���������֤��Ѳ������ʤ��褦�����
     * ���Ƥ���������</p>
     *
     * @pattern-name Template Method �ѥ�����
     * @pattern-role Primitive Operation
     *
     * @param event ���Τ��줿���٥��
     * @return ����������ȯ�Ф�������Τ� true
     */
    protected boolean condition(RuleEvent event) {
        /* �ǥե���ȤǤϥ��٥�Ȥ��Ф��ƾ��ȯ�Ф��� */
        return true;
    }

    /**
     * <p>���Υ᥽�åɤˤ� ECA �롼��ˤ����륢��������������ޤ���</p>
     *
     * @pattern-name Template Method �ѥ�����
     * @pattern-role Primitive Operation
     *
     * @param event ���Τ��줿���٥��
     */
    protected abstract void action(RuleEvent event);

}
