/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.rule;

import java.util.EventListener;

import org.apache.hadoop.conf.Configuration;


/**
 * @author hanhlh
 *
 */
public interface Rule extends EventListener {

	public void initialize(Configuration conf);

    /**
     * <p>���٥�Ȥ����Τ�����뤿��Υ᥽�åɤǤ�����Ͽ���Ƥ��륤�٥�Ȥ�
     * ȯ�������Ȥ��� RuleManager ����ƤӽФ���ޤ���</p>
     *
     * @pattern-name Observer �ѥ�����
     * @pattern-role Notify ���ڥ졼�����
     *
     * @param event ���Τ���륤�٥��
     */
    public void dispatch(RuleEvent event);

    /**
     * <p>���Υ롼���ͭ���ˤ��ޤ���</p>
     */
    public void enable();

    /**
     * <p>���Υ롼���̵���ˤ��ޤ���</p>
     */
    public void disable();

}
