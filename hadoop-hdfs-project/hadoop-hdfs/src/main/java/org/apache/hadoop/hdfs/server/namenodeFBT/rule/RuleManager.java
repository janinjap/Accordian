/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.rule;

import org.apache.hadoop.hdfs.server.namenodeFBT.ExceptionHandler;




/**
 * @author hanhlh
 *
 */
public interface RuleManager {

	/**
     * <p>���ꤵ�줿�����פΥ��٥�Ȥ�ȯ�������Ȥ��˼¹Ԥ���롼��򤳤�
     * RuleManager ����Ͽ���ޤ���</p>
     *
     * @param type Ŭ���оݤΥ��٥�ȥ��饹
     * @param rule �롼��
     */
    public void register(Class type, Rule rule);

    /**
     * <p>���ꤵ�줿�����פΥ��٥�Ȥ�ȯ�������Ȥ��˼¹Ԥ���褦����Ͽ
     * ����Ƥ���롼��򤳤� RuleManager ���������ޤ���</p>
     *
     * @param type Ŭ���оݤΥ��٥�ȥ��饹
     * @param rule �롼��
     */
    public void unregister(Class type, Rule rule);

    /**
     * <p>���� RuleManager ����Ͽ���줿���٤ƤΥ롼��������ޤ���</p>
     */
    public void flush();

    /**
     * <p>���٥�Ȥ�ȯ�������Τ��ޤ������� event �μ¹Ի��Υ��饹�˱�����
     * �롼�뤬�ȥꥬ������ޤ���</p>
     *
     * @param event ȯ���������٥��
     */
    public void dispatch(RuleEvent event);

    /**
     * <p>���٥�Ȥ�ȯ�������Τ��ޤ������� event �μ¹Ի��Υ��饹�˱�����
     * �롼�뤬�ȥꥬ������ޤ����롼��μ¹�����㳰��ȯ���������ˤϡ�
     * �����㳰�����ꤵ�줿�ϥ�ɥ���Ϥ���ޤ���</p>
     *
     * @param event ȯ���������٥��
     * @param exHandler �㳰���������ϥ�ɥ�
     */
    public void dispatch(RuleEvent event, ExceptionHandler exHandler);

}
