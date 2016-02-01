/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.service;

import org.apache.hadoop.hdfs.server.namenodeFBT.utils.StringUtility;

/**
 * @author hanhlh
 *
 */
public class ServiceException extends Exception {

	public ServiceException(Throwable cause) {
         super();

        /* J2SE 1.4 �ʹߤǤ�����㳰���������������� */
        //super(cause);
        StringUtility.debugSpace("ServiceException");
    }

}
