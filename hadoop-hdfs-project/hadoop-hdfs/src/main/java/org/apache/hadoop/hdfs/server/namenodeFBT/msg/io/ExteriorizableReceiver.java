/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.msg.io;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.BlockingQueue;


import org.apache.hadoop.hdfs.server.namenodeFBT.msg.Message;
import org.apache.hadoop.hdfs.server.namenodeFBT.msg.MessageHandler;
import org.apache.hadoop.hdfs.server.namenodeFBT.msg.PooledReceiver;

/**
 * @author hanhlh
 *
 */
public class ExteriorizableReceiver extends PooledReceiver {

	public ExteriorizableReceiver(int port, BlockingQueue<Message> queue,
            int threads) throws IOException, SocketException {
        super(port, queue, threads);
    }

    public ExteriorizableReceiver(int port, MessageHandler defaultHandler,
            BlockingQueue<Message> queue, int threads) throws IOException,
            SocketException {
        super(port, defaultHandler, queue, threads);
    }

    public void run() {
    	System.out.println("ExteriorizableReceiver run line 38");
        startForwarder();
        new Thread(_observer, "ReceiverObserver").start();

        while (_active.get()) {
            try {
                _ssock.setSoTimeout(5000);
                Socket sock = _ssock.accept();
                _executor.execute(new ExteriorizableReceiverThread(sock));
            } catch (SocketTimeoutException ste) {
                /* ignore */
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    protected class ExteriorizableReceiverThread extends ReceiverThread {

        /**
         * <p> �����ǻ��ꤵ�줿�����åȤ����å�����s �������ꡤ
         * �б�����ϥ�ɥ��ư���ƽ����򤹤륪�֥������Ȥ�������ޤ���</p>
         * @param sock ��Ω�������ͥ������
         */
        public ExteriorizableReceiverThread(Socket sock) {
            super(sock);
        }

        /**
         * �����åȤ����å�����s �������ꡤ
         * �б�����ϥ�ɥ��ư���ƽ����򤷤ޤ���
         *
         * �ϥ�ɥ���ν����ϡ��ϥ�ɥ��������Ū�˥���åɤ�������ʤ��¤ꡤ
         * ���Υ���åɤ�Ʊ������åɤǹԤ��ޤ���
         * @throws IOException
         */
        protected void receive(InputStream is) throws IOException {
            ExteriorInputStream eis =
                new ExteriorInputStream(new BufferedInputStream(is));

            Message message = null;
            MessageHandler handler = null;
            while (true) {
                try {
                    message = (Message) eis.readExterior();

                    handler = getHandler(message.getHandlerID());

                    handler.handle(message);
                } catch (EOFException e) {
                    eis.close();
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("error\n" + message.getClass() + "\n" +
                            message + "\n" + handler + "\n");
                }
            }
        }
    } // end-of class ExteriorizalbeReceiverThread
}
