/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.utils;

/**
 * @author hanhlh
 *
 */
public class ThreadPool extends Thread {
	// instance attributes ////////////////////////////////////////////////////

    /** �������åȤ� Runnable ���֥������� */
    private Runnable _target;

    /** ���Υ���åɥס����°���륹��åɤ� ThreadGroup */
    private ThreadGroup _tgroup;

    /** ���Υ���åɥס��뤬���Ѥ��륹��å� */
    private Thread[] _workers;

    // constructors ///////////////////////////////////////////////////////////

    public ThreadPool(Runnable target, int nthreads) {
        this("ThreadPool", target, nthreads);
    }

    public ThreadPool(String name, Runnable target, int nthreads) {
        _target = target;
        _tgroup = new ThreadGroup(name);
		_tgroup.setDaemon(true);
        _workers = new Thread[nthreads];
		for (int i = 0; i < _workers.length; i++) {
			_workers[i] = new Thread(_tgroup, _target, name + i);
			_workers[i].setDaemon(true);
		}
    }

    // interface Runnable /////////////////////////////////////////////////////

    /**
     * <p>�������åȥ��֥������Ȥ򥹥�åɥס���ˤ�ä�����˼¹Ԥ��ޤ���
     * �������¹Է�̤����뤿��ˤϡ��������åȤ� run() �᥽�åɤμ�����
     * ����åɥ����դǤ���ɬ�פ�����ޤ���</p>
     */
    public void run() {
        for (int i = 0; i < _workers.length; i++) {
            _workers[i].start();
        }
    }

    // instance methods ///////////////////////////////////////////////////////

    public void shutdown() {
        for (int i = 0; i < _workers.length; i++) {
            try {
                _workers[i].join();
            } catch (InterruptedException e) {
                /* ignore */
            }
        }
    }

}
