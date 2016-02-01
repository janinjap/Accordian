/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.lock;

/**
 * @author hanhlh
 *
 */
public class Lock {
// class attribute ////////////////////////////////////////////////////////

    /* ��å��⡼�ɤ���� */
    public static final int	NONE = 0;
    public static final int	IS   = 1;
    public static final int	IX   = 2;
    public static final int	S    = 3;
    public static final int	SIX  = 4;
    public static final int	X    = 5;
    public static final int	CONF = -1; // lock confliction

    /** ��å��ޥȥꥯ�� */
    public static final int[][] matrix = new int[][] {
        /*           NONE  IS    IX    S     SIX   X   */
        /* NONE */ { NONE, IS,   IX,   S,    SIX,  X    },
        /* IS   */ { IS,   IS,   IX,   S,    SIX,  CONF },
        /* IX   */ { IX,   IX,   IX,   CONF, CONF, CONF },
        /* S    */ { S,    S,    CONF, S,    CONF, CONF },
        /* SIX  */ { SIX,  SIX,  CONF, CONF, CONF, CONF },
        /* X    */ { X,    CONF, CONF, CONF, CONF, CONF }
    };

    /** ��å��⡼�ɤ�ʸ����ɽ�� */
    public static final String[] modeString = new String[] {
        "NONE", "IS", "IX", "S", "SIX", "X"
    };

    // instance attribute /////////////////////////////////////////////////////

    /** ��å����ݻ����Ƥ��륭�� */
    protected LockKey _key;

    /** ��å��оݤȤʤ륪�֥������� */
    protected LockObject _obj;

    /** ��å��⡼�� */
    protected int _mode;

    protected String _log;

    // constructors ///////////////////////////////////////////////////////////

    public Lock(LockKey key, LockObject obj, int mode) {
        _key = key;
        _obj = obj;
        _mode = mode;
    }

    public String toString() {
        return _key.toString() + "(" + _mode + ")[" + _log + "]";
    }

    public void setLog(String log) {
        _log = log;
    }

}
