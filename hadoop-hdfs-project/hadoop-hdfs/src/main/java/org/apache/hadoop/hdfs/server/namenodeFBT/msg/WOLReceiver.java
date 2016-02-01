/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.msg;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.writeOffLoading.WriteOffLoadingCommand;
import org.apache.hadoop.hdfs.server.namenodeFBT.FBTDirectory;
import org.apache.hadoop.hdfs.server.namenodeFBT.NameNodeFBT;
import org.apache.hadoop.hdfs.server.namenodeStaticPartition.NameNodeStaticPartition;

/**
 * @author hanhlh
 *
 */
public class WOLReceiver implements Runnable {



	// Processed at namenode stream-handler
	public static final byte OP_XFER_FBTDIRECTORY = (byte) 10;
	public static final byte OP_XFER_FSNAMESYSTEM = (byte) 11;
	public static final byte OP_XFER_WOLCOMMAND = (byte) 12;
	public static final byte OP_XFER_INODE = (byte) 13;
	public static final byte OP_XFER_OBJECT = (byte) 14;

	protected final ServerSocket _ssock;


	public WOLReceiver(int port) throws IOException {
		_ssock = new ServerSocket(port);
	}

	protected void terminate() throws IOException {
        _ssock.close();
    }

    private void receive(Socket sock) throws IOException {
    	//StringUtility.debugSpace("receiver running line 98");
        sock.setTcpNoDelay(true);
        sock.setKeepAlive(true);
        ObjectInputStream ois =
            new ObjectInputStream(
                new BufferedInputStream(sock.getInputStream()));

        while (true) {
        }
    }

    public void run() {
		//System.out.println("Receiver running. line 135");

        while (true) {
            try {
            	//System.out.println("Receiver running. line 140");
                receive(_ssock.accept());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

	}


}
