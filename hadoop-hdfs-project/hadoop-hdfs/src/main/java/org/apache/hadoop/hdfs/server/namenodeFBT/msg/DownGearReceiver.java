/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.msg;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.writeOffLoading.WriteOffLoadingCommand;
import org.apache.hadoop.hdfs.server.namenodeFBT.FBTDirectory;
import org.apache.hadoop.hdfs.server.namenodeFBT.NameNodeFBT;
import org.apache.hadoop.hdfs.server.namenodeStaticPartition.NameNodeStaticPartition;

/**
 * @author hanhlh
 *
 */
public class DownGearReceiver implements Runnable {


	protected final ServerSocket _ssock;

	private FBTDirectory _directory;

	private FSNamesystem _namesystem;

	private NameNodeFBT _namenodeFBT;

	private NameNodeStaticPartition _namenodeSP;

	public DownGearReceiver(int port, NameNodeFBT nn) throws IOException {
		try {
			_ssock = new ServerSocket(port);
			_namenodeFBT = nn;
			run();
		} catch (BindException be) {
			throw be;
		}
	}


	public DownGearReceiver(int port, NameNodeStaticPartition nn) throws IOException {
		try {
			_ssock = new ServerSocket(port);
			_namenodeSP = nn;
			run();
		} catch (BindException be) {
			throw be;
		}
	}
	@Override
	public void run() {
		//while (true) {
			try {
				receive(_ssock.accept());
			} catch (IOException ie) {
				ie.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO �����������catch ������
				e.printStackTrace();
			}
		//}
	}

	private void receive(Socket sock) throws IOException, ClassNotFoundException {
		sock.setTcpNoDelay(true);
		sock.setKeepAlive(true);

		ObjectInputStream ois = new ObjectInputStream(
									new BufferedInputStream(sock.getInputStream()));


		while (true) {
			try {
				Object obj = ois.readObject();
				if (obj instanceof FBTDirectory) {
					_directory = (FBTDirectory) obj;

					if (_namenodeFBT!=null) {
						 ((NameNodeFBT) _namenodeFBT).getTransferedDirectoryHandlerDownGear(_directory);
					}
				} else if (obj instanceof FSNamesystem) {
					_namesystem = (FSNamesystem) obj;
					if (_namenodeSP!=null) {
						_namenodeSP.getTransferedDirectoryHandler(_namesystem);
					}
				}
			} catch (EOFException e) {
				//e.printStackTrace();
				ois.close();
				sock.close();
				break;
			}
		}
	}

	protected void terminate() throws IOException {
		_ssock.close();
	}

	public FBTDirectory getDirectory() {
		if (_directory!=null) {
			return _directory;
		}
		return null;
	}

	public FSNamesystem getFSNamesystem() {
		if (_namesystem!=null) {
			return _namesystem;
		}
		return null;
	}
	public void setNameNodeFBT(NameNodeFBT namenodeFBT) {
		_namenodeFBT = namenodeFBT;
	}
	public void setNameNodeStaticPartition(NameNodeStaticPartition namenodeSP) {
		_namenodeSP = namenodeSP;
	}
}
