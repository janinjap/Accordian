/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.msg;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hdfs.server.blockmanagement.BlocksMap;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.writeOffLoading.WriteOffLoadingCommand;
import org.apache.hadoop.hdfs.server.namenodeFBT.FBTDirectory;
import org.apache.hadoop.hdfs.server.namenodeFBT.NameNodeFBT;
import org.apache.hadoop.hdfs.server.namenodeFBT.Node;
import org.apache.hadoop.hdfs.server.namenodeFBT.msg.PooledReceiver.ReceiverThread;
import org.apache.hadoop.hdfs.server.namenodeFBT.utils.GroupedThreadFactory;
import org.apache.hadoop.hdfs.server.namenodeFBT.utils.StringUtility;
import org.apache.hadoop.hdfs.server.namenodeStaticPartition.NameNodeStaticPartition;

/**
 * @author hanhlh
 *
 */
public class PooledWOLReceiver extends WOLReceiver {

	protected final ThreadPoolExecutor _executor;

	protected AtomicBoolean _active;

	private NameNodeFBT _namenodeFBT;
	private NameNodeStaticPartition _namenodeSP;

	public PooledWOLReceiver(int port) throws IOException {
		super(port);
		_executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
                new GroupedThreadFactory("WOLReceiver"));
		_active = new AtomicBoolean(true);
	}

	public void setNamenodeFBT(NameNodeFBT nn) {
		_namenodeFBT = nn;
	}

	public void setNamenodeStaticPartition(NameNodeStaticPartition nnsp) {
		_namenodeSP = nnsp;
	}

	public void run() {
		while (_active.get()) {
			//System.out.println("PooledWOLReceiver runs");
	        try {
	            //_ssock.setSoTimeout(5000);
	            Socket sock = _ssock.accept();
	            ReceiverThread rt  = new ReceiverThread(sock);
	            if (_namenodeFBT!=null) {
	            	rt.setNameNodeFBT(_namenodeFBT);
	            }
	            if (_namenodeSP!=null) {
	            	rt.setNameNodeStaticPartition(_namenodeSP);
	            }
	            _executor.execute(rt);
	            //rt.run();

	        } catch (SocketTimeoutException ste) {
	            // NOP
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }
	}

	protected class ReceiverThread implements Runnable {


		/** TCP/IP socket */
        private final Socket _sock;

    	private FBTDirectory _directory;

    	private FSNamesystem _namesystem;

    	private WriteOffLoadingCommand _wolCommand;

    	protected NameNodeFBT _namenodeFBT;

    	private NameNodeStaticPartition _namenodeSP;

        public ReceiverThread(Socket sock) {
            _sock = sock;
        }

        public void setNameNodeFBT(NameNodeFBT nn) {
        	_namenodeFBT = nn;
        }
        public void setNameNodeStaticPartition(NameNodeStaticPartition nnsp) {
        	_namenodeSP =  nnsp;
        }
        public void terminate() throws IOException {
            System.out.println("socket " + _sock.toString() + " closed");
            _sock.close();
        }

		@Override
		public void run() {
			// TODO �������������純�����鴻���
			try {
				receive();
			} catch (IOException e) {
				// TODO �����������catch ������
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO �����������catch ������
				e.printStackTrace();
			}
		}
		private void receive() throws IOException, ClassNotFoundException {
			StringUtility.debugSpace("PooledWOLReceiver receive");
			_sock.setTcpNoDelay(true);
			_sock.setKeepAlive(true);
			System.out.println("socket,"+_sock.toString());
			ObjectInputStream ois = new ObjectInputStream(
										new BufferedInputStream(_sock.getInputStream()));

			while (true) {
				try {
					byte operation = ois.readByte();
					if (operation == OP_XFER_FBTDIRECTORY) {
						_directory = (FBTDirectory) ois.readObject();
						if (_namenodeFBT!=null) {
							_namenodeFBT.getTransferedDirectoryHandler(_directory);
						}
					} else if (operation == OP_XFER_FSNAMESYSTEM) {
						_namesystem = (FSNamesystem) ois.readObject();
						if (_namenodeSP!=null) {
							_namenodeSP.getTransferedDirectoryHandler(_namesystem);
						}
					} else if (operation == OP_XFER_WOLCOMMAND) {
						_wolCommand = (WriteOffLoadingCommand) ois.readObject();
						if (_namenodeFBT!=null) {
							_namenodeFBT.writeOffLoadingCommandHandler(_wolCommand);
						}
					} else if (operation == OP_XFER_INODE) {
						if (_namenodeFBT!=null) {
							_namenodeFBT.receive((INode) ois.readObject(),
									ois.readUTF(),
									_sock.getInetAddress().getHostName());
						}
					}	else if (operation == OP_XFER_OBJECT) {
						System.out.println("get object");
						if (_namenodeFBT!=null) {
							Object obj = ois.readObject();
							if (obj instanceof BlocksMap) {
								_namenodeFBT.receive((BlocksMap) obj,
										ois.readUTF(),         				   //src
										ois.readUTF(),						   //targetnode
										//_sock.getInetAddress().getHostName());
										ois.readUTF());//owner
							} else if (obj instanceof Node) {
								_namenodeFBT.receive((Node) obj,			
										ois.readUTF(),						//src
										ois.readUTF(),						//targetnode
										_sock.getInetAddress().getHostName()); //owner
							}else if (obj instanceof Node[]) {
								_namenodeFBT.receive((Node[]) obj,
										ois.readUTF(), 						//src
										ois.readUTF(),						//targetnode
										ois.readUTF(),						//owner
										ois.readInt(),						//oldGear
										ois.readInt());						//newGear
							}
						}
					}
				} catch (EOFException e) {
					e.printStackTrace();
					ois.close();
					break;
				}
			}
		}


	}

}
