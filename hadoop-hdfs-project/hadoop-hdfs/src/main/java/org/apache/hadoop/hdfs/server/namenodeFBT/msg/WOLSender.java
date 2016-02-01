/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.msg;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hdfs.server.namenodeFBT.Call;
import org.apache.hadoop.hdfs.server.namenodeFBT.NameNodeFBT;
import org.apache.hadoop.hdfs.server.namenodeFBT.Node;
import org.apache.hadoop.hdfs.server.namenodeFBT.Request;
import org.apache.hadoop.hdfs.server.namenodeFBT.msg.Sender.OutputConnection;
import org.apache.hadoop.hdfs.server.namenodeFBT.utils.StringUtility;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap;
import org.apache.hadoop.hdfs.server.namenode.INode;
/**
 * @author hanhlh
 *
 */
public class WOLSender implements Runnable {


	private final String _source;

	private final BlockingQueue<TransferObjects> _sendQueue;

	protected final ConcurrentHashMap<String, OutputConnection>
												_connections;

	public WOLSender(String source) {
		_source = source;
		_sendQueue = new ArrayBlockingQueue<TransferObjects>(256);
		_connections = new ConcurrentHashMap<String, OutputConnection>();
	}

	public void terminate() throws IOException {
        synchronized(_connections) {
            Iterator<OutputConnection> iter = _connections.values().iterator();
            while (iter.hasNext()) {
                iter.next().close();
            }
            _connections.clear();
        }
    }

	public void send(TransferObjects transferObject) throws InterruptedException {
		StringUtility.debugSpace("WOLSender, "+transferObject);
		_sendQueue.put(transferObject);
	}

	/* (��Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		Object node;
		OutputConnection oc;
		String destination;
		while(true) {
			try {
				TransferObjects inodeDes = _sendQueue.take();
				node = inodeDes.getObject();
				destination = inodeDes.getDestination();
				oc = getOutputConnection(destination);
				oc.sendObject(inodeDes);
			} catch (Exception e) {
				e.printStackTrace();
				break;
			}
		}
	}

	private OutputConnection getOutputConnection(String destination) {
		OutputConnection oc = _connections.get(destination);
		if (oc == null) {
			OutputConnection newOc = new OutputConnection(destination);
            oc = _connections.putIfAbsent(destination, newOc);
            if (oc == null) {
                return newOc;
            }
		}
		return oc;
	}

	public class TransferObjects {
		private Object _object;
		private String _destination;
		private String _owner;
		private String _src;
		private int _oldGear;
		private int _newGear;

		public TransferObjects(Object object, String destination, String owner,
				String src, int oldGear, int newGear) {
			_owner = owner;
			_object = object;
			_destination = destination;
			_src = src;
			_oldGear = oldGear;
			_newGear = newGear;
		}
		public TransferObjects(Object object, String destination, String owner,
				String src) {
			_owner = owner;
			_object = object;
			_destination = destination;
			_src = src;
		}

		public String getSrc() {
			return _src;
		}

		public void setSrc(String src) {
			this._src = src;
		}

		public Object getObject() {
			return _object;
		}

		public void setObject(Object object) {
			this._object= object;
		}

		public String getDestination() {
			return _destination;
		}

		public void setDestination(String _destination) {
			this._destination = _destination;
		}

		public String getOwner() {
			return _owner;
		}

		public void setOwner(String _owner) {
			this._owner = _owner;
		}
		
		public int getOldGear() {
			return _oldGear;
		}

		public void setOldGear(int _oldGear) {
			this._oldGear = _oldGear;
		}

		public int getNewGear() {
			return _newGear;
		}

		public void setNewGear(int _newGear) {
			this._newGear = _newGear;
		}

		@Override
		public String toString() {
			return "TransferObjects [_object=" + _object + ", _destination="
					+ _destination + ", _owner=" + _owner + ", _src=" + _src
					+ ", _oldGear=" + _oldGear + ", _newGear=" + _newGear + "]";
		}

	}

	protected class OutputConnection extends ReentrantLock {

        private static final long serialVersionUID = -6655720064436932005L;

        protected final String _destination;

        private ObjectOutputStream _oos;

        protected int _sentObjectCount;

        public OutputConnection(String dest) {
            if(dest == null){
            	// 鐃峻わ申鐃緒申ep鐃緒申鐃緒申蠅件申覆鐃緒申鐃緒申僂篭愡澆鐃緒申泙鐃緒申鐃�
            	throw new UnsupportedOperationException(
                        "null parameter is not supported");
            }
        	_destination = dest;
            _oos = null;
            _sentObjectCount = 0;
        }

        public
        //synchronized
        void sendObject(TransferObjects obj) throws IOException {
        	StringUtility.debugSpace("WOLSender.send objects to "+_destination);
            lock();
            try {
                if (_oos == null) {
                	//System.out.println("oos=null");
                    Socket sock = new Socket(InetAddress.getByName(
                    							_destination),
                    							NameNodeFBT.FBT_XFERMETADATA_PORT);
                    //System.out.println("socket "+sock.toString());
                    sock.setTcpNoDelay(true);
                    sock.setKeepAlive(true);
                    _oos = new ObjectOutputStream(
                            new BufferedOutputStream(sock.getOutputStream()));
                }
                
                if (obj.getObject() instanceof Node[]) { //Write metadata node 
	                _oos.writeByte(WOLReceiver.OP_XFER_OBJECT);
	    	        _oos.writeObject(obj.getObject());
	    	        _oos.writeUTF(obj.getSrc());
	    	        _oos.writeUTF(obj.getDestination());
	    	        _oos.writeUTF(obj.getOwner());
	    	        _oos.writeInt(obj.getOldGear());
	    	        _oos.writeInt(obj.getNewGear());
	    	        _oos.flush();
	                _sentObjectCount++;
	                System.out.println("WOLsentObject "+_sentObjectCount + " to "+ _destination);
                } else if (obj.getObject() instanceof BlocksMap) {
                	_oos.writeByte(WOLReceiver.OP_XFER_OBJECT);
	    	        _oos.writeObject(obj.getObject());
	    	        _oos.writeUTF(obj.getSrc());
	    	        _oos.writeUTF(obj.getDestination());
	    	        _oos.writeUTF(obj.getOwner());
	    	        _oos.flush();
	                _sentObjectCount++;
	                System.out.println("WOLsentObject "+_sentObjectCount + " to "+ _destination);
                }
                /*if (_sentObjectCount > _maxObjectPerConnection) {
                	//_oos.flush();
                    _oos.close();
                    _oos = null;
                    _sentObjectCount = 0;
                }*/
            } catch (Exception e) {
            	//System.out.println("sendObject exception");
            	e.printStackTrace();
            }
            finally {
               unlock();
            }
        }

        public void close() throws IOException {
            lock();
            try {
                if (_oos != null) {
                    _oos.close();
                }
            } catch (Exception e) {
            	e.printStackTrace();
            }
            finally {
                unlock();
            }
        }
    }


}
