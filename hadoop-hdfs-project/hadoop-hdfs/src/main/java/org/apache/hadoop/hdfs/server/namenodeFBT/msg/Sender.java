/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.msg;
  
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hdfs.server.namenodeFBT.Call;
import org.apache.hadoop.hdfs.server.namenodeFBT.Request;
import org.apache.hadoop.hdfs.server.namenodeFBT.Response;
import org.apache.hadoop.hdfs.server.namenodeFBT.utils.StringUtility;

/**
 * @author hanhlh
 *
 */
public class Sender implements Runnable {

	// class attributes ///////////////////////////////////////////////////////


    /** Message-ID 鐃祝含まわ申襯随申鐃緒申廛鐃緒申鐃緒申鐃竣の緒申 */
    private static final SimpleDateFormat _dateformat;

    static {
        _dateformat = new SimpleDateFormat("yyyyMMddhhmmss.");
    }

    /** 鐃緒申鐃緒申鐃緒申奪鐃緒申鐃緒申鐃緒申離鐃緒申鐃緒申鐃緒申鐃緒申峭鐃�*/
    private static AtomicInteger _sequence = new AtomicInteger(0);

    // instance attributes ////////////////////////////////////////////////////

    /** 鐃緒申鐃緒申鐃緒申 Messenger 鐃塾誌申鐃術わ申鐃銃わ申鐃暑エ鐃緒申疋櫂鐃緒申鐃緒申 */
    private final EndPoint _source;

    /** 鐃緒申鐃緒申鐃術のワ申奪鐃緒申鐃緒申鐃緒申鐃緒申紂�*/
    private final BlockingQueue<Message> _sendQueue;

    private int _maxObjectPerConnection;
    /**
     * <p>鐃緒申鐃熟ワ申鐃夙リー鐃緒申離廖鐃緒申鐃藷グわ申圓鐃緒申鐃緒申鐃塾マップでわ申鐃緒申鐃緒申続鐃緒申 EndPoint
     * 鐃薯キ￥申鐃粛とわ申鐃銃￥申ObjectOutputStream 鐃緒申泪奪廚鐃緒申佑鐃緒申飮鐃殉わ申鐃緒申</p>
     */
    protected final ConcurrentHashMap<EndPoint, OutputConnection> _connections;

    private final ConcurrentHashMap<Class, Map<Request, Call>> _replyCalls;

    private final BlockingQueue<Message> _forwardQueue;

    // constructors ///////////////////////////////////////////////////////////

    /**
     * <p>鐃緒申鐃緒申鐃緒申 MessageSender 鐃緒申鐃緒申奪匹鐃緒申鐃緒申鐃緒申鐃殉わ申鐃緒申</p>
     *
     * @param source 鐃緒申鐃緒申鐃緒申鐃夙わ申鐃銃ワ申奪鐃緒申鐃緒申鐃緒申鐃緒申娉辰鐃緒申鐃�EndPoint
     */
    public Sender(EndPoint source, BlockingQueue<Message> queue,
    		int maxObjectPerConnection) {
        _source = source;
        _sendQueue = new ArrayBlockingQueue<Message>(256);
        _connections = new ConcurrentHashMap<EndPoint, OutputConnection>();
        _replyCalls = new ConcurrentHashMap<Class, Map<Request, Call>>();
        _forwardQueue = queue;
        _maxObjectPerConnection = maxObjectPerConnection;
    }

    // instance methods ///////////////////////////////////////////////////////

    public void terminate() throws IOException {
        synchronized(_connections) {
            Iterator<OutputConnection> iter = _connections.values().iterator();
            while (iter.hasNext()) {
                iter.next().close();
            }
            _connections.clear();
        }
    }

    protected void send(Message message) throws MessageException {
    	//StringUtility.debugSpace("Sender.send");
    	//NameNode.LOG.info("Sender.send()");
        try {
            if (message.getMessageID() == null) {
                message.setMessageID(generateMessageID());
            }
            if (message.getSource() == null) {
                message.setSource(_source);
            }
            if (message.getDestination() == null) {
            	throw new UnsupportedOperationException("destination is null");
            }
            /*NameNode.LOG.info("***********");
            NameNode.LOG.info("NameNodeFBT.Messenger.send message "+
            				message.toString());*/
            _sendQueue.put(message);
        } catch (Exception e) {
        	e.printStackTrace();
            throw new MessageException(e);
        }
    }

    protected Call getReplyCall(Response response) {
    	//System.out.println("Sender.getReplyCall");
    	//System.out.println("response Class "+response.getClass());
    	Map<Request, Call> calls;
    	synchronized (_replyCalls) {
    		calls = _replyCalls.get(response.getClass());
    	}
        //System.out.println("calls "+calls.size());
        return calls.get(response.getSource());
    }

    protected void addReplyCall(Call call) {
    	//System.out.println("Sender.addReplyCall");
    	//System.out.println("Call request "+call.getRequest());
        Class response = call.getResponseClass();
        //System.out.println("response "+response);
        Map<Request, Call>  calls;
        synchronized (_replyCalls) {
        	calls = _replyCalls.get(response);

	        if (calls == null) {
	        	//System.out.println("Line 124");
	            Map<Request, Call> newCalls =
	                new ConcurrentHashMap<Request, Call>();
	            calls = _replyCalls.putIfAbsent(response, newCalls);
	            if (calls == null) {
	            	//System.out.println("Line 129");
	                newCalls.put(call.getRequest(), call);
	               // System.out.println("newCalls size "+newCalls.size());
	                return;
	            }
	        }
        }

        calls.put(call.getRequest(), call);
        //System.out.println("calls "+calls.size());
    }

    protected void removeReplyCall(Call call) {
    	synchronized (_replyCalls) {
	        Map calls = _replyCalls.get(call.getResponseClass());
	        calls.remove(call.getRequest());
    	}
    }

    private
    //synchronized
    OutputConnection getOutputConnection(EndPoint destination) {
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

    /**
     * <p>鐃粛ットワー鐃緒申鐃緒申里鐃緒申戮討鐃�Messenger 鐃祝わ申鐃緒申鐃銃ワ申法鐃緒申鐃緒申鐃�Message-ID
     * 文鐃緒申鐃緒申鐃緒申鐃緒申鐃緒申鐃緒申鐃緒申屬鐃緒申泙鐃緒申鐃�/p>
     *
     * @return 鐃緒申法鐃緒申鐃緒申鐃�Message-ID
     */
    private String generateMessageID() {
        String messageID =
            _dateformat.format(new Date(System.currentTimeMillis()));
        return messageID + _sequence.incrementAndGet() + "@" + _source;
    }

    // interface Runnable /////////////////////////////////////////////////////

    /**
     * <p>鐃緒申鐃緒申鐃緒申鐃遵ー鐃緒申鐃緒申録鐃緒申鐃曙た鐃緒申奪鐃緒申鐃緒申鐃緒申鐃緒申鐃緒申鐃緒申鐃緒申泙鐃緒申鐃緒申鐃緒申離瓮緒申奪匹鐃�
     * 鐃緒申鐃緒申奪疋鐃緒申鐃緒申佞砲覆辰討鐃緒申泙鐃緒申鐃�/p>
     */
    public void run() {
        Message message;
        OutputConnection oc;
        EndPoint destination;
        while (true) {
            try {
                message = _sendQueue.take();
                //System.out.println("message "+message);
                message.sendPrepare();
                destination = message.getDestination();
                if (destination.equals(_source)) {
                	//System.out.println("newly forward message locally at "+_source);
                    _forwardQueue.put((Message) message.clone());
                } else {
                    oc = getOutputConnection(destination);
                    //System.out.println("oc "+oc);
                    oc.sendObject(message.clone());
                    //oc.sendObject(message);
                }
            } catch (Exception e) {
            	//System.out.println("Sender exception "+e);
                e.printStackTrace();
                break;
            }
        }
    }

protected class OutputConnection extends ReentrantLock {

        private static final long serialVersionUID = -6655720064436932005L;

        protected final EndPoint _endPoint;

        private ObjectOutputStream _oos;

        protected int _sentObjectCount;

        public OutputConnection(EndPoint ep) {
            if(ep == null){
            	// 鐃峻わ申鐃緒申ep鐃緒申鐃緒申蠅件申覆鐃緒申鐃緒申僂篭愡澆鐃緒申泙鐃緒申鐃�
            	throw new UnsupportedOperationException(
                        "null parameter is not supported");
            }
        	_endPoint = ep;
            _oos = null;
            _sentObjectCount = 0;
        }

        public
        //synchronized
        void sendObject(Object obj) throws IOException {
        	StringUtility.debugSpace("Sender.sendObject() to "+_endPoint);
            lock();
            try {
                if (_oos == null) {
                	//System.out.println("oos=null");
                    Socket sock = _endPoint.createSocket();
                    //System.out.println("socket "+sock.toString());
                    sock.setTcpNoDelay(true);
                    sock.setKeepAlive(true);
                    _oos = new ObjectOutputStream(
                            new BufferedOutputStream(sock.getOutputStream()));
                }
                _oos.writeObject(obj);
                //_oos.writeObject(obj);
                //System.out.println("oos write object");
                _oos.flush();
                _sentObjectCount++;
                System.out.println("sentObject "+_sentObjectCount + " to "+ _endPoint);
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
