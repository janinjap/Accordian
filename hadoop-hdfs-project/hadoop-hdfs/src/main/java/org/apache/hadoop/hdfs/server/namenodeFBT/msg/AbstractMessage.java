/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.msg;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.namenodeFBT.msg.io.ExteriorInputStream;
import org.apache.hadoop.hdfs.server.namenodeFBT.msg.io.ExteriorOutputStream;



/**
 * @author hanhlh
 *
 */
public abstract class AbstractMessage implements Message {

	 /**
	 *
	 */
	private static final long serialVersionUID = 1L;

	// instance attributes ////////////////////////////////////////////////////

    /** ������ Messenger �Υ���ɥݥ���� */
    protected EndPoint _source;

    /** ������ Messenger �Υ���ɥݥ���� */
    protected EndPoint _destination;

    /** ��å��������̻� */
    protected String _messageID;

    /** ��å������ϥ�ɥ鼱�̻� */
    protected String _handlerID;

 // constructors ///////////////////////////////////////////////////////////

    public AbstractMessage() {
        _source = null;
        _destination = null;
        _messageID = null;
    }

 // instance methods ///////////////////////////////////////////////////////

    public String toString() {
        return new StringBuffer()
        .append("From: ")
        .append(_source)
        .append("\nTo: ")
        .append(_destination)
        .append("\nMessage-ID: ")
        .append(_messageID)
        .append("\nMessage-Handler: ")
        .append((_handlerID == null) ? "default" : _handlerID)
        .append("\n")
        .toString();
    }


	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.msg.Message#getMessageID()
	 */
	public String getMessageID() {
		return _messageID;
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.msg.Message#setMessageID(java.lang.String)
	 */
	public void setMessageID(String messageID) {
		_messageID = messageID;
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.msg.Message#getHandlerID()
	 */
	public String getHandlerID() {
		return _handlerID;
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.msg.Message#setHandlerID(java.lang.String)
	 */
	public void setHandlerID(String handlerID) {
		_handlerID = handlerID;
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.msg.Message#getSource()
	 */
	public EndPoint getSource() {
		return _source;
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.msg.Message#setSource(org.apache.hadoop.hdfs.server.namenodeFBT.msg.EndPoint)
	 */
	public void setSource(EndPoint source) {
		_source = source;
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.msg.Message#getDestination()
	 */
	public EndPoint getDestination() {
		return _destination;
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.msg.Message#setDestination(org.apache.hadoop.hdfs.server.namenodeFBT.msg.EndPoint)
	 */
	public void setDestination(EndPoint destination) {
		_destination = destination;
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.msg.Message#sendPrepare()
	 */
	public void sendPrepare() {
		// TODO ��ư�������줿�᥽�åɡ�������

	}
	public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	public void writeExterior(ExteriorOutputStream out) throws IOException {
        EndPoint.write(_source, out);
        EndPoint.write(_destination, out);
        out.writeAscii(_messageID);
        out.writeAscii(_handlerID);
    }

    public void readExterior(ExteriorInputStream in) throws IOException {
        _source = EndPoint.read(in);
        _destination = EndPoint.read(in);
        _messageID = in.readAscii();
        _handlerID = in.readAscii();
    }


}
