/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.rule;

import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenodeFBT.Response;
import org.apache.hadoop.hdfs.server.namenodeFBT.VPointer;

/**
 * @author hanhlh
 *
 */
public class GetAdditionalBlockResponse extends Response{

	// instance attributes ///////////////////////////////////////////////

    /**
	 *
	 */
	private static final long serialVersionUID = 1L;

	/**
     * ������� _value ������ȥ꡼����Ƥ��� LeafNode ��ؤ��ݥ���
     */
    protected final VPointer _vp;

    /**
     * B-Tree �����˻��Ѥ�������
     */
    protected final String _key;

    /**
     * ������̤Ǥ���ǡ���
     */

    protected final LocatedBlock _locatedBlock;
    /**
     * ź�դ��줿��å�����
     */
    //protected final String _message;

    // Constructors //////////////////////////////////////////////////////

    public GetAdditionalBlockResponse(GetAdditionalBlockRequest request,
    						VPointer vp,
            				String key,
            				LocatedBlock locatedBlock,
            				String message) {
        super(request);
        _vp = vp;
        _key = key;
        _locatedBlock = locatedBlock;
        //_message = message;
    }

    public GetAdditionalBlockResponse(GetAdditionalBlockRequest request,
    						VPointer vp,
            				String key,
            				LocatedBlock locatedBlock) {
        super(request);
        _vp = vp;
        _key = key;
        _locatedBlock = locatedBlock;
        //_message = null;
    }

    public GetAdditionalBlockResponse(GetAdditionalBlockRequest request,
    								String key,
    								LocatedBlock locatedBlock) {
        this(request, null, key, locatedBlock, null);
    }

    public GetAdditionalBlockResponse(GetAdditionalBlockRequest request,
    								String key) {
        this(request, null, key, null);
    }

    public GetAdditionalBlockResponse(GetAdditionalBlockRequest request) {
        this(request, request.getKey());
    }

    // accessors /////////////////////////////////////////////////////////

    public VPointer getVPointer() {
        return _vp;
    }

    public String getKey() {
        return _key;
    }

    public LocatedBlock getLocatedBlock() {
        return _locatedBlock;
    }
    /*public String getMessage() {
    	return _message;
    }*/

	@Override
	public String toString() {
		return "GetAdditionalBlockResponse [_vp=" + _vp + ", _key=" + _key
				+ ", _locatedBlock=" + _locatedBlock
				+ "]";
	}


    // instance methods //////////////////////////////////////////////////


}
