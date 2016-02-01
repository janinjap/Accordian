package org.apache.hadoop.hdfs.server.namenodeFBT.rule;

import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenodeFBT.Response;
import org.apache.hadoop.hdfs.server.namenodeFBT.VPointer;

public class GetBlockLocationsResponse extends Response{

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

    protected final LocatedBlocks _locatedBlocks;
    /**
     * ź�դ��줿��å�����
     */
    //protected final String _message;

    // Constructors //////////////////////////////////////////////////////

    public GetBlockLocationsResponse(GetBlockLocationsRequest request,
    						VPointer vp,
            				String key,
            				LocatedBlocks locatedBlocks,
            				String message) {
        super(request);
        _vp = vp;
        _key = key;
        _locatedBlocks = locatedBlocks;
        //_message = message;
    }

    public GetBlockLocationsResponse(GetBlockLocationsRequest request,
    						VPointer vp,
            				String key,
            				LocatedBlocks locatedBlocks) {
        super(request);
        _vp = vp;
        _key = key;
        _locatedBlocks = locatedBlocks;
        //_message = null;
    }

    public GetBlockLocationsResponse(GetBlockLocationsRequest request,
    								String key,
    								LocatedBlocks locatedBlocks) {
        this(request, null, key, locatedBlocks);
    }

    public GetBlockLocationsResponse(GetBlockLocationsRequest request,
    								String key) {
        this(request, null, key, null);
    }

    public GetBlockLocationsResponse(GetBlockLocationsRequest request) {
        this(request, request.getKey());
    }

    // accessors /////////////////////////////////////////////////////////

    public VPointer getVPointer() {
        return _vp;
    }

    public String getKey() {
        return _key;
    }

    public LocatedBlocks getLocatedBlocks() {
        return _locatedBlocks;
    }


	@Override
	public String toString() {
		return "GetBlockLocationsResponse [_vp=" + _vp + ", _key=" + _key
				+ ", _locatedBlocks=" + _locatedBlocks
				+"]";
	}


	}






