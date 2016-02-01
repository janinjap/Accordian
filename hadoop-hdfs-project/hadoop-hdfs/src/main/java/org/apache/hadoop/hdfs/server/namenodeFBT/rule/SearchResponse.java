/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.rule;


import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenodeFBT.LeafEntry;
import org.apache.hadoop.hdfs.server.namenodeFBT.LeafNode;
import org.apache.hadoop.hdfs.server.namenodeFBT.LeafValue;
import org.apache.hadoop.hdfs.server.namenodeFBT.Response;
import org.apache.hadoop.hdfs.server.namenodeFBT.VPointer;

/**
 * @author hanhlh
 *
 */
public class SearchResponse extends Response {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	// instance attributes ///////////////////////////////////////////////

    /**
     * ������� _value ������ȥ꡼����Ƥ��� LeafNode ��ؤ��ݥ���
     */
    protected final VPointer _vp;

    /**
     * B-Tree �����˻��Ѥ�������
     */
    protected final String _key;

    protected final String _fileName;
    /**
     * ������̤Ǥ���ǡ���
     */

    protected final LeafValue _value;
    protected final INode _inode;
    /**
     * ź�դ��줿��å�����
     */
    protected final String _message;

    // Constructors //////////////////////////////////////////////////////

    /**
     * SearchRequest ���Ф��뿷�����������֥������Ȥ��������ޤ�.
     * ������ȯ�������������Ȥʤ�
     * �׵ᥪ�֥������� (SearchRequest) ��Ϳ����ɬ�פ�����ޤ�.
     *
     * @param request �����˵��������׵ᥪ�֥������� (SearchRequest)
     * @param vp value ������ȥ꡼����Ƥ��� LeafNode ��ؤ��ݥ���
     * @param key B-Tree ��������
     * @param value ������̥ǡ�����
     * @param message ź�դ��줿��å�����
     */
    public SearchResponse(SearchRequest request, VPointer vp,
            				String key, String fileName,
            				LeafValue value, String message) {
        super(request);
        _vp = vp;
        _key = key;
        _fileName = fileName;
        _value = value;
        _message = message;
        _inode = null;
    }
    /*
    public SearchResponse(SearchRequest request, VPointer vp,
								String key,
								LeafValue value, String message) {
		super(request);
		_vp = vp;
		_key = key;
		_fileName = null;
		_inode = null;
		_value = value;
		_message = message;
    }*/

    public SearchResponse(SearchRequest request, VPointer vp,
            				String key, LeafValue value) {
        super(request);
        _vp = vp;
        _key = key;
        _fileName = null;
        _inode = null;
        _value = value;
        _message = null;
    }

    public SearchResponse(SearchRequest request, VPointer vp,
			String key, INode inode) {
		super(request);
		_vp = vp;
		_key = key;
		_fileName = null;
		_inode = inode;
		_value = null;
		_message = null;
    }

    public SearchResponse(SearchRequest request, VPointer vp,
			String key, INode inode, String message) {
		super(request);
		_vp = vp;
		_key = key;
		_fileName = null;
		_value = null;
		_message = message;
		_inode = inode;
    }

    /**
     * SearchRequest ���Ф��뿷�����������֥������Ȥ��������ޤ�.
     * ������ȯ�������������Ȥʤ�
     * �׵ᥪ�֥������� (SearchRequest) ��Ϳ����ɬ�פ�����ޤ�.
     *
     * @param request �����˵��������׵ᥪ�֥������� (SearchRequest)
     * @param key B-Tree ��������
     */
    public SearchResponse(SearchRequest request, String key, INode inode) {
        this(request, null, key, inode, null);
    }

    public SearchResponse(SearchRequest request, String key, String fileName) {
        this(request, null, key, fileName, null, null);
    }

    public SearchResponse(SearchRequest request, String key) {
    	this(request, null, key, null, null);
    }
    /**
     * SearchRequest ���Ф��뿷�����������֥������Ȥ��������ޤ�.
     * ������ȯ�������������Ȥʤ�
     * �׵ᥪ�֥������� (SearchRequest) ��Ϳ����ɬ�פ�����ޤ�.
     *
     * @param request �����˵��������׵ᥪ�֥������� (SearchRequest)
     */
    public SearchResponse(SearchRequest request) {
        this(request, request.getKey());
    }

    // accessors /////////////////////////////////////////////////////////

    public VPointer getVPointer() {
        return _vp;
    }

    public String getKey() {
        return _key;
    }

    public LeafValue getValue() {
        return _value;
    }
    public INode getINode() {
    	return _inode;
    }
    public String getMessage() {
    	return _message;
    }

	@Override
	public String toString() {
		return "SearchResponse [_vp=" + _vp + ", _key=" + _key + ", _inode="
				+ _inode + ", _message=" + _message + "]";
	}



    // instance methods //////////////////////////////////////////////////


}
