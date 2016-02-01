/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenodeFBT.msg.MessageException;
import org.apache.hadoop.hdfs.server.namenodeFBT.utils.StringUtility;


/**
 * @author hanhlh
 *
 */
public class LeafNode extends AbstractNode implements Serializable{

	/**
	 *
	 */
	private static final long serialVersionUID = -2218464919419288812L;
	private String _nodeType;
	//instances
	/**
	 * List of Directories' paths. Key have to be normalized.
	 * Ex: /home/user/, /home/root/, /, /etc/
	 *
	 * */
	protected List<String> _keys;

	protected List<Integer> _accessCounts;

	protected List<INode> _iNodes;

    protected String _highKey;

    protected List<Boolean> _shouldTransfer;

    private NodeIdentifier _nodeIdentifier;

    /**
     * 鐃緒申(鐃緒申鐃緒申鐃粛わ申鐃緒申)鐃塾ペ￥申鐃緒申鐃舜のワ申鐃緒申鐃宿ワ申鐃�
     */
    protected Pointer _rightSideLink;
    protected Pointer _leftSideLink;
    protected boolean _isDummy;
    protected boolean _deleteBit;

    //constructor
	public LeafNode(FBTDirectory directory) {
		super(directory);
		_nodeType = "leaf_";
		_nodeIdentifier = new NodeIdentifier(directory.getOwner(),
				directory.getNodeSequence().getAndIncrement());
		_keys = new ArrayList<String>(_directory.getLeafFanout()+1);
		_iNodes = new ArrayList<INode>(_directory.getLeafFanout()+1);
		_accessCounts = new ArrayList<Integer> (_directory.getLeafFanout() + 1);
		_shouldTransfer = new ArrayList<Boolean> (_directory.getLeafFanout() + 1);
        _highKey = null;
        _rightSideLink = null;
        _leftSideLink = null;
        _isDummy = false;
        _deleteBit = false;
        _directory.decrementEntryCount();

	}


	//key = directoryPath
	//fileName = fileName
	//src={key+fileName}
	// Example: src = /home/hanhlh/fbt.txt
	// key=/home/hanhlh, fileName=fbt.txt
	public LeafNode(FBTDirectory directory, String key) {
		this(directory);
		_keys = new ArrayList<String>(_directory.getLeafFanout()+1);
		_iNodes = new ArrayList<INode>(_directory.getLeafFanout()+1);
		_accessCounts = new ArrayList<Integer>(_directory.getLeafFanout() + 1);
		_shouldTransfer = new ArrayList<Boolean>(_directory.getLeafFanout() + 1);
		_keys.add(key);
		/*INode inode = new INode(key);
		_iNodes.add(inode);*/
		_accessCounts.add(new Integer(0));
		_highKey = null;
        _rightSideLink = null;
        _leftSideLink = null;
        _isDummy = false;
        _deleteBit = false;
	}

	public LeafNode(FBTDirectory directory, String key, INode inode) {
		this(directory);
		_keys = new ArrayList<String>(_directory.getLeafFanout()+1);
		//_values = new ArrayList<LeafValue>(_directory.getMaxFilePerDirectory());
		_accessCounts = new ArrayList<Integer>(_directory.getLeafFanout() + 1);
		_shouldTransfer = new ArrayList<Boolean>(_directory.getLeafFanout() + 1);
		_keys.add(key);
		//_values.add(leafValue);
		_iNodes = new ArrayList<INode>(_directory.getLeafFanout()+1);
		_iNodes.add(inode);
		_accessCounts.add(new Integer(0));
		_highKey = null;
		_rightSideLink = null;
		_leftSideLink = null;
		_isDummy = false;
		_deleteBit = false;
	}

		// instance methods ///////////////////////////////////////////////////////


	public List<String> get_keys() {
		return _keys;
	}

	public List<Boolean> get_shouldTransfer() {
		return _shouldTransfer;
	}

	@Override
	public String toString() {
		return "LeafNode [_nodeType=" + _nodeType + ", _keys=" + _keys
				+ ", _accessCounts=" + _accessCounts
				+ ", _highKey=" + _highKey + ", _nodeIdentifier="
				+ _nodeIdentifier + ", _leftSideLink=" + _leftSideLink +
				", _rightSideLink = "+ _rightSideLink+ ", _isDummy="
				+ _isDummy + ", _deleteBit=" + _deleteBit + "]";
	}
	public NodeIdentifier getNodeIdentifier() {
		return _nodeIdentifier;
	}
	public boolean get_isDummy() {
		return _isDummy;
	}

	public void set_isDummy(boolean _isDummy) {
		this._isDummy = _isDummy;
	}

	public boolean get_deleteBit() {
		return _deleteBit;
	}

	public void set_deleteBit(boolean _deleteBit) {
		this._deleteBit = _deleteBit;
	}

	public Pointer get_RightSideLink() {
		return _rightSideLink;
	}

	public void set_RightSideLink(Pointer _sideLink) {
		this._rightSideLink = _sideLink;
	}

	public Pointer get_LeftSideLink() {
		return _leftSideLink;
	}

	public void set_LeftsideLink(Pointer _sideLink) {
		this._leftSideLink = _sideLink;
	}
	public String get_highKey() {
		return _highKey;
	}

	public void set_highKey(String _highKey) {
		this._highKey = _highKey;
	}

	public String getNodeNameID() {
		return getNodeType() + getNodeIdentifier().toString();
	}

	public String getNodeType(){
		return _nodeType;
	}
	public void accept(NodeVisitor visitor, VPointer self) throws
						MessageException, NotReplicatedYetException, IOException {
		try {
			visitor.visit(this, self);
		} catch (QuotaExceededException e) {
			e.printStackTrace();
		}
	}

	public String getKey() {
		if (_keys.size() > 0) {
			return (String) _keys.get(0);
		} else {
			return null;
		}
	}

	public void addLeafValue(int keyPosition, String key, LeafValue leafValue) {
		if (_directory.getAccessCountFlg()) {
            _directory.incrementAccessCount();
            //_accessCounts.add(keyPosition, new Integer(1));
        } else {
            //_accessCounts.add(keyPosition, new Integer(0));
        }
		_keys.add(keyPosition, key);
		//System.out.println("LeafNode.addLeafValue done at "+ getNodeNameID());
		/*for (int i=0;i<_keys.size();i++) {
			////System.out.println("(key, value)["+i+"]: " +
					"("+_keys.get(i)+", "+_values.get(i)+"]");
		}*/

	}

	public void addINode(int position, String key, INode inode) {
		StringUtility.debugSpace("LeafNode.addINode "+key
							+" at "+ getNodeNameID()
							+" position "+position);
		if (_directory.getAccessCountFlg()) {
            _directory.incrementAccessCount();
            //_accessCounts.add(keyPosition, new Integer(1));
        } else {
            //_accessCounts.add(keyPosition, new Integer(0));
        }
		/*System.out.println("beforeAdd");
		for (int i = 0; i<_keys.size(); i++) {
        	System.out.println(getNodeNameID()+"***key["+i+"]: "+_keys.get(i));
        }*/
		_keys.add(position, key);
		_iNodes.add(position, inode);
		_shouldTransfer.add(position, false);
		setModified(true);
		System.out.println("addINode setModified "+isModified());
		//System.out.println("afterAdd");
		/*for (int i = 0; i<_keys.size(); i++) {
        	System.out.println(getNodeNameID()+"***key["+i+"]: "+_keys.get(i));
        }*/
	}

	public void replaceINode(int position, String key, INode inode) {
		//StringUtility.debugSpace("LeafNode.replaceINode, "+key+","+position);
		synchronized(_keys) {
			synchronized(_iNodes) {
				_keys.set(position, key);
				_iNodes.set(position, inode);
				_shouldTransfer.set(position, false);
				setModified(true);
			}
		}
	}

	public void replaceINode(String key, INode inode) {
		//StringUtility.debugSpace("LeafNode.replaceINode, "+key);
		synchronized (_keys) {
			synchronized (_iNodes) {
/*			for (int i = 0; i<_keys.size(); i++) {
				System.out.println(getNodeNameID()+"***key["+i+"]: "+_keys.get(i));
			}
*/			int pos = this.binaryLocate(key);
			inode.setLocalName(key.substring(
									key.lastIndexOf("/")+1));
			_keys.set(pos-1, key);
			_iNodes.set(pos-1, inode);
			_shouldTransfer.set(pos-1, false);
			setModified(true);
/*			System.out.println("afterReplace");
			for (int i = 0; i<_keys.size(); i++) {
				System.out.println(getNodeNameID()+"***key["+i+"]: "+_keys.get(i));
			}
*/			}
		}
	}

	public synchronized INode removeINode(int position) {
		StringUtility.debugSpace("LeafNode.removeINode, position "+position);
		_keys.remove(position);
		_shouldTransfer.remove(position);
		setModified(true);
		return _iNodes.remove(position);
	}
	public INode getINode(int position) {
		return _iNodes.get(position);
	}

	public int binaryLocate(String key) {

        int bottom = 0;
        int top = _keys.size() - 1;
        int middle = 0;
        while (bottom <= top) {
           middle = (bottom + top + 1) / 2;
           int difference = key.compareTo((String) _keys.get(middle));

           if (difference < 0) {
               top = middle - 1;
           } else if (difference == 0) {
               return middle + 1;
           } else {
               bottom = middle + 1;
           }
        }

        return -bottom;
    }


	public String getKey(int pos) {
		return _keys.get(pos);
	}

	public boolean getShouldTransfer(int pos) {
		return (_shouldTransfer.get(pos)==null)? false : _shouldTransfer.get(pos);
	}

	public void setShouldTransfer(int pos, boolean shouldTransfer) {
		_shouldTransfer.set(pos, shouldTransfer);
	}
	public boolean isFullLeafEntriesPerNode() {
		//return _values.size() >= _directory.getLeafFanout();
		return _iNodes.size() >= _directory.getLeafFanout();
	}

	public boolean isFullLeafEntry(int keyPosition) {
		//return _values.get(keyPosition).getLeafEntries().size()
		//						>= _directory.getMaxFilePerDirectory();
		return true;
	}

	public boolean isOverLeafEntriesPerNode() {
		return _iNodes.size() > _directory.getLeafFanout();
	}

	public boolean isOverLeafEntry(int keyPosition) {
		//return _values.get(keyPosition).getLeafEntries().size()
		//						> _directory.getMaxFilePerDirectory();
		return true;
	}

	/**
     * 鐃緒申鐃緒申 LeafNode 鐃緒申分鐃巡し鐃殉わ申鐃緒申
     * @return 鐃緒申鐃塾ノ￥申鐃宿の縁申半分鐃緒申
     */
    public LeafNode split() {
        //StringUtility.debugSpace("Leaf Node splits ");
    	int half  = (_directory.getLeafFanout() + 1) / 2;
    	//System.out.println("half "+half);
        List<String> klist = _keys.subList(half, _keys.size());
        List<INode> elist = _iNodes.subList(half, _iNodes.size());
        List<Boolean> shouldTransferList = _shouldTransfer.subList(half, _shouldTransfer.size());

        LeafNode newLeaf = new LeafNode(_directory);
        synchronized (_directory.getLocalNodeMapping()) {
        	_directory.getLocalNodeMapping().put(newLeaf.
        						getNodeIdentifier().toString(), newLeaf);
        }
        //System.out.println("Create Leaf node "+newLeaf.getNodeNameID());
        newLeaf._keys.addAll(klist);
        newLeaf._iNodes.addAll(elist);
        newLeaf._shouldTransfer.addAll(shouldTransferList);
        newLeaf.setModified(isModified());
        newLeaf._highKey = _highKey;
        newLeaf._rightSideLink = _rightSideLink;
        newLeaf._leftSideLink = getPointer().getPointer();

        klist.clear();
        elist.clear();
        _highKey = newLeaf.getKey(0);
        _rightSideLink = newLeaf.getPointer().getPointer();
        //System.out.println("_highkey "+_highKey);
        //System.out.println("_sideLink "+_sideLink.toString());
        //System.out.println("Curren keys at new leafNode "+newLeaf.getNodeNameID());
        /*for (int i = 0; i<newLeaf._keys.size(); i++) {
        	System.out.println(newLeaf.getNodeNameID()+"***key["+i+"]: "+newLeaf._keys.get(i));
        }
        System.out.println("Current keys at leaf "+getNodeNameID());
        for (int i = 0; i<_keys.size(); i++) {
        	System.out.println(this.getNodeNameID()+"***key["+i+"]: "+_keys.get(i));
        }*/
        setModified(true);
        return newLeaf;
    }

    public int size() {
        //return _values.size();
    	return _iNodes.size();
    }

    public void removeAll() {
        _keys.clear();
        //_values.clear();
        _iNodes.clear();
    }

    public List<INode> getINodes() {
    	return _iNodes;
    }

    public String getNameNodeID() {
    	return getNodeType()+getNodeIdentifier().toString();
    }


}
