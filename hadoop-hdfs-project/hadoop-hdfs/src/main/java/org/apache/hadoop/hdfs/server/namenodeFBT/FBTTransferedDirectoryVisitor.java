/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
//import org.apache.hadoop.hdfs.server.namenode.BlocksMap;
import org.apache.hadoop.hdfs.server.blockmanagement.*;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenodeFBT.msg.MessageException;
import org.apache.hadoop.hdfs.server.namenodeFBT.msg.WOLSender;
import org.apache.hadoop.hdfs.server.namenodeFBT.msg.WOLSender.TransferObjects;
import org.apache.hadoop.hdfs.server.namenodeFBT.utils.StringUtility;

/**
 * @author hanhlh
 *
 */
public class FBTTransferedDirectoryVisitor extends FBTNodeVisitor{

	private String _targetMachine;

	private String _owner;

	private ArrayList<Node> _transferedNodes;
	
	private boolean _downGear;
	
	private boolean _upGear;
	
	private int _currentGear;
	
	private int _nextGear;

	public FBTTransferedDirectoryVisitor(FBTDirectory directory) {
		super(directory);
		_targetMachine = null;
		_owner =null;
		_transferedNodes = new ArrayList<Node>();
		_downGear = NameNode.gearManager.getDownGear();
		_upGear = NameNode.gearManager.getUpGear();
	}

	public FBTTransferedDirectoryVisitor(FBTDirectory directory,
							String targetMachine) {
		super(directory);
		_targetMachine = targetMachine;
		_owner = null;
		_transferedNodes = new ArrayList<Node>();
		_downGear = NameNode.gearManager.getDownGear();
		_upGear = NameNode.gearManager.getUpGear();
	}


	public FBTTransferedDirectoryVisitor(FBTDirectory directory,
			String targetMachine, String owner) {
		super(directory);
		_targetMachine = targetMachine;
		_owner = owner;
		_transferedNodes = new ArrayList<Node>();
		_downGear = NameNode.gearManager.getDownGear();
		_upGear = NameNode.gearManager.getUpGear();
	}
	public FBTTransferedDirectoryVisitor(FBTDirectory directory,
			String targetMachine, String owner, boolean downGear, boolean upGear) {
		super(directory);
		_targetMachine = targetMachine;
		_owner = owner;
		_transferedNodes = new ArrayList<Node>();
		_downGear = downGear;
		_upGear = upGear;
	}
	
	public FBTTransferedDirectoryVisitor(FBTDirectory directory,
			String targetMachine, String owner, int currentGear, int nextGear) {
		super(directory);
		_targetMachine = targetMachine;
		_owner = owner;
		_transferedNodes = new ArrayList<Node>();
		_currentGear = currentGear;
		_nextGear = nextGear;
		_downGear = (currentGear>nextGear)?true:false;
		_upGear =  (currentGear<nextGear)?true:false;
	}
	public void setTargetMachine(String target) {
		_targetMachine = target;
	}
	public String getTargetMachine() {
		return this._targetMachine;
	}
	public List<Node> getTransferedNodes() {
		return this._transferedNodes;
	}
	
	@Override
	public void run() {
		try {
			_directory.accept(this);
			//transfer nodes of FBTDirectory structure
			if (_transferedNodes.size()!=0) {
				System.out.println("downGear, upGear: "+_downGear+", "+_upGear);
				this.send(_transferedNodes, _targetMachine, _owner, _currentGear, _nextGear);
			}
			//transfer blockMap
			//this.send(NameNodeFBT.getEndPointFBTMapping().get(_owner).
			//		blocksMap, _targetMachine, _owner);
			this.send(_directory.blocksMap, _targetMachine, _owner);
			
		} catch (NotReplicatedYetException e) {
			// TODO �����������catch ������
			e.printStackTrace();
		} catch (MessageException e) {
			// TODO �����������catch ������
			e.printStackTrace();
		} catch (IOException e) {
			// TODO �����������catch ������
			e.printStackTrace();
		}
	}

	@Override
	public void visit(MetaNode meta, VPointer self) throws MessageException,
			NotReplicatedYetException, IOException {
		StringUtility.debugSpace("FBTTransferedDirectory.visit "+meta.getNameNodeID());
		if (_upGear && !_downGear) {
			if (meta.isModified()) {
				if (meta.getNodeIdentifier().getOwner().equals(_targetMachine)) {
					_transferedNodes.add(meta);
				}
				IndexNode root = (IndexNode) _directory.getNode(meta.getRootPointer());
				root.accept(this, self);
				PointerNode pointerNode =
						(PointerNode) _directory.getNode(meta.getPointerEntry());
				pointerNode.accept(this, meta.getPointerEntry());
	
			}
		} else if (_downGear && !_upGear) {
			if (meta.isModified()) {
				//if (meta.getNodeIdentifier().getOwner().equals(_targetMachine)) {
				//_transferedNodes.add(meta);
				//}
				IndexNode root = (IndexNode) _directory.getNode(meta.getRootPointer());
				root.accept(this, self);
				PointerNode pointerNode =
						(PointerNode) _directory.getNode(meta.getPointerEntry());
				pointerNode.accept(this, meta.getPointerEntry());
	
			}
		} else {
			System.out.println("Transfer Directory error. Neither upGear nor downGear");
		}

	}

	@Override
	public void visit(IndexNode index, VPointer self) throws MessageException,
			NotReplicatedYetException, IOException {
		StringUtility.debugSpace("FBTTransferedDirectory.visit "+index.getNameNodeID()+" modified? "+
									index.isModified());
		index.key2String();
		index.toString();
		if (_upGear && !_downGear) {
			if (index.isModified() && index.getNodeIdentifier().getOwner().equals(_targetMachine)) {
				_transferedNodes.add(index);
			}
		} else if (_downGear && !_upGear) {
			if (index.isModified()) {
				_transferedNodes.add(index);
				
			}
		}
		List<VPointer> _children = index.get_children();
		for (VPointer child:_children) {
			System.out.println("going to visit "+child);
			if (_directory.isLocal(child)) {
				Node node = _directory.getNode(child);
				node.accept(this, child);
			}
		}
		//index.setModified(false);
	}

	@Override
	public void visit(LeafNode leaf, VPointer self)
			throws QuotaExceededException, MessageException,
			NotReplicatedYetException, IOException {
		StringUtility.debugSpace("FBTTransferedDirectory.visit "+leaf.getNameNodeID()+ " modified? "+
									leaf.isModified());
		leaf.toString();
		if (_upGear && !_downGear) {
			if (leaf.isModified() && leaf.getNodeIdentifier().getOwner().equals(_targetMachine)) {
				_transferedNodes.add(leaf);
				VPointer sideLink = leaf.get_RightSideLink();
				if (sideLink!=null && _directory.isLocal(sideLink)) {
					Node node = _directory.getNode(sideLink);
					node.accept(this, sideLink);
				}
			}
		} else if (_downGear && !_upGear) {
			if (leaf.isModified()) {
				_transferedNodes.add(leaf);
			}
			VPointer sideLink = leaf.get_RightSideLink();
			if (sideLink!=null && _directory.isLocal(sideLink)) {
				Node node = _directory.getNode(sideLink);
				node.accept(this, sideLink);
			}
		}
		//leaf.setModified(false);
	}

	@Override
	public void visit(PointerNode pointer, VPointer self)
			throws NotReplicatedYetException, MessageException, IOException {
		// TODO �������������純�����鴻���
		StringUtility.debugSpace("FBTTransferedDirectory.visit "+pointer.getNameNodeID());
		if (_upGear && !_downGear) {
			if (pointer.isModified() && pointer.getNodeIdentifier().getOwner().equals(_targetMachine)) {
				_transferedNodes.add(pointer);
			}
		} else if (_downGear && !_upGear) {
			if (pointer.isModified()) {
				_transferedNodes.add(pointer);
			}
		}
		//pointer.setModified(false);
	}

	/**
	 * nodes: arrays of FBT's structure node like index node, leaf node
	 * targetMachine: destination machine
	 * owner: the owner of FBT's structure
	 * */
	private void send(ArrayList<Node> nodes,
					String targetMachine, String owner,
					int currentGear, int nextGear) {
		StringUtility.debugSpace("send to "+targetMachine+" with owner "+owner);
		WOLSender sender = NameNodeFBT.getWOLSender();

		try {
			//for (Node node:nodes) {
				TransferObjects objs = sender.new TransferObjects(
								nodes.toArray(new Node[nodes.size()]), targetMachine, owner, "",
								currentGear, nextGear);
				sender.send(objs);
			//}
		} catch (InterruptedException e) {
			// TODO �����������catch ������
			e.printStackTrace();
		}
	}

	/**
	 * blockMap: blockMap
	 * targetMachine
	 * owner
	 * */
	private void send(BlocksMap blockMap, String targetMachine, String owner) {
		StringUtility.debugSpace("send blockMap to "+targetMachine);
		System.out.println("blockMap "+blockMap.getMap().values());
		WOLSender sender = NameNodeFBT.getWOLSender();

		TransferObjects obj = sender.new TransferObjects(
								blockMap, targetMachine, owner,"");
		try {
			sender.send(obj);
		} catch (InterruptedException e) {
			// TODO �����������catch ������
			e.printStackTrace();
		}
	}

}
