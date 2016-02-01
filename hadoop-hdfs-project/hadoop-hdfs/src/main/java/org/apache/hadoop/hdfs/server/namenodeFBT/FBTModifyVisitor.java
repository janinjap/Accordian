/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenodeFBT.msg.MessageException;
import org.apache.hadoop.hdfs.server.namenodeFBT.utils.StringUtility;

/**
 * @author hanhlh
 *
 */
public class FBTModifyVisitor extends FBTNodeVisitor {

	private ArrayList<String> _activateNodes;
	private int _previousGear;
	private int _nextGear;
	private boolean _upGear;
	private boolean _downGear;
	private ConcurrentHashMap<String, String[]> _backUpMapping;
	private boolean _nonActivateNode;

	public FBTModifyVisitor(FBTDirectory directory) {
		super(directory);
		_previousGear = 1;
		_activateNodes = new ArrayList<String>();
		_nextGear = 0;
		_upGear = false;
		_downGear = true;
		_backUpMapping = null;
		_nonActivateNode = false;

	}
	public FBTModifyVisitor(FBTDirectory directory,
			int oldGear, int newGear,
			ConcurrentHashMap<String, String[]> backUpMapping) {
		//TODO initialize the activateNodes
		super(directory);
		_previousGear = oldGear;
		_nextGear = newGear;
		_upGear = (_previousGear<_nextGear)? true:false;
		_downGear = (_previousGear>_nextGear)? true:false;
		_backUpMapping = backUpMapping;
		_activateNodes = new ArrayList<String>();
		_nonActivateNode = false; //implemented at activateNodes
	}

	public FBTModifyVisitor(FBTDirectory directory,
			int oldGear, int newGear,
			ConcurrentHashMap<String, String[]> backUpMapping,
			boolean nonactivateNode) {
		//TODO initialize the activateNodes
		super(directory);
		_previousGear = oldGear;
		_nextGear = newGear;
		_upGear = (_previousGear<_nextGear)? true:false;
		_downGear = (_previousGear>_nextGear)? true:false;
		_backUpMapping = backUpMapping;
		_activateNodes = new ArrayList<String>();
		_nonActivateNode = nonactivateNode; //implemented at nonActiveNode
	}


	public ArrayList<String> getActivateNodes() {
		return _activateNodes;
	}

	public int getPreviousGear() {
		return _previousGear;
	}

	public void setActivateNodes(ArrayList<String> activateNodes) {
		_activateNodes = activateNodes;
	}

	public void setPreviousGear(int previousGear) {
		_previousGear = previousGear;
	}
	@Override
	public void run() {
		try {
			_directory.accept(this);
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
		StringUtility.debugSpace("FBTModifyVisitor.visit "+meta.getNameNodeID());
		System.out.println(meta);
		IndexNode root = (IndexNode) _directory.getNode(meta.getRootPointer());
		root.accept(this, self);
		PointerNode pointerNode =
				(PointerNode) _directory.getNode(meta.getPointerEntry());
		pointerNode.accept(this, meta.getPointerEntry());
	}

	@Override
	public void visit(IndexNode index, VPointer self) throws MessageException,
			NotReplicatedYetException, IOException {
		// TODO �������������純�����鴻���
		StringUtility.debugSpace("FBTModifyVisitor.visit "+index.getNameNodeID());
		//System.out.println(index);
		List<VPointer> _children = index.get_children();
		List<VPointer> _pointers = index.get_pointers();
		for (VPointer vp : _pointers) {
			System.out.println("old pointer: "+vp.toString());
		}
		int position=0;
		if (!_nonActivateNode) {
			if (_downGear) {	
				for (VPointer child : _children) {
					System.out.println("old child: "+child.toString());
					String fbtOwner = child.getPointer().getFBTOwner();
					child = modifyPointer(child, fbtOwner, _activateNodes);
					if (child!=null) {
						System.out.println("replace by: "+child.toString());
						index.replaceEntry(position, child);
					}
					position++;
				}
				System.out.println();
				/*for (VPointer pointer : _pointers) {
					System.out.println("old: "+pointer.toString());
					if (_directory.isLocalDirectory(pointer)) {
						pointer.getPointer().setPartitionID(_directory.getSelfPartID());
						System.out.println("new: "+pointer.toString());
					}
	
				}
				index.setPointers(_pointers);*/
				for (VPointer child : _children) {
					System.out.println("new child: "+child.toString());
				}
				System.out.println();
				if (index.getSideLink()!=null) {
					System.out.println("sideLink "+index.getSideLink());
					VPointer newSideLink = modifyPointer(index.getSideLink(),
										index.getSideLink().getPointer().getFBTOwner(),
										getActivateNodes());
					if(newSideLink!=null) {
						System.out.println("new sideLink "+newSideLink);
					}
					index.set_sideLink(newSideLink);
				}
				System.out.println();
			} //else if (_previousGear < _nextGear) { //upGear
				else if (_upGear) {
				//1 = low gear
				for (VPointer child : _children) {
					System.out.println("old child: "+child.toString());
					String fbtOwner = child.getPointer().getFBTOwner();
					child = modifyPointer(child, fbtOwner, _activateNodes);
					System.out.println("new child: "+child.toString());
					if (child!=null) {
						index.replaceEntry(position, child);
					}
					//System.out.println("position: "+position);
					position++;
				}
				for (VPointer pointer : _pointers) {
					if (_directory.isLocalDirectory(pointer)) {
						pointer.getPointer().setPartitionID(_directory.getSelfPartID());
					}
				}
				index.setPointers(_pointers);
				if (index.getSideLink()!=null) {
					VPointer newSideLink = modifyPointer(index.getSideLink(),
										index.getSideLink().getPointer().getFBTOwner(),
										getActivateNodes());
	
					index.set_sideLink(newSideLink);
				}
	
				//System.out.println("modify index done ");
				//System.out.println(index);
	
				for (VPointer child : index.get_children()) {
					if (_directory.isLocal(child)) {
						Node node = _directory.getNode(child);
						node.accept(this, child);
					}
				}
			} else if (_nonActivateNode) {
				System.out.println("implemented at nonactivateNode "+_nonActivateNode);
				if (_downGear) {	
					for (VPointer child : _children) {
						System.out.println("old child: "+child.toString());
						String fbtOwner = child.getPointer().getFBTOwner();
						child = modifyPointer(child, fbtOwner, _activateNodes);
						if (child!=null) {
							System.out.println("replace by: "+child.toString());
							index.replaceEntry(position, child);
						}
						position++;
					}
					System.out.println();
					/*for (VPointer pointer : _pointers) {
						System.out.println("old: "+pointer.toString());
						if (_directory.isLocalDirectory(pointer)) {
							pointer.getPointer().setPartitionID(_directory.getSelfPartID());
							System.out.println("new: "+pointer.toString());
						}
		
					}
					index.setPointers(_pointers);*/
					for (VPointer child : _children) {
						System.out.println("new child: "+child.toString());
					}
					System.out.println();
					if (index.getSideLink()!=null) {
						System.out.println("sideLink "+index.getSideLink());
						VPointer newSideLink = modifyPointer(index.getSideLink(),
											index.getSideLink().getPointer().getFBTOwner(),
											getActivateNodes());
						if(newSideLink!=null) {
							System.out.println("new sideLink "+newSideLink);
						}
						index.set_sideLink(newSideLink);
					}
					System.out.println();
				} else if (_upGear) {
					for (VPointer child : _children) {
						System.out.println("old child: "+child.toString());
						String fbtOwner = child.getPointer().getFBTOwner();
						child = modifyPointer(child, fbtOwner, _activateNodes);
						System.out.println("new child: "+child.toString());
						if (child!=null) {
							index.replaceEntry(position, child);
						}
						//System.out.println("position: "+position);
						position++;
					}
					for (VPointer pointer : _pointers) {
						if (_directory.isLocalDirectory(pointer)) {
							pointer.getPointer().setPartitionID(_directory.getSelfPartID());
						}
					}
					index.setPointers(_pointers);
					if (index.getSideLink()!=null) {
						VPointer newSideLink = modifyPointer(index.getSideLink(),
											index.getSideLink().getPointer().getFBTOwner(),
											getActivateNodes());
		
						index.set_sideLink(newSideLink);
					}
		
					//System.out.println("modify index done ");
					//System.out.println(index);
		
					for (VPointer child : index.get_children()) {
						if (_directory.isLocal(child)) {
							Node node = _directory.getNode(child);
							node.accept(this, child);
						}
					}
				}
			}
		}
		System.out.println(index);
			
	}

	@Override
	public void visit(LeafNode leaf, VPointer self)
			throws QuotaExceededException, MessageException,
			NotReplicatedYetException, IOException {
		//StringUtility.debugSpace("FBTModifyVisitor.visit "+leaf.getNodeNameID());
		//System.out.println(leaf);
	}

	@Override
	public void visit(PointerNode pointer, VPointer self)
			throws NotReplicatedYetException, MessageException, IOException {
		//StringUtility.debugSpace("FBTModifyVisitor.visit "+pointer.getNameNodeID());
		//System.out.println(pointer);
		if (!_nonActivateNode) {
			if (_downGear) {
				System.out.println("pointer "+pointer);
				PointerSet entries = pointer.getEntry();
				for (Iterator iter=(Iterator) entries.iterator(); iter.hasNext();) {
					VPointer vp = (VPointer) iter.next();
	
					vp = modifyPointerNode(vp,
										vp.getPointer().getFBTOwner(),
										getActivateNodes());
				}
				//pointer.setEntry(entries);
				//System.out.println("after modified pointer"+ pointer);
				System.out.println("pointer after modified: "+pointer);
	
			} //else if (_previousGear < _nextGear) { //upGear
			  else if (_upGear) {
				PointerSet entries = pointer.getEntry();
				for (Iterator iter=(Iterator) entries.iterator(); iter.hasNext();) {
					VPointer vp = (VPointer) iter.next();
					vp = modifyPointerNode(vp,
										vp.getPointer().getFBTOwner(),
										getActivateNodes());
				}
				pointer.setEntry(entries);
				System.out.println("after modified pointer"+ pointer);
			}
		} else if (_nonActivateNode) {
			System.out.println("implemented at nonactivateNode "+_nonActivateNode);
		}
	}

	public VPointer modifyPointer(VPointer target, String fbtOwner,
			ArrayList<String> activateNodes) {
		if (!_nonActivateNode) {
			if (_downGear) {
				if (!activateNodes.contains(fbtOwner)) {
						String[] backUp = _backUpMapping.get(fbtOwner);
						System.out.println("backUp :"+Arrays.toString(backUp));
						String backUpNode = backUp[backUp.length-1-_nextGear]; //next replica
						target.getPointer().setPartitionID(100+Integer.parseInt(
											backUpNode.substring(3)));
						return target;
					}
	
			} //else if (_previousGear < _nextGear) { //upGear
			  else if (_upGear) {
				//if (!activateNodes.contains(fbtOwner)) {
					target.getPointer().setPartitionID(100+
						Integer.parseInt(fbtOwner.substring(
									3)));
					return target;
				//}
			}
		} else if (_nonActivateNode) {
			System.out.println("implemented at nonactivateNode "+_nonActivateNode);
			if (_downGear) {
				if (!activateNodes.contains(fbtOwner)) {
						String[] backUp = _backUpMapping.get(fbtOwner);
						System.out.println("backUp :"+Arrays.toString(backUp));
						String backUpNode = backUp[backUp.length-1-_nextGear]; //next replica
						target.getPointer().setPartitionID(100+Integer.parseInt(
											backUpNode.substring(3)));
						return target;
					}
			} else if (_upGear) {
				//if (!activateNodes.contains(fbtOwner)) {
					target.getPointer().setPartitionID(100+
						Integer.parseInt(fbtOwner.substring(
									3)));
					return target;
				//}
			}
		}
		return null;
	}

	public VPointer modifyPointerNode(VPointer target, String fbtOwner,
			ArrayList<String> activateNodes) {
		//if (_previousGear > _nextGear) {  //downGear
		if (_downGear) {
			if (!activateNodes.contains(fbtOwner)) {
				String[] backUpNodes = this._backUpMapping.get(fbtOwner);
				String backUp = backUpNodes[backUpNodes.length-1-_nextGear];
				target.getPointer().setPartitionID(100+
						Integer.parseInt(backUp.substring(3)));
			}
		} //else if (_previousGear < _nextGear) { //upGear
		else if (_upGear) {
			if (!activateNodes.contains(fbtOwner)) {
				target.getPointer().setPartitionID(100+
					Integer.parseInt(fbtOwner.substring(
							3)));
			}
		}
	return target;
	}

}
