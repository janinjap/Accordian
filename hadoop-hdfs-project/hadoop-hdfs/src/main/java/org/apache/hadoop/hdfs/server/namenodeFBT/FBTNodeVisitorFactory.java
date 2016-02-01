/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT;

import org.apache.hadoop.hdfs.server.namenode.NameNode;

/**
 * @author hanhlh
 *
 */
public class FBTNodeVisitorFactory implements NodeVisitorFactory {

	private final FBTDirectory _directory;

	public FBTNodeVisitorFactory(FBTDirectory directory) {
		_directory = directory;
	}
	public NodeVisitor createSearchVisitor() {
		// TODO ��ư�������줿�᥽�åɡ�������

		return null;
	}

	public NodeVisitor createInsertVisitor() {
		// TODO ��ư�������줿�᥽�åɡ�������
		NameNode.LOG.info("FBTNodeVisitorFactory create InsertVisitor");
		return (new FBTInsertVisitor(_directory));
	}
	public NodeVisitor createDeleteVisitor() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}

	public NodeVisitor createDumpVisitor() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}

	public NodeVisitor createInsertModifyVisitor() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}

	public NodeVisitor createDeleteModifyVisitor() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}

	public NodeVisitor createMigrateVisitor() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}

	public NodeVisitor createMigrateModifyVisitor() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}

	public NodeVisitor createRangeSearchVisitor() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}
	public NodeVisitor createGetAdditionalBlockVisitor() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}
	public NodeVisitor createGetBlockLocationsVisitor() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}
	public NodeVisitor createCompleteFileVisitor() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}
	@Override
	public NodeVisitor createGetBlockLocationsVisitor(FBTDirectory directory) {
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}


}
