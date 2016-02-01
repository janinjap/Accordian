/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.blink;

import org.apache.hadoop.hdfs.server.namenodeFBT.FBTDirectory;
import org.apache.hadoop.hdfs.server.namenodeFBT.NodeVisitor;
import org.apache.hadoop.hdfs.server.namenodeFBT.NodeVisitorFactory;

/**
 * @author hanhlh
 *
 */
public class FBLTLcfblNodeVisitorFactory implements NodeVisitorFactory {

	private final FBTDirectory _directory;

	public FBLTLcfblNodeVisitorFactory(FBTDirectory directory) {
        _directory = directory;
    }
	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.NodeVisitorFactory#createSearchVisitor()
	 */
	public NodeVisitor createSearchVisitor() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.NodeVisitorFactory#createInsertVisitor()
	 */
	public NodeVisitor createInsertVisitor() {
		// TODO ��ư�������줿�᥽�åɡ�������
		System.out.println("FBLTLcfblNodeVisitorFactory.createInsertLcfblVisitor");
		return new FBLTInsertLcfblVisitor(_directory);
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.NodeVisitorFactory#createDeleteVisitor()
	 */
	public NodeVisitor createDeleteVisitor() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.NodeVisitorFactory#createDumpVisitor()
	 */
	public NodeVisitor createDumpVisitor() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.NodeVisitorFactory#createInsertModifyVisitor()
	 */
	public NodeVisitor createInsertModifyVisitor() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.NodeVisitorFactory#createDeleteModifyVisitor()
	 */
	public NodeVisitor createDeleteModifyVisitor() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.NodeVisitorFactory#createMigrateVisitor()
	 */
	public NodeVisitor createMigrateVisitor() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.NodeVisitorFactory#createMigrateModifyVisitor()
	 */
	public NodeVisitor createMigrateModifyVisitor() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.NodeVisitorFactory#createRangeSearchVisitor()
	 */
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
		return null;
	}
	@Override
	public NodeVisitor createGetBlockLocationsVisitor(FBTDirectory directory) {
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}
}
