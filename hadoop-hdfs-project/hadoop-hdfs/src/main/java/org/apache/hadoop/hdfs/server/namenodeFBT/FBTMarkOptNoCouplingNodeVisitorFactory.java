/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT;

import java.io.Serializable;

import org.apache.hadoop.hdfs.server.namenodeFBT.incopt.FBTDeleteIncOptVisitor;
import org.apache.hadoop.hdfs.server.namenodeFBT.incopt.FBTDeleteModifyIncOptVisitor;




/**
 * @author hanhlh
 *
 */
public class FBTMarkOptNoCouplingNodeVisitorFactory implements
		NodeVisitorFactory, Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private final FBTDirectory _directory;

	public FBTMarkOptNoCouplingNodeVisitorFactory(FBTDirectory directory) {
        _directory = directory;
    }
	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.NodeVisitorFactory#createSearchVisitor()
	 */
	public NodeVisitor createSearchVisitor() {
		return new FBTSearchNoCouplingVisitor(_directory);
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.NodeVisitorFactory#createInsertVisitor()
	 */
	public NodeVisitor createInsertVisitor() {
		return new FBTInsertMarkOptNoCouplingVisitor(_directory);
	}
	public NodeVisitor createInsertModifyVisitor() {
        return new FBTInsertModifyMarkOptVisitor(_directory);
    }
	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.NodeVisitorFactory#createDeleteVisitor()
	 */
	public NodeVisitor createDeleteVisitor() {
		return new FBTDeleteIncOptVisitor(_directory);
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.NodeVisitorFactory#createDumpVisitor()
	 */
	public NodeVisitor createDumpVisitor() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}


	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.NodeVisitorFactory#createDeleteModifyVisitor()
	 */
	public NodeVisitor createDeleteModifyVisitor() {
		return new FBTDeleteModifyIncOptVisitor(_directory);
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
		return new FBTRangeSearchBayerVisitor(_directory);
	}
	public NodeVisitor createGetAdditionalBlockVisitor() {
		return new FBTGetAdditionalBlockNoCouplingVisitor(_directory);
	}
	public NodeVisitor createGetBlockLocationsVisitor() {
		return new FBTGetBlockLocationsNoCouplingVisitor(_directory);
	}
	public NodeVisitor createCompleteFileVisitor() {
		return new FBTCompleteFileNoCouplingVisitor(_directory);
	}

	public NodeVisitor createGetBlockLocationsVisitor(FBTDirectory directory) {
		return new FBTGetBlockLocationsNoCouplingVisitor(directory);
	}

}
