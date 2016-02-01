/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.blink.rule;


import org.apache.hadoop.hdfs.server.namenodeFBT.response.ResponseClassFactory;
import org.apache.hadoop.hdfs.server.namenodeFBT.rule.FBTInsertMarkOptResponse;
import org.apache.hadoop.hdfs.server.namenodeFBT.rule.SynchronizeRootResponse;

/**
 * @author hanhlh
 *
 */
public class FBLTBLinkResponseClassFactory implements ResponseClassFactory {

	public FBLTBLinkResponseClassFactory() {
        // NOP
    }
	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.response.ResponseClassFactory#createDumpResponseClass()
	 */
	public Class<?> createDumpResponseClass() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.response.ResponseClassFactory#createSearchResponseClass()
	 */
	public Class<?> createSearchResponseClass() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.response.ResponseClassFactory#createInsertResponseClass()
	 */
	public Class<?> createInsertResponseClass() {
		return FBTInsertMarkOptResponse.class;
	}
	public Class createInsertResponseClass2() {
        return FBLTInsertModifyResponse.class;
    }

	public Class<?> createSynchronizeRootResponseClass() {
		return SynchronizeRootResponse.class;
	}
	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.response.ResponseClassFactory#createDeleteResponseClass()
	 */
	public Class<?> createDeleteResponseClass() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.response.ResponseClassFactory#createMigrateResponseClass()
	 */
	public Class<?> createMigrateResponseClass() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}
	public Class<?> createGetAdditionalBlockResponseClass() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}
	public Class<?> createGetBlockLocationsResponseClass() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}
	public Class<?> createRangeSearchResponseClass() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}
	public Class<?> createCompleteFileResponseClass() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}

}
