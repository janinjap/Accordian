/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.response;

import org.apache.hadoop.hdfs.server.namenodeFBT.rule.DeleteResponse;
import org.apache.hadoop.hdfs.server.namenodeFBT.rule.DumpResponse;
import org.apache.hadoop.hdfs.server.namenodeFBT.rule.InsertResponse;
import org.apache.hadoop.hdfs.server.namenodeFBT.rule.SearchResponse;


/**
 * @author hanhlh
 *
 */
public class DefaultResponseClassFactory implements ResponseClassFactory{

	//	constructors ///////////////////////////////////////////////////////////

    /**
     * ɸ��� ResponseClassFactory ���饹���������ޤ�.
     */
    public DefaultResponseClassFactory() {
        // NOP
    }
	public Class<?> createDumpResponseClass() {
		return DumpResponse.class;
	}

	public Class<?> createSearchResponseClass() {
		return SearchResponse.class;
	}

	public Class<?> createInsertResponseClass() {
		return InsertResponse.class;
	}

	public Class<?> createDeleteResponseClass() {
		return DeleteResponse.class;
	}

	public Class<?> createMigrateResponseClass() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}
	public Class<?> createSynchronizeRootResponseClass() {
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
	public Class<?> createCompleteFileResponseClass() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}
	public Class<?> createRangeSearchResponseClass() {
		// TODO ��ư�������줿�᥽�åɡ�������
		return null;
	}

}
