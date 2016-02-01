/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT;


/**
 * @author hanhlh
 *
 */
public class NullContactor implements NodeContactor {

	private static NullContactor _singleton = new NullContactor();

	public static NullContactor getInstance() {
        return _singleton;
    }
	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.NodeContactor#contact(org.apache.hadoop.hdfs.server.namenodeFBT.MetaNode)
	 */
	public void contact(MetaNode meta) {

	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.NodeContactor#contact(org.apache.hadoop.hdfs.server.namenodeFBT.IndexNode)
	 */
	public void contact(IndexNode index) {

	}

	/* (�� Javadoc)
	 * @see org.apache.hadoop.hdfs.server.namenodeFBT.NodeContactor#contact(org.apache.hadoop.hdfs.server.namenodeFBT.LeafNode)
	 */
	public void contact(LeafNode leaf) {

	}

	public void contact(PointerNode pointer) {

	}

}
