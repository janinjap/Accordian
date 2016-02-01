/**
 *
 */
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * @author hanhlh
 *
 */
public interface TransferMetadataProtocol extends
									VersionedProtocol{

	public static final long versionID = 1L;
	public void send(String src, INode inode, String toMachine);
	public void receive(INode inode, String src, String fromMachine);
	public void receive(String src);

}
