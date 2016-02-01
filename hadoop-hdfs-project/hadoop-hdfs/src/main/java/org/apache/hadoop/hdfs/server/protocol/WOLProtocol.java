/**
 *
 */
package org.apache.hadoop.hdfs.server.protocol;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenodeFBT.FBTDirectory;
import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * @author hanhlh
 *
 */
public interface WOLProtocol extends VersionedProtocol {


	public static final long versionID = 0L;

	public boolean transferDirectory(String targetMachine,
					Configuration conf,
					String transferBlockMode) throws IOException, ClassNotFoundException;

	public boolean transferDeferredDirectory(String targetMachine,
					String owner, Configuration conf);

	public void getTransferedDirectory(FBTDirectory directory);
}
