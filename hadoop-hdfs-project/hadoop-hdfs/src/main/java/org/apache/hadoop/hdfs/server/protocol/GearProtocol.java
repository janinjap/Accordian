/**
 *
 */
package org.apache.hadoop.hdfs.server.protocol;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenodeFBT.FBTDirectory;
import org.apache.hadoop.hdfs.server.namenodeFBT.FBTModifyVisitor;
import org.apache.hadoop.hdfs.server.namenodeFBT.NameNodeFBT;
import org.apache.hadoop.hdfs.server.namenodeFBT.utils.StringUtility;
import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * @author hanhlh
 *
 */
public interface GearProtocol extends VersionedProtocol {

	public static final long versionID = 0L;

	public int getCurrentGear();

	public int setCurrentGear(int gear);

	public int catchSetCurrentGear(int gear);

	public int upGear();

	public int downGear();

	public boolean modifyStructure(int oldGear, int newGear);
	public boolean catchModifyStructure(int oldGear, int newGear);

	public boolean transferDeferredNamespace(String targetMachine,
								String transferBlocksCommandIssueMode,
								String transferBlockMode);

	public boolean transferDeferredDirectory(String targetMachine, Configuration conf,
								String transferBlockMode);

	public boolean transferDeferredNamespace(String targetMachine,
								String owner
								);
	public boolean catchTransferDefferedNamespace(String targetMachine,
								String owner);

	public boolean catchTransferDefferedNamespace(String targetMachine,
			String owner, int currentGear, int nextGear);
	
	public boolean transferDeferredDirectory(String targetMachine,
								String owner,
								Configuration conf);
	public boolean setNameSpaceAccessDelay();
	public boolean catchSetNameSpaceAccessDelay();


}
