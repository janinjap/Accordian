/**
 *
 */
package org.apache.hadoop.hdfs.server.gear;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.ipc.ProtocolSignature;

/**
 * @author hanhlh
 *
 */
public class GearManagerStaticPartition extends GearManager{

	private TreeMap<String, FSNamesystem> _endPointNamesystemMapping;

	public GearManagerStaticPartition(String gearConfig) {
		super(gearConfig, "StaticPartition");
		// TODO 自動生成されたコンストラクター・スタブ
	}

	@Override
	public boolean modifyStructure(int oldGear, int newGear) {
		// TODO 自動生成されたメソッド・スタブ
		return false;
	}

	public void setEndPointNamesystemMapping(TreeMap<String, FSNamesystem>
												endPointNamesystemMapping) {
		this._endPointNamesystemMapping = endPointNamesystemMapping;
	}

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }
}
