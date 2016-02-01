/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.utils;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author hanhlh
 *
 */
public class DataPlacementGenerator {

	public static int numNodes;
	public static int numGears;

	public static HashMap<Integer, ArrayList<Integer>> gearStructures;

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		ArrayList<Integer> nodeConf = new ArrayList<Integer>();
		String usage =
			"Usage: DataPlacementGenerator " +
			"  -numNodes <number of nodes> " +
			"  -numGear <number of gears> " +
			"  -nodeConf <#nodesAtGear1 #nodesAtGear2 ...> ";

		for (int i = 0; i < args.length; i++) { // parse command line
			if (args[i].equals("-numNodes")) {
				numNodes = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-numGears")) {
				numGears = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-nodeConf")) {
				int sum=0;
				for (int j=0;j<numGears;j++) {
					int numNodesTmp = Integer.parseInt(args[++i]);
					nodeConf.add(numNodesTmp);
					sum += numNodesTmp;
				}
				if (sum!=numNodes) {
					System.err.println("Invalid gear configuration.");
					System.exit(-1);
				}
				for (int j=0; i<numGears; j++) {
					if (nodeConf.get(j) % 2 !=0) {
						System.err.println("Invalid gear configuration." +
								"Each #nodes per gear should be an even number.");
						System.exit(-1);
					}
				}
			} else {
				System.out.println(usage);
				System.exit(-1);
			}
		}

		gearStructures = new HashMap<Integer, ArrayList<Integer>>();

		for (int gear= 1; gear <= numGears; gear++) {
			System.out.println("gear, "+gear);
			ArrayList<Integer> gearListIndex =  new ArrayList<Integer>();
			int previousGearNumNodes=0;
			for (int previousGear=1; previousGear<gear; previousGear++) {
				previousGearNumNodes+=nodeConf.get(previousGear);
			}
			System.out.println("previousGearNumNodes, "+previousGearNumNodes);
			int numNodesAtThisGear = nodeConf.get(gear-1);
			System.out.println("numNodesAtThisGear, "+numNodesAtThisGear);
			//left
			for (int index=0; index<numNodesAtThisGear/2; index++){
				System.out.println("add "+(numNodes/2 - (numNodesAtThisGear-1))+index);
				gearListIndex.add((numNodes/2 - (numNodesAtThisGear-1))+index);
			}
			//right
			for (int index=numNodesAtThisGear/2; index<numNodesAtThisGear; index++) {
				System.out.println("add "+(numNodes/2 + (numNodesAtThisGear-1)) +index+
						previousGearNumNodes);
				gearListIndex.add((numNodes/2 + (numNodesAtThisGear-1)) +index+
							previousGearNumNodes);
			}
			gearStructures.put(gear, gearListIndex);
		}

		System.out.println("Results:");
		for(int gear=0; gear<numGears; gear++) {
			System.out.println("Nodes at Gear "+gear);
			System.out.println(gearStructures.get(gear).toString());
		}
	}

}
