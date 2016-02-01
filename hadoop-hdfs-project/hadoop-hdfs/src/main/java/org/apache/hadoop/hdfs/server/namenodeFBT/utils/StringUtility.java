/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.utils;

import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hdfs.server.namenodeFBT.FBTDirectory;



/**
 * @author hanhlh
 *
 */
public class StringUtility {

	static int PAD_LIMIT = 10;
	 public static String toMaxRadixString(int i) {
	        return Integer.toString(i, Character.MAX_RADIX);
	    }

	    public static String toMaxRadixTwoDigitString(int i) {
	        return toMaxRadixString(i, 2);
	    }

	    public static String toMaxRadixString(int i, int digit) {
	        return toString(i, digit, Character.MAX_RADIX);
	    }

	    public static String toString(int i, int digit, int radix) {
	        String buf = Integer.toString(i, radix);
	        return StringUtils.leftPad(buf, digit, '0');
	    }
	/*TODO: recoding this*/
	    public static String generateDefaultSeparator(int partitionID, int datanodeNumber,
	    		String experimentDir, int low, int high) {
	    	//16 nodes: edn9, edn11-16, edn17-26
	    	int separator;
	    	if (datanodeNumber==8) {
	    		switch (partitionID) {
	    			case 131:
	    				partitionID = 109;
	    				break;
	    			case 111:
	    				partitionID = 110;
	    				break;
	    			case 112:
	    				partitionID = 111;
	    				break;
				case 113:
					partitionID = 112;
					break;
				case 114:
					partitionID = 113;
					break;
				case 115:
					partitionID = 114;
					break;
				case 117:
					partitionID = 115;
					break;
				case 118:
					partitionID = 116;
					break;
	    		}

	    	} /*else if (datanodeNumber==16) {

	    	} else if (datanodeNumber==2) {

	    	}*/
	    	else if (datanodeNumber==4) {
	    		switch (partitionID) {
    			case 131:
    				partitionID = 109;
    				break;
    			case 111:
    				partitionID = 110;
    				break;
    			case 112:
    				partitionID = 111;
    				break;
			case 113:
				partitionID = 112;
				break;
			}
	    	}
		else if (datanodeNumber==20) {
			switch (partitionID) {
			case 111:
				partitionID = 110;
				break;
			case 112:
				partitionID = 111;
				break;
			case 113:
                                partitionID = 112;
                                break;
			case 114:
                                partitionID = 113;
                                break;
			case 115:
                                partitionID = 114;
                                break;
			case 117:
                                partitionID = 115;
                                break;
			case 118:
                                partitionID = 116;
                                break;
			case 119:
                                partitionID = 117;
                                break;
			case 132:
                                partitionID = 118;
                                break;
			case 121:
                                partitionID = 119;
                                break;
			case 123:
                                partitionID = 120;
                                break;
			case 124:
                                partitionID = 121;
                                break;
			case 125:
                                partitionID = 122;
                                break;
			case 126:
                                partitionID = 123;
                                break;
			case 127:
                                partitionID = 124;
                                break;
			case 128:
                                partitionID = 125;
                                break;
			case 129:
                                partitionID = 126;
                                break;
			case 130:
                                partitionID = 127;
                                break;
			case 131:
                                partitionID = 128;
                                break;
			}
		}
	    	else {
	    		System.out.println("wrong datanode number");
	    	}

	    	/*switch (datanodeNumber) {
	    		case 8 :
	    			System.out.println("partitionID, "+partitionID);
	    				    			//separator = low + (partitionID-109+1)*(high-low)/(datanodeNumber);

	    		case 16 :
	    			switch (partitionID) {
	    				case 111:
	    					partitionID = 110;
	    					break;
	    				case 112:
	    					partitionID = 111;
	    					break;
	    				case 113:
	    					partitionID = 112;
	    					break;
	    				case 114:
	    					partitionID = 113;
	    					break;
	    				case 115:
	    					partitionID = 114;
	    					break;
	    				case 117:
	    					partitionID = 115;
	    					break;
	    				case 118:
	    					partitionID = 116;
	    					break;
    					case 119:
	    					partitionID = 117;
	    					break;
    					case 120:
    						partitionID = 118;
    						break;
						case 121:
							partitionID = 119;
							break;
						case 122:
				    		partitionID = 120;
	    					break;
    					case 123:
	    					partitionID = 121;
	    					break;
    					case 124:
	    					partitionID = 122;
	    					break;
    					case 125:
	    					partitionID = 123;
	    					break;
    					case 126:
	    					partitionID = 124;
	    					break;
	    			}
	    		case 2: {
	    			switch (partitionID) {

	    				case 111:
	    					partitionID = 110;
	    					break;
	    			}
	    		}
*/			
		separator = low + (partitionID-109+1)*(high-low)/(datanodeNumber);
			    	/*System.out.println("separator "+separator);
			    	return experimentDir.concat(String.format("%06d", separator));*/
	    	//separator = partitionID - 109 + 2;
    		//System.out.println("new partitionID, "+partitionID);
   			//System.out.println("Separator "+experimentDir.
   			//						concat(String.format("/%02d/",separator)));

   		return experimentDir.concat(String.format("/%06d",separator));
	    }

	    public static String[] generateRange(int namenodeID, int datanodeNumber,
	    		String experimentDir, int low, int high) {
	    	String[] range = new String[2];
	    	if (namenodeID>0 && namenodeID<=datanodeNumber) {

		    	int start = low +(namenodeID-1)*(high-low)/(datanodeNumber);
		    	int end = low + (namenodeID)*(high-low)/(datanodeNumber)-1;
		    	if (namenodeID == 1) {
		    		range[0] = experimentDir;
		    	} else {
		    		range[0] = experimentDir.
		    					//concat("input/").
		    					concat(String.format("%06d", start));
		    	//System.out.println("generateRange.start "+range[0]);
		    	}

		    	if (namenodeID == datanodeNumber) {
		    		range[1] = experimentDir.concat("z"+Character.MAX_VALUE);
		    	} else {
		    		range[1] = experimentDir.
		    					//concat("input/").
		    					concat(String.format("%06d", end));
		    	}

		    	//System.out.println("generateRange.end "+range[1]);
	    	}
	    	return range;
	    }
	@SuppressWarnings("static-access")
	public static void debugSpace(String className) {
	    	System.out.println(FBTDirectory.SPACE);
	    	Calendar calendar = Calendar.getInstance();
	    	System.out.println(String.format("%04d-%02d-%02d %02d:%02d:%02d %s",
	    										calendar.get(calendar.YEAR),
	    										calendar.get(calendar.MONTH),
	    										calendar.get(calendar.DATE),
	    										calendar.get(calendar.HOUR_OF_DAY),
	    										calendar.get(calendar.MINUTE),
	    										calendar.get(calendar.SECOND),
	    										className));
	    }
	}
