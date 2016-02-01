/**
 *
 */
package org.apache.hadoop.hdfs.server.namenodeFBT.utils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.hdfs.server.namenodeFBT.msg.EndPoint;

/**
 * @author hanhlh
 *
 */

public class PropertyArranger {

	/**
	 * ���� str = "{1-6,128-8,144}" ���Ф��ơ�[1, 2, ..., 6, 128, ..., 8, 144]���֤���
	 * �ޤ������� str = "adisk{1-160}" ���Ф��Ƥϡ�[adisk1, ..., adisk160]���֤���
	 * ���� str �� '{'��'}' �ǰϤ��Ƥ��ʤ�ʸ����ξ�硤
	 * ����ʸ����Τߤ�ޤ�ꥹ�Ȥ��֤���
	 *
	 * @param str "{1-6,128-8,144}" �ޤ��� "adisk{1-160}"
	 *          �Τ褦��Ϣ�ַ����Υꥹ�Ȳ������ʸ����
	 * @return �嵭�ε�§�˽��ä����󤵤줿�ꥹ��
	 */

	public static List<String> arrangeSerialName(String str) {
        List<String> list = new ArrayList<String>();

        if (str.indexOf('{') == -1) {
            list.add(str);
        } else {
            String coStr = substringBefore(str, "{");
            String eachStr = substringBetween(str, "{", "}");

            StringTokenizer stComma = new StringTokenizer(eachStr, ",");
            while (stComma.hasMoreTokens()) {
                String token = stComma.nextToken().trim();
                int sepPos = token.indexOf('-');
                if (sepPos == -1) {
                    list.add(coStr + token);
                } else {
                    String pre = token.substring(0, sepPos).trim();
                    String post = token.substring(sepPos + 1).trim();
                    int first = Integer.parseInt(pre);
                    int last = Integer.parseInt(post);
                    if (first <= last) {
                        for (int i = first; i <= last; i++) {
                            list.add(coStr + String.valueOf(i));
                        }
                    } else {
                        for (int i = first; i >= last; i--) {
                            list.add(coStr + String.valueOf(i));
                        }
                    }
                }
            }
        }

        return list;
    }

	public static String substringBefore(String str, String separator)
    {
        if(str == null || separator == null || str.length() == 0)
            return str;
        if(separator.length() == 0)
            return "";
        int pos = str.indexOf(separator);
        if(pos == -1)
            return str;
        else
            return str.substring(0, pos);
    }

	public static String substringBetween(String str, String start, String end)
    {
        if(str == null || start == null || end == null || str.length() == 0)
            return str;
        if(start.length() == 0)
            return "";
        if(end.length() == 0)
            return "";
        int startPos = str.indexOf(start);
        int endPos = str.indexOf(end);

        if(startPos == -1)
            return str;
        else if(endPos == -1)
            return str;
        else
            return str.substring(startPos+1, endPos);
    }
    public static List<String> getPartitions(List<EndPoint> endpoints) {
        List<String> partitions = new ArrayList<String>();

        Iterator<EndPoint> eIter = endpoints.iterator();
        while (eIter.hasNext()) {
            EndPoint ep = eIter.next();
            int diskID = 0xff & ep.getInetAddress().getAddress()[3];
            partitions.add(String.valueOf(diskID));
        }

        return partitions;
    }

}
