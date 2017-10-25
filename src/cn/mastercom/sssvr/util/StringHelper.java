package cn.mastercom.sssvr.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * 鍘绘帀涓ゅご鐨勭┖鏍�
 * @author xing
 *
 */
public class StringHelper
{

	public static String SideTrim(String srcstr, String trimstr )
	{
		if (srcstr == null || srcstr.length() == 0 || trimstr == null || trimstr.length() == 0)
		{
			return srcstr;
		}

		int epos = 0;

		String regpattern = "[" + trimstr + "]*+";
		Pattern pattern = Pattern.compile(regpattern, Pattern.CASE_INSENSITIVE);

		StringBuffer buffer = new StringBuffer(srcstr).reverse();
		Matcher matcher = pattern.matcher(buffer);
		if (matcher.lookingAt())
		{
			epos = matcher.end();
			srcstr = new StringBuffer(buffer.substring(epos)).reverse().toString();
		}

		matcher = pattern.matcher(srcstr);
		if (matcher.lookingAt())
		{
			epos = matcher.end();
			srcstr = srcstr.substring(epos);
		}

		return srcstr;
	}

	
}
