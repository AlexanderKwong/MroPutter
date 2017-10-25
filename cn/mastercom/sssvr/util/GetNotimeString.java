package cn.mastercom.sssvr.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class GetNotimeString
{
	public static String returnTimeString()
	{
		return new SimpleDateFormat("yyyy-MM-dd hh:mm:ss ").format(new Date());
	}

}
