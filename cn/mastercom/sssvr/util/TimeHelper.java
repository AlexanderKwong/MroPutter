package cn.mastercom.sssvr.util;

public class TimeHelper
{
	
     public static int getRoundDayTime(int time)//s
     {
    	return (time + 8 * 3600) / 86400 * 86400 - 8 * 3600;
     }
	
	
	
}
