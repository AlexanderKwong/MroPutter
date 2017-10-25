package cn.mastercom.sssvr.util;

public class Func
{
    public static boolean checkFreqIsLtDx(int freq)
    {
    	if(freq < 30000 || freq == 40340)
    	{
    		return true;
    	}
    	return false;
    }
	
	
	
}
