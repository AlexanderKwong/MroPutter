package cn.mastercom.sssvr.util;

public class RandomNum
{
	/**
	 * +- 0.0002,num 填写任意值
	 * 
	 * @param num
	 * @return
	 */
	public static double returnRandom(double num)
	{
		double random = Math.random() * num * (-1) + Math.random() * num;
		return random;
	}
}
