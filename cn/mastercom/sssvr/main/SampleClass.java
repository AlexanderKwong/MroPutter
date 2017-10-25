package cn.mastercom.sssvr.main;

public class SampleClass
{
	private String mergeMr;
	private int ilongitude;
	private int ilatitude;
	private int flag;// 1：表示定位成功得到的sample 0：表示定位失败得到的没有位置的sample
	private int level;// 判断是楼宇定位到、还是coverface定位到的（level=-1）
	private long mmeues1paid;

	public SampleClass(String mergeMr, int ilongitude, int ilatitude, long mmeues1paid)
	{
		this.mergeMr = mergeMr;
		this.ilongitude = ilongitude;
		this.ilatitude = ilatitude;
		this.mmeues1paid = mmeues1paid;
	}

	public long getMmeues1paid()
	{
		return mmeues1paid;
	}

	public int getLevel()
	{
		return level;
	}

	public void setLevel(int level)
	{
		this.level = level;
	}

	public String getMergeMr()
	{
		return mergeMr;
	}

	public void setMergeMr(String mergeMr)
	{
		this.mergeMr = mergeMr;
	}

	public int getIlongitude()
	{
		return ilongitude;
	}

	public void setIlongitude(int ilongitude)
	{
		this.ilongitude = ilongitude;
	}

	public int getIlatitude()
	{
		return ilatitude;
	}

	public void setIlatitude(int ilatitude)
	{
		this.ilatitude = ilatitude;
	}

	public int getFlag()
	{
		return flag;
	}

	public void setFlag(int flag)
	{
		this.flag = flag;
	}

}
