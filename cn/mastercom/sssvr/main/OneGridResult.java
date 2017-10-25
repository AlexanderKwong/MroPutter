package cn.mastercom.sssvr.main;

import java.util.ArrayList;

public class OneGridResult
{
	private ArrayList<Double> oneresult;
	private int level;
	private int ilongitude;
	private int ilatitude;
	private int buildingId;

	public OneGridResult(ArrayList<Double> oneresult, int level, int ilongitude, int ilatitude, int buildingId)
	{
		this.oneresult = oneresult;
		this.level = level;
		this.ilongitude = ilongitude;
		this.ilatitude = ilatitude;
		this.buildingId = buildingId;
	}

	public int getBuildingId()
	{
		return buildingId;
	}

	public void setBuildingId(int buildingId)
	{
		this.buildingId = buildingId;
	}

	public ArrayList<Double> getOneresult()
	{
		return oneresult;
	}

	public int getLevel()
	{
		return level;
	}

	public int getIlongitude()
	{
		return ilongitude;
	}

	public int getIlatitude()
	{
		return ilatitude;
	}

	public void setOneresult(ArrayList<Double> oneresult)
	{
		this.oneresult = oneresult;
	}

	public void setLevel(int level)
	{
		this.level = level;
	}

	public void setIlongitude(int ilongitude)
	{
		this.ilongitude = ilongitude;
	}

	public void setIlatitude(int ilatitude)
	{
		this.ilatitude = ilatitude;
	}

}
