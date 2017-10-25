package cn.mastercom.sssvr.util;

import java.util.HashMap;
import java.util.Map;

public class Stat_CellGrid_4G
{
	public int icityid;
	public int iLac;
	public int iCi;
	public int startTime;
	public int endTime;
	public int iduration;
	public int idistance;
	public int isamplenum;

	public int itllongitude;
	public int itllatitude;
	public int ibrlongitude;
	public int ibrlatitude;
	public Stat_Sample_4G tStat;

	public int UserCount_4G;
	public int UserCount_3G;
	public int UserCount_2G;
	public int UserCount_4GFall;
	public int XdrCount;
	public int MrCount;

	public int RSRQ_nTotal;
	public int RSRQ_nSum;
	public long[] RSRQ_nCount = new long[6];
	public long RSRP_nCount7;// [-113,-141]

	public Map<Long, MyInt> imsiMap;

	public Stat_CellGrid_4G()
	{
		tStat = new Stat_Sample_4G();
		imsiMap = new HashMap<Long, MyInt>();
		Clear();
	}

	public void Clear()
	{
		UserCount_4G = 0;
		UserCount_3G = 0;
		UserCount_2G = 0;
		UserCount_4GFall = 0;
		XdrCount = 0;
		MrCount = 0;

		tStat.Clear();
		imsiMap.clear();
	};

	public static Stat_CellGrid_4G FillData(String[] values, int startPos)
	{
		int i = startPos;

		Stat_CellGrid_4G item = new Stat_CellGrid_4G();
		item.icityid = Integer.parseInt(values[i++]);
		item.iLac = Integer.parseInt(values[i++]);
		item.iCi = Integer.parseInt(values[i++]);
		item.startTime = Integer.parseInt(values[i++]);
		item.endTime = Integer.parseInt(values[i++]);
		item.iduration = Integer.parseInt(values[i++]);
		item.idistance = Integer.parseInt(values[i++]);
		item.isamplenum = Integer.parseInt(values[i++]);
		item.itllongitude = Integer.parseInt(values[i++]);
		item.itllatitude = Integer.parseInt(values[i++]);
		item.ibrlongitude = Integer.parseInt(values[i++]);
		item.ibrlatitude = Integer.parseInt(values[i++]);
		item.tStat.RSRP_nTotal = Integer.parseInt(values[i++]);
		item.tStat.RSRP_nSum = Long.parseLong(values[i++]);

		for (int j = 0; j < item.tStat.RSRP_nCount.length; j++)
		{
			item.tStat.RSRP_nCount[j] = Integer.parseInt(values[i++]);
		}

		item.tStat.SINR_nTotal = Integer.parseInt(values[i++]);
		item.tStat.SINR_nSum = Long.parseLong(values[i++]);

		for (int j = 0; j < item.tStat.SINR_nCount.length; j++)
		{
			item.tStat.SINR_nCount[j] = Integer.parseInt(values[i++]);
		}

		item.tStat.RSRP100_SINR0 = Integer.parseInt(values[i++]);
		item.tStat.RSRP105_SINR0 = Integer.parseInt(values[i++]);
		item.tStat.RSRP110_SINR3 = Integer.parseInt(values[i++]);
		item.tStat.RSRP110_SINR0 = Integer.parseInt(values[i++]);

		item.tStat.UpLen = Long.parseLong(values[i++]);
		item.tStat.DwLen = Long.parseLong(values[i++]);
		item.tStat.DurationU = Long.parseLong(values[i++]);
		item.tStat.DurationD = Long.parseLong(values[i++]);
		item.tStat.AvgUpSpeed = Float.parseFloat(values[i++]);
		item.tStat.MaxUpSpeed = Float.parseFloat(values[i++]);
		item.tStat.AvgDwSpeed = Float.parseFloat(values[i++]);
		item.tStat.MaxDwSpeed = Float.parseFloat(values[i++]);

		item.tStat.UpLen_1M = Long.parseLong(values[i++]);
		item.tStat.DwLen_1M = Long.parseLong(values[i++]);
		item.tStat.DurationU_1M = Long.parseLong(values[i++]);
		item.tStat.DurationD_1M = Long.parseLong(values[i++]);
		item.tStat.AvgUpSpeed_1M = Float.parseFloat(values[i++]);
		item.tStat.MaxUpSpeed_1M = Float.parseFloat(values[i++]);
		item.tStat.AvgDwSpeed_1M = Float.parseFloat(values[i++]);
		item.tStat.MaxDwSpeed_1M = Float.parseFloat(values[i++]);

		item.UserCount_4G = Integer.parseInt(values[i++]);
		item.UserCount_3G = Integer.parseInt(values[i++]);
		item.UserCount_2G = Integer.parseInt(values[i++]);
		item.UserCount_4GFall = Integer.parseInt(values[i++]);
		item.XdrCount = Integer.parseInt(values[i++]);
		item.MrCount = Integer.parseInt(values[i++]);

		item.RSRQ_nTotal = Integer.parseInt(values[i++]);
		item.RSRQ_nSum = Integer.parseInt(values[i++]);
		for (int j = 0; j < item.RSRQ_nCount.length; j++)
		{
			item.RSRQ_nCount[j] = Integer.parseInt(values[i++]);
		}

		return item;
	}

}
