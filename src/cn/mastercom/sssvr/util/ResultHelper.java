package cn.mastercom.sssvr.util;

public class ResultHelper
{
	public static final String TabMark = "\t";

	public static String getPutCell_4G(Stat_Cell_4G item)
	{
		// String samKey = RowKeyMaker.DtLteCellKey(0, 100, item.iCI,
		// item.startTime);

		StringBuffer sb = new StringBuffer();
		// sb.append(samKey);sb.append(TabMark);

		sb.append(item.icityid);
		sb.append(TabMark);
		sb.append(item.iLAC);
		sb.append(TabMark);
		sb.append(item.iCI);
		sb.append(TabMark);
		sb.append(item.startTime);
		sb.append(TabMark);
		sb.append(item.endTime);
		sb.append(TabMark);
		sb.append(item.iduration);
		sb.append(TabMark);
		sb.append(item.idistance);
		sb.append(TabMark);
		sb.append(item.isamplenum);
		sb.append(TabMark);
		sb.append(item.xdrCount);
		sb.append(TabMark);
		sb.append(item.mroCount);
		sb.append(TabMark);
		sb.append(item.mroxdrCount);
		sb.append(TabMark);
		sb.append(item.mreCount);
		sb.append(TabMark);
		sb.append(item.mrexdrCount);
		sb.append(TabMark);

		sb.append(item.origLocXdrCount);
		sb.append(TabMark);
		sb.append(item.totalLocXdrCount);
		sb.append(TabMark);
		sb.append(item.validLocXdrCount);
		sb.append(TabMark);
		sb.append(item.dtXdrCount);
		sb.append(TabMark);
		sb.append(item.cqtXdrCount);
		sb.append(TabMark);
		sb.append(item.dtexXdrCount);
		sb.append(TabMark);

		sb.append(item.tStat.RSRP_nTotal);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP_nSum);
		sb.append(TabMark);

		for (int i = 0; i < item.tStat.RSRP_nCount.length; i++)
		{
			sb.append(item.tStat.RSRP_nCount[i]);
			sb.append(TabMark);
		}
		sb.append(item.tStat.SINR_nTotal);
		sb.append(TabMark);
		sb.append(item.tStat.SINR_nSum);
		sb.append(TabMark);

		for (int i = 0; i < item.tStat.SINR_nCount.length; i++)
		{
			sb.append(item.tStat.SINR_nCount[i]);
			sb.append(TabMark);
		}
		sb.append(item.tStat.RSRP100_SINR0);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP105_SINR0);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP110_SINR3);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP110_SINR0);
		sb.append(TabMark);

		sb.append(item.tStat.UpLen);
		sb.append(TabMark);
		sb.append(item.tStat.DwLen);
		sb.append(TabMark);
		sb.append(item.tStat.DurationU);
		sb.append(TabMark);
		sb.append(item.tStat.DurationD);
		sb.append(TabMark);
		sb.append(item.tStat.AvgUpSpeed);
		sb.append(TabMark);
		sb.append(item.tStat.MaxUpSpeed);
		sb.append(TabMark);
		sb.append(item.tStat.AvgDwSpeed);
		sb.append(TabMark);
		sb.append(item.tStat.MaxDwSpeed);
		sb.append(TabMark);

		sb.append(item.tStat.UpLen_1M);
		sb.append(TabMark);
		sb.append(item.tStat.DwLen_1M);
		sb.append(TabMark);
		sb.append(item.tStat.DurationU_1M);
		sb.append(TabMark);
		sb.append(item.tStat.DurationD_1M);
		sb.append(TabMark);
		sb.append(item.tStat.AvgUpSpeed_1M);
		sb.append(TabMark);
		sb.append(item.tStat.MaxUpSpeed_1M);
		sb.append(TabMark);
		sb.append(item.tStat.AvgDwSpeed_1M);
		sb.append(TabMark);
		sb.append(item.tStat.MaxDwSpeed_1M);
		sb.append(TabMark);

		sb.append(item.sfcnJamSamCount);
		sb.append(TabMark);
		sb.append(item.sdfcnJamSamCount);

		return sb.toString();
	}

	public static String getPutGrid_4G_FREQ(Stat_Grid_Freq_4G item)
	{
		StringBuffer sb = new StringBuffer();

		sb.append(item.icityid);
		sb.append(TabMark);
		sb.append(item.freq);
		sb.append(TabMark);
		sb.append(item.startTime);
		sb.append(TabMark);
		sb.append(item.endTime);
		sb.append(TabMark);
		sb.append(item.iduration);
		sb.append(TabMark);
		sb.append(item.idistance);
		sb.append(TabMark);
		sb.append(item.isamplenum);
		sb.append(TabMark);
		sb.append(item.itllongitude);
		sb.append(TabMark);
		sb.append(item.itllatitude);
		sb.append(TabMark);
		sb.append(item.ibrlongitude);
		sb.append(TabMark);
		sb.append(item.ibrlatitude);
		sb.append(TabMark);

		sb.append(item.tStat.RSRP_nTotal);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP_nSum);
		sb.append(TabMark);

		for (int i = 0; i < item.tStat.RSRP_nCount.length; i++)
		{
			sb.append(item.tStat.RSRP_nCount[i]);
			sb.append(TabMark);
		}
		sb.append(item.tStat.SINR_nTotal);
		sb.append(TabMark);
		sb.append(item.tStat.SINR_nSum);
		sb.append(TabMark);

		for (int i = 0; i < item.tStat.SINR_nCount.length; i++)
		{
			sb.append(item.tStat.SINR_nCount[i]);
			sb.append(TabMark);
		}
		sb.append(item.tStat.RSRP100_SINR0);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP105_SINR0);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP110_SINR3);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP110_SINR0);
		sb.append(TabMark);
		sb.append(item.RSRP_nCount7);
		return sb.toString();
	}

	public static String getPutCell_Freq(Stat_Cell_Freq item)
	{
		StringBuffer sb = new StringBuffer();

		sb.append(item.icityid);
		sb.append(TabMark);
		sb.append(item.iLAC);
		sb.append(TabMark);
		sb.append(item.iCI);
		sb.append(TabMark);
		sb.append(item.freq);
		sb.append(TabMark);
		sb.append(item.startTime);
		sb.append(TabMark);
		sb.append(item.endTime);
		sb.append(TabMark);
		sb.append(item.iduration);
		sb.append(TabMark);
		sb.append(item.idistance);
		sb.append(TabMark);
		sb.append(item.isamplenum);
		sb.append(TabMark);

		sb.append(item.RSRP_nTotal);
		sb.append(TabMark);
		sb.append(item.RSRP_nSum);
		sb.append(TabMark);

		for (int i = 0; i < item.RSRP_nCount.length; i++)
		{
			sb.append(item.RSRP_nCount[i]);
			sb.append(TabMark);
		}

		sb.append(item.RSRQ_nTotal);
		sb.append(TabMark);
		sb.append(item.RSRQ_nSum);
		sb.append(TabMark);

		for (int i = 0; i < item.RSRQ_nCount.length; i++)
		{
			if (i == item.RSRQ_nCount.length - 1)
			{
				sb.append(item.RSRQ_nCount[i]);
			}
			else
			{
				sb.append(item.RSRQ_nCount[i]);
				sb.append(TabMark);
			}
		}

		return sb.toString();
	}

	public static String getPutCellGrid_4G(Stat_CellGrid_4G item)
	{
		StringBuffer sb = new StringBuffer();

		sb.append(item.icityid);
		sb.append(TabMark);
		sb.append(item.iLac);
		sb.append(TabMark);
		sb.append(item.iCi);
		sb.append(TabMark);
		sb.append(item.startTime);
		sb.append(TabMark);
		sb.append(item.endTime);
		sb.append(TabMark);
		sb.append(item.iduration);
		sb.append(TabMark);
		sb.append(item.idistance);
		sb.append(TabMark);
		sb.append(item.isamplenum);
		sb.append(TabMark);
		sb.append(item.itllongitude);
		sb.append(TabMark);
		sb.append(item.itllatitude);
		sb.append(TabMark);
		sb.append(item.ibrlongitude);
		sb.append(TabMark);
		sb.append(item.ibrlatitude);
		sb.append(TabMark);

		sb.append(item.tStat.RSRP_nTotal);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP_nSum);
		sb.append(TabMark);

		for (int i = 0; i < item.tStat.RSRP_nCount.length; i++)
		{
			sb.append(item.tStat.RSRP_nCount[i]);
			sb.append(TabMark);
		}
		sb.append(item.tStat.SINR_nTotal);
		sb.append(TabMark);
		sb.append(item.tStat.SINR_nSum);
		sb.append(TabMark);

		for (int i = 0; i < item.tStat.SINR_nCount.length; i++)
		{
			sb.append(item.tStat.SINR_nCount[i]);
			sb.append(TabMark);
		}
		sb.append(item.tStat.RSRP100_SINR0);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP105_SINR0);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP110_SINR3);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP110_SINR0);
		sb.append(TabMark);
		sb.append(item.tStat.UpLen);
		sb.append(TabMark);
		sb.append(item.tStat.DwLen);
		sb.append(TabMark);
		sb.append(item.tStat.DurationU);
		sb.append(TabMark);
		sb.append(item.tStat.DurationD);
		sb.append(TabMark);
		sb.append(item.tStat.AvgUpSpeed);
		sb.append(TabMark);
		sb.append(item.tStat.MaxUpSpeed);
		sb.append(TabMark);
		sb.append(item.tStat.AvgDwSpeed);
		sb.append(TabMark);
		sb.append(item.tStat.MaxDwSpeed);
		sb.append(TabMark);

		sb.append(item.tStat.UpLen_1M);
		sb.append(TabMark);
		sb.append(item.tStat.DwLen_1M);
		sb.append(TabMark);
		sb.append(item.tStat.DurationU_1M);
		sb.append(TabMark);
		sb.append(item.tStat.DurationD_1M);
		sb.append(TabMark);
		sb.append(item.tStat.AvgUpSpeed_1M);
		sb.append(TabMark);
		sb.append(item.tStat.MaxUpSpeed_1M);
		sb.append(TabMark);
		sb.append(item.tStat.AvgDwSpeed_1M);
		sb.append(TabMark);
		sb.append(item.tStat.MaxDwSpeed_1M);
		sb.append(TabMark);

		sb.append(item.UserCount_4G);
		sb.append(TabMark);
		sb.append(item.UserCount_3G);
		sb.append(TabMark);
		sb.append(item.UserCount_2G);
		sb.append(TabMark);
		sb.append(item.UserCount_4GFall);
		sb.append(TabMark);
		sb.append(item.XdrCount);
		sb.append(TabMark);
		sb.append(item.MrCount);

		sb.append(TabMark);
		sb.append(item.RSRQ_nTotal);
		sb.append(TabMark);
		sb.append(item.RSRQ_nSum);
		for (int j = 0; j < item.RSRQ_nCount.length; j++)
		{
			sb.append(TabMark);
			sb.append(item.RSRQ_nCount[j]);
		}
		sb.append(TabMark);
		sb.append(item.RSRP_nCount7);

		return sb.toString();
	}
}
