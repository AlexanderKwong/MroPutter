package cn.mastercom.sssvr.util;

import cn.mastercom.sssvr.util.Sample_4G;

public class Grid
{
	public int icityid;
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
	// [-40 -20) [-20 -16) [-16 -12)[-12 -8) [-8 0)[0,]
	public long RSRP_nCount7;// (-113,-141]

	// ----新添加rsrq统计

	public Grid()
	{
		tStat = new Stat_Sample_4G();
		// imsiMap = new HashMap<Long, MyInt>();
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
		// imsiMap.clear();
	};

	public void statisticNewSample(Sample_4G sample, int gridSize)
	{
		icityid = sample.cityID;
		startTime = TimeHelper.getRoundDayTime(sample.itime);// 一天的开始
		endTime = startTime + 86400;// 一天的结束
		isamplenum++;

		if (gridSize == 10)
		{
			itllongitude = (sample.ilongitude / 1000) * 1000;// 左上
			itllatitude = (sample.ilatitude / 900) * 900 + 900;
			ibrlongitude = (sample.ilongitude / 1000) * 1000 + 1000;// 右下
			ibrlatitude = (sample.ilatitude / 900) * 900;
		}
		else if (gridSize == 40)
		{
			itllongitude = (sample.ilongitude / 4000) * 4000;// 左上
			itllatitude = (sample.ilatitude / 3600) * 3600 + 3600;
			ibrlongitude = (sample.ilongitude / 4000) * 4000 + 4000;// 右下
			ibrlatitude = (sample.ilatitude / 3600) * 3600;
		}

		tStat.RSRP_nTotal++;
		if (sample.LteScRSRP != -1000000)
		{
			tStat.RSRP_nSum += sample.LteScRSRP;
		}
		MrCount++;

		RSRQ_nTotal++;
		if (sample.LteScRSRQ != -1000000)
		{
			RSRQ_nSum += sample.LteScRSRQ;
		}

		statisticRSRPandSinrAndRSRQ(sample);
	}

	public void statisticSample(Sample_4G sample)
	{
		isamplenum++;
		tStat.RSRP_nTotal++;
		if (sample.LteScRSRP != -10000)
		{
			tStat.RSRP_nSum += sample.LteScRSRP;
		}
		MrCount++;

		RSRQ_nTotal++;
		if (sample.LteScRSRQ != -10000)
		{
			RSRQ_nSum += sample.LteScRSRQ;
		}

		statisticRSRPandSinrAndRSRQ(sample);
	}

	public void statisticRSRPandSinrAndRSRQ(Sample_4G sample)
	{
		// RSRP_nCount[6];
		// [-141,-110),[-110,-105),[-105,-100),[-100,-95),[-95,-85),[-85,)
		if (sample.LteScRSRP < -113)
		{
			RSRP_nCount7++;
		}
		if (sample.LteScRSRP < -110)
		{
			tStat.RSRP_nCount[0]++;
		}
		else if (sample.LteScRSRP < -105)
		{
			tStat.RSRP_nCount[1]++;
		}
		else if (sample.LteScRSRP < -100)
		{
			tStat.RSRP_nCount[2]++;
		}
		else if (sample.LteScRSRP < -95)
		{
			tStat.RSRP_nCount[3]++;
		}
		else if (sample.LteScRSRP < -85)
		{
			tStat.RSRP_nCount[4]++;
		}
		else
		{
			tStat.RSRP_nCount[5]++;
		}

		if (sample.LteScSinrUL != StaticConfig.Int_Abnormal)
		{
			tStat.SINR_nTotal++; // 总数

			tStat.SINR_nSum += sample.LteScSinrUL;

			// int SINR_nCount[8]; //
			// [-20,0),[0,5),[5,10),[10,15),[15,20),[20,25),[25,50),[50,)
			if (sample.LteScSinrUL < 0)
			{
				tStat.SINR_nCount[0]++;
			}
			else if (sample.LteScSinrUL < 5)
			{
				tStat.SINR_nCount[1]++;
			}
			else if (sample.LteScSinrUL < 10)
			{
				tStat.SINR_nCount[2]++;
			}
			else if (sample.LteScSinrUL < 15)
			{
				tStat.SINR_nCount[3]++;
			}
			else if (sample.LteScSinrUL < 20)
			{
				tStat.SINR_nCount[4]++;
			}
			else if (sample.LteScSinrUL < 25)
			{
				tStat.SINR_nCount[5]++;
			}
			else if (sample.LteScSinrUL < 50)
			{
				tStat.SINR_nCount[6]++;
			}
			else
			{
				tStat.SINR_nCount[7]++;
			}

			if (sample.LteScRSRP != StaticConfig.Int_Abnormal)
			{
				if ((sample.LteScRSRP >= -100) && (sample.LteScSinrUL >= -3))
				{
					tStat.RSRP100_SINR0++;
				}
				if ((sample.LteScRSRP >= -103) && (sample.LteScSinrUL >= -3))
				{
					tStat.RSRP105_SINR0++;
				}
				if ((sample.LteScRSRP >= -110) && (sample.LteScSinrUL >= -3))
				{
					tStat.RSRP110_SINR3++;
				}
				if ((sample.LteScRSRP >= -113) && (sample.LteScSinrUL >= -3))
				{
					tStat.RSRP110_SINR0++;
				}
			}
		}
		// [-40 -20) [-20 -16) [-16 -12)[-12 -8) [-8 0)[0,)
		if (sample.LteScRSRQ < -20 && sample.LteScRSRQ >= -40)
		{
			RSRQ_nCount[0]++;
		}
		else if (sample.LteScRSRQ < -16)
		{
			RSRQ_nCount[1]++;
		}
		else if (sample.LteScRSRQ < -12)
		{
			RSRQ_nCount[2]++;
		}
		else if (sample.LteScRSRQ < -8)
		{
			RSRQ_nCount[3]++;
		}
		else if (sample.LteScRSRQ < 0)
		{
			RSRQ_nCount[4]++;
		}
		else if (sample.LteScRSRQ >= 0)
		{
			RSRQ_nCount[5]++;
		}
	}

	public String returnContent()
	{
		StringBuffer sbf = new StringBuffer();
		sbf.append(icityid);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(startTime);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(endTime);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(iduration);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(idistance);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(isamplenum);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(itllongitude);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(itllatitude);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(ibrlongitude);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(ibrlatitude);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(tStat.RSRP_nTotal);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(tStat.RSRP_nSum);
		sbf.append(StaticConfig.DataSlipter);
		for (int j = 0; j < tStat.RSRP_nCount.length; j++)
		{
			sbf.append(tStat.RSRP_nCount[j]);
			sbf.append(StaticConfig.DataSlipter);
		}
		sbf.append(tStat.SINR_nTotal);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(tStat.SINR_nSum);
		sbf.append(StaticConfig.DataSlipter);
		for (int j = 0; j < tStat.SINR_nCount.length; j++)
		{
			sbf.append(tStat.SINR_nCount[j]);
			sbf.append(StaticConfig.DataSlipter);
		}
		sbf.append(tStat.RSRP100_SINR0);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(tStat.RSRP105_SINR0);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(tStat.RSRP110_SINR3);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(tStat.RSRP110_SINR0);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(tStat.UpLen);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(tStat.DwLen);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(tStat.DurationU);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(tStat.DurationD);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(tStat.AvgUpSpeed);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(tStat.MaxUpSpeed);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(tStat.AvgDwSpeed);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(tStat.MaxDwSpeed);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(tStat.UpLen_1M);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(tStat.DwLen_1M);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(tStat.DurationU_1M);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(tStat.DurationD_1M);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(tStat.AvgUpSpeed_1M);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(tStat.MaxUpSpeed_1M);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(tStat.AvgDwSpeed_1M);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(tStat.MaxDwSpeed_1M);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(UserCount_4G);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(UserCount_3G);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(UserCount_2G);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(UserCount_4GFall);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(XdrCount);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(MrCount);

		sbf.append(StaticConfig.DataSlipter);
		sbf.append(RSRQ_nTotal);
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(RSRQ_nSum);
		for (int j = 0; j < RSRQ_nCount.length; j++)
		{
			sbf.append(StaticConfig.DataSlipter);
			sbf.append(RSRQ_nCount[j]);
		}
		sbf.append(StaticConfig.DataSlipter);
		sbf.append(RSRP_nCount7);
		return sbf.toString();
	}

}
