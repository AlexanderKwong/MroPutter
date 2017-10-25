package cn.mastercom.sssvr.util;

public class CellData_4G
{
	private int lac;
	private long eci;
	private int startTime;
	private int endTime;
	private Stat_Cell_4G lteCell;

	public CellData_4G(int cityID, int lac, long eci, int startTime, int endTime)
	{
		this.lac = lac;
		this.eci = eci;
		this.startTime = startTime;
		this.endTime = endTime;

		lteCell = new Stat_Cell_4G();
		lteCell.Clear();

		lteCell.icityid = cityID;
		lteCell.startTime = startTime;
		lteCell.endTime = endTime;
		lteCell.iLAC = lac;
		lteCell.wRAC = 0;
		lteCell.iCI = eci;
	}

	public int getLac()
	{
		return lac;
	}

	public long getEci()
	{
		return eci;
	}

	public Stat_Cell_4G getLteCell()
	{
		return lteCell;
	}

	public void dealSample(Sample_4G sample)
	{
		// boolean isSampleMro = sample.flag.toUpperCase().equals("MRO");
		// boolean isSampleMre = sample.flag.toUpperCase().equals("MRE");
		boolean isSampleMro = true;
		boolean isSampleMre = true;

		// 小区统计
		lteCell.iduration += sample.duration;
		if (isSampleMro || isSampleMre)
		{
			lteCell.isamplenum++;
			statMro(sample, lteCell.tStat);

			int result = isSampleJam(sample);
			if (result == 1 || result == 2)
			{
				lteCell.sfcnJamSamCount++;
			}

			if (result == 2 || result == 3)
			{
				lteCell.sdfcnJamSamCount++;
			}

			if (isSampleMro)
			{
				lteCell.mroCount++;
				if (sample.IMSI > 0)
				{
					lteCell.mroxdrCount++;
				}
			} else if (isSampleMre)
			{
				lteCell.mreCount++;
				if (sample.IMSI > 0)
				{
					lteCell.mrexdrCount++;
				}
			}
		} else
		{
			lteCell.xdrCount++;

			if (sample.ilongitude > 0 && sample.isOriginalLoction())
			{
				lteCell.totalLocXdrCount++;

				if (sample.loctp.equals("ll") || sample.loctp.equals("ll2")
						|| sample.loctp.equals("wf") && sample.radius <= 100 && sample.radius >= 0)
				{
					lteCell.validLocXdrCount++;
				}

				if (sample.testType == StaticConfig.TestType_DT)
				{
					lteCell.dtXdrCount++;
				} else if (sample.testType == StaticConfig.TestType_CQT)
				{
					lteCell.cqtXdrCount++;
				} else if (sample.testType == StaticConfig.TestType_DT_EX)
				{
					lteCell.dtexXdrCount++;
				}
			}

			statEvt(sample, lteCell.tStat);
		}

	}

	public static void statMro(Sample_4G sample, Stat_Sample_4G statItem)
	{
		if (sample.LteScRSRP != StaticConfig.Int_Abnormal)
		{
			statItem.RSRP_nTotal++;

			statItem.RSRP_nSum += sample.LteScRSRP;

			// RSRP_nCount[6]; //
			// [-141,-110),[-110,-105),[-105,-100),[-100,-95),[-95,-85),[-85,)
			if (sample.LteScRSRP < -110)
			{
				statItem.RSRP_nCount[0]++;
			} else if (sample.LteScRSRP < -105)
			{
				statItem.RSRP_nCount[1]++;
			} else if (sample.LteScRSRP < -100)
			{
				statItem.RSRP_nCount[2]++;
			} else if (sample.LteScRSRP < -95)
			{
				statItem.RSRP_nCount[3]++;
			} else if (sample.LteScRSRP < -85)
			{
				statItem.RSRP_nCount[4]++;
			} else
			{
				statItem.RSRP_nCount[5]++;
			}
		}

		if (sample.LteScSinrUL != StaticConfig.Int_Abnormal)
		{
			statItem.SINR_nTotal++; // 总数

			statItem.SINR_nSum += sample.LteScSinrUL;

			// int SINR_nCount[8]; //
			// [-20,0),[0,5),[5,10),[10,15),[15,20),[20,25),[25,50),[50,)
			if (sample.LteScSinrUL < 0)
			{
				statItem.SINR_nCount[0]++;
			} else if (sample.LteScSinrUL < 5)
			{
				statItem.SINR_nCount[1]++;
			} else if (sample.LteScSinrUL < 10)
			{
				statItem.SINR_nCount[2]++;
			} else if (sample.LteScSinrUL < 15)
			{
				statItem.SINR_nCount[3]++;
			} else if (sample.LteScSinrUL < 20)
			{
				statItem.SINR_nCount[4]++;
			} else if (sample.LteScSinrUL < 25)
			{
				statItem.SINR_nCount[5]++;
			} else if (sample.LteScSinrUL < 50)
			{
				statItem.SINR_nCount[6]++;
			} else
			{
				statItem.SINR_nCount[7]++;
			}

			if (sample.LteScRSRP != StaticConfig.Int_Abnormal)
			{
				if ((sample.LteScRSRP >= -100) && (sample.LteScSinrUL >= -3))
				{
					statItem.RSRP100_SINR0++;
				}
				if ((sample.LteScRSRP >= -103) && (sample.LteScSinrUL >= -3))
				{
					statItem.RSRP105_SINR0++;
				}
				if ((sample.LteScRSRP >= -110) && (sample.LteScSinrUL >= -3))
				{
					statItem.RSRP110_SINR3++;
				}
				if ((sample.LteScRSRP >= -113) && (sample.LteScSinrUL >= -3))
				{
					statItem.RSRP110_SINR0++;
				}
			}
		}
	}

	public static void statEvt(Sample_4G sample, Stat_Sample_4G statItem)
	{
		if (sample.IPDataUL > 0)
		{
			statItem.UpLen += sample.IPDataUL;

			statItem.DurationU += sample.duration;

			statItem.MaxUpSpeed = Math.max(statItem.MaxUpSpeed, (float) sample.IPThroughputUL);

			if (statItem.DurationU > 0)
				statItem.AvgUpSpeed = (float) (statItem.UpLen / (statItem.DurationU / 1000.0) * 8.0) / 1024;

			if (sample.IPDataUL >= 1024 * 1024)
			{
				statItem.UpLen_1M += sample.IPDataUL;

				statItem.MaxUpSpeed_1M = Math.max(statItem.MaxUpSpeed_1M, (float) sample.IPThroughputUL);

				statItem.DurationU_1M += sample.duration;

				if (statItem.DurationU_1M > 0)
					statItem.AvgUpSpeed_1M = (float) (statItem.UpLen_1M / (statItem.DurationU_1M / 1000.0) * 8.0)
							/ 1024;
			}

		}

		if (sample.IPDataDL > 0)
		{
			statItem.DwLen += sample.IPDataDL;

			statItem.DurationD += sample.duration;

			statItem.MaxDwSpeed = Math.max(statItem.MaxDwSpeed, (float) sample.IPThroughputDL);

			if (statItem.DurationD > 0)
				statItem.AvgDwSpeed = (float) (statItem.DwLen / (statItem.DurationD / 1000.0) * 8.0) / 1024;

			if (sample.IPDataDL >= 1024 * 1024)
			{
				statItem.DwLen_1M += sample.IPDataDL;

				statItem.MaxDwSpeed_1M = Math.max(statItem.MaxDwSpeed_1M, (float) sample.IPThroughputDL);

				statItem.DurationD_1M += sample.duration;

				if (statItem.DurationD_1M > 0)
					statItem.AvgDwSpeed_1M = (float) (statItem.DwLen_1M / (statItem.DurationD_1M / 1000.0) * 8.0)
							/ 1024;

			}
		}

	}

	public int isSampleJam(Sample_4G tsam)
	{
		int sfcnJamCellCount = 0;
		int dfcnJamCellCount = 0;

		if ((tsam.LteScRSRP < -50 && tsam.LteScRSRP > -150) && tsam.LteScRSRP > -110)
		{
			for (NC_LTE item : tsam.tlte)
			{
				if ((item.LteNcRSRP < -50 && item.LteNcRSRP > -150) && item.LteNcRSRP - tsam.LteScRSRP > -6)
				{
					if (tsam.Earfcn == item.LteNcEarfcn)
					{
						sfcnJamCellCount++;
					} else
					{
						dfcnJamCellCount++;
					}
				}
			}
		}

		int result = 0;
		if (sfcnJamCellCount >= 3)
		{
			result = 1;
		}

		if (sfcnJamCellCount + dfcnJamCellCount >= 3)
		{
			if (result == 1)
			{
				result = 2;
			} else
			{
				result = 3;
			}

		}

		return result;
	}

}
