package cn.mastercom.sssvr.util;

public class LteStatHelper
{

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

	public static void statMro(Sample_4G sample, Stat_Sample_4G statItem)
	{
		if (sample.LteScRSRP != StaticConfig.Int_Abnormal)
		{
			statItem.RSRP_nTotal++;

			statItem.RSRP_nSum += sample.LteScRSRP;

			// RSRP_nCount[6]; //
			// [-141,-110),[-110,-95),[-95,-80),[-80,-65),[-65,-50),[-50,)
			if (sample.LteScRSRP < -110)
			{
				statItem.RSRP_nCount[0]++;
			} else if (sample.LteScRSRP < -95)
			{
				statItem.RSRP_nCount[1]++;
			} else if (sample.LteScRSRP < -80)
			{
				statItem.RSRP_nCount[2]++;
			} else if (sample.LteScRSRP < -65)
			{
				statItem.RSRP_nCount[3]++;
			} else if (sample.LteScRSRP < -50)
			{
				statItem.RSRP_nCount[4]++;
			} else
			{
				statItem.RSRP_nCount[5]++;
			}
		}

		if (sample.LteScSinrUL != StaticConfig.Int_Abnormal)
		{
			statItem.SINR_nTotal++;

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

}
