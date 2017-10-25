package cn.mastercom.sssvr.util;

public class GridData_Freq
{
	private int startTime;
	private int endTime;
	private Stat_Grid_Freq_4G lteGrid;

	public GridData_Freq(int startTime, int endTime, int freq)
	{
		this.startTime = startTime;
		this.endTime = endTime;
		lteGrid = new Stat_Grid_Freq_4G();
		lteGrid.freq = freq;
	}

	public Stat_Grid_Freq_4G getLteGrid()
	{
		return lteGrid;
	}

	public int getStartTime()
	{
		return startTime;
	}

	public int getEndTime()
	{
		return endTime;
	}

	public void dealSample(Sample_4G sample, int rsrp, int rsrq)
	{
		// boolean isMroSample = sample.flag.toUpperCase().equals("MRO");
		// boolean isMreSample = sample.flag.toUpperCase().equals("MRE");
		boolean isMroSample =true;
		boolean isMreSample=true;

		lteGrid.isamplenum++;
		if (isMroSample || isMreSample)
		{
			statMro(rsrp, rsrq, lteGrid.tStat);
		} else
		{

		}
	}

	public void statMro(int RSRP, int RSRQ, Stat_Sample_4G statItem)
	{
		if (RSRP < 0 && RSRP > -200)
		{
			statItem.RSRP_nTotal++;

			statItem.RSRP_nSum += RSRP;

			// RSRP_nCount[6]; //

			// [-141,-110),[-110,-105),[-105,-100),[-100,-95),[-95,-85),[-85,)
			if (RSRP < -113)
			{
				lteGrid.RSRP_nCount7++;// [-113,-141]
			}
			if (RSRP < -110)
			{
				statItem.RSRP_nCount[0]++;
			} else if (RSRP < -105)
			{
				statItem.RSRP_nCount[1]++;
			} else if (RSRP < -100)
			{
				statItem.RSRP_nCount[2]++;
			} else if (RSRP < -95)
			{
				statItem.RSRP_nCount[3]++;
			} else if (RSRP < -85)
			{
				statItem.RSRP_nCount[4]++;
			} else
			{
				statItem.RSRP_nCount[5]++;
			}
		}
	}

	public void finalDeal()
	{

	}

}
