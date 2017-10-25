package cn.mastercom.sssvr.util;

import java.util.HashSet;
import java.util.Set;

public class SIGNAL_MR_All
{
	public SIGNAL_MR_SC tsc;
	public short[] nccount;// 4
	public NC_LTE[] tlte;// 6
	public NC_TDS[] ttds;// 6
	public NC_GSM[] tgsm;// 6
	public SC_FRAME[] trip;// 10

	public SIGNAL_MR_All()
	{
		Clear();
	}

	public void Clear()
	{
		tsc = new SIGNAL_MR_SC();
		nccount = new short[4];
		tlte = new NC_LTE[6];
		ttds = new NC_TDS[6];
		tgsm = new NC_GSM[6];
		trip = new SC_FRAME[10];

		for (int i = 0; i < nccount.length; i++)
			nccount[i] = 0;

		for (int i = 0; i < 6; i++)
		{
			tlte[i] = new NC_LTE();
			tlte[i].Clear();
			ttds[i] = new NC_TDS();
			ttds[i].Clear();
			tgsm[i] = new NC_GSM();
			tgsm[i].Clear();
		}

		for (int i = 0; i < trip.length; i++)
		{
			trip[i] = new SC_FRAME();
			trip[i].Clear();
		}
		
		freqSet.clear();
		pos = 0;

	}

	public String GetData()
	{
		StringBuffer res = new StringBuffer();

		res.append(tsc.GetDataBefore());
		
		for (int data : nccount)
		{
			res.append(data);
			res.append(StaticConfig.DataSlipter);
		}

		for (NC_LTE data : tlte)
		{
			res.append(data.GetData());
			res.append(StaticConfig.DataSlipter);
		}

		for (NC_TDS data : ttds)
		{
			res.append(data.GetData());
			res.append(StaticConfig.DataSlipter);
		}

		for (NC_GSM data : tgsm)
		{
			res.append(data.GetData());
			res.append(StaticConfig.DataSlipter);
		}

		for (SC_FRAME data : trip)
		{
			res.append(data.GetData());
			res.append(StaticConfig.DataSlipter);
		}
		
		res.append(tsc.GetDataAfter());

		return StringHelper.SideTrim(res.toString(), StaticConfig.DataSlipter);
	}

	private String tmStr;

	public String GetDataEx()
	{
		tmStr = GetData();
		//tmStr = tmStr.replace("" + StaticConfig.Int_Abnormal, "");
		//tmStr = tmStr.replace("" + StaticConfig.Short_Abnormal, "");
		return tmStr;
	}

	/*public boolean FillData(Object[] args)
	{
		tsc.FillData(args);
		String values[] = (String[]) args[0];
		Integer i = (Integer) args[1];
		for (int ii = 0; ii < nccount.length; ii++)
			nccount[ii] = DataGeter.GetTinyInt(values[i + ii]);
		i += nccount.length;

		args[1] = i;
		for (int ii = 0; ii < tlte.length; ii++)
			tlte[ii].FillData(args);

		for (int ii = 0; ii < ttds.length; ii++)
			ttds[ii].FillData(args);

		for (int ii = 0; ii < tgsm.length; ii++)
			tgsm[ii].FillData(args);

		for (int ii = 0; ii < trip.length; ii++)
			trip[ii].FillData(args);

		return true;

	}
*/
	//锟�锟斤拷淇濈暀寮傞鐨勫満锟�
	//灏嗚仈閫氬拰鐢典俊鐨勬祴閲忕粨鏋滀繚瀛樺埌5涓狦SM閭诲尯锟�涓猅D閭诲尯閲岄潰锛屽叡9涓鐐癸級
	//鑱旓拷?锟�600,1650,40340
	//鐢典俊锟�775,1800,1825,1850,75,100	
	private Set<Integer> freqSet = new HashSet<Integer>();
	private int pos = 0;
	public boolean fillNclte_Freq(NC_LTE item)
	{
		if(item.LteNcEarfcn < 0)
		{
			return false;
		}
		
		if(!Func.checkFreqIsLtDx(item.LteNcEarfcn))
		{
			return false;
		}
		
		if(!freqSet.contains(item.LteNcEarfcn))
		{
			freqSet.add(item.LteNcEarfcn);
			
			if (pos >= 0 && pos <= 4)
			{
				if(item.LteNcRSRP < 0 && item.LteNcRSRP > -200)
				{
					tgsm[pos + 1].GsmNcellBcch = item.LteNcEarfcn;
					tgsm[pos + 1].GsmNcellBsic = item.LteNcPci;
					tgsm[pos + 1].GsmNcellCarrierRSSI = (item.LteNcRSRP + 200) * 1000 + (item.LteNcRSRQ + 200);
				}
			}
			else if(pos >= 5 && pos <= 8)
			{
				if(item.LteNcRSRP < 0 && item.LteNcRSRP > -200)
				{
					ttds[pos - 3].TdsNcellUarfcn = item.LteNcEarfcn;
					ttds[pos - 3].TdsCellParameterId = item.LteNcPci;
					ttds[pos - 3].TdsPccpchRSCP = (item.LteNcRSRP + 200) * 1000 + (item.LteNcRSRQ + 200);
				}
			}
			else 
			{
				return false;
			}
			
			pos++;
		}
		
		return true;
	}
	

}
