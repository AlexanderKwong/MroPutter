package cn.mastercom.sssvr.util;

public class NC_LTE
{
	public int LteNcRSRP;
	public int LteNcRSRQ;
	public int LteNcEarfcn;
	public int LteNcPci;

	public void Clear()
	{
		LteNcRSRP = StaticConfig.Int_Abnormal;
		LteNcRSRQ = StaticConfig.Int_Abnormal;
		LteNcEarfcn = StaticConfig.Int_Abnormal;
		LteNcPci = StaticConfig.Int_Abnormal;
	}
	
	public String GetData()
	{
		StringBuffer res = new StringBuffer();
		res.append(LteNcRSRP);
		res.append(StaticConfig.DataSlipter);
		res.append(LteNcRSRQ);
		res.append(StaticConfig.DataSlipter);
		res.append(LteNcEarfcn);
		res.append(StaticConfig.DataSlipter);
		res.append(LteNcPci);

        return 	res.toString();		
	}
	
	public void FillData(Object[] args)
	{	
		String[] values = (String[])args[0];
		Integer i = (Integer)args[1];
		
		LteNcRSRP = DataGeter.GetInt(values[i++]);
		LteNcRSRQ = DataGeter.GetInt(values[i++]);
		LteNcEarfcn = DataGeter.GetInt(values[i++]);
		LteNcPci = DataGeter.GetInt(values[i++]);
		
		args[1] = i;
	}
	
}
