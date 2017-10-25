package cn.mastercom.sssvr.util;

public class NC_GSM
{
	public int GsmNcellCarrierRSSI;
	public int GsmNcellBcch;
	public int GsmNcellBsic; // NCC(3bit)+BCC(3bit)

	public void Clear()
	{
		GsmNcellCarrierRSSI = StaticConfig.Int_Abnormal;
		GsmNcellBcch = StaticConfig.Int_Abnormal;
		GsmNcellBsic = StaticConfig.Int_Abnormal;
	}
	
	public String GetData()
	{
		StringBuffer res = new StringBuffer();
		
		res.append(GsmNcellCarrierRSSI);
		res.append(StaticConfig.DataSlipter);
		res.append(GsmNcellBcch);
		res.append(StaticConfig.DataSlipter);
		res.append(GsmNcellBsic);

        return 	res.toString();		
	}
	
	public void FillData(Object[] args)
	{	
		String[] values = (String[])args[0];
		Integer i = (Integer)args[1];
		
		GsmNcellCarrierRSSI = DataGeter.GetInt(values[i++]);
		GsmNcellBcch = DataGeter.GetInt(values[i++]);
		GsmNcellBsic = DataGeter.GetInt(values[i++]);
		
		args[1] = i;
	}
	
}
