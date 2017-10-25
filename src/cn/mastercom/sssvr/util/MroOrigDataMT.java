package cn.mastercom.sssvr.util;

public class MroOrigDataMT
{
	public String beginTime;// yyyy-MM-dd HH:mm:ss.000
	public int ENBId;
	public String UserLabel;
	public int CellId;
	public int Earfcn;
	public int SubFrameNbr;
	public int MmeCode;
	public int MmeGroupId;
	public long MmeUeS1apId;
	public int Weight;
	public String EventType;
	public int LteScRSRP;
	public int LteNcRSRP;
	public int LteScRSRQ;
	public int LteNcRSRQ;
	public int LteScEarfcn;
	public int LteScPci;
	public int LteNcEarfcn;
	public int LteNcPci;
	public int GsmNcellCarrierRSSI;
	public int GsmNcellBcch;
	public int GsmNcellNcc;
	public int GsmNcellBcc;
	public int TdsPccpchRSCP;
	public int TdsNcellUarfcn;
	public int TdsCellParameterId;
	public int LteScBSR;
	public int LteScRTTD;
	public int LteScTadv;
	public int LteScAOA;
	public int LteScPHR;
	public int LteScRIP;
	public int LteScSinrUL;
	public int[] LteScPlrULQci;
	public int[] LteScPlrDLQci;
	public int LteScRI1;
	public int LteScRI2;
	public int LteScRI4;
	public int LteScRI8;
	public int LteScPUSCHPRBNum;
	public int LteScPDSCHPRBNum;
	public int LteSceNBRxTxTimeDiff;
	public int eci;

	public MroOrigDataMT()
	{
		beginTime = "";
		ENBId = StaticConfig.Int_Abnormal;
		UserLabel = "";
		CellId = StaticConfig.Int_Abnormal;
		Earfcn = StaticConfig.Int_Abnormal;
		SubFrameNbr = StaticConfig.Int_Abnormal;
		MmeCode = StaticConfig.Int_Abnormal;
		MmeGroupId = StaticConfig.Int_Abnormal;
		MmeUeS1apId = StaticConfig.Int_Abnormal;
		Weight = StaticConfig.Int_Abnormal;
		EventType = "";
		LteScRSRP = StaticConfig.Int_Abnormal;
		LteNcRSRP = StaticConfig.Int_Abnormal;
		LteScRSRQ = StaticConfig.Int_Abnormal;
		LteNcRSRQ = StaticConfig.Int_Abnormal;
		LteScEarfcn = StaticConfig.Int_Abnormal;
		LteScPci = StaticConfig.Int_Abnormal;
		LteNcEarfcn = StaticConfig.Int_Abnormal;
		LteNcPci = StaticConfig.Int_Abnormal;
		GsmNcellCarrierRSSI = StaticConfig.Int_Abnormal;
		GsmNcellBcch = StaticConfig.Int_Abnormal;
		GsmNcellNcc = StaticConfig.Int_Abnormal;
		GsmNcellBcc = StaticConfig.Int_Abnormal;
		TdsPccpchRSCP = StaticConfig.Int_Abnormal;
		TdsNcellUarfcn = StaticConfig.Int_Abnormal;
		TdsCellParameterId = StaticConfig.Int_Abnormal;
		LteScBSR = StaticConfig.Int_Abnormal;
		LteScRTTD = StaticConfig.Int_Abnormal;
		LteScTadv = StaticConfig.Int_Abnormal;
		LteScAOA = StaticConfig.Int_Abnormal;
		LteScPHR = StaticConfig.Int_Abnormal;
		LteScRIP = StaticConfig.Int_Abnormal;
		LteScSinrUL = StaticConfig.Int_Abnormal;
		LteScPlrULQci = new int[9];
		for (int i = 0; i < LteScPlrULQci.length; ++i)
		{
			LteScPlrULQci[i] = StaticConfig.Int_Abnormal;
		}

		LteScPlrDLQci = new int[9];
		for (int i = 0; i < LteScPlrDLQci.length; ++i)
		{
			LteScPlrDLQci[i] = StaticConfig.Int_Abnormal;
		}

		LteScRI1 = StaticConfig.Int_Abnormal;
		LteScRI2 = StaticConfig.Int_Abnormal;
		LteScRI4 = StaticConfig.Int_Abnormal;
		LteScRI8 = StaticConfig.Int_Abnormal;
		LteScPUSCHPRBNum = StaticConfig.Int_Abnormal;
		LteScPDSCHPRBNum = StaticConfig.Int_Abnormal;
		LteSceNBRxTxTimeDiff = StaticConfig.Int_Abnormal;
		eci = StaticConfig.Int_Abnormal;
	}

	public boolean FillData(String[] values, int startPos, String type)
	{
		if (type.equals(""))
		{
			int i = startPos;

			beginTime = values[i++];
			ENBId = DataGeter.GetInt(values[i++]);
			UserLabel = values[i++];
			CellId = DataGeter.GetInt(values[i++]);
			if (CellId > 256)
			{
				CellId = CellId % 256;
			}
			Earfcn = DataGeter.GetInt(values[i++]);
			SubFrameNbr = DataGeter.GetInt(values[i++]);
			MmeCode = DataGeter.GetInt(values[i++]);
			MmeGroupId = DataGeter.GetInt(values[i++]);
			MmeUeS1apId = DataGeter.GetInt(values[i++]);
			Weight = DataGeter.GetInt(values[i++]);
			EventType = values[i++];
			LteScRSRP = DataGeter.GetInt(values[i++]);
			LteNcRSRP = DataGeter.GetInt(values[i++]);
			LteScRSRQ = DataGeter.GetInt(values[i++]);
			LteNcRSRQ = DataGeter.GetInt(values[i++]);
			LteScEarfcn = DataGeter.GetInt(values[i++]);
			LteScPci = DataGeter.GetInt(values[i++]);
			LteNcEarfcn = DataGeter.GetInt(values[i++]);
			LteNcPci = DataGeter.GetInt(values[i++]);
			GsmNcellCarrierRSSI = DataGeter.GetInt(values[i++]);
			GsmNcellBcch = DataGeter.GetInt(values[i++]);
			GsmNcellNcc = DataGeter.GetInt(values[i++]);
			GsmNcellBcc = DataGeter.GetInt(values[i++]);
			try
			{
				String temp = values[i++];
				TdsPccpchRSCP = DataGeter.GetInt(temp);
			} catch (Exception e)
			{
				TdsPccpchRSCP = -10000;
			}
			TdsNcellUarfcn = DataGeter.GetInt(values[i++]);
			TdsCellParameterId = DataGeter.GetInt(values[i++]);
			LteScBSR = DataGeter.GetInt(values[i++]);
			LteScRTTD = DataGeter.GetInt(values[i++]);
			LteScTadv = DataGeter.GetInt(values[i++]);
			LteScAOA = DataGeter.GetInt(values[i++]);
			LteScPHR = DataGeter.GetInt(values[i++]);
			LteScRIP = DataGeter.GetInt(values[i++]);
			LteScSinrUL = DataGeter.GetInt(values[i++]);

			for (int ii = 0; ii < LteScPlrULQci.length; ++ii)
			{
				i++;
				LteScPlrULQci[ii] = 0;
				// LteScPlrULQci[ii] = DataConverter.GetInt(values[i++]);
			}

			for (int ii = 0; ii < LteScPlrDLQci.length; ++ii)
			{
				i++;
				LteScPlrDLQci[ii] = 0;
				// LteScPlrDLQci[ii] = DataConverter.GetInt(values[i++]);
			}

			LteScRI1 = DataGeter.GetInt(values[i++]);
			LteScRI2 = DataGeter.GetInt(values[i++]);
			LteScRI4 = DataGeter.GetInt(values[i++]);
			LteScRI8 = DataGeter.GetInt(values[i++]);
			LteScPUSCHPRBNum = DataGeter.GetInt(values[i++]);
			LteScPDSCHPRBNum = DataGeter.GetInt(values[i++]);
			LteSceNBRxTxTimeDiff = DataGeter.GetInt(values[i++]);
			eci = ENBId * 256 + CellId;

			// 鍒濆锟�杞寲锟�甯歌锟� if(LteNcRSRP >= 0) LteNcRSRP -= 141;
			if (LteNcRSRQ >= 0)
				LteNcRSRQ = LteNcRSRQ / 2 - 20;
			if (LteNcRSRP >= 0)
				LteNcRSRP -= 141;
			if (GsmNcellCarrierRSSI >= 0)
				GsmNcellCarrierRSSI -= 101;
			if (TdsPccpchRSCP >= 0)
				TdsPccpchRSCP -= 116;

			if (LteScRSRP >= 0)
				LteScRSRP -= 141;
			if (LteScRSRQ >= 0)
				LteScRSRQ = LteScRSRQ / 2 - 20;
			if (LteScSinrUL >= 0)
				LteScSinrUL -= 11;
		} else if (type.equals("shenyang"))
		{
			returnShenyangMr(values);
		}
		return true;
	}

	public void returnShenyangMr(String[] values)
	{
		if (values[7].indexOf(":") > 0)
		{
			eci = DataGeter.GetInt(values[7].substring(0, values[7].indexOf(":")));
		} else
		{
			eci = DataGeter.GetInt(values[7]);
		}

		beginTime = values[11].replace("T", " ");
		ENBId = DataGeter.GetInt(values[6]);
		UserLabel = values[5];

		CellId = eci % 256;
		Earfcn = DataGeter.GetInt(values[22]);
		SubFrameNbr = DataGeter.GetInt("");
		MmeCode = DataGeter.GetInt(values[10]);
		MmeGroupId = DataGeter.GetInt(values[9]);
		MmeUeS1apId = DataGeter.GetInt(values[8]);
		Weight = 0;
		EventType = "";
		LteScRSRP = DataGeter.GetInt(values[12]);
		LteNcRSRP = DataGeter.GetInt(values[13]);
		LteScRSRQ = DataGeter.GetInt(values[14]);
		LteNcRSRQ = DataGeter.GetInt(values[15]);
		LteScEarfcn = DataGeter.GetInt(values[22]);
		LteScPci = DataGeter.GetInt(values[23]);
		LteNcEarfcn = DataGeter.GetInt(values[25]);
		LteNcPci = DataGeter.GetInt(values[26]);
		GsmNcellCarrierRSSI = DataGeter.GetInt(values[38]);
		GsmNcellBcch = DataGeter.GetInt(values[37]);
		GsmNcellNcc = DataGeter.GetInt(values[39]);
		GsmNcellBcc = DataGeter.GetInt(values[40]);
		TdsPccpchRSCP = DataGeter.GetInt(values[41]);
		TdsNcellUarfcn = DataGeter.GetInt(values[42]);
		TdsCellParameterId = DataGeter.GetInt(values[43]);
		LteScBSR = DataGeter.GetInt("");
		LteScRTTD = DataGeter.GetInt(values[52]);
		LteScTadv = DataGeter.GetInt(values[16]);
		LteScAOA = DataGeter.GetInt(values[18]);
		LteScPHR = DataGeter.GetInt(values[17]);
		LteScRIP = DataGeter.GetInt("");
		LteScSinrUL = DataGeter.GetInt(values[19]);

		for (int ii = 0; ii < LteScPlrULQci.length; ++ii)
		{
			LteScPlrULQci[ii] = 0;
			// LteScPlrULQci[ii] = DataConverter.GetInt(values[i++]);
		}

		for (int ii = 0; ii < LteScPlrDLQci.length; ++ii)
		{
			LteScPlrDLQci[ii] = 0;
			// LteScPlrDLQci[ii] = DataConverter.GetInt(values[i++]);
		}

		LteScRI1 = DataGeter.GetInt("");
		LteScRI2 = DataGeter.GetInt("");
		LteScRI4 = DataGeter.GetInt("");
		LteScRI8 = DataGeter.GetInt("");
		LteScPUSCHPRBNum = DataGeter.GetInt(values[53]);
		LteScPDSCHPRBNum = DataGeter.GetInt(values[54]);
		LteSceNBRxTxTimeDiff = DataGeter.GetInt(values[51]);

		// 鍒濆锟�杞寲锟�甯歌锟� if(LteNcRSRP >= 0) LteNcRSRP -= 141;
		if (LteNcRSRQ >= 0)
			LteNcRSRQ = LteNcRSRQ / 2 - 20;
		if (LteNcRSRP >= 0)
			LteNcRSRP -= 141;
		if (GsmNcellCarrierRSSI >= 0)
			GsmNcellCarrierRSSI -= 101;
		if (TdsPccpchRSCP >= 0)
			TdsPccpchRSCP -= 116;

		if (LteScRSRP >= 0)
			LteScRSRP -= 141;
		if (LteScRSRQ >= 0)
			LteScRSRQ = LteScRSRQ / 2 - 20;
		if (LteScSinrUL >= 0)
			LteScSinrUL -= 11;
	}

}
