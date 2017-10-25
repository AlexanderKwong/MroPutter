package cn.mastercom.sssvr.util;

public class SIGNAL_MR_SC
{
	public int cityID;
	public int SampleID;
	public int itime;
	public int wtimems;
	public int bms;
	public int ilongitude;
	public int ilatitude;
	public int ispeed;
	public int imode;
	public int iLAC;
	public int iCI;
	public int Eci;
	public long IMSI;
	public String MSISDN;
	public String UETac;
	public String UEBrand;
	public String UEType;
	public int serviceType;
	public int serviceSubType;
	public String urlDomain;
	public long IPDataUL;
	public long IPDataDL;
	public int duration;
	public float IPThroughputUL;
	public float IPThroughputDL;
	public int IPPacketUL;
	public int IPPacketDL;
	public int TCPReTranPacketUL;
	public int TCPReTranPacketDL;
	public int sessionRequest;
	public int sessionResult;
	public int eventType;
	public int userType;
	public String eNBName;
	public int eNBLongitude;
	public int eNBLatitude;
	public int eNBDistance;
	public String flag;
	public int ENBId;
	public String UserLabel;
	public int CellId;
	public int Earfcn;
	public int SubFrameNbr;
	public int MmeCode;
	public int MmeGroupId;
	public long MmeUeS1apId;
	public int Weight;
	public int LteScRSRP;
	public int LteScRSRQ;
	public int LteScEarfcn;
	public int LteScPci;
	public int LteScBSR;
	public int LteScRTTD;
	public int LteScTadv;
	public int LteScAOA;
	public int LteScPHR;
	public int LteScRIP;
	public int LteScSinrUL;
	public int LocFillType;
	public int testType;
	public int location;
	public int dist;
	public int radius;
	public String loctp;
	public int indoor;
	public String networktype;
	public String label;
	public int simuLongitude;
	public int simuLatitude;
	public int moveDirect;
	public String mrType;
	public int dfcnJamCellCount;
	public int sfcnJamCellCount;

	public SIGNAL_MR_SC()
	{
		Clear();
	}

	public void Clear()
	{
		cityID = StaticConfig.Int_Abnormal;
		SampleID = StaticConfig.Int_Abnormal;
		itime = StaticConfig.Int_Abnormal;
		wtimems = StaticConfig.Int_Abnormal;
		bms = StaticConfig.Int_Abnormal;
		ilongitude = StaticConfig.Int_Abnormal;
		ilatitude = StaticConfig.Int_Abnormal;
		ispeed = StaticConfig.Int_Abnormal;
		imode = StaticConfig.Int_Abnormal;
		iLAC = StaticConfig.Int_Abnormal;
		iCI = StaticConfig.Int_Abnormal;
		Eci = StaticConfig.Int_Abnormal;
		IMSI = StaticConfig.Long_Abnormal;
		MSISDN = "";
		UETac = "";
		UEBrand = "";
		UEType = "";
		serviceType = StaticConfig.Int_Abnormal;
		serviceSubType = StaticConfig.Int_Abnormal;
		urlDomain = "";
		IPDataUL = StaticConfig.Long_Abnormal;
		IPDataDL = StaticConfig.Long_Abnormal;
		duration = StaticConfig.Int_Abnormal;
		IPThroughputUL = StaticConfig.Float_Abnormal;
		IPThroughputDL = StaticConfig.Float_Abnormal;
		IPPacketUL = StaticConfig.Int_Abnormal;
		IPPacketDL = StaticConfig.Int_Abnormal;
		TCPReTranPacketUL = StaticConfig.Int_Abnormal;
		TCPReTranPacketDL = StaticConfig.Int_Abnormal;
		sessionRequest = StaticConfig.Int_Abnormal;
		sessionResult = StaticConfig.Int_Abnormal;
		eventType = StaticConfig.Int_Abnormal;
		userType = StaticConfig.Int_Abnormal;
		eNBName = "";
		eNBLongitude = StaticConfig.Int_Abnormal;
		eNBLatitude = StaticConfig.Int_Abnormal;
		eNBDistance = StaticConfig.Int_Abnormal;
		flag = "";
		ENBId = StaticConfig.Int_Abnormal;
		UserLabel = "";
		CellId = StaticConfig.Int_Abnormal;
		Earfcn = StaticConfig.Int_Abnormal;
		SubFrameNbr = StaticConfig.Int_Abnormal;
		MmeCode = StaticConfig.Int_Abnormal;
		MmeGroupId = StaticConfig.Int_Abnormal;
		MmeUeS1apId = StaticConfig.Long_Abnormal;
		Weight = StaticConfig.Int_Abnormal;
		LteScRSRP = StaticConfig.Int_Abnormal;
		LteScRSRQ = StaticConfig.Int_Abnormal;
		LteScEarfcn = StaticConfig.Int_Abnormal;
		LteScPci = StaticConfig.Int_Abnormal;
		LteScBSR = StaticConfig.Int_Abnormal;
		LteScRTTD = StaticConfig.Int_Abnormal;
		LteScTadv = StaticConfig.Int_Abnormal;
		LteScAOA = StaticConfig.Int_Abnormal;
		LteScPHR = StaticConfig.Int_Abnormal;
		LteScRIP = -32768;
		LteScSinrUL = StaticConfig.Int_Abnormal;
		LocFillType = StaticConfig.Int_Abnormal;
		testType = StaticConfig.Int_Abnormal;
		location = StaticConfig.Int_Abnormal;
		dist = StaticConfig.Int_Abnormal;
		radius = StaticConfig.Int_Abnormal;
		loctp = "";
		indoor = StaticConfig.Int_Abnormal;
		networktype = "";
		label = "";
		simuLongitude = StaticConfig.Int_Abnormal;
		simuLatitude = StaticConfig.Int_Abnormal;
		moveDirect = StaticConfig.Int_Abnormal;
		mrType = "";
		dfcnJamCellCount = StaticConfig.Int_Abnormal;
		sfcnJamCellCount = StaticConfig.Int_Abnormal;
	}

	public String GetDataBefore()
	{
		StringBuffer res = new StringBuffer();
		res.append(cityID);
		res.append(StaticConfig.DataSlipter);
		res.append(SampleID);
		res.append(StaticConfig.DataSlipter);
		res.append(itime);
		res.append(StaticConfig.DataSlipter);
		res.append(wtimems);
		res.append(StaticConfig.DataSlipter);
		res.append(bms);
		res.append(StaticConfig.DataSlipter);
		res.append(ilongitude);
		res.append(StaticConfig.DataSlipter);
		res.append(ilatitude);
		res.append(StaticConfig.DataSlipter);
		res.append(ispeed);
		res.append(StaticConfig.DataSlipter);
		res.append(imode);
		res.append(StaticConfig.DataSlipter);
		res.append(iLAC);
		res.append(StaticConfig.DataSlipter);
		res.append(iCI);
		res.append(StaticConfig.DataSlipter);
		res.append(Eci);
		res.append(StaticConfig.DataSlipter);
		res.append(IMSI);
		res.append(StaticConfig.DataSlipter);
		res.append(MSISDN);
		res.append(StaticConfig.DataSlipter);
		res.append(UETac);
		res.append(StaticConfig.DataSlipter);
		res.append(UEBrand);
		res.append(StaticConfig.DataSlipter);
		res.append(UEType);
		res.append(StaticConfig.DataSlipter);
		res.append(serviceType);
		res.append(StaticConfig.DataSlipter);
		res.append(serviceSubType);
		res.append(StaticConfig.DataSlipter);
		res.append(urlDomain);
		res.append(StaticConfig.DataSlipter);
		res.append(IPDataUL);
		res.append(StaticConfig.DataSlipter);
		res.append(IPDataDL);
		res.append(StaticConfig.DataSlipter);
		res.append(duration);
		res.append(StaticConfig.DataSlipter);
		res.append(IPThroughputUL);
		res.append(StaticConfig.DataSlipter);
		res.append(IPThroughputDL);
		res.append(StaticConfig.DataSlipter);
		res.append(IPPacketUL);
		res.append(StaticConfig.DataSlipter);
		res.append(IPPacketDL);
		res.append(StaticConfig.DataSlipter);
		res.append(TCPReTranPacketUL);
		res.append(StaticConfig.DataSlipter);
		res.append(TCPReTranPacketDL);
		res.append(StaticConfig.DataSlipter);
		res.append(sessionRequest);
		res.append(StaticConfig.DataSlipter);
		res.append(sessionResult);
		res.append(StaticConfig.DataSlipter);
		res.append(eventType);
		res.append(StaticConfig.DataSlipter);
		res.append(userType);
		res.append(StaticConfig.DataSlipter);
		res.append(eNBName);
		res.append(StaticConfig.DataSlipter);
		res.append(eNBLongitude);
		res.append(StaticConfig.DataSlipter);
		res.append(eNBLatitude);
		res.append(StaticConfig.DataSlipter);
		res.append(eNBDistance);
		res.append(StaticConfig.DataSlipter);
		res.append(flag);
		res.append(StaticConfig.DataSlipter);
		res.append(ENBId);
		res.append(StaticConfig.DataSlipter);
		res.append(UserLabel);
		res.append(StaticConfig.DataSlipter);
		res.append(CellId);
		res.append(StaticConfig.DataSlipter);
		res.append(Earfcn);
		res.append(StaticConfig.DataSlipter);
		res.append(SubFrameNbr);
		res.append(StaticConfig.DataSlipter);
		res.append(MmeCode);
		res.append(StaticConfig.DataSlipter);
		res.append(MmeGroupId);
		res.append(StaticConfig.DataSlipter);
		res.append(MmeUeS1apId);
		res.append(StaticConfig.DataSlipter);
		res.append(Weight);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScRSRP);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScRSRQ);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScEarfcn);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScPci);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScBSR);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScRTTD);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScTadv);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScAOA);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScPHR);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScRIP);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScSinrUL);
		res.append(StaticConfig.DataSlipter);
		return res.toString();
	}

	public String GetDataAfter()
	{
		StringBuffer res = new StringBuffer();
		res.append(LocFillType);
		res.append(StaticConfig.DataSlipter);
		res.append(testType);
		res.append(StaticConfig.DataSlipter);
		res.append(location);
		res.append(StaticConfig.DataSlipter);
		res.append(dist);
		res.append(StaticConfig.DataSlipter);
		res.append(radius);
		res.append(StaticConfig.DataSlipter);
		res.append(loctp);
		res.append(StaticConfig.DataSlipter);
		res.append(indoor);
		res.append(StaticConfig.DataSlipter);
		res.append(networktype);
		res.append(StaticConfig.DataSlipter);
		res.append(label);
		res.append(StaticConfig.DataSlipter);
		res.append(simuLongitude);
		res.append(StaticConfig.DataSlipter);
		res.append(simuLatitude);
		res.append(StaticConfig.DataSlipter);
		res.append(moveDirect);
		res.append(StaticConfig.DataSlipter);
		res.append(mrType);
		res.append(StaticConfig.DataSlipter);
		res.append(dfcnJamCellCount);
		res.append(StaticConfig.DataSlipter);
		res.append(sfcnJamCellCount);
		res.append(StaticConfig.DataSlipter);
		return res.toString();
	}

	/*
	 * public void FillDataOld(Object[] args) { String[] values =
	 * (String[])args[0]; Integer i = (Integer)args[1];
	 * 
	 * cityID = Integer.parseInt(values[i++]); SampleID =
	 * Integer.parseInt(values[i++]); itime = Integer.parseInt(values[i++]);
	 * wtimems = Integer.parseInt(values[i++]); ilongitude =
	 * Integer.parseInt(values[i++]); ilatitude = Integer.parseInt(values[i++]);
	 * IMSI = Long.parseLong(values[i++]); TAC = Integer.parseInt(values[i++]);
	 * ENBId = Integer.parseInt(values[i++]); UserLabel = values[i++]; CellId =
	 * Long.parseLong(values[i++]); Eci = Long.parseLong(values[i++]); Earfcn =
	 * Integer.parseInt(values[i++]); SubFrameNbr =
	 * Integer.parseInt(values[i++]); MmeCode = Integer.parseInt(values[i++]);
	 * MmeGroupId = Integer.parseInt(values[i++]); MmeUeS1apId =
	 * Long.parseLong(values[i++]); Weight = Integer.parseInt(values[i++]);
	 * EventType = values[i++]; LteScRSRP = Integer.parseInt(values[i++]);
	 * LteScRSRQ = Integer.parseInt(values[i++]); LteScEarfcn =
	 * Integer.parseInt(values[i++]); LteScPci = Integer.parseInt(values[i++]);
	 * 0~503 LteScBSR = Integer.parseInt(values[i++]); LteScRTTD =
	 * Integer.parseInt(values[i++]); LteScTadv = Integer.parseInt(values[i++]);
	 * LteScAOA = Integer.parseInt(values[i++]); LteScPHR =
	 * Integer.parseInt(values[i++]); LteScSinrUL =
	 * Integer.parseInt(values[i++]); LteScRIP = Integer.parseInt(values[i++]);
	 * 
	 * for(int ii = 0; ii<LteScPlrULQci.length; ++ii) { LteScPlrULQci[ii] =
	 * Integer.parseInt(values[i++]); }
	 * 
	 * for(int ii = 0; ii<LteScPlrDLQci.length; ++ii) { LteScPlrDLQci[ii] =
	 * Integer.parseInt(values[i++]); }
	 * 
	 * LteScRI1 = Integer.parseInt(values[i++]); LteScRI2 =
	 * Integer.parseInt(values[i++]); LteScRI4 = Integer.parseInt(values[i++]);
	 * LteScRI8 = Integer.parseInt(values[i++]); LteScPUSCHPRBNum =
	 * Integer.parseInt(values[i++]); LteScPDSCHPRBNum =
	 * Integer.parseInt(values[i++]); LteSceNBRxTxTimeDiff =
	 * Integer.parseInt(values[i++]);
	 * 
	 * args[1] = i; }
	 * 
	 * public void FillData(Object[] args) { String[] values =
	 * (String[])args[0]; Integer i = (Integer)args[1];
	 * 
	 * cityID = DataGeter.GetInt(values[i++]); SampleID =
	 * DataGeter.GetInt(values[i++]); itime = DataGeter.GetInt(values[i++]);
	 * wtimems = DataGeter.GetInt(values[i++]); ilongitude =
	 * DataGeter.GetInt(values[i++]); ilatitude = DataGeter.GetInt(values[i++]);
	 * IMSI = DataGeter.GetLong(values[i++]); TAC =
	 * DataGeter.GetInt(values[i++]); ENBId = DataGeter.GetInt(values[i++]);
	 * UserLabel = DataGeter.GetString(values[i++]); CellId =
	 * DataGeter.GetLong(values[i++]); Eci = DataGeter.GetLong(values[i++]);
	 * Earfcn = DataGeter.GetInt(values[i++]); SubFrameNbr =
	 * DataGeter.GetInt(values[i++]); MmeCode = DataGeter.GetInt(values[i++]);
	 * MmeGroupId = DataGeter.GetInt(values[i++]); MmeUeS1apId =
	 * DataGeter.GetLong(values[i++]); Weight = DataGeter.GetInt(values[i++]);
	 * EventType = DataGeter.GetString(values[i++]); LteScRSRP =
	 * DataGeter.GetInt(values[i++]); LteScRSRQ = DataGeter.GetInt(values[i++]);
	 * LteScEarfcn = DataGeter.GetInt(values[i++]); LteScPci =
	 * DataGeter.GetInt(values[i++]); 0~503 LteScBSR =
	 * DataGeter.GetInt(values[i++]); LteScRTTD = DataGeter.GetInt(values[i++]);
	 * LteScTadv = DataGeter.GetInt(values[i++]); LteScAOA =
	 * DataGeter.GetInt(values[i++]); LteScPHR = DataGeter.GetInt(values[i++]);
	 * LteScSinrUL = DataGeter.GetInt(values[i++]); LteScRIP =
	 * DataGeter.GetInt(values[i++]);
	 * 
	 * for(int ii = 0; ii<LteScPlrULQci.length; ++ii) { LteScPlrULQci[ii] =
	 * DataGeter.GetInt(values[i++]); }
	 * 
	 * for(int ii = 0; ii<LteScPlrDLQci.length; ++ii) { LteScPlrDLQci[ii] =
	 * DataGeter.GetInt(values[i++]); }
	 * 
	 * LteScRI1 = DataGeter.GetInt(values[i++]); LteScRI2 =
	 * DataGeter.GetInt(values[i++]); LteScRI4 = DataGeter.GetInt(values[i++]);
	 * LteScRI8 = DataGeter.GetInt(values[i++]); LteScPUSCHPRBNum =
	 * DataGeter.GetInt(values[i++]); LteScPDSCHPRBNum =
	 * DataGeter.GetInt(values[i++]); LteSceNBRxTxTimeDiff =
	 * DataGeter.GetInt(values[i++]);
	 * 
	 * args[1] = i; }
	 */
}
