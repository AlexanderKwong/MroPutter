package cn.mastercom.sssvr.util;

import java.util.ArrayList;
import java.util.List;

public class Sample_4G
{
	public int cityID;
	public int SampleID;
	public int itime;
	public short wtimems;
	public byte bms;
	public int ilongitude;
	public int ilatitude;
	public int ispeed;
	public short imode;
	public int iLAC;
	public long iCI;
	public long Eci;
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
	public double IPThroughputUL;
	public double IPThroughputDL;
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
	public long CellId;
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
	public short[] nccount;
	public NC_LTE[] tlte;
	public NC_TDS[] ttds;
	public NC_GSM[] tgsm;
	public SC_FRAME[] trip;
	public int LocFillType;// 0是正常????是经纬度回填
	public int testType;
	public int location;
	public long dist;
	public double radius;
	public String loctp;
	public int indoor;
	public String networktype;
	public String lable;// static,low,unknow,high
	public int simuLongitude;
	public int simuLatitude;
	public int moveDirect;
	public String mrType;
	public int dfcnJamCellCount;// 异频干扰小区个数
	public int sfcnJamCellCount;// 同频干扰小区个数s

	public int LteScPUSCHPRBNum;
	public int LteScPDSCHPRBNum;
	public int LteSceNBRxTxTimeDiff;
	// new add 前后5分钟小区切换信息
	public String eciSwitchList;
	// -------------------------------------------mr新统计分YD/LT/DX 2017.6.14

	// 新添加字段表示经纬度来源和运动状态
	public String xdrid;// 关联XDRID
	public String wifilist;// wifi列表
	public String wifimac1;// wifi地址
	public int wifirssi1;// wifi强度
	public String wifimac2;// wifi地址
	public int wifirssi2;// wifi强度
	public int LteScRSRP_DX; // 电信主服场强
	public int LteScRSRQ_DX; // 电信主服信号质量
	public int LteScEarfcn_DX; // 电信主服频点
	public int LteScPci_DX; // 电信主服PCI
	public int LteScRSRP_LT; // 联通主服场强
	public int LteScRSRQ_LT; // 联通主服信号质量
	public int LteScEarfcn_LT; // 联通主服频点
	public int LteScPci_LT; // 联通主服PCI

	public int locSource;// 位置来源ott/gps/fg
	public int samState;// int or out

	public int Overlap;
	public int OverlapAll;
	public int speed;

	public Sample_4G()
	{
		nccount = new short[4];
		tlte = new NC_LTE[6];
		ttds = new NC_TDS[6];
		tgsm = new NC_GSM[6];
		trip = new SC_FRAME[10];
		LocFillType = 0;

		location = -1;
		dist = -1;
		radius = -1;
		loctp = "";
		indoor = -1;

		simuLongitude = 0;
		simuLatitude = 0;

		Clear();
	}

	public void Clear()
	{
		ENBId = StaticConfig.Int_Abnormal;
		CellId = StaticConfig.Long_Abnormal;
		Earfcn = StaticConfig.Int_Abnormal;
		MmeCode = StaticConfig.Int_Abnormal;
		MmeGroupId = StaticConfig.Int_Abnormal;
		MmeUeS1apId = StaticConfig.Int_Abnormal;
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
		LteScSinrUL = StaticConfig.Int_Abnormal;
		for (int i = 0; i < nccount.length; i++)
		{
			nccount[i] = 0;
		}

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

		testType = -1;

		MSISDN = "";
		UETac = "";
		UEBrand = "";
		UEType = "";
		urlDomain = "";
		eNBName = "";
		flag = "";
		UserLabel = "";
		loctp = "";
		networktype = "";
		lable = "";

		moveDirect = -1;
		mrType = "";

		LocFillType = 0;

		location = -1;
		dist = -1;
		radius = -1;
		loctp = "";
		indoor = -1;

		simuLongitude = 0;
		simuLatitude = 0;

		sfcnJamCellCount = 0;
		dfcnJamCellCount = 0;
	};

	public boolean isOriginalLoction()
	{
		if (indoor >= 0)
		{
			return true;
		}
		return false;
	}

	public NC_LTE getNclte_Freq(int freq)
	{
		NC_LTE resLte = null;

		switch (freq)
		{
		case 1650:
			if (tgsm[1].GsmNcellBcch != 1650)
			{
				break;
			}
			resLte = new NC_LTE();
			resLte.LteNcEarfcn = tgsm[1].GsmNcellBcch;
			resLte.LteNcPci = tgsm[1].GsmNcellBsic;
			resLte.LteNcRSRP = tgsm[1].GsmNcellCarrierRSSI / 1000 - 200;
			resLte.LteNcRSRQ = tgsm[1].GsmNcellCarrierRSSI % 1000 - 200;
			break;

		case 1850:
			if (tgsm[2].GsmNcellBcch != 1850)
			{
				break;
			}
			resLte = new NC_LTE();
			resLte.LteNcEarfcn = tgsm[2].GsmNcellBcch;
			resLte.LteNcPci = tgsm[2].GsmNcellBsic;
			resLte.LteNcRSRP = tgsm[2].GsmNcellCarrierRSSI / 1000 - 200;
			resLte.LteNcRSRQ = tgsm[2].GsmNcellCarrierRSSI % 1000 - 200;
			break;

		case 2560:
			if (tgsm[3].GsmNcellBcch != 2560)
			{
				break;
			}
			resLte = new NC_LTE();
			resLte.LteNcEarfcn = tgsm[3].GsmNcellBcch;
			resLte.LteNcPci = tgsm[3].GsmNcellBsic;
			resLte.LteNcRSRP = tgsm[3].GsmNcellCarrierRSSI / 1000 - 200;
			resLte.LteNcRSRQ = tgsm[3].GsmNcellCarrierRSSI % 1000 - 200;
			break;

		case 1862:
			if (tgsm[4].GsmNcellBcch != 1862)
			{
				break;
			}
			resLte = new NC_LTE();
			resLte.LteNcEarfcn = tgsm[4].GsmNcellBcch;
			resLte.LteNcPci = tgsm[4].GsmNcellBsic;
			resLte.LteNcRSRP = tgsm[4].GsmNcellCarrierRSSI / 1000 - 200;
			resLte.LteNcRSRQ = tgsm[4].GsmNcellCarrierRSSI % 1000 - 200;
			break;

		case 1865:
			if (tgsm[5].GsmNcellBcch != 1865)
			{
				break;
			}
			resLte = new NC_LTE();
			resLte.LteNcEarfcn = tgsm[5].GsmNcellBcch;
			resLte.LteNcPci = tgsm[5].GsmNcellBsic;
			resLte.LteNcRSRP = tgsm[5].GsmNcellCarrierRSSI / 1000 - 200;
			resLte.LteNcRSRQ = tgsm[5].GsmNcellCarrierRSSI % 1000 - 200;
			break;

		case 1867:
			if (ttds[2].TdsNcellUarfcn != 1867)
			{
				break;
			}
			resLte = new NC_LTE();
			resLte.LteNcEarfcn = ttds[2].TdsNcellUarfcn;
			resLte.LteNcPci = ttds[2].TdsCellParameterId;
			resLte.LteNcRSRP = ttds[2].TdsPccpchRSCP / 1000 - 200;
			resLte.LteNcRSRQ = ttds[2].TdsPccpchRSCP % 1000 - 200;
			break;

		case 1870:
			if (ttds[3].TdsNcellUarfcn != 1870)
			{
				break;
			}
			resLte = new NC_LTE();
			resLte.LteNcEarfcn = ttds[3].TdsNcellUarfcn;
			resLte.LteNcPci = ttds[3].TdsCellParameterId;
			resLte.LteNcRSRP = ttds[3].TdsPccpchRSCP / 1000 - 200;
			resLte.LteNcRSRQ = ttds[3].TdsPccpchRSCP % 1000 - 200;
			break;

		case 2117:
			if (ttds[4].TdsNcellUarfcn != 2117)
			{
				break;
			}
			resLte = new NC_LTE();
			resLte.LteNcEarfcn = ttds[4].TdsNcellUarfcn;
			resLte.LteNcPci = ttds[4].TdsCellParameterId;
			resLte.LteNcRSRP = ttds[4].TdsPccpchRSCP / 1000 - 200;
			resLte.LteNcRSRQ = ttds[4].TdsPccpchRSCP % 1000 - 200;
			break;

		case 2120:
			if (ttds[5].TdsNcellUarfcn != 2120)
			{
				break;
			}
			resLte = new NC_LTE();
			resLte.LteNcEarfcn = ttds[5].TdsNcellUarfcn;
			resLte.LteNcPci = ttds[5].TdsCellParameterId;
			resLte.LteNcRSRP = ttds[5].TdsPccpchRSCP / 1000 - 200;
			resLte.LteNcRSRQ = ttds[5].TdsPccpchRSCP % 1000 - 200;
			break;

		default:
			break;
		}

		return resLte;
	}

	public List<NC_LTE> getNclte_Freq()
	{
		List<NC_LTE> itemList = new ArrayList<NC_LTE>();
		for (int i = 0; i < 9; ++i)
		{
			NC_LTE resLte = null;
			if (i <= 4)
			{
				if (tgsm[i + 1].GsmNcellBcch > 0)
				{
					resLte = new NC_LTE();
					resLte.LteNcEarfcn = tgsm[i + 1].GsmNcellBcch;
					resLte.LteNcPci = tgsm[i + 1].GsmNcellBsic;
					resLte.LteNcRSRP = tgsm[i + 1].GsmNcellCarrierRSSI / 1000 - 200;
					resLte.LteNcRSRQ = tgsm[i + 1].GsmNcellCarrierRSSI % 1000 - 200;
				}
				else
				{
					break;
				}
			}
			else
			{
				if (ttds[i - 3].TdsNcellUarfcn > 0)
				{
					resLte = new NC_LTE();
					resLte.LteNcEarfcn = ttds[i - 3].TdsNcellUarfcn;
					resLte.LteNcPci = ttds[i - 3].TdsCellParameterId;
					resLte.LteNcRSRP = ttds[i - 3].TdsPccpchRSCP / 1000 - 200;
					resLte.LteNcRSRQ = ttds[i - 3].TdsPccpchRSCP % 1000 - 200;
				}
				else
				{
					break;
				}
			}

			if (resLte != null)
			{
				itemList.add(resLte);
			}
		}
		return itemList;
	}

	public void returnSample(String values[])
	{
		int i = 0;
		cityID = Integer.parseInt(values[i++]);
		SampleID = Integer.parseInt(values[i++]);
		itime = Integer.parseInt(values[i++]);
		wtimems = Short.parseShort(values[i++]);
		bms = Byte.parseByte(values[i++]);
		ilongitude = Integer.parseInt(values[i++]);
		ilatitude = Integer.parseInt(values[i++]);
		ispeed = Integer.parseInt(values[i++]);
		imode = Short.parseShort(values[i++]);
		iLAC = Integer.parseInt(values[i++]);
		iCI = Long.parseLong(values[i++]);
		Eci = Long.parseLong(values[i++]);
		IMSI = Long.parseLong(values[i++]);
		MSISDN = values[i++];
		UETac = values[i++];
		UEBrand = values[i++];
		UEType = values[i++];
		serviceType = Integer.parseInt(values[i++]);
		serviceSubType = Integer.parseInt(values[i++]);
		urlDomain = values[i++];
		IPDataUL = Long.parseLong(values[i++]);
		IPDataDL = Long.parseLong(values[i++]);
		duration = Integer.parseInt(values[i++]);
		IPThroughputUL = Double.parseDouble(values[i++]);
		IPThroughputDL = Double.parseDouble(values[i++]);
		IPPacketUL = Integer.parseInt(values[i++]);
		IPPacketDL = Integer.parseInt(values[i++]);
		TCPReTranPacketUL = Integer.parseInt(values[i++]);
		TCPReTranPacketDL = Integer.parseInt(values[i++]);
		sessionRequest = Integer.parseInt(values[i++]);
		sessionResult = Integer.parseInt(values[i++]);
		try
		{
			eventType = Integer.parseInt(values[i++]);
		}
		catch (NumberFormatException e)
		{
			eventType = -1000000;
		}
		userType = Integer.parseInt(values[i++]);
		eNBName = values[i++];
		eNBLongitude = Integer.parseInt(values[i++]);
		eNBLatitude = Integer.parseInt(values[i++]);
		eNBDistance = Integer.parseInt(values[i++]);
		flag = values[i++];
		ENBId = Integer.parseInt(values[i++]);
		UserLabel = values[i++];
		CellId = Long.parseLong(values[i++]);
		Earfcn = Integer.parseInt(values[i++]);
		SubFrameNbr = Integer.parseInt(values[i++]);
		MmeCode = Integer.parseInt(values[i++]);
		MmeGroupId = Integer.parseInt(values[i++]);
		MmeUeS1apId = Long.parseLong(values[i++]);
		Weight = Integer.parseInt(values[i++]);
		LteScRSRP = Integer.parseInt(values[i++]);
		LteScRSRQ = Integer.parseInt(values[i++]);
		LteScEarfcn = Integer.parseInt(values[i++]);
		LteScPci = Integer.parseInt(values[i++]);
		LteScBSR = Integer.parseInt(values[i++]);
		LteScRTTD = Integer.parseInt(values[i++]);
		LteScTadv = Integer.parseInt(values[i++]);
		LteScAOA = Integer.parseInt(values[i++]);
		LteScPHR = Integer.parseInt(values[i++]);
		LteScRIP = Integer.parseInt(values[i++]);
		LteScSinrUL = Integer.parseInt(values[i++]);
		for (int j = 0; j < nccount.length; j++)
		{
			nccount[j] = Short.parseShort(values[i++]);
		}
		for (int j = 0; j < 6; j++)
		{
			tlte[j].LteNcRSRP = Integer.parseInt(values[i++]);
			tlte[j].LteNcRSRQ = Integer.parseInt(values[i++]);
			tlte[j].LteNcEarfcn = Integer.parseInt(values[i++]);
			tlte[j].LteNcPci = Integer.parseInt(values[i++]);
		}
		for (int j = 0; j < 6; j++)
		{
			ttds[j].TdsPccpchRSCP = Integer.parseInt(values[i++]);
			ttds[j].TdsNcellUarfcn = Integer.parseInt(values[i++]);
			ttds[j].TdsCellParameterId = Integer.parseInt(values[i++]);
		}

		for (int j = 0; j < 6; j++)
		{
			tgsm[j].GsmNcellCarrierRSSI = Integer.parseInt(values[i++]);
			tgsm[j].GsmNcellBcch = Integer.parseInt(values[i++]);
			tgsm[j].GsmNcellBsic = Integer.parseInt(values[i++]);
		}
		for (int j = 0; j < trip.length; j++)
		{
			trip[j].Earfcn = Integer.parseInt(values[i++]);
			trip[j].SubFrame = Short.parseShort(values[i++]);
			trip[j].LteScRIP = Short.parseShort(values[i++]);
		}
		LocFillType = Integer.parseInt(values[i++]);// 0是正常????是经纬度回填
		testType = Integer.parseInt(values[i++]);
		location = Integer.parseInt(values[i++]);
		dist = Long.parseLong(values[i++]);
		radius = Double.parseDouble(values[i++]);
		loctp = values[i++];
		indoor = Integer.parseInt(values[i++]);
		networktype = values[i++];
		lable = values[i++];// static,low,unknow,high
		simuLongitude = Integer.parseInt(values[i++]);
		simuLatitude = Integer.parseInt(values[i++]);
		moveDirect = Integer.parseInt(values[i++]);
		mrType = values[i++];
		dfcnJamCellCount = Integer.parseInt(values[i++]);// 异频干扰小区个数
		sfcnJamCellCount = Integer.parseInt(values[i++]);// 同频干扰小区个数
	}

	public static void main(String args[])
	{
		String temp = "6401	1500865424	337	1064903410	386435249	-1	0	460027952869216	null	null	null	0	null	0	149993347	81311564	-89	-6	38400	289	0	-1000000	9	169	33	0	24	2	0	0	-101	-6	38400	304	-104	-6	38400	221	-1000000	-1000000	-1000000	-1000000	-1000000	-1000000	-1000000	-1000000	-1000000	-1000000	-1000000	-1000000	-1000000	-1000000	-1000000	-1000000	-1000000	-1000000	-1000000	-1000000	-1000000	-1000000	-1000000	-1000000	-1000000	65	65	0	0	0	0	0	0	0	0	4	3916	40	wf	-1	static";
		Sample_4G sample = new Sample_4G();
		sample.returnTBMRSample(temp.split("\t", -1));
		System.out.println(sample.ilongitude + " " + sample.ilatitude + " " + sample.Eci + " " + sample.LteScRSRP + " " + sample.cityID);
		NC_LTE[] tlte = sample.tlte;
		for (NC_LTE nc : tlte)
		{
			System.out.println(nc.LteNcEarfcn + " " + nc.LteNcPci + " " + nc.LteNcRSRP);
		}
	}

	public void returnTBMRSample(String values[])
	{
		int i = 0;
		cityID = Integer.parseInt(values[i++]);
		itime = Integer.parseInt(values[i++]);
		wtimems = Short.parseShort(values[i++]);
		ilongitude = Integer.parseInt(values[i++]);
		ilatitude = Integer.parseInt(values[i++]);
		ispeed = Integer.parseInt(values[i++]);
		imode = Short.parseShort(values[i++]);
		IMSI = Long.parseLong(values[i++]);
		xdrid = values[i++];
		wifilist = values[i++];
		wifimac1 = values[i++];
		wifirssi1 = Integer.parseInt(values[i++]);
		wifimac2 = values[i++];
		wifirssi2 = Integer.parseInt(values[i++]);
		Eci = Long.parseLong(values[i++]);
		MmeUeS1apId = Long.parseLong(values[i++]);
		LteScRSRP = Integer.parseInt(values[i++]);
		LteScRSRQ = Integer.parseInt(values[i++]);
		LteScEarfcn = Integer.parseInt(values[i++]);
		LteScPci = Integer.parseInt(values[i++]);
		LteScBSR = Integer.parseInt(values[i++]);
		LteScRTTD = Integer.parseInt(values[i++]);
		LteScTadv = Integer.parseInt(values[i++]);
		LteScAOA = Integer.parseInt(values[i++]);
		LteScPHR = Integer.parseInt(values[i++]);
		LteScRIP = Integer.parseInt(values[i++]);
		LteScSinrUL = Integer.parseInt(values[i++]);
		nccount[0] = Short.parseShort(values[i++]);
		nccount[1] = Short.parseShort(values[i++]);
		nccount[2] = Short.parseShort(values[i++]);
		for (int j = 0; j < 6; j++)
		{
			tlte[j].LteNcRSRP = Integer.parseInt(values[i++]);
			tlte[j].LteNcRSRQ = Integer.parseInt(values[i++]);
			tlte[j].LteNcEarfcn = Integer.parseInt(values[i++]);
			tlte[j].LteNcPci = Integer.parseInt(values[i++]);
		}

		for (int j = 0; j < 3; j++)
		{
			tgsm[j].GsmNcellCarrierRSSI = Integer.parseInt(values[i++]);
			tgsm[j].GsmNcellBcch = Integer.parseInt(values[i++]);
			tgsm[j].GsmNcellBsic = Integer.parseInt(values[i++]);
		}
		Overlap = Integer.parseInt(values[i++]);
		OverlapAll = Integer.parseInt(values[i++]);
		LteScRSRP_DX = Integer.parseInt(values[i++]);
		LteScRSRQ_DX = Integer.parseInt(values[i++]);
		LteScEarfcn_DX = Integer.parseInt(values[i++]);
		LteScPci_DX = Integer.parseInt(values[i++]);
		LteScRSRP_LT = Integer.parseInt(values[i++]);
		LteScRSRQ_LT = Integer.parseInt(values[i++]);
		LteScEarfcn_LT = Integer.parseInt(values[i++]);
		LteScPci_LT = Integer.parseInt(values[i++]);
		location = Integer.parseInt(values[i++]);
		dist = Long.parseLong(values[i++]);
		radius = Double.parseDouble(values[i++]);
		loctp = values[i++];
		speed = Integer.parseInt(values[i++]);
		UserLabel = values[i++];

	}

	public int returnSampleTenOrForty()
	{
		if (imode == -2)
		{
			return 10;
		}
		else if ((ilongitude / 4000) * 4000 + 2000 == ilongitude)
		{
			return 40;
		}
		else
		{
			return 10;
		}
	}

	public String GetData()
	{
		StringBuffer sb = new StringBuffer();
		String TabMark = StaticConfig.DataSlipter;
		// sb.append(samKey);sb.append(TabMark);

		sb.append(cityID);
		sb.append(TabMark);
		sb.append(SampleID);
		sb.append(TabMark);
		sb.append(itime);
		sb.append(TabMark);
		sb.append(wtimems);
		sb.append(TabMark);
		sb.append(bms);
		sb.append(TabMark);
		sb.append(ilongitude);
		sb.append(TabMark);
		sb.append(ilatitude);
		sb.append(TabMark);
		sb.append(ispeed);
		sb.append(TabMark);
		sb.append(imode);
		sb.append(TabMark);
		sb.append(iLAC);
		sb.append(TabMark);
		sb.append(iCI);
		sb.append(TabMark);
		sb.append(Eci);
		sb.append(TabMark);
		sb.append(StringUtil.EncryptStringToLong(IMSI + ""));
		sb.append(TabMark);
		sb.append(StringUtil.EncryptStringToLong(MSISDN + ""));
		sb.append(TabMark);
		sb.append(UETac);
		sb.append(TabMark);
		sb.append(UEBrand);
		sb.append(TabMark);
		sb.append(UEType);
		sb.append(TabMark);
		sb.append(serviceType);
		sb.append(TabMark);
		sb.append(serviceSubType);
		sb.append(TabMark);
		sb.append(urlDomain);
		sb.append(TabMark);
		sb.append(IPDataUL);
		sb.append(TabMark);
		sb.append(IPDataDL);
		sb.append(TabMark);
		sb.append(duration);
		sb.append(TabMark);
		sb.append(IPThroughputUL);
		sb.append(TabMark);
		sb.append(IPThroughputDL);
		sb.append(TabMark);
		sb.append(IPPacketUL);
		sb.append(TabMark);
		sb.append(IPPacketDL);
		sb.append(TabMark);
		sb.append(TCPReTranPacketUL);
		sb.append(TabMark);
		sb.append(TCPReTranPacketDL);
		sb.append(TabMark);
		sb.append(sessionRequest);
		sb.append(TabMark);
		sb.append(sessionResult);
		sb.append(TabMark);
		sb.append(eventType);
		sb.append(TabMark);
		sb.append(userType);
		sb.append(TabMark);
		sb.append(eNBName);
		sb.append(TabMark);
		sb.append(eNBLongitude);
		sb.append(TabMark);
		sb.append(eNBLatitude);
		sb.append(TabMark);
		sb.append(eNBDistance);
		sb.append(TabMark);
		sb.append(flag);
		sb.append(TabMark);
		sb.append(ENBId);
		sb.append(TabMark);
		sb.append(UserLabel);
		sb.append(TabMark);
		sb.append(CellId);
		sb.append(TabMark);
		sb.append(Earfcn);
		sb.append(TabMark);
		sb.append(SubFrameNbr);
		sb.append(TabMark);
		sb.append(MmeCode);
		sb.append(TabMark);
		sb.append(MmeGroupId);
		sb.append(TabMark);
		sb.append(MmeUeS1apId);
		sb.append(TabMark);
		sb.append(Weight);
		sb.append(TabMark);
		sb.append(LteScRSRP);
		sb.append(TabMark);
		sb.append(LteScRSRQ);
		sb.append(TabMark);
		sb.append(LteScEarfcn);
		sb.append(TabMark);
		sb.append(LteScPci);
		sb.append(TabMark);
		sb.append(LteScBSR);
		sb.append(TabMark);
		sb.append(LteScRTTD);
		sb.append(TabMark);
		sb.append(LteScTadv);
		sb.append(TabMark);
		sb.append(LteScAOA);
		sb.append(TabMark);
		sb.append(LteScPHR);
		sb.append(TabMark);
		sb.append(LteScRIP);
		sb.append(TabMark);
		sb.append(LteScSinrUL);
		sb.append(TabMark);
		sb.append(nccount[0]);
		sb.append(TabMark);
		sb.append(nccount[1]);
		sb.append(TabMark);
		sb.append(nccount[2]);
		sb.append(TabMark);
		sb.append(nccount[3]);
		sb.append(TabMark);

		for (int i = 0; i < tlte.length; ++i)
		{
			sb.append(tlte[i].LteNcRSRP);
			sb.append(TabMark);
			sb.append(tlte[i].LteNcRSRQ);
			sb.append(TabMark);
			sb.append(tlte[i].LteNcEarfcn);
			sb.append(TabMark);
			sb.append(tlte[i].LteNcPci);
			sb.append(TabMark);
		}

		for (int i = 0; i < ttds.length; ++i)
		{
			sb.append(ttds[i].TdsPccpchRSCP);
			sb.append(TabMark);
			sb.append(ttds[i].TdsNcellUarfcn);
			sb.append(TabMark);
			sb.append(ttds[i].TdsCellParameterId);
			sb.append(TabMark);
		}

		for (int i = 0; i < tgsm.length; ++i)
		{
			sb.append(tgsm[i].GsmNcellCarrierRSSI);
			sb.append(TabMark);
			sb.append(tgsm[i].GsmNcellBcch);
			sb.append(TabMark);
			sb.append(tgsm[i].GsmNcellBsic);
			sb.append(TabMark);
		}

		for (int i = 0; i < trip.length; ++i)
		{
			sb.append(trip[i].Earfcn);
			sb.append(TabMark);
			sb.append(trip[i].SubFrame);
			sb.append(TabMark);
			sb.append(trip[i].LteScRIP);
			sb.append(TabMark);
		}
		sb.append(LocFillType);
		sb.append(TabMark);

		sb.append(testType);
		sb.append(TabMark);
		sb.append(location);
		sb.append(TabMark);
		sb.append(dist);
		sb.append(TabMark);
		sb.append(radius);
		sb.append(TabMark);
		sb.append(loctp);
		sb.append(TabMark);
		sb.append(indoor);
		sb.append(TabMark);
		sb.append(networktype);
		sb.append(TabMark);
		sb.append(lable);
		sb.append(TabMark);
		sb.append(simuLongitude);
		sb.append(TabMark);
		sb.append(simuLatitude);
		sb.append(TabMark);

		sb.append(moveDirect);
		sb.append(TabMark);
		sb.append(mrType);
		sb.append(TabMark);

		sb.append(dfcnJamCellCount);
		sb.append(TabMark);
		sb.append(sfcnJamCellCount);

		return sb.toString();
	}
}
