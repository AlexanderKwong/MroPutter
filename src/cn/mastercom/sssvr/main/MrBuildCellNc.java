package cn.mastercom.sssvr.main;

import cn.mastercom.sssvr.util.IResultTable;
import cn.mastercom.sssvr.util.LteCellConfig;
import cn.mastercom.sssvr.util.LteCellInfo;
import cn.mastercom.sssvr.util.NC_LTE;
import cn.mastercom.sssvr.util.ResultHelper;

public class MrBuildCellNc implements IResultTable
{
	public int iCityID = 0;
	public int iBuildingID = 0;
	public int iECI = 0;
	public int iEarfcn = 0;
	public int iPci = 0;
	public int iTime = 0;
	public int iASNei_MRCnt = 0;
	public double fASNei_RSRPValue = 0;

	public MrBuildCellNc(MrBuildCell item, NC_LTE nclte)
	{
		iCityID = item.iCityID;
		iBuildingID = item.iBuildingID;
		iECI = item.iECI;
		iEarfcn = nclte.LteNcEarfcn;
		iPci = nclte.LteNcPci;
		iTime = item.iTime;
	}

	public static final String TypeName = "mrbuildcellnc";

	public static String getKey(MrBuildCell item, NC_LTE nclte)
	{
		// 此处按天计算,所以没有添加时间因素
		return item.iCityID + "_" + item.iBuildingID + "_" + item.iECI + "_" + nclte.LteNcEarfcn + "_" + nclte.LteNcPci;
	}

	public void Stat(NC_LTE nclte)
	{
		if (nclte.LteNcRSRP != -1000000)
		{
			iASNei_MRCnt++;
			fASNei_RSRPValue += nclte.LteNcRSRP;
		}
	}

	@Override
	public String toLine()
	{
		StringBuffer sb = new StringBuffer();
		String tabMark = ResultHelper.TabMark;

		sb.append(iCityID);
		sb.append(tabMark);
		sb.append(iBuildingID);
		sb.append(tabMark);
		sb.append(iECI);
		sb.append(tabMark);
		sb.append(getNbEci());
		sb.append(tabMark);
		sb.append(iEarfcn);
		sb.append(tabMark);
		sb.append(iPci);
		sb.append(tabMark);
		sb.append(iTime);
		sb.append(tabMark);
		sb.append(iASNei_MRCnt);
		sb.append(tabMark);
		sb.append(fASNei_RSRPValue);
		return sb.toString();
	}

	public long getNbEci()
	{
		LteCellInfo nbCell = null;
		try
		{
			nbCell = LteCellConfig.GetInstance().getNearestCell(iECI, iEarfcn, iPci);
			if (nbCell != null)
				return nbCell.eci;
		}
		catch (Exception e)
		{
		}
		return -1;
	}

}
