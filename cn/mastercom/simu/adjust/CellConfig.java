package cn.mastercom.simu.adjust;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.mastercom.sssvr.util.GisFunction;
import cn.mastercom.sssvr.util.LteCellInfo;

public class CellConfig
{
	public Map<Long, LteCellInfo> lteCellInfoMap;
	public Map<Long, List<LteCellInfo>> fcnPciLteCellMap;
	private static CellConfig instance;

	private CellConfig()
	{
	}

	public static CellConfig getInstance()
	{
		if (instance == null)
		{
			instance = new CellConfig();
		}
		return instance;
	}

	public static void main(String args[])
	{
		CellConfig.getInstance().loadLteCell("D:\\tb_cfg_city_cell_yunnan.txt");
	}

	public boolean loadLteCell(String filePath)
	{
		try
		{
			BufferedReader reader = null;
			lteCellInfoMap = new HashMap<Long, LteCellInfo>();
			fcnPciLteCellMap = new HashMap<Long, List<LteCellInfo>>();
			try
			{ 
				reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
				String strData;
				String[] values;
				long eci;
				long fcnPciKey;
				List<LteCellInfo> fcnPciList = null;
				while ((strData = reader.readLine()) != null)
				{
					if (strData.trim().length() == 0)
					{
						continue;
					}
					try
					{
						values = strData.split("\t", -1);
						if (values.length < 12)
						{
							System.out.println("cell config error: " + strData);
							return false;
						}
						LteCellInfo item = LteCellInfo.FillData(values);
						if (item.enbid > 0 && item.cellid > 0)
						{
							eci = item.enbid * 256 + item.cellid;
							lteCellInfoMap.put(eci, item);
						}
						if (item.pci > 0 && item.fcn > 0 && item.cityid > 0)
						{
							fcnPciKey = Long.parseLong(String.format("%02d%05d%03d", item.cityid, item.fcn, item.pci));
							fcnPciList = fcnPciLteCellMap.get(fcnPciKey);
							if (fcnPciList == null)
							{
								fcnPciList = new ArrayList<LteCellInfo>();
								fcnPciLteCellMap.put(fcnPciKey, fcnPciList);
							}
							fcnPciList.add(item);
						}
					}
					catch (Exception e)
					{
						OutLog.dosom(e);
						return false;
					}
				}
			}
			catch (Exception e)
			{
				OutLog.dosom(e);
				return false;
			}
			finally
			{
				if (reader != null)
				{
					reader.close();
				}
			}

		}
		catch (Exception e)
		{
			OutLog.dosom(e);
			return false;
		}

		return true;
	}

	public LteCellInfo getNearestCell(int longtitude, int latitude, int cityID, int fcn, int pci)
	{
		if (longtitude <= 0 || latitude <= 0 || cityID <= 0 || fcn <= 0 || pci <= 0)
		{
			return null;
		}

		long fcnPciKey = Long.parseLong(String.format("%02d%05d%03d", cityID, fcn, pci));
		List<LteCellInfo> fcnPciList = null;
		fcnPciList = fcnPciLteCellMap.get(fcnPciKey);
		if (fcnPciList == null)
		{
			return null;
		}
		int distance = Integer.MAX_VALUE;
		int curDistance = 0;
		LteCellInfo nearestCell = null;
		for (LteCellInfo item : fcnPciList)
		{
			if (Math.abs(longtitude - item.ilongitude) > 600000 || Math.abs(latitude - item.ilatitude) > 600000)
				continue;
			curDistance = (int) GisFunction.GetDistance(longtitude, latitude, item.ilongitude, item.ilatitude);
			if (curDistance < distance)
			{
				nearestCell = item;
				distance = curDistance;
			}
		}
		return nearestCell;
	}
}
