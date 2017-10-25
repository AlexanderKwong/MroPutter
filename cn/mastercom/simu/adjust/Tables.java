package cn.mastercom.simu.adjust;

import java.util.Collection;
import java.util.HashMap;

public class Tables
{
	private HashMap<NearMapKey, Long> nearNcEciMap = new HashMap<NearMapKey, Long>();// 主小区邻区关系表；（这个主小区下这个频点pci最近的小区）
	private HashMap<CellGridKey, OTT_Cell_Grid> gpsOttCellGridMap = new HashMap<CellGridKey, OTT_Cell_Grid>();// Gps指纹库校准表
	private HashMap<CellGridKey, OTT_Cell_Grid> wifiOttCellGridMap = new HashMap<CellGridKey, OTT_Cell_Grid>();// wifi指纹库校准表
	private int dealedSampleNum = 0;

	public Collection<OTT_Cell_Grid> getGpsOttCellGridMapValues()
	{
		return gpsOttCellGridMap.values();
	}

	public Collection<OTT_Cell_Grid> getWifiOttCellGridMapValues()
	{
		return wifiOttCellGridMap.values();
	}

	public synchronized long getOrPutNearEci(NearMapKey key, NearMapKey putKey, long ncEci)
	{
		if (key != null && nearNcEciMap.get(key) == null)
		{
			return 0;
		}
		else if (putKey != null && ncEci != 0)
		{
			nearNcEciMap.put(putKey, ncEci);
			return 0L;
		}
		return nearNcEciMap.get(key);
	}

	public synchronized OTT_Cell_Grid getOrPutGpsOttCellGrid(CellGridKey key, CellGridKey putKey, OTT_Cell_Grid cellGrid)
	{
		if (putKey != null && cellGrid != null)
		{
			gpsOttCellGridMap.put(putKey, cellGrid);
			return null;
		}
		return gpsOttCellGridMap.get(key);
	}

	public synchronized OTT_Cell_Grid getOrPutWifiCellGrid(CellGridKey key, CellGridKey putKey, OTT_Cell_Grid cellGrid)
	{
		if (putKey != null && cellGrid != null)
		{
			wifiOttCellGridMap.put(putKey, cellGrid);
			return null;
		}
		return wifiOttCellGridMap.get(key);
	}

	public synchronized int dealedSampleNum()
	{
		dealedSampleNum++;
		if (dealedSampleNum % 100 == 0)
		{
			System.out.println("总采样点文件数：【" + CreatOttCellGrid.sampleSize + "】，已经处理采样点文件个数【" + dealedSampleNum + "】剩余采样点文件数【" + (CreatOttCellGrid.sampleSize - dealedSampleNum) + "】");
		}
		return dealedSampleNum;
	}
}
