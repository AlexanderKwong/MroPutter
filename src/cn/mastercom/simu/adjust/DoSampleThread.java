package cn.mastercom.simu.adjust;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.Callable;
import java.util.zip.GZIPInputStream;
import cn.mastercom.sssvr.util.LteCellInfo;
import cn.mastercom.sssvr.util.NC_LTE;
import cn.mastercom.sssvr.util.Sample_4G;

public class DoSampleThread implements Callable<String>
{
	public Tables tables;
	public String samplePath;

	public DoSampleThread(Tables tables, String samplePath)
	{
		this.tables = tables;
		this.samplePath = samplePath;
	}

	@Override
	public String call() throws Exception
	{
		doSampleFile(samplePath);
		return "finish";
	}

	public void doSampleFile(String path)
	{
		try
		{
			InputStream in = null;
			if (path.endsWith(".gz"))
			{
				in = new GZIPInputStream(new FileInputStream(new File(path)));
			}
			else
			{
				in = new FileInputStream(new File(path));
			}
			BufferedReader bf = new BufferedReader(new InputStreamReader(in));
			String line = "";
			Sample_4G sample = null;
			while ((line = bf.readLine()) != null)
			{
				sample = new Sample_4G();
				try
				{
					sample.returnSample(line.split("\t", -1));
					// sample.returnTBMRSample(line.split("\t", -1));
					doStat(sample);
				}
				catch (Exception e)
				{
					OutLog.dosom(e);
					continue;
				}
			}
			tables.dealedSampleNum();
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			OutLog.dosom(e);
		}
	}

	/**
	 * 统计
	 */
	public void doStat(Sample_4G sample)
	{
		CellGridKey key;
		if (sample.loctp.equals("wf"))
		{
			key = new CellGridKey().returnFortyCellGridKey(sample.ilongitude, sample.ilatitude, sample.Eci);
			OTT_Cell_Grid cellgrid = tables.getOrPutWifiCellGrid(key, null, null);
			if (cellgrid == null)
			{
				LteCellInfo cellInfo = CellConfig.getInstance().lteCellInfoMap.get(sample.Eci);
				if (cellInfo != null)
				{
					cellgrid = new OTT_Cell_Grid(key.eci, key.longtitude, key.latitude, cellInfo.pci, cellInfo.fcn);
				}
				else
				{
					cellgrid = new OTT_Cell_Grid(key.eci, key.longtitude, key.latitude, 0, 0);// 找不到相应的频点pci
				}
				tables.getOrPutWifiCellGrid(null, key, cellgrid);
			}
			cellgrid.deal(sample.LteScRSRP);
			// 邻区统计
			NC_LTE[] tlte = sample.tlte;
			for (NC_LTE nc : tlte)
			{
				if (nc.LteNcEarfcn == -1000000 || nc.LteNcPci == -100000)
				{
					break;
				}
				long ncEci = getNearestNcEci(new NearMapKey(sample.Eci, nc.LteNcEarfcn, nc.LteNcPci), sample.ilongitude, sample.ilatitude, sample.cityID);
				key = new CellGridKey().returnFortyCellGridKey(sample.ilongitude, sample.ilatitude, ncEci);
				cellgrid = tables.getOrPutWifiCellGrid(key, null, null);
				if (cellgrid == null)
				{
					cellgrid = new OTT_Cell_Grid(key.eci, key.longtitude, key.latitude, nc.LteNcPci, nc.LteNcEarfcn);// 找不到相应的频点pci
					tables.getOrPutWifiCellGrid(null, key, cellgrid);
				}
				cellgrid.deal(nc.LteNcRSRP);
			}
		}
		else if (sample.loctp.equals("ll"))
		{
			// 主小区统计
			key = new CellGridKey().returnTenCellGridKey(sample.ilongitude, sample.ilatitude, sample.Eci);
			OTT_Cell_Grid cellgrid = tables.getOrPutGpsOttCellGrid(key, null, null);
			if (cellgrid == null)
			{
				LteCellInfo cellInfo = CellConfig.getInstance().lteCellInfoMap.get(sample.Eci);
				if (cellInfo != null)
				{
					cellgrid = new OTT_Cell_Grid(key.eci, key.longtitude, key.latitude, cellInfo.pci, cellInfo.fcn);
				}
				else
				{
					cellgrid = new OTT_Cell_Grid(key.eci, key.longtitude, key.latitude, 0, 0);// 找不到相应的频点pci
				}
				tables.getOrPutGpsOttCellGrid(null, key, cellgrid);
			}
			cellgrid.deal(sample.LteScRSRP);
			// 邻区统计
			NC_LTE[] tlte = sample.tlte;
			for (NC_LTE nc : tlte)
			{
				if (nc.LteNcEarfcn == -1000000 || nc.LteNcPci == -100000)
				{
					break;
				}
				long ncEci = getNearestNcEci(new NearMapKey(sample.Eci, nc.LteNcEarfcn, nc.LteNcPci), sample.ilongitude, sample.ilatitude, sample.cityID);
				key = new CellGridKey().returnTenCellGridKey(sample.ilongitude, sample.ilatitude, ncEci);
				cellgrid = tables.getOrPutGpsOttCellGrid(key, null, null);
				if (cellgrid == null)
				{
					cellgrid = new OTT_Cell_Grid(key.eci, key.longtitude, key.latitude, nc.LteNcPci, nc.LteNcEarfcn);// 找不到相应的频点pci
					tables.getOrPutGpsOttCellGrid(null, key, cellgrid);
				}
				cellgrid.deal(nc.LteNcRSRP);
			}
		}
	}

	/**
	 * 工参需要预先加载// 得到最近的邻区
	 * 
	 * @param key
	 * @param longtitude
	 * @param latitude
	 * @param cityID
	 * @return
	 */
	public long getNearestNcEci(NearMapKey key, int longtitude, int latitude, int cityID)
	{
		if (tables.getOrPutNearEci(key, null, 0) == 0)
		{
			LteCellInfo lteInfo = CellConfig.getInstance().getNearestCell(longtitude, latitude, cityID, key.fcn, key.pci);
			// 没有找到eci
			if (lteInfo == null)
			{
				return 0;
			}
			tables.getOrPutNearEci(null, key, lteInfo.eci);
			return lteInfo.eci;
		}
		else
		{
			return tables.getOrPutNearEci(key, null, 0);
		}
	}

}
