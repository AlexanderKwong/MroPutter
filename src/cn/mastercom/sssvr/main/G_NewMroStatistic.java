package cn.mastercom.sssvr.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.zip.GZIPInputStream;
import cn.mastercom.sssvr.util.IResultTable;
import cn.mastercom.sssvr.util.Sample_4G;
import cn.mastercom.sssvr.util.TimeHelper;

public class G_NewMroStatistic implements Callable
{
	private String filepath;
	private Map<String, IResultTable> mrOutGridMap;
	private Map<String, IResultTable> mrBuildMap;
	private Map<String, IResultTable> mrInGridMap;
	private Map<String, IResultTable> mrBuildCellMap;
	private Map<String, IResultTable> mrInGridCellMap;
	private Map<String, IResultTable> mrOutGridCellMap;
	private Map<String, IResultTable> mrStatCellMap;
	private Map<String, IResultTable> mrBuildCellNcMap;
	private Map<String, IResultTable> mrInGridCellNcMap;
	private Map<String, IResultTable> mrOutGridCellNcMap;
	private Map<String, IResultTable> topicCellIsolatedMap;
	private static Object lock = new Object();
	private int statTime;

	public G_NewMroStatistic(String filepath, Map<String, IResultTable> mrOutGridMap, Map<String, IResultTable> mrBuildMap, Map<String, IResultTable> mrInGridMap, Map<String, IResultTable> mrBuildCellMap, Map<String, IResultTable> mrInGridCellMap, Map<String, IResultTable> mrOutGridCellMap, Map<String, IResultTable> mrStatCellMap, Map<String, IResultTable> mrBuildCellNcMap, Map<String, IResultTable> mrInGridCellNcMap, Map<String, IResultTable> mrOutGridCellNcMap,
			Map<String, IResultTable> topicCellIsolatedMap)
	{
		this.filepath = filepath;
		this.mrOutGridMap = mrOutGridMap;
		this.mrBuildMap = mrBuildMap;
		this.mrInGridMap = mrInGridMap;
		this.mrBuildCellMap = mrBuildCellMap;
		this.mrInGridCellMap = mrInGridCellMap;
		this.mrOutGridCellMap = mrOutGridCellMap;
		this.mrStatCellMap = mrStatCellMap;
		this.mrBuildCellNcMap = mrBuildCellNcMap;
		this.mrInGridCellNcMap = mrInGridCellNcMap;
		this.mrOutGridCellNcMap = mrOutGridCellNcMap;
		this.topicCellIsolatedMap = topicCellIsolatedMap;
	}

	@Override
	public Object call() throws Exception
	{
		File sampleFile = new File(filepath);
		BufferedReader bf = null;
		if (filepath.endsWith(".gz"))
		{
			bf = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(sampleFile))));
		}
		else
		{
			bf = new BufferedReader(new InputStreamReader(new FileInputStream(sampleFile)));
		}
		String line = "";
		String temp[] = null;
		try
		{
			while ((line = bf.readLine()) != null)
			{
				// 组成sample
				temp = line.split("\\t", -1);
				Sample_4G sample = new Sample_4G();
				try
				{
					sample.returnSample(temp);
				}
				catch (Exception e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
					continue;
				}
				// 各种统计
				synchronized (lock)
				{
					statTime = TimeHelper.getRoundDayTime(sample.itime);
					dealMr(sample, sampleFile);
				}
			}
			DealOneDayData.ncellDealFiles++;
			if (DealOneDayData.ncellDealFiles % 50 == 0)
			{
				System.out.println("TotalFiles:" + DealOneDayData.nTotalFiles + ", CellStatisticDealFill:" + DealOneDayData.ncellDealFiles);
			}
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
			bf.close();
		}
		return "finish";
	}

	public void dealMr(Sample_4G sample, File sampleFile)
	{
		statMrStatCell(sample);
		statTopicCellIsolated(sample);

		if (sampleFile.getParentFile().getName().startsWith("TB_DTSIGNAL"))
		{
			statMrOutGrid(sample);
			statMrOutGridCell(sample);
		}

		if (sampleFile.getParentFile().getName().startsWith("TB_CQTSIGNAL"))
		{
			statMrBuild(sample);
			statMrInGrid(sample);
			statMrBuildCell(sample);
			statMrInGridCell(sample);
		}
	}

	private void statMrOutGrid(Sample_4G sample)
	{
		String key = MrOutGrid.getKey(sample);
		MrOutGrid item = (MrOutGrid) mrOutGridMap.get(key);
		if (item == null)
		{
			item = new MrOutGrid(sample, statTime);
			mrOutGridMap.put(key, item);
		}
		item.Stat(sample);
	}

	private void statMrBuild(Sample_4G sample)
	{
		String key = MrBuild.getKey(sample);
		MrBuild item = (MrBuild) mrBuildMap.get(key);
		if (item == null)
		{
			item = new MrBuild(sample, statTime);
			mrBuildMap.put(key, item);
		}
		item.Stat(sample);
	}

	private void statMrInGrid(Sample_4G sample)
	{
		String key = MrInGrid.getKey(sample);
		MrInGrid item = (MrInGrid) mrInGridMap.get(key);
		if (item == null)
		{
			item = new MrInGrid(sample, statTime);
			mrInGridMap.put(key, item);
		}
		item.Stat(sample);
	}

	private void statMrBuildCell(Sample_4G sample)
	{
		String key = MrBuildCell.getKey(sample);
		MrBuildCell item = (MrBuildCell) mrBuildCellMap.get(key);
		if (item == null)
		{
			item = new MrBuildCell(sample, statTime);
			mrBuildCellMap.put(key, item);
		}
		item.Stat(sample, mrBuildCellNcMap);
	}

	private void statMrInGridCell(Sample_4G sample)
	{
		String key = MrInGridCell.getKey(sample);
		MrInGridCell item = (MrInGridCell) mrInGridCellMap.get(key);
		if (item == null)
		{
			item = new MrInGridCell(sample, statTime);
			mrInGridCellMap.put(key, item);
		}
		item.Stat(sample, mrInGridCellNcMap);
	}

	private void statMrOutGridCell(Sample_4G sample)
	{
		String key = MrOutGridCell.getKey(sample);
		MrOutGridCell item = (MrOutGridCell) mrOutGridCellMap.get(key);
		if (item == null)
		{
			item = new MrOutGridCell(sample, statTime);
			mrOutGridCellMap.put(key, item);
		}
		item.Stat(sample, mrOutGridCellNcMap);
	}

	private void statMrStatCell(Sample_4G sample)
	{
		String key = MrStatCell.getKey(sample);
		MrStatCell item = (MrStatCell) mrStatCellMap.get(key);
		if (item == null)
		{
			item = new MrStatCell(sample, statTime);
			mrStatCellMap.put(key, item);
		}
		item.Stat(sample);
	}

	private void statTopicCellIsolated(Sample_4G sample)
	{
		String key = TopicCellIsolated.getKey(sample);
		TopicCellIsolated item = (TopicCellIsolated) topicCellIsolatedMap.get(key);
		if (item == null)
		{
			item = new TopicCellIsolated(sample, statTime);
			topicCellIsolatedMap.put(key, item);
		}
		item.Stat(sample);
	}
}
