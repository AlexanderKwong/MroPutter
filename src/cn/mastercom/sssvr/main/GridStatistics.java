package cn.mastercom.sssvr.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import org.apache.log4j.Logger;
import java.util.HashMap;
import cn.mastercom.sssvr.util.CalendarEx;
import cn.mastercom.sssvr.util.CellData_4G;
import cn.mastercom.sssvr.util.CellData_Freq;
import cn.mastercom.sssvr.util.CellFreqItem;
import cn.mastercom.sssvr.util.CellGridTimeKey;
import cn.mastercom.sssvr.util.FileOpt;
import cn.mastercom.sssvr.util.Stat_CellGrid_4G;
import cn.mastercom.sssvr.util.Stat_Grid_Freq_4G;
import cn.mastercom.sssvr.util.TimeHelper;
import cn.mastercom.sssvr.util.LocalFile;
import cn.mastercom.sssvr.util.LteStatHelper;
import cn.mastercom.sssvr.util.MyInt;
import cn.mastercom.sssvr.util.NC_LTE;
import cn.mastercom.sssvr.util.ResultHelper;
import cn.mastercom.sssvr.util.Sample_4G;
import cn.mastercom.sssvr.util.Grid;
import cn.mastercom.sssvr.util.GridData_Freq;
import cn.mastercom.sssvr.util.GridItem;
import cn.mastercom.sssvr.util.GridTimeKey;
import cn.mastercom.sssvr.util.IResultTable;

public class GridStatistics extends Thread
{
	public static void main(String args[])
	{
		GridStatistics gridStatistics = new GridStatistics();
		gridStatistics.start();// 启动栅格统计功能
	}

	@Override
	@SuppressWarnings("unchecked")
	public void run()
	{
		FingerprintFixMr fixMr = new FingerprintFixMr();
		FingerprintFixMr.returnConfig();
		String samplePaths = FingerprintFixMr.samplePath;
		while (!MainSvr.bExitFlag)
		{
			// 接下来进行栅格统计
			HashMap<Integer, ArrayList<String>> samplePathMap = returnDayFolderPath(samplePaths);// 将sample数据所在文件夹按照天分组
			if (samplePathMap.size() > 0)
			{
				ExecutorService gridExec = Executors.newFixedThreadPool(1);// 建立线程池
				for (int date : samplePathMap.keySet())
				{
					gridExec.submit(new DealOneDayData(samplePathMap.get(date), FingerprintFixMr.gridPath, FingerprintFixMr.sampleEventPath, samplePaths, FingerprintFixMr.noFixSamplePath, FingerprintFixMr.statisticPath));
				}
				gridExec.shutdown();
				try
				{
					gridExec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
			System.out.println("sample 数据全部处理完毕，等待下一批sample数据生成，休息一分钟... ...");
			try
			{
				Thread.sleep(60000);
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}
	}

	/**
	 * 将samplePaths下的文件夹按照日期分组
	 * 
	 * @param sourcePaths
	 * @return
	 */
	public static HashMap<Integer, ArrayList<String>> returnDayFolderPath(String sourcePaths)
	{
		HashMap<Integer, ArrayList<String>> folderPathMap = new HashMap<Integer, ArrayList<String>>();
		HashMap<Integer, Integer> dayMap = new HashMap<Integer, Integer>();// 记录不能进行统计的天

		ArrayList<String> templist = null;
		int day = 0;
		if (sourcePaths.length() == 0)
		{
			return null;
		}
		else
		{
			File file = new File(sourcePaths);
			if (file.exists())
			{
				File[] fileArray = file.listFiles();
				for (File path : fileArray)
				{
					if (!path.isDirectory())
						continue;
					File[] fileArraySub = path.listFiles();
					if (fileArraySub.length == 0)
						continue;

					long lastModifyTime = path.lastModified() / 1000L;
					final CalendarEx cal = new CalendarEx(new Date());
					day = Integer.parseInt(path.getName().split("_")[4]);
					templist = folderPathMap.get(day);
					if (templist == null && !dayMap.containsKey(day) && lastModifyTime + FingerprintFixMr.waittime * 60 <= cal._second)
					{
						templist = new ArrayList<String>();
						templist.add(path.getAbsolutePath());
						folderPathMap.put(day, templist);
					}
					else if (!dayMap.containsKey(day) && lastModifyTime + FingerprintFixMr.waittime * 60 <= cal._second)
					{
						templist.add(path.getAbsolutePath());
					}
					else if (lastModifyTime + FingerprintFixMr.waittime * 60 > cal._second)
					{
						if (templist != null)
						{
							folderPathMap.remove(day);
						}
						if (!dayMap.containsKey(day))
						{
							dayMap.put(day, 1);
						}
					}
				}
			}
		}
		return folderPathMap;
	}
}

class GridStatistic implements Callable
{
	private ConcurrentHashMap<GridTimeKey, Grid> cqtgrid_forty_map;
	private ConcurrentHashMap<GridTimeKey, Grid> cqtgrid_ten_map;
	private ConcurrentHashMap<GridTimeKey, Grid> dtgrid_forty_map;
	private ConcurrentHashMap<GridTimeKey, Grid> dtgrid_ten_map;
	private ConcurrentHashMap<GridTimeKey, Grid> grid_forty_Map;
	private ConcurrentHashMap<GridTimeKey, Grid> grid_ten_Map;
	private String filepath;
	private static Object lock = new Object();

	static Logger log = Logger.getLogger(GridStatistic.class.getName());

	public GridStatistic(ConcurrentHashMap<GridTimeKey, Grid> cqtgrid_forty_map, ConcurrentHashMap<GridTimeKey, Grid> cqtgrid_ten_map, ConcurrentHashMap<GridTimeKey, Grid> dtgrid_forty_map, ConcurrentHashMap<GridTimeKey, Grid> dtgrid_ten_map, ConcurrentHashMap<GridTimeKey, Grid> grid_forty_Map, ConcurrentHashMap<GridTimeKey, Grid> grid_ten_Map, String filepath)
	{
		this.cqtgrid_forty_map = cqtgrid_forty_map;
		this.cqtgrid_ten_map = cqtgrid_ten_map;
		this.dtgrid_forty_map = dtgrid_forty_map;
		this.dtgrid_ten_map = dtgrid_ten_map;
		this.grid_forty_Map = grid_forty_Map;
		this.grid_ten_Map = grid_ten_Map;
		this.filepath = filepath;
	}

	@Override
	public String call() throws Exception
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
		if (sampleFile.getParentFile().getName().startsWith("TB_CQTSIGNAL"))
		{
			gridStatistic(bf, cqtgrid_ten_map, cqtgrid_forty_map);
		}
		else if (sampleFile.getParentFile().getName().startsWith("TB_DTSIGNAL"))
		{
			gridStatistic(bf, dtgrid_ten_map, dtgrid_forty_map);
		}
		else if (sampleFile.getParentFile().getName().startsWith("TB_DTEXSIGNAL"))
		{
			gridStatistic(bf, null, null);
		}
		return "finish";
	}

	public void dealSample(Sample_4G sample, ConcurrentHashMap<GridTimeKey, Grid> gridStatisticMap, int gridSize)
	{
		GridTimeKey key = null;
		Grid item = null;
		key = returnkey(sample.cityID, 0, sample.ilongitude, sample.ilatitude, gridSize);// time_cityid_ilongitude_ilatitude
		// dt、cqt统计
		synchronized (lock)
		{
			item = gridStatisticMap.get(key);
			if (item == null)
			{
				item = new Grid();
				item.statisticNewSample(sample, gridSize);
				gridStatisticMap.put(key, item);
			}
			else
			{
				item.statisticSample(sample);
			}
		}
	}

	public void gridStatistic(BufferedReader bf, ConcurrentHashMap<GridTimeKey, Grid> grid_ten_StatisticMap, ConcurrentHashMap<GridTimeKey, Grid> grid_forty_StatisticMap)
	{
		String line = "";
		String temp[] = null;
		try
		{
			while ((line = bf.readLine()) != null)
			{
				temp = line.split("\\t", -1);
				Sample_4G sample = new Sample_4G();
				try
				{
					sample.returnSample(temp);
				}
				catch (Exception e)
				{
					continue;
				}
				if (grid_ten_StatisticMap != null && grid_forty_StatisticMap != null)
				{
					dealSample(sample, grid_ten_StatisticMap, 10);
					dealSample(sample, grid_forty_StatisticMap, 40);
				}
				// 全量统计
				dealSample(sample, grid_ten_Map, 10);
				dealSample(sample, grid_forty_Map, 40);
			}
			DealOneDayData.nDealFiles++;
			if (DealOneDayData.nDealFiles % 50 == 0)
			{
				System.out.println("TotalFiles:" + DealOneDayData.nTotalFiles + ", GridStatisticDealFiles:" + DealOneDayData.nDealFiles);
			}
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
			try
			{
				bf.close();
			}
			catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public GridTimeKey returnkey(int cityid, int time, int itllongitude, int itllatitude, int gridSize)
	{
		GridTimeKey key = new GridTimeKey();
		if (gridSize == 40)
		{
			key.gridTimeKey(cityid, time, (itllongitude / 4000) * 4000, (itllatitude / 3600) * 3600);
		}
		else if (gridSize == 10)
		{
			key.gridTimeKey(cityid, time, (itllongitude / 1000) * 1000, (itllatitude / 900) * 900);
		}
		return key;
	}
}

class writtingNewMrThread implements Runnable
{
	private Map<String, IResultTable> map;
	private File file;

	public writtingNewMrThread(Map<String, IResultTable> tempMap, File file)
	{
		this.map = tempMap;
		this.file = file;
	}

	@Override
	public void run()
	{
		// TODO Auto-generated method stub
		BufferedWriter Dw = null;
		try
		{
			Dw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
			for (String s : map.keySet())
			{
				Dw.write(map.get(s).toLine());
				Dw.newLine();
			}
			map.clear();// 清空内存
			Dw.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				Dw.close();
			}
			catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

}

class writtingThread implements Runnable
{
	private ConcurrentHashMap<GridTimeKey, Grid> map;
	private File file;

	public writtingThread(ConcurrentHashMap<GridTimeKey, Grid> map, File file)
	{
		this.map = map;
		this.file = file;
	}

	@Override
	public void run()
	{
		Grid grid = null;
		BufferedWriter Dw = null;
		try
		{
			Dw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
			for (GridTimeKey s : map.keySet())
			{
				grid = map.get(s);
				Dw.write(grid.returnContent());
				Dw.newLine();
			}
			map.clear();// 清空内存
			Dw.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				Dw.close();
			}
			catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}

class writtingCellFreqThread implements Runnable
{
	private ConcurrentHashMap<CellFreqItem, CellData_Freq> cellDataFreqMap;
	private File file;

	public writtingCellFreqThread(ConcurrentHashMap<CellFreqItem, CellData_Freq> cellDataFreqMap, File file)
	{
		this.cellDataFreqMap = cellDataFreqMap;
		this.file = file;
	}

	@Override
	public void run()
	{
		CellData_Freq cellFreq = null;
		BufferedWriter Dw = null;
		try
		{
			Dw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
			for (CellFreqItem s : cellDataFreqMap.keySet())
			{
				cellFreq = cellDataFreqMap.get(s);
				Dw.write(ResultHelper.getPutCell_Freq(cellFreq.getLteCell()));
				Dw.newLine();
			}
			cellDataFreqMap.clear();
			Dw.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				Dw.close();
			}
			catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}

class writtingCellGridThread implements Runnable
{
	private ConcurrentHashMap<CellGridTimeKey, Stat_CellGrid_4G> cellGridDataMap;
	private File file;

	public writtingCellGridThread(ConcurrentHashMap<CellGridTimeKey, Stat_CellGrid_4G> cellGridDataMap, File file)
	{
		this.cellGridDataMap = cellGridDataMap;
		this.file = file;
	}

	@Override
	public void run()
	{
		Stat_CellGrid_4G cellGrid = null;
		BufferedWriter Dw = null;
		try
		{
			Dw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
			for (CellGridTimeKey s : cellGridDataMap.keySet())
			{
				cellGrid = cellGridDataMap.get(s);
				Dw.write(ResultHelper.getPutCellGrid_4G(cellGrid));
				Dw.newLine();
			}
			cellGridDataMap.clear();
			Dw.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				Dw.close();
			}
			catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}

/**
 * 写freq数据
 * 
 * @author xing
 *
 */
class writtingFreqThread implements Runnable
{
	private ConcurrentHashMap<Integer, ConcurrentHashMap<GridItem, GridData_Freq>> map;
	private File file;

	public writtingFreqThread(ConcurrentHashMap<Integer, ConcurrentHashMap<GridItem, GridData_Freq>> map, File file)
	{
		this.map = map;
		this.file = file;
	}

	@Override
	public void run()
	{
		ConcurrentHashMap<GridItem, GridData_Freq> gridData = null;
		GridData_Freq gridFreq = null;
		BufferedWriter Dw = null;
		try
		{
			Dw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
			for (int s : map.keySet())
			{
				gridData = map.get(s);
				for (GridItem temp : gridData.keySet())
				{
					gridFreq = gridData.get(temp);
					Dw.write(ResultHelper.getPutGrid_4G_FREQ(gridFreq.getLteGrid()));
					Dw.newLine();
				}
			}
			map.clear();
			Dw.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				Dw.close();
			}
			catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}

class writtingCellThread implements Runnable
{
	private ConcurrentHashMap<Long, CellData_4G> map;
	private File file;

	public writtingCellThread(ConcurrentHashMap<Long, CellData_4G> map, File file)
	{
		this.map = map;
		this.file = file;
	}

	@Override
	public void run()
	{

		CellData_4G celldata = null;
		BufferedWriter Dw = null;
		try
		{
			Dw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
			for (long s : map.keySet())
			{
				celldata = map.get(s);
				Dw.write(ResultHelper.getPutCell_4G(celldata.getLteCell()));
				Dw.newLine();
			}
			map.clear();
			Dw.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				Dw.close();
			}
			catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}

class MoveFileThread implements Callable
{
	private String oldFileName;
	private String newFileName;
	ConcurrentHashMap<String, Integer> moveHash;

	public MoveFileThread(String oldFileName, String newFileName, ConcurrentHashMap<String, Integer> moveHash)
	{
		this.oldFileName = oldFileName;
		this.newFileName = newFileName;
		this.moveHash = moveHash;
	}

	@Override
	public String call() throws Exception
	{
		File oldFile = new File(oldFileName);
		File newFile = new File(newFileName);
		if (!newFile.getParentFile().exists())
		{
			newFile.getParentFile().mkdirs();
		}
		boolean flag = oldFile.renameTo(newFile);
		System.out.println("移动" + oldFileName + "文件到指定目录" + newFileName + (flag ? "成功" : "失败"));
		if (flag)
		{
			moveHash.put(oldFile.getParent(), 1);// 记录移动了哪些文件，全部移动完成之后删除源目录下的这些文件夹
		}
		return "finish";
	}
}

class DealOneDayData implements Callable
{
	ArrayList<String> OneDaysSmplePaths = null;
	String gridPath = "";
	String sampleEventPath = "";
	String noFixSamplePath = "";
	String samplePaths = null;
	String statisticPath = "";

	public DealOneDayData(ArrayList<String> OneDaysSmplePaths, String gridPath, String sampleEventPath, String samplePaths, String noFixSamplePath, String statisticPath)
	{
		this.OneDaysSmplePaths = OneDaysSmplePaths;
		this.gridPath = gridPath;
		this.sampleEventPath = sampleEventPath;
		this.samplePaths = samplePaths;// 移动这个程序目录下的数据
		this.noFixSamplePath = noFixSamplePath;
		this.statisticPath = statisticPath;
	}

	public static int nTotalFiles = 0;
	public static int nDealFiles = 0;
	public static int nfreqDealFiles = 0;
	public static int ncellDealFiles = 0;
	public static int nfreqCellFiles = 0;

	@SuppressWarnings("unchecked")
	@Override
	public String call() throws Exception
	{
		try
		{
			List<String> fileList = LocalFile.getAllFiles(OneDaysSmplePaths, "", 0);
			List<String> statisticFileList = new ArrayList<String>();
			if (fileList.size() > 0)
			{
				nTotalFiles = fileList.size();
				nDealFiles = 0;
				nfreqDealFiles = 0;
				ncellDealFiles = 0;
				nfreqCellFiles = 0;
				ConcurrentHashMap<String, Integer> moveHash = new ConcurrentHashMap<String, Integer>();
				String date = "";

				ConcurrentHashMap<GridTimeKey, Grid> cqtgrid_forty_map = new ConcurrentHashMap<GridTimeKey, Grid>();
				ConcurrentHashMap<GridTimeKey, Grid> cqtgrid_ten_map = new ConcurrentHashMap<GridTimeKey, Grid>();

				ConcurrentHashMap<GridTimeKey, Grid> dtgrid_forty_map = new ConcurrentHashMap<GridTimeKey, Grid>();
				ConcurrentHashMap<GridTimeKey, Grid> dtgrid_ten_map = new ConcurrentHashMap<GridTimeKey, Grid>();

				ConcurrentHashMap<GridTimeKey, Grid> grid_forty_Map = new ConcurrentHashMap<GridTimeKey, Grid>();// grid总量统计dt、cqt
				ConcurrentHashMap<GridTimeKey, Grid> grid_ten_Map = new ConcurrentHashMap<GridTimeKey, Grid>();

				ConcurrentHashMap<Integer, ConcurrentHashMap<GridItem, GridData_Freq>> freqcqt_forty_map = new ConcurrentHashMap<Integer, ConcurrentHashMap<GridItem, GridData_Freq>>();
				ConcurrentHashMap<Integer, ConcurrentHashMap<GridItem, GridData_Freq>> freqcqt_ten_map = new ConcurrentHashMap<Integer, ConcurrentHashMap<GridItem, GridData_Freq>>();

				ConcurrentHashMap<Integer, ConcurrentHashMap<GridItem, GridData_Freq>> freqdt_forty_map = new ConcurrentHashMap<Integer, ConcurrentHashMap<GridItem, GridData_Freq>>();
				ConcurrentHashMap<Integer, ConcurrentHashMap<GridItem, GridData_Freq>> freqdt_ten_map = new ConcurrentHashMap<Integer, ConcurrentHashMap<GridItem, GridData_Freq>>();

				ConcurrentHashMap<Long, CellData_4G> cellDataMap = new ConcurrentHashMap<Long, CellData_4G>();

				ConcurrentHashMap<CellFreqItem, CellData_Freq> cellDataFreqMap = new ConcurrentHashMap<CellFreqItem, CellData_Freq>();

				ConcurrentHashMap<CellGridTimeKey, Stat_CellGrid_4G> cellGrid_forty_DataMap = new ConcurrentHashMap<CellGridTimeKey, Stat_CellGrid_4G>();
				ConcurrentHashMap<CellGridTimeKey, Stat_CellGrid_4G> cellGrid_ten_DataMap = new ConcurrentHashMap<CellGridTimeKey, Stat_CellGrid_4G>();

				ConcurrentHashMap<String, IResultTable> mrOutGridMap = new ConcurrentHashMap<String, IResultTable>();
				ConcurrentHashMap<String, IResultTable> mrBuildMap = new ConcurrentHashMap<String, IResultTable>();
				ConcurrentHashMap<String, IResultTable> mrInGridMap = new ConcurrentHashMap<String, IResultTable>();
				ConcurrentHashMap<String, IResultTable> mrBuildCellMap = new ConcurrentHashMap<String, IResultTable>();
				ConcurrentHashMap<String, IResultTable> mrInGridCellMap = new ConcurrentHashMap<String, IResultTable>();
				ConcurrentHashMap<String, IResultTable> mrOutGridCellMap = new ConcurrentHashMap<String, IResultTable>();
				ConcurrentHashMap<String, IResultTable> mrStatCellMap = new ConcurrentHashMap<String, IResultTable>();
				ConcurrentHashMap<String, IResultTable> mrBuildCellNcMap = new ConcurrentHashMap<String, IResultTable>();
				ConcurrentHashMap<String, IResultTable> mrInGridCellNcMap = new ConcurrentHashMap<String, IResultTable>();
				ConcurrentHashMap<String, IResultTable> mrOutGridCellNcMap = new ConcurrentHashMap<String, IResultTable>();
				ConcurrentHashMap<String, IResultTable> topicCellIsolatedMap = new ConcurrentHashMap<String, IResultTable>();

				date = new File(new File(fileList.get(0)).getParent()).getName().split("_")[4];
				ExecutorService exec = Executors.newFixedThreadPool(1);// 建立栅格统计线程池
				ExecutorService freqexec = Executors.newFixedThreadPool(5);// 建立异频统计线程池
				ExecutorService cellexec = Executors.newFixedThreadPool(5);// 建立小区统计线程池
				ExecutorService cellFreqexec = Executors.newFixedThreadPool(5);// 建立小区异频统计线程池
				ExecutorService mrStatistic = Executors.newFixedThreadPool(5);// 建立mro统计线程池
				System.out.println("开始计算" + date + "日的sample数据");
				for (String filepath : fileList)
				{
					// 小区统计统计所有的sample
					cellexec.submit(new CellStatistic(filepath, cellDataMap, cellGrid_forty_DataMap, cellGrid_ten_DataMap));// 小区统计/小区栅格统计
					cellFreqexec.submit(new CellFreqStatistic(filepath, cellDataFreqMap));// 小区异频统计
					// 栅格统计和异频统计只统计dt和cqt的，没定位到的sample不统计
					if (filepath.contains("TB_SIGNAL_NOFIXSAMPLE"))
					{
						nDealFiles++;
						nfreqDealFiles++;
						continue;
					}
					exec.submit(new GridStatistic(cqtgrid_forty_map, cqtgrid_ten_map, dtgrid_forty_map, dtgrid_ten_map, grid_forty_Map, grid_ten_Map, filepath));// 栅格统计
					freqexec.submit(new FreqStatistic(freqcqt_forty_map, freqcqt_ten_map, freqdt_forty_map, freqdt_ten_map, filepath));// 异频统计
					mrStatistic.submit(new G_NewMroStatistic(filepath, mrOutGridMap, mrBuildMap, mrInGridMap, mrBuildCellMap, mrInGridCellMap, mrOutGridCellMap, mrStatCellMap, mrBuildCellNcMap, mrInGridCellNcMap, mrOutGridCellNcMap, topicCellIsolatedMap));
				}

				File tempfile = null;
				File dtgridpath = null;
				File dtgrid_ten_path = null;
				Thread dtgridThread = null;
				Thread dtgrid_ten_Thread = null;

				File cqtgridpath = null;
				File cqtgrid_ten_path = null;
				Thread cqtgridThread = null;
				Thread cqtgrid_ten_Thread = null;

				File allgridPath = null;
				File allgrid_ten_path = null;
				Thread allgridThread = null;
				Thread allgrid_ten_Thread = null;

				File cellFreqPath = null;
				Thread cellFreqThread = null;

				File cellGridPath = null;
				File cellGrid_ten_Path = null;
				Thread cellGridThread = null;
				Thread cellGrid_ten_Thread = null;

				File freqcqtGridpath = null;
				File freqcqtGrid_ten_path = null;
				Thread freqcqtGridThread = null;
				Thread freqcqtGrid_ten_Thread = null;

				File freqdtGridpath = null;
				File freqdtGrid_ten_path = null;
				Thread freqdtGridThread = null;
				Thread freqdtGrid_ten_Thread = null;

				File cellPath = null;
				Thread cellThread = null;

				Thread mrOutGridThread = null;
				Thread mrBuildThread = null;
				Thread mrInGridThread = null;
				Thread mrBuildCellThread = null;
				Thread mrInGridCellThread = null;
				Thread mrOutGridCellThread = null;
				Thread mrStatCellThread = null;
				Thread mrBuildCellNcThread = null;
				Thread mrInGridCellNcThread = null;
				Thread mrOutGridCellNcThread = null;
				Thread topicCellThread = null;

				// 栅格统计完毕
				{
					exec.shutdown();
					exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);// 等待子线程执行完毕再执行主线程
					System.out.println(date + "栅格统计完毕，接下来将数据写入文件！");
					String time = new SimpleDateFormat("yyyyMMddhhmmss").format(new Date());
					if (dtgrid_forty_map.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_DTSIGNAL_Grid_01_" + date);
						if (!tempfile.exists())
						{
							tempfile.mkdirs();
						}
						dtgridpath = new File(gridPath + "\\TB_DTSIGNAL_Grid_01_" + date + "\\dt" + time + ".txt");
						dtgridThread = new Thread(new writtingThread(dtgrid_forty_map, dtgridpath));
						dtgridThread.start();
					}
					if (dtgrid_ten_map.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_DTSIGNAL_Grid10_01_" + date);
						if (!tempfile.exists())
						{
							tempfile.mkdirs();
						}
						dtgrid_ten_path = new File(gridPath + "\\TB_DTSIGNAL_Grid10_01_" + date + "\\dt" + time + ".txt");
						dtgrid_ten_Thread = new Thread(new writtingThread(dtgrid_ten_map, dtgrid_ten_path));
						dtgrid_ten_Thread.start();
					}

					if (cqtgrid_forty_map.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_CQTSIGNAL_Grid_01_" + date);
						if (!tempfile.exists())
						{
							tempfile.mkdirs();
						}
						cqtgridpath = new File(gridPath + "\\TB_CQTSIGNAL_Grid_01_" + date + "\\cqt" + time + ".txt");
						cqtgridThread = new Thread(new writtingThread(cqtgrid_forty_map, cqtgridpath));
						cqtgridThread.start();
					}
					if (cqtgrid_ten_map.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_CQTSIGNAL_Grid10_01_" + date);
						if (!tempfile.exists())
						{
							tempfile.mkdirs();
						}
						cqtgrid_ten_path = new File(gridPath + "\\TB_CQTSIGNAL_Grid10_01_" + date + "\\cqt" + time + ".txt");
						cqtgrid_ten_Thread = new Thread(new writtingThread(cqtgrid_ten_map, cqtgrid_ten_path));
						cqtgrid_ten_Thread.start();
					}

					if (grid_forty_Map.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_SIGNAL_Grid_01_" + date);
						if (!tempfile.exists())
						{
							tempfile.mkdirs();
						}
						allgridPath = new File(gridPath + "\\TB_SIGNAL_Grid_01_" + date + "\\grid" + time + ".txt");
						allgridThread = new Thread(new writtingThread(grid_forty_Map, allgridPath));
						allgridThread.start();
					}
					if (grid_ten_Map.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_SIGNAL_Grid10_01_" + date);
						if (!tempfile.exists())
						{
							tempfile.mkdirs();
						}
						allgrid_ten_path = new File(gridPath + "\\TB_SIGNAL_Grid10_01_" + date + "\\grid" + time + ".txt");
						allgrid_ten_Thread = new Thread(new writtingThread(grid_ten_Map, allgrid_ten_path));
						allgrid_ten_Thread.start();
					}
				}
				// 小区异频统计完毕
				{
					cellFreqexec.shutdown();
					cellFreqexec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);// 等待子线程执行完毕再执行主线程
					System.out.println(date + "小区异频统计完毕，接下来将数据写入文件！");
					String time = new SimpleDateFormat("yyyyMMddhhmmss").format(new Date());
					if (cellDataFreqMap.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_Freq_SIGNAL_Cell_01_" + date);
						if (!tempfile.exists())
						{
							tempfile.mkdirs();
						}
						cellFreqPath = new File(gridPath + "\\TB_Freq_SIGNAL_Cell_01_" + date + "\\cellFreqStatistic" + time + ".txt");
						cellFreqThread = new Thread(new writtingCellFreqThread(cellDataFreqMap, cellFreqPath));
						cellFreqThread.start();
					}
				}
				// 栅格异频统计完毕
				{
					freqexec.shutdown();
					freqexec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);// 等待子线程执行完毕再执行主线程
					System.out.println(date + "栅格异频统计完毕，接下来将数据写入文件！");
					String time = new SimpleDateFormat("yyyyMMddhhmmss").format(new Date());
					if (freqcqt_forty_map.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_Freq_CQTSIGNAL_Grid_01_" + date);
						if (!tempfile.exists())
						{
							tempfile.mkdirs();
						}
						freqcqtGridpath = new File(gridPath + "\\TB_Freq_CQTSIGNAL_Grid_01_" + date + "\\cqt" + time + ".txt");
						freqcqtGridThread = new Thread(new writtingFreqThread(freqcqt_forty_map, freqcqtGridpath));
						freqcqtGridThread.start();
					}
					if (freqcqt_ten_map.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_Freq_CQTSIGNAL_Grid10_01_" + date);
						if (!tempfile.exists())
						{
							tempfile.mkdirs();
						}
						freqcqtGrid_ten_path = new File(gridPath + "\\TB_Freq_CQTSIGNAL_Grid10_01_" + date + "\\cqt" + time + ".txt");
						freqcqtGrid_ten_Thread = new Thread(new writtingFreqThread(freqcqt_ten_map, freqcqtGrid_ten_path));
						freqcqtGrid_ten_Thread.start();
					}

					if (freqdt_forty_map.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_Freq_DTSIGNAL_Grid_01_" + date);
						if (!tempfile.exists())
						{
							tempfile.mkdirs();
						}
						freqdtGridpath = new File(gridPath + "\\TB_Freq_DTSIGNAL_Grid_01_" + date + "\\dt" + time + ".txt");
						freqdtGridThread = new Thread(new writtingFreqThread(freqdt_forty_map, freqdtGridpath));
						freqdtGridThread.start();
					}
					if (freqdt_ten_map.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_Freq_DTSIGNAL_Grid10_01_" + date);
						if (!tempfile.exists())
						{
							tempfile.mkdirs();
						}
						freqdtGrid_ten_path = new File(gridPath + "\\TB_Freq_DTSIGNAL_Grid10_01_" + date + "\\dt" + time + ".txt");
						freqdtGrid_ten_Thread = new Thread(new writtingFreqThread(freqdt_ten_map, freqdtGrid_ten_path));
						freqdtGrid_ten_Thread.start();
					}
				}
				{// 新添加mr统计
					mrStatistic.shutdown();
					mrStatistic.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);// 等待子线程执行完毕再执行主线程
					String time = new SimpleDateFormat("yyyyMMddhhmmss").format(new Date());
					System.out.println(date + "新添加mr统计完毕，接下来将数据写入文件！");
					if (mrOutGridMap.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_MR_OUTGRID_" + date + "" + time + ".txt");
						if (!tempfile.getParentFile().exists())
						{
							tempfile.getParentFile().mkdirs();
						}
						mrOutGridThread = new Thread(new writtingNewMrThread(mrOutGridMap, tempfile));
						mrOutGridThread.start();

					}
					if (mrBuildMap.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_MR_BUILD_" + date + "" + time + ".txt");
						if (!tempfile.getParentFile().exists())
						{
							tempfile.getParentFile().mkdirs();
						}
						mrBuildThread = new Thread(new writtingNewMrThread(mrBuildMap, tempfile));
						mrBuildThread.start();
					}
					if (mrInGridMap.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_MR_INGRID_" + date + "" + time + ".txt");
						if (!tempfile.getParentFile().exists())
						{
							tempfile.getParentFile().mkdirs();
						}
						mrInGridThread = new Thread(new writtingNewMrThread(mrInGridMap, tempfile));
						mrInGridThread.start();
					}
					if (mrBuildCellMap.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_MR_BUILD_CELL_" + date + "" + time + ".txt");
						if (!tempfile.getParentFile().exists())
						{
							tempfile.getParentFile().mkdirs();
						}
						mrBuildCellThread = new Thread(new writtingNewMrThread(mrBuildCellMap, tempfile));
						mrBuildCellThread.start();
					}
					if (mrInGridCellMap.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_MR_INGRID_CELL_" + date + "" + time + ".txt");
						if (!tempfile.getParentFile().exists())
						{
							tempfile.getParentFile().mkdirs();
						}
						mrInGridCellThread = new Thread(new writtingNewMrThread(mrInGridCellMap, tempfile));
						mrInGridCellThread.start();
					}
					if (mrOutGridCellMap.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_MR_OUTGRID_CELL_" + date + "" + time + ".txt");
						if (!tempfile.getParentFile().exists())
						{
							tempfile.getParentFile().mkdirs();
						}
						mrOutGridCellThread = new Thread(new writtingNewMrThread(mrOutGridCellMap, tempfile));
						mrOutGridCellThread.start();
					}
					if (mrStatCellMap.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_MR_CELL_" + date + "" + time + ".txt");
						if (!tempfile.getParentFile().exists())
						{
							tempfile.getParentFile().mkdirs();
						}
						mrStatCellThread = new Thread(new writtingNewMrThread(mrStatCellMap, tempfile));
						mrStatCellThread.start();
					}
					if (mrBuildCellNcMap.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_MR_BUILD_CELLPARE_" + date + "" + time + ".txt");
						if (!tempfile.getParentFile().exists())
						{
							tempfile.getParentFile().mkdirs();
						}
						mrBuildCellNcThread = new Thread(new writtingNewMrThread(mrBuildCellNcMap, tempfile));
						mrBuildCellNcThread.start();
					}
					if (mrInGridCellNcMap.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_MR_INGRID_CELLPARE_" + date + "" + time + ".txt");
						if (!tempfile.getParentFile().exists())
						{
							tempfile.getParentFile().mkdirs();
						}
						mrInGridCellNcThread = new Thread(new writtingNewMrThread(mrInGridCellNcMap, tempfile));
						mrInGridCellNcThread.start();
					}
					if (mrOutGridCellNcMap.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_MR_OUTGRID_CELLPARE_" + date + "" + time + ".txt");
						if (!tempfile.getParentFile().exists())
						{
							tempfile.getParentFile().mkdirs();
						}
						mrOutGridCellNcThread = new Thread(new writtingNewMrThread(mrOutGridCellNcMap, tempfile));
						mrOutGridCellNcThread.start();
					}
					if (topicCellIsolatedMap.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_TOPIC_CELL_ISOLATED_" + date + "" + time + ".txt");
						if (!tempfile.getParentFile().exists())
						{
							tempfile.getParentFile().mkdirs();
						}
						topicCellThread = new Thread(new writtingNewMrThread(topicCellIsolatedMap, tempfile));
						topicCellThread.start();
					}
				}
				// 小区统计完毕
				{
					cellexec.shutdown();
					cellexec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);// 等待子线程执行完毕再执行主线程
					System.out.println(date + "小区统计完毕，接下来将数据写入文件！");
					String time = new SimpleDateFormat("yyyyMMddhhmmss").format(new Date());
					if (cellGrid_forty_DataMap.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_SIGNAL_CellGrid_01_" + date);
						if (!tempfile.exists())
						{
							tempfile.mkdirs();
						}
						cellGridPath = new File(gridPath + "\\TB_SIGNAL_CellGrid_01_" + date + "\\cellGridStatistic" + time + ".txt");
						cellGridThread = new Thread(new writtingCellGridThread(cellGrid_forty_DataMap, cellGridPath));
						cellGridThread.start();
					}
					if (cellGrid_ten_DataMap.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_SIGNAL_CellGrid10_01_" + date);
						if (!tempfile.exists())
						{
							tempfile.mkdirs();
						}
						cellGrid_ten_Path = new File(gridPath + "\\TB_SIGNAL_CellGrid10_01_" + date + "\\cellGridStatistic" + time + ".txt");
						cellGrid_ten_Thread = new Thread(new writtingCellGridThread(cellGrid_ten_DataMap, cellGrid_ten_Path));
						cellGrid_ten_Thread.start();
					}
					if (cellDataMap.size() > 0)
					{
						tempfile = new File(gridPath + "\\TB_SIGNAL_Cell_01_" + date);
						if (!tempfile.exists())
						{
							tempfile.mkdirs();
						}
						cellPath = new File(gridPath + "\\TB_SIGNAL_Cell_01_" + date + "\\cellstatsitc" + time + ".txt");
						cellThread = new Thread(new writtingCellThread(cellDataMap, cellPath));
						cellThread.start();
					}
				}
				if (cqtgridThread != null)
				{
					cqtgridThread.join();
				}
				if (cqtgrid_ten_Thread != null)
				{
					cqtgrid_ten_Thread.join();
				}

				if (dtgridThread != null)
				{
					dtgrid_ten_Thread.join();
				}
				if (dtgrid_ten_Thread != null)
				{
					dtgrid_ten_Thread.join();
				}

				if (allgridThread != null)
				{
					allgridThread.join();
				}
				if (allgrid_ten_Thread != null)
				{
					allgrid_ten_Thread.join();
				}

				if (cellFreqThread != null)
				{
					cellFreqThread.join();
				}

				if (cellGridThread != null)
				{
					cellGridThread.join();
				}
				if (cellGrid_ten_Thread != null)
				{
					cellGrid_ten_Thread.join();
				}

				if (freqcqtGridThread != null)
				{
					freqcqtGridThread.join();
				}
				if (freqcqtGrid_ten_Thread != null)
				{
					freqcqtGrid_ten_Thread.join();
				}

				if (freqdtGridThread != null)
				{
					freqdtGridThread.join();
				}
				if (freqdtGrid_ten_Thread != null)
				{
					freqdtGrid_ten_Thread.join();
				}

				if (cellThread != null)
				{
					cellThread.join();
				}
				if (mrOutGridThread != null)
				{
					mrOutGridThread.join();
				}
				if (mrBuildThread != null)
				{
					mrBuildThread.join();
					;
				}
				if (mrInGridThread != null)
				{
					mrInGridThread.join();
				}
				if (mrBuildCellThread != null)
				{
					mrBuildCellThread.join();
				}
				if (mrInGridCellThread != null)
				{
					mrInGridCellThread.join();
				}
				if (mrOutGridCellThread != null)
				{
					mrOutGridCellThread.join();
				}
				if (mrStatCellThread != null)
				{
					mrStatCellThread.join();
				}
				if (mrBuildCellNcThread != null)
				{
					mrBuildCellNcThread.join();
				}
				if (mrInGridCellNcThread != null)
				{
					mrInGridCellNcThread.join();
				}
				if (mrOutGridCellNcThread != null)
				{
					mrOutGridCellNcThread.join();
				}
				if (topicCellThread != null)
				{
					topicCellThread.join();
				}

				System.out.println(date + "栅格、小区、小区栅格、小区异频和异频统计、mr新增统计，数据写入文件完毕，接下来将sample和各种统计数据移到指定目录");
				// 文件生成后将sample数据挪到入库目录
				if (cqtgridpath != null)
				{
					statisticFileList.add(cqtgridpath.getPath());
				}
				if (cqtgrid_ten_path != null)
				{
					statisticFileList.add(cqtgrid_ten_path.getPath());
				}

				if (dtgridpath != null)
				{
					statisticFileList.add(dtgridpath.getPath());
				}
				if (dtgrid_ten_path != null)
				{
					statisticFileList.add(dtgrid_ten_path.getPath());
				}

				if (allgridPath != null)
				{
					statisticFileList.add(allgridPath.getPath());
				}
				if (allgrid_ten_path != null)
				{
					statisticFileList.add(allgrid_ten_path.getPath());
				}

				if (cellFreqPath != null)
				{
					statisticFileList.add(cellFreqPath.getPath());
				}

				if (cellGridPath != null)
				{
					statisticFileList.add(cellGridPath.getPath());
				}
				if (cellGrid_ten_Path != null)
				{
					statisticFileList.add(cellGrid_ten_Path.getPath());
				}

				if (freqcqtGridpath != null)
				{
					statisticFileList.add(freqcqtGridpath.getPath());
				}
				if (freqcqtGrid_ten_path != null)
				{
					statisticFileList.add(freqcqtGrid_ten_path.getPath());
				}

				if (freqdtGridpath != null)
				{
					statisticFileList.add(freqdtGridpath.getPath());
				}
				if (freqdtGrid_ten_path != null)
				{
					statisticFileList.add(freqdtGrid_ten_path.getPath());
				}

				if (cellPath != null)
				{
					statisticFileList.add(cellPath.getPath());
				}

				// noFixSamplePath中数据不会上传到hdfs，先清空noFixSamplePath
				File noFixPath = new File(noFixSamplePath);
				if (noFixPath.exists())
				{
					FileOpt.delAllFile(noFixSamplePath);
				}
				// 移动sample文件到相应目录
				ExecutorService moveSampleExec = Executors.newFixedThreadPool(10);// 建立线程池
				for (String filepath : fileList)
				{
					moveSampleExec.submit(new MoveFileThread(filepath, getNewPath(filepath, samplePaths, gridPath, sampleEventPath, noFixSamplePath), moveHash));
				}
				moveSampleExec.shutdown();
				// 移动统计文件到相应目录
				ExecutorService moveStatisticExec = Executors.newFixedThreadPool(5);// 建立线程池
				for (String filepath : statisticFileList)
				{
					moveStatisticExec.submit(new MoveFileThread(filepath, FileOpt.renamePath(filepath, gridPath, statisticPath), moveHash));
				}
				moveStatisticExec.shutdown();

				moveSampleExec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);// 等待子线程执行完毕在执行主线程
				moveStatisticExec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

				System.out.println("数据移动完成!接下来删除原文件所在文件夹");
				for (String s : moveHash.keySet())
				{
					File delFile = new File(s);
					if (delFile.exists())
					{
						boolean flag = delFile.delete();
						System.out.println("删除文件夹" + delFile.getPath() + (flag ? "成功" : "失败"));
					}
				}
				moveHash.clear();// 删除完成之后要清空
				System.out.println(date + "sample数据已经处理完毕！");
			}
			else
			{
				System.out.println("暂时没有新sample数据生成");
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return "finish";
	}

	public static String getNewPath(String path, String samplePath, String gridpath, String bckFixSamplePath, String bcknoFixSamplePath)
	{
		String newPath = "";
		if (path.contains(samplePath) && !path.contains("TB_SIGNAL_NOFIXSAMPLE"))
		{
			newPath = path.replace(samplePath, bckFixSamplePath);
			return newPath;
		}
		else if (path.contains(samplePath) && path.contains("TB_SIGNAL_NOFIXSAMPLE"))
		{
			newPath = path.replace(samplePath, bcknoFixSamplePath);
			return newPath;
		}
		if (path.contains(gridpath))
		{
			newPath = path.replace(gridpath, bckFixSamplePath);
		}
		return newPath;
	}
}

class FreqStatistic implements Callable
{
	ConcurrentHashMap<Integer, ConcurrentHashMap<GridItem, GridData_Freq>> freqcqt_forty_map;
	ConcurrentHashMap<Integer, ConcurrentHashMap<GridItem, GridData_Freq>> freqcqt_ten_map;
	ConcurrentHashMap<Integer, ConcurrentHashMap<GridItem, GridData_Freq>> freqdt_forty_map;
	ConcurrentHashMap<Integer, ConcurrentHashMap<GridItem, GridData_Freq>> freqdt_ten_map;
	private static Object lock = new Object();

	private String filepath;

	public FreqStatistic(ConcurrentHashMap<Integer, ConcurrentHashMap<GridItem, GridData_Freq>> freqcqt_forty_map, ConcurrentHashMap<Integer, ConcurrentHashMap<GridItem, GridData_Freq>> freqcqt_ten_map, ConcurrentHashMap<Integer, ConcurrentHashMap<GridItem, GridData_Freq>> freqdt_forty_map, ConcurrentHashMap<Integer, ConcurrentHashMap<GridItem, GridData_Freq>> freqdt_ten_map, String filepath)
	{
		this.freqcqt_forty_map = freqcqt_forty_map;
		this.freqcqt_ten_map = freqcqt_ten_map;
		this.freqdt_forty_map = freqdt_forty_map;
		this.freqdt_ten_map = freqdt_ten_map;
		this.filepath = filepath;
	}

	@Override
	public String call() throws Exception
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
		if (sampleFile.getParentFile().getName().startsWith("TB_CQTSIGNAL"))
		{
			freqStatistic(bf, freqcqt_ten_map, freqcqt_forty_map);
		}
		else if (sampleFile.getParentFile().getName().startsWith("TB_DTSIGNAL"))
		{
			freqStatistic(bf, freqdt_ten_map, freqdt_forty_map);
		}
		return "finish";
	}

	public void freqStatistic(BufferedReader bf, ConcurrentHashMap<Integer, ConcurrentHashMap<GridItem, GridData_Freq>> freq_ten_map, ConcurrentHashMap<Integer, ConcurrentHashMap<GridItem, GridData_Freq>> freq_forty_map)
	{
		String line = "";
		String temp[] = null;
		try
		{
			while ((line = bf.readLine()) != null)
			{
				temp = line.split("\\t", -1);

				Sample_4G sample = new Sample_4G();
				sample.returnSample(temp);
				returnFreqMap(sample, freq_ten_map, TimeHelper.getRoundDayTime(sample.itime), 10);
				returnFreqMap(sample, freq_forty_map, TimeHelper.getRoundDayTime(sample.itime), 40);
			}

			DealOneDayData.nfreqDealFiles++;
			if (DealOneDayData.nfreqDealFiles % 50 == 0)
			{
				System.out.println("TotalFiles:" + DealOneDayData.nTotalFiles + ", FreqStatisticDealFiles:" + DealOneDayData.nfreqDealFiles);
			}
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			System.out.println(line);
			e.printStackTrace();
		}
		finally
		{
			try
			{
				bf.close();
			}
			catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * 将sample按照频点组织成map
	 * 
	 * @param sample
	 * @param gridDataFreqMap
	 * @param statTime
	 */

	public void returnFreqMap(Sample_4G sample, ConcurrentHashMap<Integer, ConcurrentHashMap<GridItem, GridData_Freq>> gridDataFreqMap, int statTime, int gridSize)
	{
		// 异频统计
		// 只统计mro数据
		// if (sample.flag.toUpperCase().equals("MRO"))
		// {
		// 栅格统计
		if (sample.ilongitude > 0 && sample.ilatitude > 0)
		{
			List<NC_LTE> itemList = sample.getNclte_Freq();

			for (NC_LTE nc_LTE : itemList)
			{
				synchronized (lock)
				{
					ConcurrentHashMap<GridItem, GridData_Freq> freqDataMap = gridDataFreqMap.get(nc_LTE.LteNcEarfcn);
					if (freqDataMap == null)
					{
						freqDataMap = new ConcurrentHashMap<GridItem, GridData_Freq>();
						gridDataFreqMap.put(nc_LTE.LteNcEarfcn, freqDataMap);
					}

					GridItem gridItem = GridItem.GetGridItem(sample.cityID, sample.ilongitude, sample.ilatitude, gridSize);
					GridData_Freq gridData = freqDataMap.get(gridItem);
					if (gridData == null)
					{
						gridData = new GridData_Freq(statTime, statTime + 86400, nc_LTE.LteNcEarfcn);
						Stat_Grid_Freq_4G lteGrid = gridData.getLteGrid();
						lteGrid.icityid = sample.cityID;
						lteGrid.itllongitude = gridItem.getTLLongitude();
						lteGrid.itllatitude = gridItem.getTLLatitude();
						lteGrid.ibrlongitude = gridItem.getBRLongitude();
						lteGrid.ibrlatitude = gridItem.getBRLatitude();
						lteGrid.startTime = statTime;
						lteGrid.endTime = statTime + 86400;
						freqDataMap.put(gridItem, gridData);
					}
					gridData.dealSample(sample, nc_LTE.LteNcRSRP, nc_LTE.LteNcRSRQ);
				}
			}
		}
	}
	// }
}

class CellStatistic implements Callable
{
	private String filepath;
	private ConcurrentHashMap<Long, CellData_4G> cellDataMap;
	private ConcurrentHashMap<CellGridTimeKey, Stat_CellGrid_4G> cellGrid_forty_DataMap;
	private ConcurrentHashMap<CellGridTimeKey, Stat_CellGrid_4G> cellGrid_ten_DataMap;
	private static Object lock = new Object();

	public CellStatistic(String filepath, ConcurrentHashMap<Long, CellData_4G> cellDataMap, ConcurrentHashMap<CellGridTimeKey, Stat_CellGrid_4G> cellGrid_forty_DataMap, ConcurrentHashMap<CellGridTimeKey, Stat_CellGrid_4G> cellGrid_ten_DataMap)
	{
		this.filepath = filepath;
		this.cellDataMap = cellDataMap;
		this.cellGrid_forty_DataMap = cellGrid_forty_DataMap;
		this.cellGrid_ten_DataMap = cellGrid_ten_DataMap;
	}

	@Override
	public String call() throws Exception
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
		CellData_4G cellData = null;
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
					System.out.println(line);
					continue;
				}

				// 小区统计
				int starttime = TimeHelper.getRoundDayTime(sample.itime);
				int endtime = starttime + 86400;
				synchronized (lock)
				{
					cellData = cellDataMap.get(sample.Eci);
					if (cellData == null)
					{
						cellData = new CellData_4G(sample.cityID, sample.iLAC, sample.Eci, starttime, endtime);
						cellDataMap.put(sample.Eci, cellData);
					}
					cellData.dealSample(sample);
				}
				// 小区栅格统计
				if (filepath.contains("TB_SIGNAL_NOFIXSAMPLE"))// nofixedsample不参与小区栅格统计
				{
					continue;
				}
				synchronized (lock)
				{
					cellgridStatistic(sample, cellGrid_ten_DataMap, starttime, endtime, 10);
					cellgridStatistic(sample, cellGrid_forty_DataMap, starttime, endtime, 40);
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

	public void cellgridStatistic(Sample_4G sample, ConcurrentHashMap<CellGridTimeKey, Stat_CellGrid_4G> cellGridDataMap, int starttime, int endtime, int gridSize)
	{
		CellGridTimeKey cellgridkey = null;
		cellgridkey = new CellGridTimeKey(sample.Eci, sample.ilongitude, sample.ilatitude, gridSize);

		Stat_CellGrid_4G lteGrid = cellGridDataMap.get(cellgridkey);
		if (lteGrid == null)
		{
			lteGrid = new Stat_CellGrid_4G();
			lteGrid.icityid = sample.cityID;
			lteGrid.iLac = sample.iLAC;
			lteGrid.iCi = (int) sample.Eci;
			lteGrid.itllongitude = cellgridkey.getItllongitude();
			lteGrid.itllatitude = cellgridkey.getItllatitude();
			lteGrid.ibrlongitude = cellgridkey.getIbrlongitude();
			lteGrid.ibrlatitude = cellgridkey.getIbrlatitude();
			lteGrid.startTime = starttime;
			lteGrid.endTime = endtime;
			cellGridDataMap.put(cellgridkey, lteGrid);
		}
		// boolean isMroSample = sample.flag.toUpperCase().equals("MRO");
		// boolean isMreSample = sample.flag.toUpperCase().equals("MRE");
		boolean isMroSample = true;
		boolean isMreSample = true;
		lteGrid.isamplenum++;
		if (isMroSample || isMreSample)
		{
			lteGrid.MrCount++;
			LteStatHelper.statMro(sample, lteGrid.tStat);
		}
		else
		{
			lteGrid.XdrCount++;
			lteGrid.iduration += sample.duration;
			LteStatHelper.statEvt(sample, lteGrid.tStat);

			// 只有xdr，才算用户的个数，mr不用算
			if (sample.IMSI > 0)
			{
				MyInt item = lteGrid.imsiMap.get(sample.IMSI);
				if (item == null)
				{
					item = new MyInt(0);
					lteGrid.imsiMap.put(sample.IMSI, item);
				}
				item.data++;
			}
		}

		if (sample.LteScRSRQ > -100)
		{
			lteGrid.RSRQ_nTotal++;
			if (sample.LteScRSRQ != -1000000)
			{
				lteGrid.RSRQ_nSum += sample.LteScRSRQ;
			}
			// [-40 -20) [-20 -16) [-16 -12)[-12 -8) [-8 0)[0,)
			if (sample.LteScRSRQ < -20 && sample.LteScRSRQ >= -40)
			{
				lteGrid.RSRQ_nCount[0]++;
			}
			else if (sample.LteScRSRQ < -16)
			{
				lteGrid.RSRQ_nCount[1]++;
			}
			else if (sample.LteScRSRQ < -12)
			{
				lteGrid.RSRQ_nCount[2]++;
			}
			else if (sample.LteScRSRQ < -8)
			{
				lteGrid.RSRQ_nCount[3]++;
			}
			else if (sample.LteScRSRQ < 0)
			{
				lteGrid.RSRQ_nCount[4]++;
			}
			else if (sample.LteScRSRQ >= 0)
			{
				lteGrid.RSRQ_nCount[5]++;
			}
			if (sample.LteScRSRP < -113)
			{
				lteGrid.RSRP_nCount7++;
			}
		}
	}
}

class CellFreqStatistic implements Callable
{
	private String filepath;
	private ConcurrentHashMap<CellFreqItem, CellData_Freq> cellDataFreqMap;
	private static Object lock = new Object();

	public CellFreqStatistic(String filepath, ConcurrentHashMap<CellFreqItem, CellData_Freq> cellDataFreqMap)
	{
		super();
		this.filepath = filepath;
		this.cellDataFreqMap = cellDataFreqMap;
	}

	@Override
	public String call() throws Exception
	{
		// TODO Auto-generated method stub
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
		CellData_Freq cellFreqData = null;

		try
		{
			while ((line = bf.readLine()) != null)
			{
				temp = line.split("\\t", -1);

				Sample_4G sample = new Sample_4G();
				sample.returnSample(temp);
				int starttime = TimeHelper.getRoundDayTime(sample.itime);
				int endtime = starttime + 86400;
				List<NC_LTE> itemList = sample.getNclte_Freq();
				for (NC_LTE item : itemList)
				{
					CellFreqItem cellfreqKey = new CellFreqItem(sample.Eci, item.LteNcEarfcn);
					synchronized (lock)
					{
						cellFreqData = cellDataFreqMap.get(cellfreqKey);
						if (cellFreqData == null)
						{
							cellFreqData = new CellData_Freq(sample.cityID, sample.iLAC, sample.Eci, item.LteNcEarfcn, starttime, endtime);
							cellDataFreqMap.put(cellfreqKey, cellFreqData);
						}

						cellFreqData.dealSample(sample, item.LteNcRSRP, item.LteNcRSRQ);
					}
				}
			}
			DealOneDayData.nfreqCellFiles++;
			if (DealOneDayData.nfreqCellFiles % 50 == 0)
			{
				System.out.println("TotalFiles:" + DealOneDayData.nTotalFiles + ", CellFreqStatisticDealFill:" + DealOneDayData.nfreqCellFiles);
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

}
