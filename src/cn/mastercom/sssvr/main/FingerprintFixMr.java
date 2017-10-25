package cn.mastercom.sssvr.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import cn.mastercom.sssvr.util.GetNotimeString;
import cn.mastercom.sssvr.util.GridTimeKey;
import cn.mastercom.sssvr.util.LocalFile;
import cn.mastercom.sssvr.util.ReturnConfig;

@SuppressWarnings({ "unused", "rawtypes", "unchecked" })

public class FingerprintFixMr extends Thread
{
	static HashMap<Integer, PubParticipation> gongcanMap = new HashMap<Integer, PubParticipation>();// 将公参中有用信息组织成map
	static HashMap<Integer, PubParticipation> indoorGongcanMap = new HashMap<Integer, PubParticipation>();// 将公参中室内分布信息组织成map
	static HashMap<Integer, Integer> indoorGongcanEnbidMap = new HashMap<Integer, Integer>();// 记录室分公参的enbid
	public static String path = "";
	public static String samplePath = "";
	public static int range = 3;
	public static String gridPath = "";
	public static int cellNum = 0;
	public static String sampleEventPath = "";
	public static String statisticPath = "";
	public static String mrBackPath = "";
	public static int waittime = 0;
	public static String noFixSamplePath = "";
	public static int totalFile = 0;
	public static int dealFile = 0;
	public static int threadNum = 20;
	public static double rsrp_min = -120;
	public static int gridCellNum = 30;
	public static double rsrp_Dvalue = 30;

	/**
	 * 将指纹库按栅格和enbid分别组装一下
	 */

	public static void returnShangeAndCellArray(ArrayList<File> simuFile, GridClass[] figureShangeArray,
			HashMap<Integer, ArrayList<Integer>> figureCellArray, HashMap<GridTimeKey, Integer> shangekeyMap,
			boolean DliRemove, int enbid)
	{
		if (simuFile == null)
			return;

		FileInputStream fs = null;
		BufferedReader bf = null;
		GridTimeKey tempkey = null;
		int ecitemp = 0;
		FigureCell figurecell = null;
		HashMap<Integer, Integer> rememberFortyLowIndex = new HashMap<Integer, Integer>();

		if (figureShangeArray.length > 0)
		{
			int createdData = 0;
			for (int i = 0; i < simuFile.size(); i++)
			{
				try
				{
					fs = new FileInputStream(simuFile.get(i));
					bf = new BufferedReader(new InputStreamReader(fs));
					String line = "";
					int index = 0;

					while ((line = bf.readLine()) != null)
					{
						String temp[] = line.split(",|\t", -1);
						try
						{
							figurecell = new FigureCell(temp);
							if (figurecell.rsrp < rsrp_min)
							{
								continue;
							}
						}
						catch (Exception e)
						{
							continue;
						}
						tempkey = figurecell.key();
						ecitemp = figurecell.ieci;
						int figure_enbid = ecitemp / 256;

						index = returnPositon(tempkey, shangekeyMap);
						int Earfcn_pci = 0;
						int cell = ecitemp % 256;
						int rsrp = (int) figurecell.rsrp * (-1);// 场强
						int key = cell * 1000 + rsrp;
						if (gongcanMap.get(ecitemp) != null)
						{// 找到该栅格中小区对应的频点和pci，找不到就不要这条数据
							Earfcn_pci = gongcanMap.get(ecitemp).getfPoint() * 1000 + gongcanMap.get(ecitemp).getPci();
						}
						else
						{
							continue;
						}
						if (index != -1)
						{
							if (figureShangeArray[index - 1] == null)
							{
								createdData++;
								figureShangeArray[index - 1] = new GridClass(tempkey);
								if (DliRemove)// 记录low_forty中的格子
								{
									rememberFortyLowIndex.put(index, 1);
								}
							}
							else
							{
								if (DliRemove && !rememberFortyLowIndex.containsKey(index))// 去重
								{
									continue;
								}
							}
							if (figure_enbid == enbid)// mr数据中主小区enbid对应的指纹
							{
								figureShangeArray[index - 1].getEci_map().put(ecitemp, figurecell);
							}
							figureShangeArray[index - 1].getEarfcn_pci_map().put(Earfcn_pci, figurecell);
						}
						if (figureCellArray != null && figure_enbid == enbid)// 记录mr数据中主小区对应栅格
						{
							ArrayList<Integer> gridIndex = figureCellArray.get(key);
							if (gridIndex == null)
							{
								gridIndex = new ArrayList<Integer>();
								figureCellArray.put(key, gridIndex);
							}
							gridIndex.add(shangekeyMap.get(tempkey) - 1);
						}
					}
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
				finally
				{
					if (bf != null)
					{
						try
						{
							bf.close();
						}
						catch (IOException e)
						{
							e.printStackTrace();
						}
					}
				}
			}
			dealFigureShangeArray(figureShangeArray);
			System.out.println(GetNotimeString.returnTimeString() + "指纹库总共栅格数量：" + figureShangeArray.length
					+ ",生成的栅格数量：" + createdData);
		}
	}

	/**
	 * 对栅格内的小区进行多重过滤； 只过滤邻区，主小区不用过滤
	 * 
	 * @param figureShangeArray
	 */
	public static void dealFigureShangeArray(GridClass[] figureShangeArray)
	{
		for (int i = 0; i < figureShangeArray.length; i++)
		{
			if (figureShangeArray[i] == null)
				continue;
			HashMap<Integer, FigureCell> earfcn_pci_map = figureShangeArray[i].getEarfcn_pci_map();
			HashMap<Integer, FigureCell> result_earfcn_pci_map = new HashMap<Integer, FigureCell>();// 过滤后的邻区表
			double maxRsrp = 0;
			List<Map.Entry<Integer, FigureCell>> earfcn_pci_List = new ArrayList<Map.Entry<Integer, FigureCell>>(
					earfcn_pci_map.entrySet());
			// 降序排序
			Collections.sort(earfcn_pci_List, new Comparator<Map.Entry<Integer, FigureCell>>()
			{
				@Override
				public int compare(Map.Entry<Integer, FigureCell> o1, Map.Entry<Integer, FigureCell> o2)
				{
					double result = o2.getValue().rsrp - o1.getValue().rsrp;
					if (result > 0)
					{
						return 1;
					}
					else if (result < 0)
					{
						return -1;
					}
					else
					{
						return 0;
					}
				}
			});
			maxRsrp = earfcn_pci_List.get(0).getValue().rsrp;// 该格子中最好的rsrp
			// 前N强保留，超过N强，两重门限过滤
			for (int j = 0; j < earfcn_pci_List.size(); j++)
			{
				Map.Entry<Integer, FigureCell> temp = earfcn_pci_List.get(j);
				if (j < gridCellNum)
				{
					result_earfcn_pci_map.put(temp.getKey(), temp.getValue());
				}
				else if (temp.getValue().rsrp < rsrp_min)
				{
					break;
				}
				else if (temp.getValue().rsrp < (maxRsrp - rsrp_Dvalue))
				{
					break;
				}
				else
				{
					result_earfcn_pci_map.put(temp.getKey(), temp.getValue());
				}
			}
			figureShangeArray[i].setEarfcn_pci_map(result_earfcn_pci_map);
		}
	}

	/**
	 * 返回指纹库中有多少个栅格
	 * 
	 * @param simuFile
	 *            指纹库文件列表
	 * @return
	 */
	public static int returnGridNum(ArrayList<File> simuFile, HashMap<GridTimeKey, Integer> shangekeyMap)
	{
		if (simuFile == null)
			return 0;
		int arraylength = shangekeyMap.size();
		FileInputStream fs = null;
		BufferedReader bf = null;
		GridTimeKey gridkey = null;
		for (int i = 0; i < simuFile.size(); i++)
		{
			try
			{
				fs = new FileInputStream(simuFile.get(i));
				bf = new BufferedReader(new InputStreamReader(fs));
				String line = "";
				while ((line = bf.readLine()) != null)
				{
					String temp[] = line.split(",|\t", -1);
					gridkey = new GridTimeKey(temp);
					if (!shangekeyMap.containsKey(gridkey))
					{// 得到栅格个数
						arraylength++;
						shangekeyMap.put(gridkey, arraylength);
					}
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			finally
			{
				if (bf != null)
				{
					try
					{
						bf.close();
					}
					catch (IOException e)
					{
						e.printStackTrace();
					}
				}
			}
		}
		System.out.println(GetNotimeString.returnTimeString() + "指纹库文件数：" + simuFile.size());
		System.out.println(GetNotimeString.returnTimeString() + "栅格数量：" + arraylength);
		return arraylength;
	}

	/**
	 * 返回指纹库中该小区应该放在哪个栅格中
	 * 
	 * @return
	 */
	public static int returnPositon(GridTimeKey shangeposition, HashMap<GridTimeKey, Integer> shangeKeyMap)
	{
		if (shangeKeyMap.containsKey(shangeposition))
		{
			return shangeKeyMap.get(shangeposition);
		}
		return -1;
	}

	/**
	 * 读取公参有用数据<eci,频点和pci>并且得到enbid中最大值和最小值
	 *
	 * @return
	 */
	public static HashMap<Integer, PubParticipation> returnGongCan()
	{
		FileInputStream fs;
		String gongcan[] = ReturnConfig.returnconfig("conf/config_figureFix.xml", "//comm//gongcan").split(",");
		for (int i = 0; i < gongcan.length; i++)
		{
			try
			{
				fs = new FileInputStream("conf\\" + gongcan[i]);
				BufferedReader bf = new BufferedReader(new InputStreamReader(fs));
				String line = "";
				int gongcanKey = 0;
				while ((line = bf.readLine()) != null)
				{
					String[] temp = line.split("\\t", -1);
					PubParticipation pp = null;
					try
					{
						pp = new PubParticipation(temp);
					}
					catch (Exception e)
					{
						continue;
					}
					gongcanKey = pp.getEnodebId() * 256 + pp.getCellid();// 根据enbid和cellid组装eci作为key
					if (!gongcanMap.containsKey(gongcanKey))
					{
						gongcanMap.put(gongcanKey, pp);
					}
					if (pp.getIndoor() == 1 && !indoorGongcanMap.containsKey(gongcanKey))
					{
						indoorGongcanMap.put(gongcanKey, pp);
						indoorGongcanEnbidMap.put(pp.getEnodebId(), 1);
					}
				}
				bf.close();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		return gongcanMap;
	}

	/**
	 * 初始化
	 */
	public static void init()
	{
		returnConfig();// 读取配置文件
		returnGongCan();
	}

	@SuppressWarnings("unchecked")
	public boolean readFile()
	{
		List<String> mrFileList = new ArrayList<String>();
		List<String> ten_simuFileList = new ArrayList<String>();
		List<String> forty_simuFileList = new ArrayList<String>();
		List<String> fortyLow_simuFileList = new ArrayList<String>();// 40*40低精度
		HashMap<String, Integer> removeMap = new HashMap<String, Integer>();
		new ReturnConfig();
		String LocalSimuFilePath = ReturnConfig.returnconfig("conf/config_figureFix.xml", "//comm//newfigureku");
		if (LocalSimuFilePath != null && LocalSimuFilePath.length() > 0)
		{
			File SimulPath = new File(LocalSimuFilePath + "//tenSimuFile");
			try
			{
				if (SimulPath.exists())
				{
					ten_simuFileList = LocalFile.getAllFiles(SimulPath, "", 0);
				}
				SimulPath = new File(LocalSimuFilePath + "//fortySimuFile");
				if (SimulPath.exists())
				{
					forty_simuFileList = LocalFile.getAllFiles(SimulPath, "", 0);
				}
				SimulPath = new File(LocalSimuFilePath + "//fortySimuFile_low");
				if (SimulPath.exists())
				{
					fortyLow_simuFileList = LocalFile.getAllFiles(SimulPath, "", 0);
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			while (!MainSvr.bExitFlag)
			{
				if (!path.equals(""))
				{
					try
					{
						mrFileList = LocalFile.getAllFiles(new File(path), "", 2);
						if (mrFileList.size() == 0)
						{
							System.out.println(GetNotimeString.returnTimeString() + "休息1分钟等待下一批Mr数据");
							Thread.sleep(60000);
						}
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
				}
				System.out.println(GetNotimeString.returnTimeString() + "samplePath:" + samplePath);
				System.out.println(GetNotimeString.returnTimeString() + "gongcanMap:" + gongcanMap.size());
				System.out.println(GetNotimeString.returnTimeString() + "IndoorGongcanMap:" + indoorGongcanMap.size());
				System.out.println(GetNotimeString.returnTimeString() + "range:" + range);
				System.out.println(GetNotimeString.returnTimeString() + "cellNum:" + cellNum);
				System.out.println(GetNotimeString.returnTimeString() + "mrBackPath:" + mrBackPath);
				System.out.println(GetNotimeString.returnTimeString() + "mrPath:" + path);
				System.out.println(GetNotimeString.returnTimeString() + "noFixSamplePath:" + noFixSamplePath);
				System.out.println(GetNotimeString.returnTimeString() + "Mro文件个数：" + mrFileList.size());
				System.out.println(GetNotimeString.returnTimeString() + "10*10指纹库文件数：" + ten_simuFileList.size());
				System.out
						.println(GetNotimeString.returnTimeString() + "40*40(高精度)指纹库文件数：" + forty_simuFileList.size());
				System.out.println(
						GetNotimeString.returnTimeString() + "40*40(低精度)指纹库文件数：" + fortyLow_simuFileList.size());

				ArrayList<Future<String>> results = new ArrayList<Future<String>>(); // Future
				Future futurn;
				totalFile = mrFileList.size();
				dealFile = 0;
				Collections.sort(mrFileList);
				HashMap<Integer, ArrayList<File>> enbidMrFilemap = returnEnbidFileMap(mrFileList, 5, "_", "mr");// mr数据按照enbid分组
				HashMap<Integer, ArrayList<File>> ten_SimuFilemap = returnEnbidFileMap(ten_simuFileList, 0, "_", "");// ten指纹库按照enbid分组
				HashMap<Integer, ArrayList<File>> forty_SimuFilemap = returnEnbidFileMap(forty_simuFileList, 0, "_",
						"");// forty指纹库按照enbid分组
				HashMap<Integer, ArrayList<File>> fortyLow_Simu_Filemap = returnEnbidFileMap(fortyLow_simuFileList, 0,
						"_", "");// forty_low指纹库按照enbid分组

				if (enbidMrFilemap == null)
				{
					System.out.println(GetNotimeString.returnTimeString() + "没有mr数据，请查看mr数据路径是否正确！");
					return false;
				}
				if (forty_SimuFilemap == null && fortyLow_Simu_Filemap == null)
				{
					System.out.println(GetNotimeString.returnTimeString() + "没有找到指纹库数据，请查看指纹库文件路径是否正确！");
					return false;
				}

				HashMap<Integer, ArrayList<Integer>> forty_figureCellArray = null;// key=
																					// //
																					// cell*1000+rsrp将指纹库按照小区进行组织
				GridClass[] ten_figureShangeArray = null;// 将指纹库按照栅格进行组织
				GridClass[] forty_figureShangeArray = null;

				HashMap<GridTimeKey, Integer> ten_shangekeyMap = null;// 10*10指纹库中栅格的序号
				HashMap<GridTimeKey, Integer> forty_shangekeyMap = null;// 40*40指纹库中栅格的序号
				HashMap<GridTimeKey, ArrayList<Integer>> ten_forty_shangeKeyMap = null;// 10*10栅格按照40*40汇聚记录栅格序号
				Object[] enbidAry = enbidMrFilemap.keySet().toArray();
				FigurePreloader fp = new FigurePreloader();

				if (enbidAry.length > 0)
				{
					fp.ten_SimuFilemap = ten_SimuFilemap;
					fp.forty_SimuFilemap = forty_SimuFilemap;
					fp.fortyLow_Simu_Filemap = fortyLow_Simu_Filemap;

					fp.SetLoadStatus(0, (int) enbidAry[0]);
					fp.start();// 启动指纹库加载
				}

				for (int i = 0; i < enbidAry.length; i++)
				{
					int s = (int) enbidAry[i];
					int nextEnb = -1;
					if (i < enbidAry.length - 1)
					{
						nextEnb = (int) enbidAry[i + 1];
					}

					try
					{
						if (forty_SimuFilemap.get(s) == null && fortyLow_Simu_Filemap.get(s) == null)
						{
							if (indoorGongcanEnbidMap.get(s) == null)
							{
								System.out.println(GetNotimeString.returnTimeString() + "  找不到enbid为" + s
										+ "的指纹库，且该数据不存在室分数据，无法为其定位！");
								dealFile = dealFile + enbidMrFilemap.get(s).size();
							}
							else
							{
								System.out.println(
										GetNotimeString.returnTimeString() + "找不到enbid为" + s + "的指纹库，将进行室分定位！");
								ExecutorService exec = Executors.newFixedThreadPool(threadNum);
								for (File dataFile : enbidMrFilemap.get(s))
								{
									exec.submit(
											new DealIndoorMr(dataFile, indoorGongcanMap, samplePath, mrBackPath, path));
									removeMap.put(dataFile.getParent(), 1);
								}
								exec.shutdown();
								try
								{
									exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
									dealFile = dealFile + enbidMrFilemap.get(s).size();
								}
								catch (InterruptedException e)
								{
									e.printStackTrace();
								}
							}
							continue;
						}

						while (true)
						{
							int loadStatus = 0;
							if ((loadStatus = fp.GetLoadStatus(s)) > 0)
							{
								if (loadStatus == 2)
								{// 预加载的不是当前enbid
									fp.SetLoadStatus(0, s);
								}
								else
								{
									System.out.println(
											GetNotimeString.returnTimeString() + "Have loaded " + s + " already.");
									forty_figureCellArray = fp.forty_figureCellArray; // cell*1000+rsrp将指纹库按照小区进行组织
									ten_figureShangeArray = fp.ten_figureShangeArray;// 将指纹库按照栅格进行组织
									forty_figureShangeArray = fp.forty_figureShangeArray;
									ten_shangekeyMap = fp.ten_shangekeyMap;// 10*10指纹库中栅格的序号
									forty_shangekeyMap = fp.forty_shangekeyMap;// 40*40指纹库中栅格的序号
									ten_forty_shangeKeyMap = fp.ten_forty_shangeKeyMap;
									if (nextEnb > 0)
									{
										fp.SetLoadStatus(0, nextEnb);// 预加载下一个ENB指纹库
									}
									break;
								}
							}
							else
							{
								try
								{
									System.out.println(
											GetNotimeString.returnTimeString() + "Not loaded " + s + " yet, wait.");
									Thread.sleep(1000);
								}
								catch (InterruptedException e)
								{
									e.printStackTrace();
								}
							}
						}
						// 改成由单独的线程加载

						System.out.println("enbid" + s + "的文件数：" + enbidMrFilemap.get(s).size());
						ExecutorService exec = Executors.newFixedThreadPool(threadNum);
						for (File mrfile : enbidMrFilemap.get(s))
						{
							exec.submit(new MutiThreadDoFixLocation(mrfile, samplePath, ten_figureShangeArray,
									ten_forty_shangeKeyMap, forty_figureShangeArray, forty_figureCellArray, gongcanMap,
									range, cellNum, mrBackPath, path, indoorGongcanMap));
							removeMap.put(mrfile.getParent(), 1);
						}
						if (enbidMrFilemap.get(s).size() > 0)
						{
							try
							{
								exec.shutdown();
								exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
								dealFile = dealFile + enbidMrFilemap.get(s).size();
								// 每个enbid的数据计算完释放空间
								{
									forty_figureCellArray = null;
									ten_figureShangeArray = null;
									forty_figureShangeArray = null;
									forty_shangekeyMap = null;
									ten_shangekeyMap = null;
									ten_forty_shangeKeyMap = null;
								}
							}
							catch (InterruptedException e1)
							{
								e1.printStackTrace();
							}
						}
						if (dealFile != 0)
						{
							System.out.println(GetNotimeString.returnTimeString() + "【【【总共文件数" + totalFile + "已经处理文件数"
									+ dealFile + "】】】，已处理【1/" + (totalFile / (double) dealFile) + "】");
						}
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
				}
				fp.bExitFlag = true;

				System.out.println(GetNotimeString.returnTimeString() + "数据定位完毕，接下来删除原目录文件夹");
				for (String removeKeys : removeMap.keySet())
				{
					LocalFile.deleteFile(removeKeys + "/DECODE_STATUS.bcp");
					File file = new File(removeKeys);
					if (file.list().length == 0)
					{
						System.out.println(GetNotimeString.returnTimeString() + "删除文件夹" + removeKeys
								+ (file.delete() ? "成功" : "失败"));
					}
					else
					{
						System.out.println(
								GetNotimeString.returnTimeString() + "文件夹" + removeKeys + "中存在文件无法删除；如果需要删除数据，请手动删除");
					}
				}
				try
				{
					System.out.println(GetNotimeString.returnTimeString() + "休息1分钟等待下一批Mr数据");
					Thread.sleep(60000);
				}
				catch (InterruptedException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		else
		{
			System.out.println(GetNotimeString.returnTimeString() + "新生成的指纹库路径没有配置，请配置后重新运行程序！");
		}
		return true;
	}

	/**
	 * 将文件按按照enbid分组
	 * 
	 * @param fileList
	 *            文件list
	 * @param enbid_position
	 *            enbid位置
	 * @param spliter
	 *            文件名的分隔符
	 * @return
	 */
	public static HashMap<Integer, ArrayList<File>> returnEnbidFileMap(List<String> fileList, int enbid_position,
			String spliter, String type)
	{
		File tempFile = null;
		HashMap<Integer, ArrayList<File>> enbidFileMap = new HashMap<Integer, ArrayList<File>>();
		if (fileList == null || fileList.size() == 0)
		{
			return enbidFileMap;
		}
		for (int i = 0; i < fileList.size(); i++)
		{
			tempFile = new File(fileList.get(i));
			if (type.equals("mr"))
			{
				if (tempFile.getName().startsWith("[err]") || !tempFile.getName().toUpperCase().contains("MRO")
						|| tempFile.getName().split(spliter).length < 6)
				{
					continue;
				}
			}
			int key = Integer.parseInt(tempFile.getName().split(spliter)[enbid_position]);
			ArrayList<File> enbidFileList = enbidFileMap.get(key);
			if (enbidFileList == null)
			{
				enbidFileList = new ArrayList<File>();
				enbidFileMap.put(key, enbidFileList);
			}
			enbidFileList.add(tempFile);
		}
		return enbidFileMap;
	}

	/**
	 * 返回“需要读取的文件”所在路径
	 *
	 * @return
	 */
	public static void returnConfig()
	{
		path = "D:\\figureFixPosition";
		samplePath = "D:\\MRSample";
		gridPath = "d:\\GridStatistic";
		sampleEventPath = "d:\\mastercom\\upload";
		mrBackPath = "";
		waittime = 1;
		noFixSamplePath = "";
		statisticPath = "";

		rsrp_min = -120;
		threadNum = 20;
		gridCellNum = 30;
		rsrp_Dvalue = 30;

		SAXReader reader = new SAXReader();

		OutputFormat format = OutputFormat.createPrettyPrint();
		format.setEncoding("GBK");// 设置XML文件的编码格式

		String filePath = "conf/config_figureFix.xml";
		File file = new File(filePath);
		if (file.exists())
		{
			try
			{
				Document doc = reader.read(file);
				List<String> list = doc.selectNodes("//comm//BcpBkPath");
				List<String> list1 = doc.selectNodes("//comm//MRSAMPLEPATH");
				List<String> list2 = doc.selectNodes("//comm//Range");
				List<String> list3 = doc.selectNodes("//comm//GridPath");
				List<String> list4 = doc.selectNodes("//comm//CellNum");
				List<String> list5 = doc.selectNodes("//comm//SampleEventPath");
				List<String> list6 = doc.selectNodes("//comm//MrBkPath");
				List<String> list7 = doc.selectNodes("//comm//WaitTime_Min");
				List<String> list8 = doc.selectNodes("//comm//bckNoFixSamplePath");
				List<String> list9 = doc.selectNodes("//comm//statisticPath");

				List<String> list10 = doc.selectNodes("//comm//rsrp_min");
				List<String> list11 = doc.selectNodes("//comm//threadNum");
				List<String> list12 = doc.selectNodes("//comm//baodiCellNum");
				List<String> list13 = doc.selectNodes("//comm//rsrp_Dvalue");

				Iterator iter = list.iterator();
				while (iter.hasNext())
				{
					Element element = (Element) iter.next();
					path = element.getText();
					break;
				}
				Iterator iter1 = list1.iterator();
				while (iter1.hasNext())
				{
					Element element = (Element) iter1.next();
					samplePath = element.getText();
					break;
				}
				Iterator iter2 = list2.iterator();
				while (iter2.hasNext())
				{
					Element element = (Element) iter2.next();
					range = Integer.parseInt(element.getText());
					break;
				}
				Iterator iter3 = list3.iterator();
				while (iter3.hasNext())
				{
					Element element = (Element) iter3.next();
					gridPath = element.getText();
					break;
				}
				Iterator iter4 = list4.iterator();
				while (iter4.hasNext())
				{
					Element element = (Element) iter4.next();
					cellNum = Integer.parseInt(element.getText());
					break;
				}
				Iterator iter5 = list5.iterator();
				while (iter5.hasNext())
				{
					Element element = (Element) iter5.next();
					sampleEventPath = element.getText();
					break;
				}
				Iterator iter6 = list6.iterator();
				while (iter6.hasNext())
				{
					Element element = (Element) iter6.next();
					mrBackPath = element.getText();
					break;
				}
				Iterator iter7 = list7.iterator();
				while (iter7.hasNext())
				{
					Element element = (Element) iter7.next();
					waittime = Integer.parseInt(element.getText());
					break;
				}
				Iterator iter8 = list8.iterator();
				while (iter8.hasNext())
				{
					Element element = (Element) iter8.next();
					noFixSamplePath = element.getText();
					break;
				}
				Iterator iter9 = list9.iterator();
				while (iter9.hasNext())
				{
					Element element = (Element) iter9.next();
					statisticPath = element.getText();
					break;
				}

				Iterator iter10 = list10.iterator();
				while (iter10.hasNext())
				{
					Element element = (Element) iter10.next();
					try
					{
						rsrp_min = Double.parseDouble(element.getText());
					}
					catch (NumberFormatException e)
					{
						rsrp_min = -125;
					}
					break;
				}
				Iterator iter11 = list11.iterator();
				while (iter11.hasNext())
				{
					Element element = (Element) iter11.next();
					try
					{
						threadNum = Integer.parseInt(element.getText());
					}
					catch (NumberFormatException e)
					{
						threadNum = 20;
					}
					break;
				}
				Iterator iter12 = list12.iterator();
				while (iter12.hasNext())
				{
					Element element = (Element) iter12.next();
					try
					{
						gridCellNum = Integer.parseInt(element.getText());
					}
					catch (NumberFormatException e)
					{
						gridCellNum = 30;
					}
					break;
				}
				Iterator iter13 = list13.iterator();
				while (iter13.hasNext())
				{
					Element element = (Element) iter13.next();
					try
					{
						rsrp_Dvalue = Double.parseDouble(element.getText());
					}
					catch (NumberFormatException e)
					{
						rsrp_Dvalue = 10;
					}
					break;
				}
			}
			catch (DocumentException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} // 读取XML文件
		}
	}

	@Override
	public void run()
	{
		FingerprintFixMr fm = new FingerprintFixMr();
		System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date()) + "--读取配置文件中... ...");
		FingerprintFixMr.init();
		System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date()) + "--读取配置文件结束");
		fm.readFile();

	}

	@SuppressWarnings("unchecked")
	public static void main(String args[])
	{
		FingerprintFixMr fm = new FingerprintFixMr();
		System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date()) + "--读取配置文件中... ...");
		FingerprintFixMr.init();
		System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date()) + "--读取配置文件结束");
		fm.readFile();
	}
}
