package cn.mastercom.sssvr.main;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import cn.mastercom.sssvr.util.GetNotimeString;
import cn.mastercom.sssvr.util.GridTimeKey;

public class FigurePreloader extends Thread
{

	boolean bExitFlag = false;
	public HashMap<Integer, ArrayList<Integer>> forty_figureCellArray = null;// key=
																				// //
																				// cell*1000+rsrp将指纹库按照小区进行组织
	public GridClass[] ten_figureShangeArray = null;// 将指纹库按照栅格进行组织
	public GridClass[] forty_figureShangeArray = null;
	public HashMap<GridTimeKey, Integer> ten_shangekeyMap = null;// 10*10指纹库中栅格的序号
	public HashMap<GridTimeKey, Integer> forty_shangekeyMap = null;// 40*40指纹库中栅格的序号
	public HashMap<GridTimeKey, ArrayList<Integer>> ten_forty_shangeKeyMap = null;// 10*10栅格按照40*40汇聚记录栅格序号
	// 在40*40的栅格中找到16个格子
	public HashMap<Integer, ArrayList<File>> ten_SimuFilemap = null;
	public HashMap<Integer, ArrayList<File>> forty_SimuFilemap = null;
	public HashMap<Integer, ArrayList<File>> fortyLow_Simu_Filemap = null;

	public static void main(String[] args)
	{
		FigurePreloader fp = new FigurePreloader();
		fp.ten_SimuFilemap = new HashMap<Integer, ArrayList<File>>();
		fp.forty_SimuFilemap = new HashMap<Integer, ArrayList<File>>();
		fp.fortyLow_Simu_Filemap = new HashMap<Integer, ArrayList<File>>();

		ArrayList<File> forty_SimuFiles = new ArrayList<File>();
		forty_SimuFiles.add(new File("D:/figureKu/fortySimuFile/585787_0.txt"));
		forty_SimuFiles.add(new File("D:/figureKu/fortySimuFile/585787_2.txt"));
		fp.forty_SimuFilemap.put(585787, forty_SimuFiles);
		fp.SetLoadStatus(0, 585787);
		FingerprintFixMr.init();
		fp.start();

		while (true)
		{
			if (fp.GetLoadStatus(585787) > 0)
			{
				System.out.println("loaded already!");
				fp.SetLoadStatus(0, 585787);
				try
				{
					Thread.sleep(10000);
					fp.bExitFlag = true;
					break;
				} catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			} else
			{
				try
				{
					System.out.println("not loaded yet, wait.");
					Thread.sleep(5000);
				} catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
		}
	}

	public int enbid = 0;
	private int loadStatus = 0;// 0 未处理 1 处理完成 2无指纹库
	Lock lock = new ReentrantLock();// 锁

	public void SetLoadStatus(int status, int enbid)
	{
		lock.lock();
		loadStatus = status;
		this.enbid = enbid;
		lock.unlock();
	}

	public int GetLoadStatus(int enbid)
	{
		if (this.enbid != enbid)
		{
			return 2;
		}

		int status = 0;
		lock.lock();
		status = loadStatus;
		lock.unlock();
		return status;
	}

	@Override
	public void run()
	{
		while (!bExitFlag)
		{
			if (GetLoadStatus(enbid) == 0)
			{
				forty_figureCellArray = new HashMap<Integer, ArrayList<Integer>>();// key=
																					// //
																					// cell*1000+rsrp将指纹库按照小区进行组织
				ten_figureShangeArray = null;// 将指纹库按照栅格进行组织
				forty_figureShangeArray = null;
				ten_shangekeyMap = new HashMap<GridTimeKey, Integer>();// 10*10指纹库中栅格的序号
				forty_shangekeyMap = new HashMap<GridTimeKey, Integer>();// 40*40指纹库中栅格的序号
				ten_forty_shangeKeyMap = new HashMap<GridTimeKey, ArrayList<Integer>>();

				// cell*1000+rsrp将指纹库按照小区进行组织
				if (ten_SimuFilemap.get(enbid) != null)
				{
					System.out.println();// 空行
					System.out.println(GetNotimeString.returnTimeString() + "enbid为" + enbid
							+ "   10*10的指纹库-------------------------------");
					long start = System.currentTimeMillis();
					ten_shangekeyMap = new HashMap<GridTimeKey, Integer>();
					int tenSimuGridNum = FingerprintFixMr.returnGridNum(ten_SimuFilemap.get(enbid), ten_shangekeyMap);
					ten_figureShangeArray = new GridClass[tenSimuGridNum];
					FingerprintFixMr.returnShangeAndCellArray(ten_SimuFilemap.get(enbid), ten_figureShangeArray, null,
							ten_shangekeyMap, false, enbid);// 组装10*10的
					ten_forty_shangeKeyMap = new HashMap<GridTimeKey, ArrayList<Integer>>();
					for (GridTimeKey gridkey : ten_shangekeyMap.keySet())// 将10*10的格子和40*40进行映射
					{
						int longitude = (gridkey.getTllongitude() / 4000) * 4000 + 2000;
						int latitude = (gridkey.getTllatitude() / 3600) * 3600 + 1800;
						int level = gridkey.getLevel();
						GridTimeKey mergeGridkey = new GridTimeKey(longitude, latitude, level);
						ArrayList<Integer> gridkeyList = ten_forty_shangeKeyMap.get(mergeGridkey);
						if (gridkeyList == null)
						{
							gridkeyList = new ArrayList<Integer>();
							ten_forty_shangeKeyMap.put(mergeGridkey, gridkeyList);
						}
						gridkeyList.add(ten_shangekeyMap.get(gridkey));
					}
					long end = System.currentTimeMillis();
					System.out.println("加载enbid" + enbid + "(10*10)的指纹库，用时【" + (end - start) + "】毫秒");
				}

				System.out.println(GetNotimeString.returnTimeString() + "enbid为" + enbid
						+ "   40*40的指纹库-------------------------------");
				long start = System.currentTimeMillis();
				forty_shangekeyMap = new HashMap<GridTimeKey, Integer>();
				int fortySimuGridNum = FingerprintFixMr.returnGridNum(forty_SimuFilemap.get(enbid), forty_shangekeyMap);
				if (fortyLow_Simu_Filemap != null && fortyLow_Simu_Filemap.containsKey(enbid))
				{// 郊区40*40的也放进forty_shangekeyMap中
					fortySimuGridNum = FingerprintFixMr.returnGridNum(fortyLow_Simu_Filemap.get(enbid),
							forty_shangekeyMap);
				}
				forty_figureShangeArray = new GridClass[fortySimuGridNum];
				forty_figureCellArray = new HashMap<Integer, ArrayList<Integer>>();
				FingerprintFixMr.returnShangeAndCellArray(forty_SimuFilemap.get(enbid), forty_figureShangeArray,
						forty_figureCellArray, forty_shangekeyMap, false, enbid);// 组装40*40的
				if (fortyLow_Simu_Filemap != null && fortyLow_Simu_Filemap.containsKey(enbid))
				{// 去重
					FingerprintFixMr.returnShangeAndCellArray(fortyLow_Simu_Filemap.get(enbid), forty_figureShangeArray,
							forty_figureCellArray, forty_shangekeyMap, true, enbid);// 组装40*40的
				}
				{
					System.out.println(GetNotimeString.returnTimeString() + "forty_figureCellArray大小:"
							+ forty_figureCellArray.size());
					long end = System.currentTimeMillis();
					System.out.println("加载enbid" + enbid + "(40*40)的指纹库，用时【" + (end - start) + "】毫秒");
				}
				SetLoadStatus(1, enbid);
			} else
			{
				try
				{
					Thread.sleep(1000);
				} catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
		}
		System.out.println("FigurePreloader exit.");
	}
};
