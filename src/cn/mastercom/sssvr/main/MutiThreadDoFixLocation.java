package cn.mastercom.sssvr.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.zip.GZIPInputStream;

import cn.mastercom.sssvr.util.FileOpt;
import cn.mastercom.sssvr.util.GetNotimeString;
import cn.mastercom.sssvr.util.GisFunction;
import cn.mastercom.sssvr.util.GridTimeKey;
import cn.mastercom.sssvr.util.MroOrigDataMT;
import cn.mastercom.sssvr.util.NC_GSM;
import cn.mastercom.sssvr.util.NC_LTE;
import cn.mastercom.sssvr.util.NC_TDS;
import cn.mastercom.sssvr.util.ReturnConfig;
import cn.mastercom.sssvr.util.SIGNAL_MR_All;
import cn.mastercom.sssvr.util.StaticConfig;

public class MutiThreadDoFixLocation implements Callable
{
	public File dataFile = null;
	public String samplePath = null;
	public String mrBackPath = null;
	public String mrPath = null;
	public String type = ReturnConfig.returnconfig("conf/config_figureFix.xml", "//comm//type");// 定位哪里的数据
	public double pianyiNum = Double
			.parseDouble(ReturnConfig.returnconfig("conf/config_figureFix.xml", "//comm//pianyiNum"));
	public double percent = Double
			.parseDouble(ReturnConfig.returnconfig("conf/config_figureFix.xml", "//comm//percent"));

	HashMap<Integer, PubParticipation> gongcanMap = new HashMap<Integer, PubParticipation>();// 将公参中有用信息组织成map
	HashMap<Integer, PubParticipation> indoorGongcanMap = new HashMap<Integer, PubParticipation>();// 将公参中室内分布信息组织成map

	Map<String, NC_LTE> ncLteMap = new HashMap<String, NC_LTE>();
	Map<String, NC_GSM> ncGsmMap = new HashMap<String, NC_GSM>();
	Map<String, NC_TDS> ncTdsMap = new HashMap<String, NC_TDS>();
	int range = 3;
	int cellNum = 0;
	GridClass[] ten_figureShangeArray = null;
	GridClass[] forty_figureShangeArray = null;
	HashMap<Integer, ArrayList<Integer>> forty_figureCellArray = null;
	HashMap<GridTimeKey, ArrayList<Integer>> ten_forty_shangeKeyMap = null;

	HashMap<Long, HashMap<Integer, ArrayList<String>>> mmeues1apidDtCqtMap = new HashMap<Long, HashMap<Integer, ArrayList<String>>>();
	HashMap<Integer, ArrayList<String>> DtCqtMap = new HashMap<Integer, ArrayList<String>>();

	public MutiThreadDoFixLocation()
	{
	}

	public MutiThreadDoFixLocation(File dateFile, String samplePath, GridClass[] ten_figureShangeArray,
			HashMap<GridTimeKey, ArrayList<Integer>> ten_forty_shangeKeyMap, GridClass[] forty_figureShangeArray, // 40米指纹库
			HashMap<Integer, ArrayList<Integer>> forty_figureCellArray, HashMap<Integer, PubParticipation> gongcanMap,
			int range, int cellNum, String mrBackPath, String mrPath,
			HashMap<Integer, PubParticipation> indoorGongcanMap)
	{
		this.samplePath = samplePath;
		dataFile = dateFile;
		this.ten_figureShangeArray = ten_figureShangeArray;
		this.ten_forty_shangeKeyMap = ten_forty_shangeKeyMap;
		this.forty_figureShangeArray = forty_figureShangeArray;
		this.forty_figureCellArray = forty_figureCellArray;
		this.gongcanMap = gongcanMap;
		this.range = range;
		this.cellNum = cellNum;
		this.mrBackPath = mrBackPath;
		this.mrPath = mrPath;
		this.indoorGongcanMap = indoorGongcanMap;
	}

	@Override
	public String call() throws Exception
	{
		if (!dataFile.getName().toUpperCase().contains("MRO"))
		{
			File newFile = new File(dataFile.getPath().replace(mrPath, mrBackPath));
			File oldFile = dataFile;
			if (mrBackPath.trim().equals(""))
			{
				System.out.println(GetNotimeString.returnTimeString() + "文件" + dataFile.getAbsolutePath()
						+ "不是MRO数据，删除文件" + (dataFile.delete() ? "成功" : "失败"));
			}
			else
			{
				System.out.println(GetNotimeString.returnTimeString() + "文件" + dataFile.getAbsolutePath()
						+ "不是MRO数据，移动文件" + (FileOpt.moveFile(oldFile, newFile) ? "成功" : "失败"));
			}
			return "finish";
		}
		ArrayList<String> indoorString = new ArrayList<String>();
		ArrayList<String> MrNoFixed = new ArrayList<String>();// 存储没有定位成功的mr
		String[] filenames = dataFile.getName().split("_");
		String date = "";
		String spliter = "bcp";
		if (filenames.length >= 7)
		{
			date = filenames[6].substring(2, 8);
		}
		else if (type.equals("shenyang"))
		{
			date = filenames[1].substring(2, 8);
			spliter = "txt";
		}
		else
		{
			String parentPath = dataFile.getParentFile().getName();
			if (parentPath.startsWith("bcp"))
			{
				date = parentPath.substring(6, 12);
			}
		}
		if (forty_figureCellArray != null && forty_figureCellArray.size() != 0)
		{
			if (type.equals("shenyang"))
			{
				dealUemrFile(MrNoFixed, indoorString);
			}
			else
			{
				dealMrFile(MrNoFixed, indoorString);
			}
			writeSampleFile(MrNoFixed, indoorString, date, spliter);
		}
		return "finish";
	}

	public void writeSampleFile(ArrayList<String> MrNoFixed, ArrayList<String> indoorString, String date,
			String spliter)
	{
		try
		{
			File tempfile = null;
			BufferedWriter dtDw = null;
			BufferedWriter cqtDw = null;
			BufferedWriter dtexDw = null;
			BufferedWriter dwNoFixed = null;
			// 将室内数据写进cqt中

			String Filename = dataFile.getName().split(spliter)[0];
			String dtFilename = "TB_DTSIGNAL_SAMPLE_01_" + date + "\\" + Filename + "sample";
			String cqtFilename = "TB_CQTSIGNAL_SAMPLE_01_" + date + "\\" + Filename + "sample";
			String sampleFilename = "TB_SIGNAL_NOFIXSAMPLE_01_" + date + "\\" + Filename + "sample";
			String dtexFilename = "TB_DTEXSIGNAL_SAMPLE_01_" + date + "\\" + Filename + "sample";

			File file = null;
			Thread indoorThread = null;
			if (indoorString.size() > 0)
			{
				tempfile = new File(samplePath + "\\TB_CQTSIGNAL_SAMPLE_01_" + date);
				if (!tempfile.exists())
				{
					tempfile.mkdirs();
				}
				file = new File(samplePath + "\\" + cqtFilename);
				if (cqtDw == null)
				{
					cqtDw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
				}
				indoorThread = new ThreadWriteDTandCQT(indoorString, cqtDw);
				indoorThread.start();
			}
			// 将匹配到的数据写进相印的文件中
			ArrayList<String> cqtSampleClassList = new ArrayList<String>();
			ArrayList<String> dtSampleClassList = new ArrayList<String>();
			ArrayList<String> dtexSampleClassList = new ArrayList<String>();
			for (Long s : mmeues1apidDtCqtMap.keySet())
			{
				HashMap<Integer, ArrayList<String>> userSample = mmeues1apidDtCqtMap.get(s);
				int cqtsize = 0;
				int dtsize = 0;
				if (userSample.get(1) == null)
				{
					cqtsize = 0;
				}
				else
				{
					cqtsize = userSample.get(1).size();
				}
				if (userSample.get(-1) == null)
				{
					dtsize = 0;
				}
				else
				{
					dtsize = userSample.get(-1).size();
				}

				if (cqtsize / (double) (dtsize + cqtsize) >= percent)
				{
					getDtCqtSampleList(cqtSampleClassList, userSample.get(1), userSample.get(-1));
				}
				else if (dtsize / (double) (dtsize + cqtsize) >= percent)
				{
					getDtCqtSampleList(dtSampleClassList, userSample.get(1), userSample.get(-1));
				}
				else
				{
					getDtCqtSampleList(dtexSampleClassList, userSample.get(1), userSample.get(-1));
				}
			}
			// if (DtCqtMap.get(1) != null && DtCqtMap.get(1).size() > 0)
			// {
			// cqtSampleClassList = DtCqtMap.get(1);
			// }
			// if (DtCqtMap.get(-1) != null && DtCqtMap.get(-1).size() > 0)
			// {
			// dtSampleClassList = DtCqtMap.get(-1);
			// }

			Thread writeDt = null;
			if (dtSampleClassList.size() > 0)
			{
				tempfile = new File(samplePath + "\\TB_DTSIGNAL_SAMPLE_01_" + date);
				if (!tempfile.exists())
				{
					tempfile.mkdirs();
				}
				file = new File(samplePath + "\\" + dtFilename);
				if (dtDw == null)
				{
					dtDw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
				}
				writeDt = new ThreadWriteDTandCQT(dtSampleClassList, dtDw);
				writeDt.start();
			}
			Thread writeCqt = null;
			if (cqtSampleClassList.size() > 0)
			{
				tempfile = new File(samplePath + "\\TB_CQTSIGNAL_SAMPLE_01_" + date);
				if (!tempfile.exists())
				{
					tempfile.mkdirs();
				}
				file = new File(samplePath + "\\" + cqtFilename);
				if (cqtDw == null)
				{
					cqtDw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
				}
				writeCqt = new ThreadWriteDTandCQT(cqtSampleClassList, cqtDw);
				writeCqt.start();
			}
			Thread writeDtex = null;
			if (dtexSampleClassList.size() > 0)
			{
				tempfile = new File(samplePath + "\\TB_DTEXSIGNAL_SAMPLE_01_" + date);
				if (!tempfile.exists())
				{
					tempfile.mkdirs();
				}
				file = new File(samplePath + "\\" + dtexFilename);
				if (dtexDw == null)
				{
					dtexDw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
				}
				writeDtex = new ThreadWriteDTandCQT(dtexSampleClassList, dtexDw);
				writeDtex.start();
			}
			Thread wirteNoFixSampleThread = null;
			if (MrNoFixed.size() > 0)
			{
				tempfile = new File(samplePath + "\\TB_SIGNAL_NOFIXSAMPLE_01_" + date);
				if (!tempfile.exists())
				{
					tempfile.mkdirs();
				}
				file = new File(samplePath + "\\" + sampleFilename);
				if (dwNoFixed == null)
				{
					dwNoFixed = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
				}
				wirteNoFixSampleThread = new ThreadWriteDTandCQT(MrNoFixed, dwNoFixed);
				wirteNoFixSampleThread.start();
			}
			if (dtSampleClassList.size() > 0)
			{
				writeDt.join();
				dtDw.close();
			}
			if (cqtSampleClassList.size() > 0 || indoorString.size() > 0)
			{
				if (indoorThread != null)
				{
					indoorThread.join();
				}
				if (writeCqt != null)
				{
					writeCqt.join();
				}
				cqtDw.close();
			}
			if (dtexSampleClassList.size() > 0)
			{
				writeDtex.join();
				dtexDw.close();
			}
			if (MrNoFixed.size() > 0)
			{
				wirteNoFixSampleThread.join();
				dwNoFixed.close();
			}
			if (mrBackPath.trim().equals(""))
			{
				System.out.println(GetNotimeString.returnTimeString() + "删除文件" + dataFile.getAbsolutePath()
						+ (dataFile.delete() ? "成功" : "失败"));
			}
			else
			{
				File newFile = new File(dataFile.getPath().replace(mrPath, mrBackPath));
				File oldFile = dataFile;
				System.out.println(GetNotimeString.returnTimeString() + "文件" + dataFile + "解析完毕，移动文件"
						+ (FileOpt.moveFile(oldFile, newFile) ? "成功" : "失败"));
			}
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void getDtCqtSampleList(ArrayList<String> SampleClassList, ArrayList<String> SampleClassListOne,
			ArrayList<String> SampleClassListTwo)
	{
		if (SampleClassListOne != null)
		{
			SampleClassList.addAll(SampleClassListOne);
		}
		if (SampleClassListTwo != null)
		{
			SampleClassList.addAll(SampleClassListTwo);
		}
	}

	public void dealMrFile(ArrayList<String> MrNoFixed, ArrayList<String> indoorString)
	{
		try
		{
			String line = "";
			ArrayList<Integer> shangeid = new ArrayList<Integer>();// 用来将相关栅格所在位置拼在一起
			ArrayList<Integer> shangeidtemp = new ArrayList<Integer>();
			int flag = 0; // 0表示按主小区没有找到栅格，1表示定位到室外，2表示定位到室内
			ArrayList<MroOrigDataMT> mrlist = new ArrayList<MroOrigDataMT>();// 用来存储每个mr的数据
			MroOrigDataMT mrocell;
			PubParticipation pp = null;
			BufferedReader bf = new BufferedReader(
					new InputStreamReader(new GZIPInputStream(new FileInputStream(dataFile))));
			String temp[] = null;
			while ((line = bf.readLine()) != null)
			{// 第二次开始的时候，再计算第一次的值，导致最后一次无法计算
				temp = line.split(",|\\t|\\|", -1);
				mrocell = new MroOrigDataMT();
				try
				{
					mrocell.FillData(temp, 0, type);
				}
				catch (Exception e)
				{
					e.printStackTrace();
					continue;
				}
				if (mrocell.Weight == 1)
				{ // 从1开始，表示另外一个mr，用此数据找到对应栅格
					if (flag == 1)// 计算上一次的室外分布的数据
					{
						if (mrlist.size() > 0 && shangeid.size() > 0)
						{
							SampleClass sample = returnSampleClass(mrlist, shangeid);
							if (sample != null && sample.getFlag() == 1)// 表示定位成功
							{
								getMmeDtOrCqtMap(mmeues1apidDtCqtMap, sample);
								// getDtOrCqtMap(DtCqtMap, sample);
							}
							else if (sample != null && sample.getFlag() != 1)
							{
								MrNoFixed.add(sample.getMergeMr());
							}
						}
						shangeid.clear();
						mrlist.clear();
					}
					else if (flag == 2)// 计算上一次室内分布的数据
					{
						if (mrlist.size() > 0 && pp != null)
						{
							SampleClass sample = returnSampleClass(mrlist, pp);
							if (sample != null)
							{
								indoorString.add(sample.getMergeMr());
							}
						}
						mrlist.clear();
						pp = null;
					}
					else if (mrlist.size() > 0)
					{
						SampleClass sample = NoFixSample(mrlist);
						if (sample != null)
						{
							MrNoFixed.add(sample.getMergeMr());
						}
						mrlist.clear();
					}

					if (indoorGongcanMap.containsKey(mrocell.eci))
					{
						flag = 2;
						mrlist.add(mrocell);// 装进新weight=1的数据
						pp = indoorGongcanMap.get(mrocell.eci);
					}
					else
					{
						flag = 0;// 没找到主小区为0;主小区找到了且在indoorGongcanMap为2；主小区找得到不在indoorGongcanMap为1
						shangeidtemp.clear();
						if (forty_figureCellArray.size() <= range * 2 + 1)
						{
							for (Integer s : forty_figureCellArray.keySet())
							{// 找到相应的栅格
								int cellid = s / 1000;
								int rsrp = (s % 1000) * (-1);// 将rsrp变成负值。
								if (cellid == mrocell.CellId && rsrp >= mrocell.LteScRSRP - range
										&& rsrp <= mrocell.LteScRSRP + range)
								{// 组织之后的指纹库map的key值是:cellid_场强
									shangeidtemp.addAll(forty_figureCellArray.get(s));
									flag = 1;// 找到对应栅格，标志置为1
								}
							}
						}
						else
						{
							for (int s : cell_rsrps(mrocell.CellId, mrocell.LteScRSRP, range))
							{
								if (forty_figureCellArray.get(s) != null)
								{
									shangeidtemp.addAll(forty_figureCellArray.get(s));
									flag = 1;// 找到对应栅格，标志置为1
								}
							}
						}
						if (shangeidtemp.size() == 0)
						{
							shangeid.clear();
						}
						else
						{
							shangeid.addAll(shangeidtemp);
						}
						mrlist.add(mrocell);// 装进新weight=1的数据
					}
				}
				else
				{
					mrlist.add(mrocell);// 装入weight！=1的数据
				}
			}
			bf.close();
			// 计算最后一次的mr 数据。
			if (flag == 1 && mrlist.size() > 0 && shangeid.size() > 0)
			{
				SampleClass sample = returnSampleClass(mrlist, shangeid);
				shangeid.clear();
				if (sample != null && sample.getFlag() == 1)// 表示定位成功
				{
					getMmeDtOrCqtMap(mmeues1apidDtCqtMap, sample);
					// getDtOrCqtMap(DtCqtMap, sample);
				}
				else if (sample != null && sample.getFlag() != 1)
				{
					MrNoFixed.add(sample.getMergeMr());
				}
			}
			else if (flag == 2 && mrlist.size() > 0 && pp != null)
			{
				SampleClass sample = returnSampleClass(mrlist, pp);
				if (sample != null)
				{
					indoorString.add(sample.getMergeMr());
				}
				mrlist.clear();
				pp = null;
			}
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void dealUemrFile(ArrayList<String> MrNoFixed, ArrayList<String> indoorString)
	{
		int eci = 0;
		long ues1apid = 0L;
		String time = "";
		String line = "";
		ArrayList<Integer> shangeid = new ArrayList<Integer>();// 用来将相关栅格所在位置拼在一起
		ArrayList<Integer> shangeidtemp = new ArrayList<Integer>();
		int flag = 0; // 0表示按主小区没有找到栅格，1表示按主小区找到栅格
		ArrayList<MroOrigDataMT> mrlist = new ArrayList<MroOrigDataMT>();// 用来存储每个mr的数据
		MroOrigDataMT mrocell;
		PubParticipation pp = null;
		try
		{
			BufferedReader bf = new BufferedReader(new InputStreamReader(new FileInputStream(dataFile)));
			String temp[] = null;
			int linenum = 0;
			int errnum = 0;

			while ((line = bf.readLine()) != null)
			{// 第二次开始的时候，再计算第一次的值，导致最后一次无法计算
				linenum++;
				temp = line.split(",|\\t|\\|", -1);
				mrocell = new MroOrigDataMT();
				try
				{
					mrocell.FillData(temp, 0, type);
				}
				catch (Exception e)
				{
					errnum++;
					if (linenum == 100 && errnum == 100)
					{
						bf.close();
						System.out.println(GetNotimeString.returnTimeString() + "异常数据" + dataFile.getPath());
						File errorFile = dataFile;
						boolean qq = errorFile
								.renameTo(new File(errorFile.getParent() + "\\[err]" + errorFile.getName()));
						if (qq)
						{
							dataFile = new File(errorFile.getParent() + "\\[err]" + errorFile.getName());
						}
						break;
					}
					else
					{
						continue;
					}
				}

				if (mrocell.eci != eci || mrocell.MmeUeS1apId != ues1apid || !mrocell.beginTime.equals(time))
				{
					eci = mrocell.eci;
					ues1apid = mrocell.MmeUeS1apId;
					time = mrocell.beginTime;
					if (forty_figureCellArray != null)// 找到enobid
					{
						if (flag == 1)// 计算上一次的室外分布的数据
						{
							if (mrlist.size() > 0 && shangeid.size() > 0)
							{
								SampleClass sample = returnSampleClass(mrlist, shangeid);
								if (sample != null && sample.getFlag() == 1)// 表示定位成功
								{
									getMmeDtOrCqtMap(mmeues1apidDtCqtMap, sample);
									// getDtOrCqtMap(DtCqtMap, sample);
								}
								else if (sample != null && sample.getFlag() != 1)
								{
									MrNoFixed.add(sample.getMergeMr());
								}
							}
							shangeid.clear();
							mrlist.clear();
						}
						else if (flag == 2)// 计算上一次室内分布的数据
						{
							if (mrlist.size() > 0 && pp != null)
							{
								SampleClass sample = returnSampleClass(mrlist, pp);
								if (sample != null)
								{
									indoorString.add(sample.getMergeMr());
								}
							}
							mrlist.clear();
							pp = null;
						}
						else if (mrlist.size() > 0)
						{
							SampleClass sample = NoFixSample(mrlist);
							if (sample != null)
							{
								MrNoFixed.add(sample.getMergeMr());
							}
							mrlist.clear();
						}
						if (indoorGongcanMap.containsKey(mrocell.eci))
						{
							flag = 2;// 室分数据
							mrlist.add(mrocell);// 装进新weight=1的数据
							pp = indoorGongcanMap.get(mrocell.eci);
						}
						else
						{
							flag = 0;// 没找到主小区为0;主小区找到了且在indoorGongcanMap为2；主小区找得到不在indoorGongcanMap为1
							shangeidtemp.clear();
							if (forty_figureCellArray.size() <= range * 2 + 1)
							{
								for (Integer s : forty_figureCellArray.keySet())
								{// 找到相应的栅格
									int cellid = s / 1000;
									int rsrp = (s % 1000) * (-1);// map的key值中rsrp*-1后，变成正值了。
									if (cellid == mrocell.CellId && rsrp >= mrocell.LteScRSRP - range
											&& rsrp <= mrocell.LteScRSRP + range)
									{// 组织之后的指纹库map的key值是:cellid_场强
										shangeidtemp.addAll(forty_figureCellArray.get(s));
										flag = 1;// 找到对应栅格，标志置为1
									}
								}
							}
							else
							{
								for (int s : cell_rsrps(mrocell.CellId, mrocell.LteScRSRP, range))
								{
									if (forty_figureCellArray.get(s) != null)
									{
										shangeidtemp.addAll(forty_figureCellArray.get(s));
										flag = 1;// 找到对应栅格，标志置为1
									}
								}
							}
							if (shangeidtemp.size() == 0)
							{
								shangeid.clear();
							}
							else
							{
								shangeid.addAll(shangeidtemp);
							}
							mrlist.add(mrocell);// 装进新weight=1的数据
						}
					}
				}
				else
				{
					mrlist.add(mrocell);// 装入weight！=1的数据
				}
			}
			bf.close();

			// 计算最后一次的mr 数据。
			if (flag == 1 && mrlist.size() > 0 && shangeid.size() > 0)
			{
				SampleClass sample = returnSampleClass(mrlist, shangeid);
				shangeid.clear();
				if (sample != null && sample.getFlag() == 1)// 表示定位成功
				{
					getMmeDtOrCqtMap(mmeues1apidDtCqtMap, sample);
					// getDtOrCqtMap(DtCqtMap, sample);
				}
				else if (sample != null && sample.getFlag() != 1)
				{
					MrNoFixed.add(sample.getMergeMr());
				}
			}
			else if (flag == 2 && mrlist.size() > 0 && pp != null)
			{
				SampleClass sample = returnSampleClass(mrlist, pp);
				if (sample != null)
				{
					indoorString.add(sample.getMergeMr());
				}
				mrlist.clear();
				pp = null;
			}
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 单条Mr没有匹配上
	 * 
	 * @param mrlist
	 * @return
	 */
	public SampleClass NoFixSample(ArrayList<MroOrigDataMT> mrlist)
	{
		SampleClass sampleClass = null;
		try
		{
			OneGridResult fixedGridResult = new OneGridResult(null, 0, 0, 0, 0);
			HashMap<String, String> mrResult = mergeMrResult(mrlist, fixedGridResult);
			if (mrResult == null)
			{
				return null;
			}
			sampleClass = new SampleClass(mrResult.get("mergeMr"), StaticConfig.Int_Abnormal, StaticConfig.Int_Abnormal,
					Long.parseLong(mrResult.get("mmeues1apid")));
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return sampleClass;
	}

	/**
	 * 根据range求出按小区组织的栅格的map的key值
	 * 
	 * @param cell
	 * @param rsrp
	 * @param range
	 * @return
	 */
	public ArrayList<Integer> cell_rsrps(int cell, int rsrp, int range)
	{
		ArrayList<Integer> cellrsrps = new ArrayList<Integer>();
		if (rsrp > 0)
		{
			rsrp = (rsrp - 141) * -1;
		}
		else
		{
			rsrp = rsrp * -1;
		}
		cellrsrps.add(cell * 1000 + rsrp);
		if (range > 0)
		{
			for (int i = 1; i < range + 1; i++)
			{
				cellrsrps.add(cell * 1000 + (rsrp + i));
				cellrsrps.add(cell * 1000 + (rsrp - i));
			}
		}
		return cellrsrps;
	}

	/**
	 * 
	 * @param map
	 *            只有两个值 key=-1：表示dt//// key=1:表示cqt
	 * @param mergeMr
	 * @param level
	 * @return
	 */
	public HashMap<Long, HashMap<Integer, ArrayList<String>>> getMmeDtOrCqtMap(
			HashMap<Long, HashMap<Integer, ArrayList<String>>> mmeues1apidDtCqtMap, SampleClass mergeMr)
	{
		int level = 0;
		if (mergeMr.getLevel() >= 0)
		{
			level = 1;
		}
		else
		{
			level = -1;
		}

		HashMap<Integer, ArrayList<String>> DtCqtSampleMap = mmeues1apidDtCqtMap.get(mergeMr.getMmeues1paid());
		if (DtCqtSampleMap == null)
		{
			DtCqtSampleMap = new HashMap<Integer, ArrayList<String>>();
			ArrayList<String> SampleList = new ArrayList<String>();
			SampleList.add(mergeMr.getMergeMr());
			DtCqtSampleMap.put(level, SampleList);
			mmeues1apidDtCqtMap.put(mergeMr.getMmeues1paid(), DtCqtSampleMap);
		}
		else
		{
			ArrayList<String> SampleList = DtCqtSampleMap.get(level);
			if (SampleList == null)
			{
				SampleList = new ArrayList<String>();
				DtCqtSampleMap.put(level, SampleList);
			}
			SampleList.add(mergeMr.getMergeMr());
		}
		return mmeues1apidDtCqtMap;
	}

	public HashMap<Integer, ArrayList<String>> getDtOrCqtMap(HashMap<Integer, ArrayList<String>> DtCqtMap,
			SampleClass mergeMr)
	{
		int level = 0;
		if (mergeMr.getLevel() >= 0)
		{
			level = 1;
		}
		else
		{
			level = -1;
		}
		ArrayList<String> templist = DtCqtMap.get(level);
		if (templist == null)
		{
			templist = new ArrayList<String>();
			DtCqtMap.put(level, templist);
		}
		templist.add(mergeMr.getMergeMr());
		return DtCqtMap;
	}

	boolean bDebug = false;

	/**
	 * 将mr数据和各个栅格对比，将场强差距最小的栅格位置作为该mr的位置，然后合并这个mr数据成为一个sample
	 * 
	 * @param mrlist
	 * @param shanggeid
	 */
	public SampleClass returnSampleClass(ArrayList<MroOrigDataMT> mrlist, ArrayList<Integer> shanggeid)
	{
		int eci = mrlist.get(0).eci;// 主小区的eci
		int mr_rsrp = mrlist.get(0).LteScRSRP;// 主小区的场强
		ArrayList<OneGridResult> result = new ArrayList<OneGridResult>();// 装进计算后的结果。
		returnResultAndLocation(mrlist, shanggeid, result, forty_figureShangeArray);// 得到mr数据和各个40*40栅格比较结果，以及各个栅格的经纬度
		if (result.size() > 0)
		{
			preResult(result);// 预处理结果result已经按照匹配到的小区数量进行升序排序了
			OneGridResult fixedGridResult = getMrSimuLocation(result);// 得到最优的栅格
			if (ten_figureShangeArray != null)
			{
				ArrayList<Integer> suitTenGridid = returnTenGridid(fixedGridResult, mr_rsrp, eci);
				if (suitTenGridid != null && suitTenGridid.size() > 0)
				{
					ArrayList<OneGridResult> tenResult = new ArrayList<OneGridResult>();
					returnResultAndLocation(mrlist, suitTenGridid, tenResult, ten_figureShangeArray);// 得到mr数据和各个10*10栅格比较结果，以及各个栅格的经纬度
					if (tenResult.size() > 0)
					{
						preResult(tenResult);// 预处理结果result已经按照匹配到的小区数量进行升序排序了
						fixedGridResult = getMrSimuLocation(tenResult);// 得到最优的栅格
					}
				}
			}
			HashMap<String, String> mrResult = mergeMrResult(mrlist, fixedGridResult);
			if (mrResult == null)
			{
				return null;
			}
			SampleClass sampleclass = new SampleClass(mrResult.get("mergeMr"), fixedGridResult.getIlongitude(),
					fixedGridResult.getIlatitude(), Long.parseLong(mrResult.get("mmeues1apid")));
			sampleclass.setFlag(1);// 1表示定位成功
			sampleclass.setLevel(fixedGridResult.getLevel());
			return sampleclass;
		}
		else
		{// mr没有找到符合要求的栅格
			SampleClass sampleclass = NoFixSample(mrlist);
			if (sampleclass != null)
			{
				sampleclass.setFlag(0);// 0表示mr没有定位成功
			}
			return sampleclass;
		}
	}

	/**
	 * 在40*40栅格基础上得到符合条件的10*10栅格
	 * 
	 * @param fixedFortyGridResult
	 * @param mr_rsrp
	 * @param eci
	 * @return
	 */
	public ArrayList<Integer> returnTenGridid(OneGridResult fixedFortyGridResult, int mr_rsrp, int eci)
	{
		ArrayList<Integer> TenGrididList = new ArrayList<Integer>();
		ArrayList<Integer> tengridList = new ArrayList<Integer>();
		GridTimeKey mergeGridkey = new GridTimeKey(fixedFortyGridResult.getIlongitude(),
				fixedFortyGridResult.getIlatitude(), fixedFortyGridResult.getLevel());
		TenGrididList = ten_forty_shangeKeyMap.get(mergeGridkey);
		if (TenGrididList == null || TenGrididList.size() == 0)
		{
			return null;
		}
		for (int i = 0; i < TenGrididList.size(); i++)
		{
			int index = TenGrididList.get(i) - 1;
			GridClass grid = ten_figureShangeArray[index];
			if (grid != null
					&& (grid.getEci_map().get(eci) != null && grid.getEci_map().get(eci).rsrp >= mr_rsrp - range
							&& grid.getEci_map().get(eci).rsrp <= mr_rsrp + range))
			{
				tengridList.add(index);
			}
		}
		return tengridList;
	}

	/**
	 * 得到mr数据和各个栅格的比较结果
	 * 
	 * @param mrlist
	 * @param shanggeid
	 * @param result
	 * @param figureShangeArray
	 */
	public void returnResultAndLocation(ArrayList<MroOrigDataMT> mrlist, ArrayList<Integer> shanggeid,
			ArrayList<OneGridResult> result, GridClass[] figureShangeArray)
	{
		ArrayList<Integer> locationTemp = null;
		OneGridResult oneGridResult = null;
		ArrayList<Double> oneresult = null;
		MroOrigDataMT mrtemp;
		HashMap<Integer, FigureCell> grid_Eci_map = null;
		HashMap<Integer, FigureCell> grid_earfcn_pci_map = null;
		for (int i = 0; i < shanggeid.size(); i++)
		{
			int index = shanggeid.get(i);
			grid_Eci_map = figureShangeArray[index].getEci_map();// 该栅格下小区数据
			grid_earfcn_pci_map = figureShangeArray[index].getEarfcn_pci_map();
			oneresult = new ArrayList<Double>();
			FigureCell figurecell = null;
			for (int j = 0; j < mrlist.size(); j++)
			{// mr中的数据和每个栅格中的数据计算出一个差值
				try
				{
					mrtemp = mrlist.get(j);
					int ecitemp = mrtemp.eci;// eci
					int scrsrp = mrtemp.LteScRSRP;// 主小区场强
					if (j == 0)
					{// 主小区
						figurecell = grid_Eci_map.get(ecitemp);
						if (figurecell == null)// 10*10的可能会存在找不到相应eci的情况
						{
							break;
						}
						oneresult.add(Math.pow(scrsrp - figurecell.rsrp, 2));
						locationTemp = figurecell.location();
					}
					// 邻区
					if (mrtemp.LteNcRSRP == StaticConfig.Int_Abnormal || mrtemp.LteNcEarfcn == StaticConfig.Int_Abnormal
							|| mrtemp.LteNcPci == StaticConfig.Int_Abnormal)
					{
						continue;
					}
					int ncrsrp = mrtemp.LteNcRSRP;
					int earfcn_pci = mrtemp.LteNcEarfcn * 1000 + mrtemp.LteNcPci;// 频点和pci
					figurecell = grid_earfcn_pci_map.get(earfcn_pci);
					if (figurecell != null)
					{
						oneresult.add(Math.pow(ncrsrp - figurecell.rsrp, 2));
					}
				}
				catch (NumberFormatException e)
				{
					e.printStackTrace();
				}
			}
			// 栅格中的小区和mr中的小区，至少匹配上cellNum个，否则丢弃该栅格
			if (oneresult.size() >= cellNum)
			{
				oneGridResult = new OneGridResult(oneresult, locationTemp.get(2), locationTemp.get(0),
						locationTemp.get(1), locationTemp.get(3));
				result.add(oneGridResult);
			}
		}
	}

	public void preResult(ArrayList<OneGridResult> result)
	{
		ArrayList<Double> tempresult = null;
		for (int i = 0; i < result.size(); i++)
		{
			tempresult = result.get(i).getOneresult();
			Object[] dd = tempresult.toArray();
			Arrays.sort(dd, 1, dd.length);// 对结果从第二个值开始排序。
			dd[0] = (double) dd[0] * 2;// 主小区 增加权重、扩大影响
			tempresult.clear();
			double sum = 0.0;
			for (int t = 0; t < (dd.length <= 4 ? dd.length : 4); t++)
			{
				sum += (double) dd[t];
				if (result.get(i).getLevel() == -1)
				{
					tempresult.add(Math.sqrt(sum + 1) / ((t + 1) * (t + 1))); // 直接保存用来比较的的结果
				}
				else
				{
					tempresult.add(Math.sqrt(sum + 1) / ((t + 1) * (t + 1) * pianyiNum)); // 直接保存用来比较的的结果
				}
			}
		}
		Collections.sort(result, new SortForListSize());// result按照oneresult.size排序
	}

	public SampleClass returnSampleClass(ArrayList<MroOrigDataMT> mrlist, PubParticipation pp)
	{
		int Ilongitude = (int) (pp.getIlongitude() * 10000000);
		int Ilatitud = (int) (pp.getIlatitud() * 10000000);
		OneGridResult fixedGridResult = new OneGridResult(null, -2, Ilongitude, Ilatitud, 0);// indoor
																								// 的采样点level值设为-2
		HashMap<String, String> mrResult = mergeMrResult(mrlist, fixedGridResult);
		if (mrResult == null)
		{
			return null;
		}
		SampleClass sampleclass = new SampleClass(mrResult.get("mergeMr"), Ilongitude, Ilatitud,
				Long.parseLong(mrResult.get("mmeues1apid")));
		return sampleclass;
	}

	public OneGridResult getMrSimuLocation(ArrayList<OneGridResult> result)
	{
		if (result.size() == 1)
		{
			return result.get(0);
		}
		else
		{
			int best_Index = 0;
			int best_size = result.get(best_Index).getOneresult().size();
			for (int i = 1; i < result.size(); i++)
			{
				int current_size = result.get(i).getOneresult().size();
				if (current_size / 2 >= best_size)
				{
					best_Index = i;
					best_size = current_size;
				}
				else if (current_size == best_size)
				{
					if ((result.get(i).getOneresult().get(current_size - 1)
							- result.get(best_Index).getOneresult().get(best_size - 1) < 0)
							|| (result.get(i).getOneresult().get(current_size - 1)
									- result.get(best_Index).getOneresult().get(best_size - 1) == 0
									&& result.get(i).getLevel() >= 0 && result.get(best_Index).getLevel() < 0))// 差值相等也偏向cqt
					{
						best_Index = i;
						best_size = current_size;
					}
				}
				else
				{
					for (int j = best_size - 1; j < current_size; j++)
					{
						if (result.get(i).getOneresult().get(j)
								- result.get(best_Index).getOneresult().get(best_size - 1) <= 0)
						{
							best_Index = i;
							best_size = current_size;
							break;
						}
					}
				}
			}
			return result.get(best_Index);
		}
	}

	/**
	 * 合并mr成为sample
	 * 
	 * @param mrlist
	 * @param ilongitude
	 * @param ilatitude
	 * @param goodValue
	 * @return
	 */

	public HashMap<String, String> mergeMrResult(ArrayList<MroOrigDataMT> mrlist, OneGridResult fixedGridResult)
	{
		// 清空,每个mr重新装配一下
		HashMap<String, String> FixedSample = new HashMap<String, String>();// 装sample以及一些需要返回的属性
		ncLteMap.clear();
		ncGsmMap.clear();
		ncTdsMap.clear();
		String mergeMr = "";
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		SIGNAL_MR_All mrResult = new SIGNAL_MR_All();
		int flag = 0;// 公共信息执行一次标记
		for (MroOrigDataMT item : mrlist)
		{
			if (flag == 0)
			{
				try
				{
					int gongcanilongitude = 0;
					int gongcanilatitude = 0;
					mrResult.tsc.cityID = 0;
					if (gongcanMap.get(item.eci) != null)
					{
						gongcanilongitude = (int) (gongcanMap.get(item.eci).getIlongitude() * 10000000);
						gongcanilatitude = (int) (gongcanMap.get(item.eci).getIlatitud() * 10000000);
						mrResult.tsc.cityID = gongcanMap.get(item.eci).getCityid();
					}
					mrResult.tsc.SampleID = 0;
					try
					{
						TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
						Date d_beginTime = format.parse(item.beginTime);
						mrResult.tsc.itime = (int) (d_beginTime.getTime() / 1000L);
					}
					catch (Exception e)
					{
						mrResult.tsc.itime = 0;
					}
					mrResult.tsc.wtimems = 0;
					mrResult.tsc.bms = 0;
					mrResult.tsc.ilongitude = fixedGridResult.getIlongitude();
					mrResult.tsc.ilatitude = fixedGridResult.getIlatitude();
					mrResult.tsc.ispeed = fixedGridResult.getBuildingId();
					mrResult.tsc.imode = fixedGridResult.getLevel();
					mrResult.tsc.iLAC = 0;
					mrResult.tsc.iCI = mrResult.tsc.Eci = item.eci;
					mrResult.tsc.IMSI = item.MmeUeS1apId;
					mrResult.tsc.MSISDN = "";
					mrResult.tsc.UETac = "";
					mrResult.tsc.UEBrand = "";
					mrResult.tsc.UEType = "";
					mrResult.tsc.serviceType = 0;
					mrResult.tsc.serviceSubType = 0;
					mrResult.tsc.urlDomain = "";
					mrResult.tsc.IPDataUL = 0;
					mrResult.tsc.IPDataDL = 0;
					mrResult.tsc.duration = 0;
					mrResult.tsc.IPThroughputUL = 0;
					mrResult.tsc.IPThroughputDL = 0;
					mrResult.tsc.IPPacketUL = 0;
					mrResult.tsc.IPPacketDL = 0;
					mrResult.tsc.TCPReTranPacketUL = 0;
					mrResult.tsc.TCPReTranPacketDL = 0;
					mrResult.tsc.sessionRequest = 0;
					mrResult.tsc.sessionResult = 0;
					mrResult.tsc.eventType = 0;
					mrResult.tsc.userType = 0;
					mrResult.tsc.eNBName = "";
					mrResult.tsc.eNBLongitude = 0;
					mrResult.tsc.eNBLatitude = 0;
					mrResult.tsc.eNBDistance = 0;
					mrResult.tsc.flag = "MRO";
					mrResult.tsc.ENBId = item.ENBId;
					mrResult.tsc.UserLabel = item.UserLabel;
					mrResult.tsc.CellId = item.CellId;
					mrResult.tsc.Earfcn = item.LteScEarfcn;
					mrResult.tsc.SubFrameNbr = 0;
					mrResult.tsc.MmeCode = item.MmeCode;
					mrResult.tsc.MmeGroupId = item.MmeGroupId;
					mrResult.tsc.MmeUeS1apId = item.MmeUeS1apId;
					mrResult.tsc.Weight = item.Weight;
					mrResult.tsc.LteScRSRP = item.LteScRSRP;
					mrResult.tsc.LteScRSRQ = item.LteScRSRQ;
					mrResult.tsc.LteScEarfcn = item.LteScEarfcn;
					mrResult.tsc.LteScPci = item.LteScPci;
					mrResult.tsc.LteScBSR = item.LteScBSR;
					mrResult.tsc.LteScRTTD = item.LteScRTTD;
					mrResult.tsc.LteScTadv = item.LteScTadv;
					mrResult.tsc.LteScAOA = item.LteScAOA;
					mrResult.tsc.LteScPHR = item.LteScPHR;
					mrResult.tsc.LteScRIP = item.LteScRIP;
					mrResult.tsc.LteScSinrUL = item.LteScSinrUL;
					if (fixedGridResult.getOneresult() == null)
					{
						mrResult.tsc.LocFillType = 0;
					}
					else
					{
						mrResult.tsc.LocFillType = fixedGridResult.getOneresult().size();
					}
					mrResult.tsc.testType = 1;
					mrResult.tsc.location = 0;
					mrResult.tsc.dist = (int) GisFunction.GetDistance(fixedGridResult.getIlongitude(),
							fixedGridResult.getIlatitude(), gongcanilongitude, gongcanilatitude);
					if (fixedGridResult.getOneresult() == null)
					{
						mrResult.tsc.radius = 0;
					}
					else
					{
						mrResult.tsc.radius = (int) (fixedGridResult.getOneresult()
								.get(fixedGridResult.getOneresult().size() - 1) * 100);
					}
					mrResult.tsc.loctp = "wf";
					mrResult.tsc.indoor = StaticConfig.Int_Abnormal;
					mrResult.tsc.networktype = null;
					mrResult.tsc.label = "static";
					mrResult.tsc.simuLongitude = 0;
					mrResult.tsc.simuLatitude = 0;
					mrResult.tsc.moveDirect = -1;
					mrResult.tsc.mrType = "";
					mrResult.tsc.dfcnJamCellCount = 0;
					mrResult.tsc.sfcnJamCellCount = 0;
					flag++;// 公共信息赋值一次就好
				}
				catch (Exception e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
					continue;
				}
			}
			statLteNbCell(mrResult, item);
			statGsmNbCell(mrResult, item);
			statTdsNbCell(mrResult, item);
		}
		if (mrResult.tsc.MmeUeS1apId <= 0 || mrResult.tsc.Eci <= 0)
		{
			return null;
		}

		// NC LTE
		List<Map.Entry<String, NC_LTE>> ncLteList = new ArrayList<Map.Entry<String, NC_LTE>>(ncLteMap.entrySet());
		Collections.sort(ncLteList, new Comparator<Map.Entry<String, NC_LTE>>()
		{
			@Override
			public int compare(Map.Entry<String, NC_LTE> o1, Map.Entry<String, NC_LTE> o2)
			{
				return o2.getValue().LteNcRSRP - o1.getValue().LteNcRSRP;
			}
		});
		int cmccLteCount = 0;
		int lteCount_Freq = 0;

		for (int i = 0; i < ncLteList.size(); ++i)
		{
			NC_LTE item = ncLteList.get(i).getValue();
			if (mrResult.fillNclte_Freq(item))
			{
				lteCount_Freq++;
			}
			else
			{
				if (cmccLteCount < mrResult.tlte.length)
				{
					mrResult.tlte[cmccLteCount] = item;
					cmccLteCount++;
				}
			}
		}
		mrResult.nccount[0] = (byte) cmccLteCount;
		mrResult.nccount[2] = (byte) (lteCount_Freq);

		// NC TDS
		// TD只保留前2个邻区
		List<Map.Entry<String, NC_TDS>> ncTdsList = new ArrayList<Map.Entry<String, NC_TDS>>(ncTdsMap.entrySet());
		Collections.sort(ncTdsList, new Comparator<Map.Entry<String, NC_TDS>>()
		{
			@Override
			public int compare(Map.Entry<String, NC_TDS> o1, Map.Entry<String, NC_TDS> o2)
			{
				return o2.getValue().TdsPccpchRSCP - o1.getValue().TdsPccpchRSCP;
			}
		});

		int count = mrResult.ttds.length < ncTdsList.size() ? mrResult.ttds.length : ncTdsList.size();
		count = count > 2 ? 2 : count;
		mrResult.nccount[1] = (byte) count;

		for (int i = 0; i < count; ++i)
		{
			mrResult.ttds[i] = ncTdsList.get(i).getValue();
		}

		// NC GSM
		// GSM只保留前1个邻区
		List<Map.Entry<String, NC_GSM>> ncGsmList = new ArrayList<Map.Entry<String, NC_GSM>>(ncGsmMap.entrySet());
		Collections.sort(ncGsmList, new Comparator<Map.Entry<String, NC_GSM>>()
		{
			@Override
			public int compare(Map.Entry<String, NC_GSM> o1, Map.Entry<String, NC_GSM> o2)
			{
				return o2.getValue().GsmNcellCarrierRSSI - o1.getValue().GsmNcellCarrierRSSI;
			}
		});
		count = mrResult.tgsm.length < ncGsmList.size() ? mrResult.tgsm.length : ncGsmList.size();
		count = count > 1 ? 1 : count;
		for (int i = 0; i < count; ++i)
		{
			mrResult.tgsm[i] = ncGsmList.get(i).getValue();
		}
		mergeMr = mrResult.GetDataEx();
		FixedSample.put("mergeMr", mergeMr);
		FixedSample.put("mmeues1apid", mrResult.tsc.MmeUeS1apId + "");
		return FixedSample;
	}

	private void statLteNbCell(SIGNAL_MR_All mrResult, MroOrigDataMT item)
	{
		if (item.LteNcRSRP != StaticConfig.Int_Abnormal && item.LteNcEarfcn > 0 && item.LteNcPci > 0)
		{
			String key = item.LteNcEarfcn + "_" + item.LteNcPci;

			NC_LTE data = ncLteMap.get(key);
			if (data == null)
			{
				data = new NC_LTE();
				data.LteNcEarfcn = item.LteNcEarfcn;
				data.LteNcPci = item.LteNcPci;
				data.LteNcRSRP = item.LteNcRSRP;
				data.LteNcRSRQ = item.LteScRSRQ;

				ncLteMap.put(key, data);
			}
			else
			{
				if (item.LteNcRSRP > data.LteNcRSRP)
				{
					data.LteNcRSRP = item.LteNcRSRP;
					data.LteNcRSRQ = item.LteNcRSRQ;
				}
			}
		}
	}

	private void statGsmNbCell(SIGNAL_MR_All mrResult, MroOrigDataMT item)
	{
		if (item.GsmNcellCarrierRSSI != StaticConfig.Int_Abnormal && item.GsmNcellBcch > 0 && item.GsmNcellBcc > 0)
		{
			String key = item.GsmNcellBcch + "_" + item.GsmNcellBcc;

			NC_GSM data = ncGsmMap.get(key);
			if (data == null)
			{
				data = new NC_GSM();
				data.GsmNcellCarrierRSSI = item.GsmNcellCarrierRSSI;
				data.GsmNcellBsic = item.GsmNcellBcc;
				data.GsmNcellBcch = item.GsmNcellBcch;

				ncGsmMap.put(key, data);
			}
			else
			{
				if (item.GsmNcellCarrierRSSI > data.GsmNcellCarrierRSSI)
				{
					data.GsmNcellCarrierRSSI = item.GsmNcellCarrierRSSI;
				}
			}

		}
	}

	private void statTdsNbCell(SIGNAL_MR_All mrResult, MroOrigDataMT item)
	{
		if (item.TdsPccpchRSCP != StaticConfig.Int_Abnormal && item.TdsNcellUarfcn > 0 && item.TdsCellParameterId > 0)
		{
			String key = item.TdsNcellUarfcn + "_" + item.TdsCellParameterId;

			NC_TDS data = ncTdsMap.get(key);
			if (data == null)
			{
				data = new NC_TDS();
				data.TdsPccpchRSCP = item.TdsPccpchRSCP;
				data.TdsNcellUarfcn = (short) item.TdsNcellUarfcn;
				data.TdsCellParameterId = (short) item.TdsCellParameterId;

				ncTdsMap.put(key, data);
			}
			else
			{
				if (item.TdsPccpchRSCP > data.TdsPccpchRSCP)
				{
					data.TdsPccpchRSCP = item.TdsPccpchRSCP;
				}
			}

		}
	}

	public int getValidValueInt(int srcValue, int targValue)
	{
		if (targValue != StaticConfig.Int_Abnormal && targValue != -32768)
		{
			return targValue;
		}
		return srcValue;
	}

	public long getValidValueLong(long srcValue, long targValue)
	{
		if (targValue != StaticConfig.Long_Abnormal)
		{
			return targValue;
		}
		return srcValue;
	}

	public String getValidValueString(String srcValue, String targValue)
	{
		if (targValue != "")
		{
			return targValue;
		}
		return srcValue;
	}

	public double returnAverage(ArrayList<Double> arrays, int begin, int end)
	{
		double sum = 0;
		for (int i = begin; i < end; i++)
		{
			sum += arrays.get(i);
		}
		return sum / (end - begin);
	}
}
