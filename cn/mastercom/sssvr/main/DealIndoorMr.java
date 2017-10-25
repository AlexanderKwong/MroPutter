package cn.mastercom.sssvr.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.zip.GZIPInputStream;

import cn.mastercom.sssvr.util.FileOpt;
import cn.mastercom.sssvr.util.GetNotimeString;
import cn.mastercom.sssvr.util.MroOrigDataMT;
import cn.mastercom.sssvr.util.ReturnConfig;

public class DealIndoorMr implements Callable
{
	public File dataFile;
	public HashMap<Integer, PubParticipation> indoorGongcanMap;
	public String samplePath = null;
	public String mrBackPath = null;
	public String mrPath = null;
	public String type = ReturnConfig.returnconfig("conf/config_figureFix.xml", "//comm//type");// 定位哪里的数据

	public DealIndoorMr(File dataFile, HashMap<Integer, PubParticipation> indoorGongcanMap, String samplePath,
			String mrBackPath, String mrPath)
	{
		this.dataFile = dataFile;
		this.indoorGongcanMap = indoorGongcanMap;
		this.samplePath = samplePath;
		this.mrBackPath = mrBackPath;
		this.mrPath = mrPath;
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
				System.out.println(GetNotimeString.returnTimeString() + "文件" + dataFile + "不是MRO数据，删除文件"
						+ (new File(mrPath).delete() ? "成功" : "失败"));
			} else
			{
				System.out.println(GetNotimeString.returnTimeString() + "文件" + dataFile + "不是MRO数据，移动文件"
						+ (FileOpt.moveFile(oldFile, newFile) ? "成功" : "失败"));
			}
			return "finish";
		}
		ArrayList<String> indoorString = new ArrayList<String>();
		String[] filenames = dataFile.getName().split("_");
		String date = "";
		String spliter = "bcp";
		if (filenames.length >= 7)
		{
			date = filenames[6].substring(2, 8);
		} else if (type.equals("shenyang"))
		{
			date = filenames[1].substring(2, 8);
			spliter = "txt";
		} else
		{
			String parentPath = dataFile.getParentFile().getName();
			if (parentPath.startsWith("bcp"))
			{
				date = parentPath.substring(6, 12);
			}
		}
		if (type.equals("shenyang"))
		{
			delUemrFile(indoorString);
		} else
		{
			delMrFile(indoorString);
		}
		writeSampleFile(indoorString, date, spliter);
		return "finish";
	}

	public void delUemrFile(ArrayList<String> indoorString)
	{
		int eci = 0;
		long ues1apid = 0L;
		String time = "";
		String line = "";
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
				} catch (Exception e)
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
					} else
					{
						continue;
					}
				}

				if (mrocell.eci != eci || mrocell.MmeUeS1apId != ues1apid || !mrocell.beginTime.equals(time))
				{
					eci = mrocell.eci;
					ues1apid = mrocell.MmeUeS1apId;
					time = mrocell.beginTime;
					if (flag == 2)// 计算上一次室内分布的数据
					{
						if (mrlist.size() > 0 && pp != null)
						{
							SampleClass sample = new MutiThreadDoFixLocation().returnSampleClass(mrlist, pp);
							if (sample != null)
							{
								indoorString.add(sample.getMergeMr());
							}
						}
						mrlist.clear();
						pp = null;
					}
					if (indoorGongcanMap.containsKey(mrocell.eci))
					{
						flag = 2;// 室分数据
						mrlist.add(mrocell);// 装进新weight=1的数据
						pp = indoorGongcanMap.get(mrocell.eci);
					} else
					{
						flag = 0;
					}
				} else if (flag == 2)
				{
					mrlist.add(mrocell);// 装入weight！=1的数据
				}
			}
			bf.close();

			// 计算最后一次的mr 数据。
			if (flag == 2 && mrlist.size() > 0 && pp != null)
			{
				SampleClass sample = new MutiThreadDoFixLocation().returnSampleClass(mrlist, pp);
				if (sample != null)
				{
					indoorString.add(sample.getMergeMr());
				}
				mrlist.clear();
				pp = null;
			}
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void delMrFile(ArrayList<String> indoorString)
	{
		try
		{
			String line = "";
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
				} catch (Exception e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
					continue;
				}
				if (mrocell.Weight == 1)
				{ // 从1开始，表示另外一个mr，用此数据找到对应栅格
					if (flag == 2)// 计算上一次室内分布的数据
					{
						if (mrlist.size() > 0 && pp != null)
						{
							MutiThreadDoFixLocation MutiTem = new MutiThreadDoFixLocation();
							MutiTem.gongcanMap = indoorGongcanMap;
							SampleClass sample = MutiTem.returnSampleClass(mrlist, pp);
							if (sample != null)
							{
								indoorString.add(sample.getMergeMr());
							}
						}
						mrlist.clear();
						pp = null;
					}
					if (indoorGongcanMap.containsKey(mrocell.eci))
					{
						flag = 2;
						mrlist.add(mrocell);// 装进新weight=1的数据
						pp = indoorGongcanMap.get(mrocell.eci);
					} else
					{
						flag = 0;// 没找到主小区为0;主小区找到了且在indoorGongcanMap为2；主小区找得到不在indoorGongcanMap为1
					}
				} else if (flag == 2)
				{
					mrlist.add(mrocell);// 装入weight！=1的数据
				}
			}
			bf.close();
			// 计算最后一次的mr 数据。
			if (flag == 2 && mrlist.size() > 0 && pp != null)
			{
				MutiThreadDoFixLocation MutiTem = new MutiThreadDoFixLocation();
				MutiTem.gongcanMap = indoorGongcanMap;
				SampleClass sample = MutiTem.returnSampleClass(mrlist, pp);
				if (sample != null)
				{
					indoorString.add(sample.getMergeMr());
				}
				mrlist.clear();
				pp = null;
			}
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void writeSampleFile(ArrayList<String> indoorString, String date, String spliter)
	{
		try
		{
			File tempfile = null;
			BufferedWriter cqtDw = null;
			// 将室内数据写进cqt中

			String Filename = dataFile.getName().split(spliter)[0];
			String cqtFilename = "TB_CQTSIGNAL_SAMPLE_01_" + date + "\\" + Filename + "sample";
			File file = null;
			if (indoorString.size() > 0)
			{
				tempfile = new File(samplePath + "\\TB_CQTSIGNAL_SAMPLE_01_" + date);
				if (!tempfile.exists())
				{
					tempfile.mkdirs();
				}
				file = new File(samplePath + "\\" + cqtFilename);
				cqtDw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
				for (int i = 0; i < indoorString.size(); i++)
				{
					String temp = indoorString.get(i);
					if (temp.split("\t", -1).length < 167)
					{
						continue;
					}
					cqtDw.write(indoorString.get(i));
					cqtDw.newLine();
				}
				cqtDw.flush();
				cqtDw.close();
				// enbid对应不上指纹库的数据，只有存在室内分布才移动文件，否则文件将留在源目录
				if (mrBackPath.trim().equals(""))
				{
					System.out.println(GetNotimeString.returnTimeString() + "删除文件" + mrPath
							+ (new File(mrPath).delete() ? "成功" : "失败"));
				} else
				{
					File newFile = new File(dataFile.getPath().replace(mrPath, mrBackPath));
					File oldFile = dataFile;
					System.out.println(GetNotimeString.returnTimeString() + "文件" + dataFile + "解析完毕，移动文件"
							+ (FileOpt.moveFile(oldFile, newFile) ? "成功" : "失败"));
				}
			} else
			{
				System.out.println(GetNotimeString.returnTimeString() + "文件" + dataFile + "不存在指纹库文件，也不存在室分数据，无法定位！");
			}
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
