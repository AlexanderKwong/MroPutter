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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import cn.mastercom.sssvr.util.ReturnConfig;

public class DealNoMergerFigure implements Runnable
{
	public String packageName;
	public File lowCoverface;
	public int num;
	public String enbidSimuFile;
	public String type;// 区分是building还是coverface，因为他们的结构不一样
	public int size;// 是40*40的还是10*10的

	public DealNoMergerFigure(String packageName, File lowCoverface, int num, String enbidSimuFile, String type,
			int size)
	{
		this.packageName = packageName;
		this.lowCoverface = lowCoverface;
		this.num = num;
		this.enbidSimuFile = enbidSimuFile;
		this.type = type;
		this.size = size;
	}

	@Override
	public void run()
	{
		BufferedReader bf = null;
		HashMap<String, BufferedWriter> IoFileMap = new HashMap<String, BufferedWriter>();
		double rsrp_min;
		try
		{
			new ReturnConfig();
			rsrp_min = Double
					.parseDouble(ReturnConfig.returnconfig("conf/config_figureFix.xml", "//comm//rsrp_min"));
		} catch (NumberFormatException e1)
		{
			rsrp_min = -125;
		}
		try
		{
			bf = new BufferedReader(new InputStreamReader(new FileInputStream(lowCoverface)));
			String line = "";
			GridKey key = null;
			StringBuffer GridFigureCellBuf = new StringBuffer();
			HashSet<Integer> set = new HashSet<Integer>();
			long i = 0;
			int cellnum = 0;// 记录同一个格子小区前N强
			try
			{
				new ReturnConfig();
				cellnum = Integer
						.parseInt(ReturnConfig.returnconfig("conf/config_figureFix.xml", "//comm//gridCellNum"));
			} catch (Exception e)
			{
				cellnum = 70;
			}

			int gridCellCount = 0;

			while ((line = bf.readLine()) != null)
			{
				i++;
				// 当前的eci
				int eci = 0;
				String[] figureArray = line.split(",|\t", -1);
				if (type.equals("coverface"))
				{
					eci = Integer.parseInt(figureArray[0]);
					if (figureArray.length < 4 || Double.parseDouble(figureArray[3]) < rsrp_min)
					{
						continue;
					}
				} else if (type.equals("building"))
				{
					eci = Integer.parseInt(figureArray[1]);
					if (figureArray.length < 6 || Double.parseDouble(figureArray[5]) < rsrp_min)
					{
						continue;
					}
				}
				// -------当前的key
				GridKey tempKey = new GridKey(line, 10);
				// 判断是否是同一个栅格
				if (GridFigureCellBuf.length() == 0)
				{
					GridFigureCellBuf.append(line + "\r\n");
					set.add(eci / 256);
					// 同一个格子的key
					key = new GridKey(line, 10);
					gridCellCount++;
				} else if (tempKey.equals(key) && gridCellCount <= cellnum)
				{
					GridFigureCellBuf.append(line + "\r\n");
					set.add(eci / 256);
					gridCellCount++;
				} else if (tempKey.equals(key) && gridCellCount > cellnum)
				{
					gridCellCount++;
				} else if (!tempKey.equals(key))
				{
					// deal
					writeEnbidFile(GridFigureCellBuf, set, enbidSimuFile, num, packageName, IoFileMap);
					// clear
					GridFigureCellBuf.setLength(0);
					set.clear();
					// put new
					key.setGridKey(tempKey);
					GridFigureCellBuf.append(line + "\r\n");
					set.add(eci / 256);
					gridCellCount = 1;
				}
				if (i % 10000 == 0)
				{
					System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date()) + "  处理"
							+ lowCoverface + "    no_merge " + (size == 10 ? "(10*10) " : "(40*40) ") + i + "条");
				}
			}
			// 写最后一次的数据
			// deal
			writeEnbidFile(GridFigureCellBuf, set, enbidSimuFile, num, packageName, IoFileMap);
			// clear
			GridFigureCellBuf.setLength(0);
			set.clear();
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally
		{
			try
			{
				bf.close();
			} catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * 将同一个栅格的数据写进出现过的enbid中
	 * 
	 * @param GridList
	 * @param enbidSimuFile
	 */
	public void writeEnbidFile(StringBuffer buf, Set<Integer> set, String enbidSimuFile, int num, String packageName,
			HashMap<String, BufferedWriter> IoFileMap)
	{
		File packagePath = new File(enbidSimuFile + "\\" + packageName);
		if (!packagePath.exists())
		{
			packagePath.mkdirs();
		}
		for (int s : set)
		{
			String FilePath = packagePath.getPath() + "\\" + s + "_" + num + ".txt";
			BufferedWriter bw = null;
			try
			{
				if (IoFileMap.size() > 200)
				{
					for (String key : IoFileMap.keySet())
					{
						IoFileMap.get(key).flush();
						IoFileMap.get(key).close();
					}
					IoFileMap.clear();
				}
				bw = IoFileMap.get(FilePath);
				if (bw == null)
				{
					bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(FilePath), true)));
					IoFileMap.put(FilePath, bw);
				}
				bw.write(buf.toString());
				bw.flush();// 否则deletebuff后数据丢失
			} catch (Exception e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
