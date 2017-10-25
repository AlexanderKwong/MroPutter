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
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import cn.mastercom.sssvr.util.ReturnConfig;

public class DealMergeGrid implements Runnable
{
	public String packageName;
	public File figureFile;
	public int num;
	public String enbidSimuFile;
	public String type;// 区分是building还是coverface，因为他们的结构不一样

	public DealMergeGrid()
	{
	}

	public DealMergeGrid(String packageName, File figureFile, int num, String enbidSimuFile, String type)
	{
		this.packageName = packageName;
		this.figureFile = figureFile;
		this.num = num;
		this.enbidSimuFile = enbidSimuFile;
		this.type = type;
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
		} catch (Exception e)
		{
			rsrp_min = -125;
		}
		try
		{
			bf = new BufferedReader(new InputStreamReader(new FileInputStream(figureFile)));
			String line = "";
			GridKey key = null;
			HashMap<Integer, FigureCell> FortyGridMap = new HashMap<Integer, FigureCell>();

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

			while ((line = bf.readLine()) != null)
			{
				i++;
				GridKey tempKey = new GridKey(line, 40);// 当前的key
				FigureCell figurecell = new FigureCell(line.split(",|\t", -1), 40);
				if (figurecell.rsrp < rsrp_min)
				{
					continue;
				}
				if (FortyGridMap.size() == 0)
				{
					MergeGrid(FortyGridMap, figurecell);
					key = new GridKey(line, 40);
				} else if (tempKey.equals(key))
				{
					MergeGrid(FortyGridMap, figurecell);
				} else if (!tempKey.equals(key))
				{
					// deal
					writeEnbidFile(FortyGridMap, enbidSimuFile, num, packageName, IoFileMap, cellnum);
					// clear
					FortyGridMap.clear();
					// put new
					MergeGrid(FortyGridMap, figurecell);
					key.setGridKey(tempKey);
				}

				if (i % 10000 == 0)
				{
					System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date()) + " 处理"
							+ figureFile + "    merge(40*40) " + i + "条");
				}
			}
			// 写最后一次的数据
			// deal
			writeEnbidFile(FortyGridMap, enbidSimuFile, num, packageName, IoFileMap, cellnum);
			// clear
			FortyGridMap.clear();
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

	public void MergeGrid(HashMap<Integer, FigureCell> GridMap, FigureCell temp)
	{
		int key = temp.ieci;
		FigureCell figurecell = GridMap.get(key);
		if (figurecell == null)
		{
			figurecell = new FigureCell(temp);
			figurecell.num = 1;
			GridMap.put(key, figurecell);
		} else
		{
			figurecell.rsrp += temp.rsrp;
			figurecell.num++;
		}
	}

	public void writeEnbidFile(HashMap<Integer, FigureCell> FortyGridMap, String enbidSimuFile, int num,
			String packageName, HashMap<String, BufferedWriter> IoFileMap, int cellnum)
	{
		// 对merge后的指纹库进行排序(升序)
		List<Map.Entry<Integer, FigureCell>> FortyGridList = new ArrayList<Map.Entry<Integer, FigureCell>>(
				FortyGridMap.entrySet());

		Collections.sort(FortyGridList, new Comparator<Map.Entry<Integer, FigureCell>>()
		{
			@Override
			public int compare(Map.Entry<Integer, FigureCell> o1, Map.Entry<Integer, FigureCell> o2)
			{
				double result = o2.getValue().rsrp / o2.getValue().num
						- o1.getValue().rsrp / o1.getValue().num;
				if (result > 0)
				{
					return 1;
				} else if (result < 0)
				{
					return -1;
				} else
				{
					return 0;
				}
			}
		});
		// 取前N强
		HashSet<Integer> set = new HashSet<Integer>();
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < (FortyGridList.size() > cellnum ? cellnum : FortyGridList.size()); i++)
		{
			set.add(FortyGridList.get(i).getKey() / 256);
			FigureCell figurecell = FortyGridList.get(i).getValue();
			figurecell.rsrp = figurecell.rsrp / figurecell.num;
			buf.append(figurecell.getFigureCell() + "\r\n");
		}

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
		buf.delete(0, buf.length());
	}
}
