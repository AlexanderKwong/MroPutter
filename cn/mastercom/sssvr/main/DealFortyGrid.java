package cn.mastercom.sssvr.main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public class DealFortyGrid implements Runnable
{
	public HashMap<Integer, FigureCell> FortyGridMap;
	public FigureCell figurecell;
	public GridKey fortykey;
	public String packageName;
	public File enbidSimuFile;
	public int ThreadNum;
	public ConcurrentHashMap<String, BufferedWriter> IoFileMap;

	public DealFortyGrid(HashMap<Integer, FigureCell> FortyGridMap, FigureCell figurecell, GridKey fortykey,
			String packageName, File enbidSimuFile, int ThreadNum, ConcurrentHashMap<String, BufferedWriter> IoFileMap)
	{
		this.FortyGridMap = FortyGridMap;
		this.figurecell = figurecell;
		this.fortykey = fortykey;
		this.packageName = packageName;
		this.enbidSimuFile = enbidSimuFile;
		this.ThreadNum = ThreadNum;
		this.IoFileMap = IoFileMap;
	}

	@Override
	public void run()
	{
		// TODO Auto-generated method stub
		try
		{
			dealFortyGrid(FortyGridMap, figurecell, fortykey, packageName, enbidSimuFile, ThreadNum, IoFileMap);
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void dealFortyGrid(HashMap<Integer, FigureCell> FortyGridMap, FigureCell fortyfigure, GridKey fortykey,
			String packageName, File enbidSimuFile, int ThreadNum, ConcurrentHashMap<String, BufferedWriter> IoFileMap)
					throws Exception
	{
		if (FortyGridMap.size() == 0)// 第一条数据
		{
			fortykey.setGridKey(fortyfigure, 40);// 记录这个40*40栅格的key
			MergeGrid(FortyGridMap, fortyfigure);
		} else
		{
			GridKey tempFortyKey = new GridKey(fortyfigure, 40, false);
			if (tempFortyKey.equals(fortykey))
			{
				MergeGrid(FortyGridMap, fortyfigure);
			} else
			{
				writeEnbidFile(FortyGridMap, enbidSimuFile.getPath(), ThreadNum, packageName, IoFileMap);
				// 写出指纹库
				FortyGridMap.clear();
				// 清空列表
				MergeGrid(FortyGridMap, fortyfigure);
				fortykey.setGridKey(tempFortyKey);
				// 装进新指纹库 记录key
			}
		}
	}

	public static void MergeGrid(HashMap<Integer, FigureCell> GridMap, FigureCell temp)
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

	public static void writeEnbidFile(HashMap<Integer, FigureCell> FortyGridMap, String enbidSimuFile, int num,
			String packageName, ConcurrentHashMap<String, BufferedWriter> IoFileMap)
	{
		HashSet<Integer> set = new HashSet<Integer>();
		StringBuffer buf = new StringBuffer();
		for (int key : FortyGridMap.keySet())
		{
			set.add(key / 256);
			FigureCell figurecell = FortyGridMap.get(key);
			figurecell.rsrp = figurecell.rsrp / (double) figurecell.num;
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
