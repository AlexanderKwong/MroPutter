package cn.mastercom.sssvr.main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DealTenGrid implements Runnable
{
	public StringBuffer tenGridBuffer;
	public Set<Integer> enbidSet;
	public FigureCell figureCell;
	public GridKey tenkey;
	public String packageName;
	public File enbidSimuFile;
	public int ThreadNum;
	public ConcurrentHashMap<String, BufferedWriter> IoFileMap;

	public DealTenGrid(StringBuffer tenGridBuffer, Set<Integer> enbidSet, FigureCell figureCell, GridKey tenkey,
			String packageName, File enbidSimuFile, int ThreadNum, ConcurrentHashMap<String, BufferedWriter> IoFileMap)
	{
		this.tenGridBuffer = tenGridBuffer;
		this.enbidSet = enbidSet;
		this.figureCell = figureCell;
		this.tenkey = tenkey;
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
			dealTenGrid(tenGridBuffer, enbidSet, figureCell, tenkey, packageName, enbidSimuFile, ThreadNum, IoFileMap);
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void dealTenGrid(StringBuffer tenGridBuffer, Set<Integer> enbidSet, FigureCell tenfigure, GridKey tenkey,
			String packageName, File enbidSimuFile, int ThreadNum, ConcurrentHashMap<String, BufferedWriter> IoFileMap)
					throws Exception
	{
		if (tenGridBuffer.length() == 0)// 第一条数据
		{
			tenkey.setGridKey(tenfigure, 10);// 记录这个10*10栅格的key
			tenGridBuffer.append(tenfigure.getFigureCell() + "\r\n");
			enbidSet.add(tenfigure.ieci / 256);
		} else
		{
			GridKey tempTenKey = new GridKey(tenfigure, 10, false);
			if (tempTenKey.equals(tenkey))
			{
				tenGridBuffer.append(tenfigure.getFigureCell() + "\r\n");
				enbidSet.add(tenfigure.ieci / 256);
			} else
			{
				writeEnbidFile(tenGridBuffer, enbidSet, enbidSimuFile.getPath(), ThreadNum, packageName, IoFileMap);
				// 写出指纹库
				tenGridBuffer.delete(0, tenGridBuffer.length());
				enbidSet.clear();
				// 清空列表和enbid
				tenGridBuffer.append(tenfigure.getFigureCell() + "\r\n");
				enbidSet.add(tenfigure.ieci / 256);
				tenkey.setGridKey(tempTenKey);
				// 装进新指纹库 记录key
			}
		}
	}

	/**
	 * 将同一个栅格的数据写进出现过的enbid中
	 * 
	 * @param GridList
	 * @param enbidSimuFile
	 */
	public static void writeEnbidFile(StringBuffer buf, Set<Integer> set, String enbidSimuFile, int num,
			String packageName, ConcurrentHashMap<String, BufferedWriter> IoFileMap)
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
