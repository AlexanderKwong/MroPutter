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
import java.util.Date;
import java.util.HashMap;

import cn.mastercom.sssvr.util.GridTimeKey;

public class AdjustSimu
{
	/**
	 * 
	 * @param localPaths
	 *            训练文件
	 * @param SimuFiles
	 *            指纹库文件
	 * @param savePath
	 *            保存路径
	 * @return
	 */
	public static String adjustSimu(ArrayList<File> localPaths, File[] SimuFiles, File savePath)
	{ 
		HashMap<GridTimeKey, FigureCell> simMap = null;// 装要校正的指纹数据
		FigureCell figureCell = null;
		BufferedReader dr = null;
		BufferedWriter Dw = null;
		String newFilePath = "";
		for (int i = 0; i < SimuFiles.length; i++)
		{
			try
			{
				dr = new BufferedReader(new InputStreamReader(new FileInputStream(SimuFiles[i])));
				String line = "";
				simMap = new HashMap<GridTimeKey, FigureCell>();
				int lines = 0;
				// 将指纹库组装成map
				while ((line = dr.readLine()) != null)
				{
					lines++;
					if (lines % 1000 == 0)
					{
						System.out.println("已经读入OTT" + lines + "行");
					}
					figureCell = new FigureCell(line.split(",|\t", -1));
					GridTimeKey key = new GridTimeKey(figureCell.ilongitude, figureCell.ilatitude, figureCell.level,
							figureCell.ieci);
					if (!simMap.containsKey(key))
					{
						simMap.put(key, figureCell);
					}
				}
				dr.close();
				// 校正
				lines = 0;
				for (int j = 0; j < localPaths.size(); j++)
				{
					dr = new BufferedReader(new InputStreamReader(new FileInputStream(localPaths.get(i))));
					while ((line = dr.readLine()) != null)
					{
						lines++;
						if (lines % 1000 == 0)
						{
							System.out.println("已经读入指纹库" + lines + "行");
						}
						figureCell = new FigureCell(line.split(",|\t", -1));
						GridTimeKey key = new GridTimeKey(figureCell.ilongitude, figureCell.ilatitude, figureCell.level,
								figureCell.ieci);
						if (simMap.containsKey(key))
						{
							simMap.get(key).rsrp = figureCell.rsrp;
						}
					}
					dr.close();
				}

				// 写新文件
				newFilePath = savePath.getPath() + "//" + new SimpleDateFormat("yyyyMMddhhmmss").format(new Date())
						+ "new_" + SimuFiles[i].getName();
				System.out.println("开始输出校准文件" + newFilePath);
				lines = 0;
				Dw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(newFilePath), true)));
				for (GridTimeKey s : simMap.keySet())
				{
					Dw.write(simMap.get(s).getFigureCell());
					Dw.newLine();
					lines++;
					if (lines % 1000 == 0)
					{
						System.out.println("输出" + lines + "行");
					}
				}
				Dw.close();
			} catch (Exception e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return newFilePath;
	}

}
