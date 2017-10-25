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

import cn.mastercom.sssvr.util.ReturnConfig;

public class HadoopFigureDealNoMergeGrid implements Runnable
{
	public File figureFile;
	public HashMap<Integer, PubParticipation> gongcanMap;
	public File outFile;

	public HadoopFigureDealNoMergeGrid(File figureFile, HashMap<Integer, PubParticipation> gongcanMap, File outFile)
	{
		this.figureFile = figureFile;
		this.gongcanMap = gongcanMap;
		this.outFile = outFile;
	}

	@Override
	public void run()
	{
		BufferedReader bf = null;
		BufferedWriter bw = null;
		PubParticipation pp = null;
		int size = 0;
		double rsrp_min;
		double best_rsrp = 0;
		int rsrp_Dvalue;
		int cellnum = 0;// 记录同一个格子小区前N强
		int baodi_cellnum = 0;// 栅格保底n强
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
			new ReturnConfig();
			cellnum = Integer
					.parseInt(ReturnConfig.returnconfig("conf/config_figureFix.xml", "//comm//gridCellNum"));
		} catch (Exception e)
		{
			cellnum = 70;
		}
		try
		{
			new ReturnConfig();
			baodi_cellnum = Integer
					.parseInt(ReturnConfig.returnconfig("conf/config_figureFix.xml", "//comm//baodiCellNum"));
		} catch (Exception e)
		{
			baodi_cellnum = 30;
		}
		try
		{
			new ReturnConfig();
			rsrp_Dvalue = Integer
					.parseInt(ReturnConfig.returnconfig("conf/config_figureFix.xml", "//comm//rsrp_Dvalue"));
		} catch (NumberFormatException e1)
		{
			rsrp_Dvalue = 30;
		}

		int gridCellCount = 0;
		if (figureFile.getPath().contains("coverface")
				&& (figureFile.getPath().contains("40m") || figureFile.getPath().contains("40M")))
		{
			size = 40;
		} else
		{
			size = 10;
		}

		try
		{
			bf = new BufferedReader(new InputStreamReader(new FileInputStream(figureFile)));
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile)));
			String line = "";
			GridKey key = null;
			StringBuffer tempBuffer = new StringBuffer();
			long i = 0;

			while ((line = bf.readLine()) != null)
			{
				i++;
				String values[] = line.split(",|\t", -1);
				FigureCell figure = null;
				try
				{
					figure = new FigureCell(values);
				} catch (Exception e)
				{
					continue;
				}
				if (gongcanMap.get(figure.ieci) == null)
				{
					continue;
				}
				GridKey tempKey = new GridKey(line, 10);// 当前指纹的key值，此处的10表示不用汇聚
				if (tempBuffer.length() == 0)// 一个格子的第一条数据
				{
					// 装进一个格子第一条数据
					putFirstCellGrid(figure, tempBuffer);
					best_rsrp = figure.rsrp;
					// 记录同一个格子的key
					key = new GridKey(line, 10);
					gridCellCount++;
				} else if (tempKey.equals(key) && gridCellCount <= cellnum)
				{
					if (gridCellCount < baodi_cellnum) // 栅格小区最少个数
					{
						pp = gongcanMap.get(figure.ieci);
						StringBuffer tip = new StringBuffer();
						tip.append("|");
						tip.append(figure.ieci);
						tip.append(",");
						tip.append(figure.rsrp);
						tip.append(",");
						tip.append(pp.getfPoint());
						tip.append(",");
						tip.append(pp.getPci());
						tip.append(",");
						tip.append((int) (pp.getIlongitude() * 10000000));
						tip.append(",");
						tip.append((int) (pp.getIlatitud() * 10000000));
						tempBuffer.append(tip);
						gridCellCount++;
					} else if (figure.rsrp < rsrp_min)// 超过保底数一重门限
					{
						gridCellCount++;
					} else if (figure.rsrp < (best_rsrp - rsrp_Dvalue))// 超过保底数二重门限
					{
						gridCellCount++;
					} else
					{
						pp = gongcanMap.get(figure.ieci);
						StringBuffer tip = new StringBuffer();
						tip.append("|");
						tip.append(figure.ieci);
						tip.append(",");
						tip.append(figure.rsrp);
						tip.append(",");
						tip.append(pp.getfPoint());
						tip.append(",");
						tip.append(pp.getPci());
						tip.append(",");
						tip.append((int) (pp.getIlongitude() * 10000000));
						tip.append(",");
						tip.append((int) (pp.getIlatitud() * 10000000));
						tempBuffer.append(tip);
						gridCellCount++;
					}
				} else if (tempKey.equals(key) && gridCellCount > cellnum)
				{
					gridCellCount++;
				} else if (!tempKey.equals(key))
				{
					bw.write(tempBuffer.toString() + "\r\n"); // 写数据
					bw.flush();
					tempBuffer.setLength(0);// 清空buff
					putFirstCellGrid(figure, tempBuffer);// 装进新格子第一条数据
					best_rsrp = figure.rsrp;
					key.setGridKey(tempKey);// 设置新格子的key
					gridCellCount = 1;
				}
				if (i % 10000 == 0)
				{
					System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date()) + "  处理"
							+ figureFile + "  no_merge " + (size == 10 ? "(10*10) " : "(40*40) ") + i + "条");
				}
			}
			// 写最后一次的数据
			bw.write(tempBuffer.toString() + "\r\n"); // 写数据
			bw.flush();
			tempBuffer.setLength(0);// 清空buff
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally
		{
			try
			{
				bf.close();
				bw.close();
			} catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void putFirstCellGrid(FigureCell figure, StringBuffer tempBuffer)
	{
		if (figureFile.getPath().contains("coverface")
				&& (figureFile.getPath().contains("40m") || figureFile.getPath().contains("40M")))
		{
			tempBuffer.append("40low_" + figure.returnFigureAll());
		} else
		{
			tempBuffer.append("10_" + figure.returnFigureAll());
		}
		PubParticipation pp = gongcanMap.get(figure.ieci);
		tempBuffer.append(",");
		tempBuffer.append(pp.getfPoint());
		tempBuffer.append(",");
		tempBuffer.append(pp.getPci());
		tempBuffer.append(",");
		tempBuffer.append((int) (pp.getIlongitude() * 10000000));
		tempBuffer.append(",");
		tempBuffer.append((int) (pp.getIlatitud() * 10000000));
	}
}
