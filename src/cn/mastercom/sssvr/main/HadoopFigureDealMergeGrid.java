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
import java.util.List;
import java.util.Map;

import cn.mastercom.sssvr.util.ReturnConfig;

public class HadoopFigureDealMergeGrid implements Runnable
{
	public File figureFile;
	public HashMap<Integer, PubParticipation> gongcanMap;
	public File outFile;

	public HadoopFigureDealMergeGrid(File figureFile, HashMap<Integer, PubParticipation> gongcanMap, File outFile)
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
		double rsrp_min;
		int rsrp_Dvalue;
		int cellnum = 0;// 栅格前N强
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
			rsrp_Dvalue = Integer
					.parseInt(ReturnConfig.returnconfig("conf/config_figureFix.xml", "//comm//rsrp_Dvalue"));
		} catch (NumberFormatException e1)
		{
			rsrp_Dvalue = 30;
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
			bf = new BufferedReader(new InputStreamReader(new FileInputStream(figureFile)));
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile)));
			String line = "";
			GridKey key = null;
			HashMap<Integer, FigureCell> FortyGridMap = new HashMap<Integer, FigureCell>();
			long i = 0;
			while ((line = bf.readLine()) != null)
			{
				i++;
				String values[] = line.split(",|\t", -1);
				FigureCell figure = null;
				try
				{
					figure = new FigureCell(values, 40);
				} catch (Exception e)
				{
					continue;
				}
				if (gongcanMap.get(figure.ieci) == null)
				{
					continue;
				}
				GridKey tempKey = new GridKey(line, 40);// 当前指纹的key值
				if (FortyGridMap.size() == 0)// 一个格子的第一条数据
				{
					new DealMergeGrid().MergeGrid(FortyGridMap, figure);
					// 记录同一个格子的key
					key = new GridKey(line, 40);
				} else if (tempKey.equals(key))
				{
					new DealMergeGrid().MergeGrid(FortyGridMap, figure);
				} else if (!tempKey.equals(key))
				{
					WriteFigureCell(FortyGridMap, bw, cellnum, rsrp_Dvalue, rsrp_min, baodi_cellnum);// 写数据
					FortyGridMap.clear();// 清空map
					new DealMergeGrid().MergeGrid(FortyGridMap, figure);// 放新格子的第一条数据
					key.setGridKey(tempKey);// 记录新格子的key
				}
				if (i % 10000 == 0)
				{
					System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date()) + "  处理"
							+ figureFile + "  merge (40*40) " + i + "条");
				}
			}
			// 写最后一次的数据
			WriteFigureCell(FortyGridMap, bw, cellnum, rsrp_Dvalue, rsrp_min, baodi_cellnum);
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

	public void WriteFigureCell(HashMap<Integer, FigureCell> FortyGridMap, BufferedWriter bw, int cellnum,
			int rsrp_Dvalue, double rsrp_min, int baodicellnum)
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
		StringBuffer buff = new StringBuffer();
		double best_rsrp = FortyGridList.get(0).getValue().rsrp / FortyGridList.get(0).getValue().num;
		for (int i = 0; i < (FortyGridList.size() > cellnum ? cellnum : FortyGridList.size()); i++)// 前70强
		{
			FigureCell figure = FortyGridList.get(i).getValue();
			figure.rsrp = figure.rsrp / figure.num;
			PubParticipation pp = gongcanMap.get(figure.ieci);
			if (i < baodicellnum) // 前30强无条件加入
			{
				if (buff.length() == 0)
				{
					buff.append("40_" + figure.returnFigureAll());
					buff.append(",");
					buff.append(pp.getfPoint());
					buff.append(",");
					buff.append(pp.getPci());
					buff.append(",");
					buff.append((int) (pp.getIlongitude() * 10000000));
					buff.append(",");
					buff.append((int) (pp.getIlatitud() * 10000000));
				} else
				{
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
					buff.append(tip);
				}
			} else if (figure.rsrp < rsrp_min)// 前 30以后一重门限
			{
				break;
			} else if (figure.rsrp < (best_rsrp - rsrp_Dvalue))// 前30以后二重门限
			{
				break;
			} else
			{
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
				buff.append(tip);
			}
		}
		try
		{
			bw.write(buff.toString() + "\r\n");
			bw.flush();
			buff.setLength(0);
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
