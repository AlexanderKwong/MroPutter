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
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;

import cn.mastercom.sssvr.util.GridTimeKey;
import cn.mastercom.sssvr.util.ReturnConfig;

public class MergeFigure
{
	public static void mergeFigure(final File[] simuFile, final File mergedSimuFile)
	{
		new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				// TODO Auto-generated method stub
				HashMap<GridTimeKey, MegerFigureCell> gridcellmap = null;// 按小区合并后装进
				HashMap<GridTimeKey, ArrayList<FigureCell>> gridmap = null;// 按栅格装进
				BufferedReader bf = null;
				int num = 0;
				try
				{
					num = Integer.parseInt(ReturnConfig.returnconfig("conf/config_figureFix.xml", "//comm//num"));// 栅格保留指小区场强前n强
				} catch (NumberFormatException e1)
				{
					num = 15;
				}
				int ilongitudRrandomNum;
				try
				{
					ilongitudRrandomNum = Integer.parseInt(
							ReturnConfig.returnconfig("conf/config_figureFix.xml", "//comm//ilongitudRrandomNum"));
				} catch (NumberFormatException e1)
				{
					ilongitudRrandomNum = 4000;
				}
				int ilatitudeRrandomNum;
				try
				{
					ilatitudeRrandomNum = Integer.parseInt(
							ReturnConfig.returnconfig("conf/config_figureFix.xml", "//comm//ilatitudeRrandomNum"));
				} catch (NumberFormatException e1)
				{
					ilatitudeRrandomNum = 3600;
				}
				System.out.println("每个栅格保留小区前" + num + "强");
				System.out.println("指纹库数量：" + simuFile.length);
				try
				{
					for (File s : simuFile)
					{
						gridcellmap = new HashMap<GridTimeKey, MegerFigureCell>();// 装指纹库信息
						gridmap = new HashMap<GridTimeKey, ArrayList<FigureCell>>();
						bf = new BufferedReader(new InputStreamReader(new FileInputStream(s)));
						String line = "";
						FigureCell figurecell = null;
						System.out.println("开始指纹库合并");
						long linenum = 0;
						while ((line = bf.readLine()) != null)
						{
							linenum++;
							try
							{
								figurecell = new FigureCell(line.split(",|\t"));
							} catch (Exception e)
							{
								continue;
							}
							int ilongitude = (figurecell.ilongitude / ilongitudRrandomNum) * ilongitudRrandomNum;
							int ilatitude = (figurecell.ilatitude / ilatitudeRrandomNum) * ilatitudeRrandomNum;
							GridTimeKey key = new GridTimeKey(ilongitude, ilatitude, figurecell.level, figurecell.ieci);
							if (!gridcellmap.containsKey(key))
							{
								MegerFigureCell megerFigureCell = new MegerFigureCell(figurecell, 1);
								megerFigureCell.getFigurecell().ilongitude = ilongitude + (ilongitudRrandomNum / 2);
								megerFigureCell.getFigurecell().ilatitude = ilatitude + (ilatitudeRrandomNum / 2);
								gridcellmap.put(key, megerFigureCell);
							} else
							{
								MegerFigureCell megerFigureCell = gridcellmap.get(key);
								megerFigureCell.getFigurecell().rsrp += figurecell.rsrp;
								megerFigureCell.setNum(megerFigureCell.getNum() + 1);
							}
							if (linenum % 10000 == 0)
							{
								System.out.println("已经处理数据" + linenum + "行");
							}
						}
						bf.close();
						System.out.println(s.getName() + "小区合并完毕！接下来将小区按照栅格合并，输出栅格中小区前" + num + "强");

						if (gridcellmap.size() > 0)
						{
							BufferedWriter dw = new BufferedWriter(
									new OutputStreamWriter(
											new FileOutputStream(
													new File(
															mergedSimuFile.getPath() + "\\"
																	+ new SimpleDateFormat("yyyyMMddhhmmss").format(
																			new Date())
																	+ "merged_" + s.getName()),
													true)));
							System.out.println("栅格合并开始");
							for (GridTimeKey key : gridcellmap.keySet())// 将小区按照栅格进行合并
							{
								MegerFigureCell megerFigureCell = gridcellmap.get(key);
								figurecell = megerFigureCell.getFigurecell();
								figurecell.rsrp = figurecell.rsrp / megerFigureCell.getNum();
								GridTimeKey gridkey = new GridTimeKey(key.getTllongitude(), key.getTllatitude(),
										key.getLevel());
								ArrayList<FigureCell> celllist = gridmap.get(gridkey);
								if (celllist == null)
								{
									celllist = new ArrayList<FigureCell>();
									gridmap.put(gridkey, celllist);
								}
								celllist.add(figurecell);
							}
							System.out.println("栅格合并结束");
							System.out.println("接下来输出栅格前" + num + "强！");
							for (GridTimeKey key : gridmap.keySet())
							{
								ArrayList<FigureCell> celllist = gridmap.get(key);
			//					celllist.sort(new FigureCellComparator());
								
								Collections.sort(celllist,new Comparator<FigureCell>()
								{

									@Override
									public int compare(FigureCell o1, FigureCell o2)
									{
										// TODO Auto-generated method stub
										if (o1.rsrp > o2.rsrp)
										{
											return -1;
										} else if (o1.rsrp < o2.rsrp)
										{
											return 1;
										}
										return 0;
									}
								});

								int cellsize = 0;
								if (celllist.size() > num)
								{
									cellsize = num;
								} else
								{
									cellsize = celllist.size();
								}
								for (int i = 0; i < cellsize; i++)
								{
									dw.write(celllist.get(i).getFigureCell());
									dw.newLine();
								}
							}
							dw.close();
							System.out.println("输出完毕！");
							System.out.println(s.getName() + "汇聚后文件已经生成！");
						}
					}
				} catch (Exception e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.out.println(e.getMessage());
				}
			}
		}).start();

	}

	public static void main(String args[])
	{
		File[] figureFile = new File[2];
		figureFile[0] = new File("C:\\Users\\xing\\Desktop\\辽阳\\liaoyangjiequ.txt");
		figureFile[1] = new File("C:\\Users\\xing\\Desktop\\辽阳\\liaoyanglouyu.txt");
		File savePath = new File("C:\\Users\\xing\\Desktop\\辽阳");
		MergeFigure.mergeFigure(figureFile, savePath);
	}
}
