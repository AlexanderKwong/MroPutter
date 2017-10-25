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
import java.util.Date;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

import cn.mastercom.sssvr.util.FileOpt;
import cn.mastercom.sssvr.util.MroOrigDataMT;
import cn.mastercom.sssvr.util.ReturnConfig;

public class AdjustGongCan
{ 
	private File gongcanPath;
	private File[] AdjustFilePaths;
	private File adjustGongcanPath;

	public AdjustGongCan(File gongcanPath, File[] adjustFilePaths)
	{
		this.gongcanPath = gongcanPath;
		AdjustFilePaths = adjustFilePaths;
		adjustGongcanPath = new File(gongcanPath.getPath().replace(gongcanPath.getName(),
				new SimpleDateFormat("yyyyMMddhhmmss").format(new Date()) + gongcanPath.getName()));
	}

	/**
	 * 将工参组装成map
	 * 
	 * @return
	 */
	public HashMap<Integer, PubParticipation> returnGongCan()
	{ 
		HashMap<Integer, PubParticipation> gongcanMap = new HashMap<Integer, PubParticipation>();
		FileInputStream fs;
		try
		{
			fs = new FileInputStream(gongcanPath);
			BufferedReader bf = new BufferedReader(new InputStreamReader(fs));
			String line = "";
			int gongcanKey = 0;
			while ((line = bf.readLine()) != null)
			{
				String[] temp = line.split(",|\t", -1);
				PubParticipation pp = null;
				try
				{
					pp = new PubParticipation(temp);
				} catch (Exception e)
				{
					continue;
				}
				gongcanKey = pp.getEnodebId() * 256 + pp.getCellid();// 根据enbid和cellid组装eci作为key
				if (!gongcanMap.containsKey(gongcanKey))
				{
					gongcanMap.put(gongcanKey, pp);
				}
			}
			bf.close();
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return gongcanMap;
	}

	public void adjustGongcan() throws Exception
	{
		new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				// TODO Auto-generated method stub
				BufferedReader bf = null;
				System.out.println("准备组织工参数据");
				HashMap<Integer, PubParticipation> gongcanMap = returnGongCan();
				System.out.println("工参数据组织完毕");
				System.out.println("gongcan size:" + gongcanMap.size());
				ArrayList<String> filelist = new ArrayList<String>();
				String type = ReturnConfig.returnconfig("conf/config_figureFix.xml", "//comm//type");// 定位哪里的数据
				for (int i = 0; i < AdjustFilePaths.length; i++)// 得到所有Mr文件
				{
					FileOpt.getFile(AdjustFilePaths[i], filelist);
				}

				if (filelist.size() == 0)
				{
					System.out.println("参与校准文件下无文件");
					System.out.println("工参修正失败");
				} else
				{
					String line = "";
					String temp[] = null;
					MroOrigDataMT mrocell;
					PubParticipation pp = null;
					int num = 0;
					if (adjustGongcanPath.exists())
					{
						adjustGongcanPath.delete();
					}
					for (String s : filelist)
					{
						File tempFile = new File(s);
						if ((!tempFile.getName().toUpperCase().contains("MRO"))
								|| (!tempFile.getName().endsWith(".gz")))
						{
							continue;
						}
						try
						{
							bf = new BufferedReader(
									new InputStreamReader(new GZIPInputStream(new FileInputStream(new File(s)))));
							while ((line = bf.readLine()) != null)
							{
								temp = line.split(",|\t", -1);
								mrocell = new MroOrigDataMT();
								try
								{
									mrocell.FillData(temp, 0, type);
								} catch (Exception e)
								{
									continue;
								}
								pp = gongcanMap.get(mrocell.eci);
								if ((mrocell.Weight == 1) && (pp != null) && mrocell.LteScEarfcn >= 0
										&& mrocell.LteScPci >= 0 && ((pp.getfPoint() != mrocell.LteScEarfcn)
												|| (pp.getPci() != mrocell.LteScPci)))
								{
									System.out.println("找到错误工参数据：" + pp.getPPToString());
									pp.setfPoint(mrocell.LteScEarfcn);
									pp.setPci(mrocell.LteScPci);
									num++;
									System.out.println("修正之后工参数据：" + pp.getPPToString());
								}
							}
						} catch (Exception e)
						{
							// TODO: handle exception
							try
							{
								bf.close();
							} catch (IOException e1)
							{
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
							continue;
						}
					}
					System.out.println("总共工参数量：" + gongcanMap.size() + ",修正工参数量：" + num);
					System.out.println("生成新工参文件中... ...");
					BufferedWriter Dw = null;
					try
					{
						Dw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(adjustGongcanPath, true)));
						for (int s : gongcanMap.keySet())
						{
							Dw.write(gongcanMap.get(s).getPPToString());
							Dw.newLine();
						}
					} catch (Exception e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					} finally
					{
						try
						{
							Dw.close();
						} catch (IOException e)
						{
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					System.out.println("生成新工参数据完毕，新生成文件路径：" + adjustGongcanPath.getPath());
				}
			}
		}).start();

	}

}
