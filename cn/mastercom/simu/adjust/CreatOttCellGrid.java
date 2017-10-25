package cn.mastercom.simu.adjust;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;

import cn.mastercom.sssvr.util.LocalFile;

public class CreatOttCellGrid
{
	public Tables table = new Tables();
	public String srcPath = "";
	public String ltePath = "";
	public String ottCellGridSavePath = "";
	public String gpsSavePath = "";
	public String wifiSavePath = "";
	public int threadNum = 1;
	public static int sampleSize;

	public static void main(String args[])
	{
		new CreatOttCellGrid().start();
	}

	public void start()
	{
		// long startTime = System.currentTimeMillis(); // 获取开始时间
		init();
		try
		{
			ExecutorService doSampleExec = Executors.newFixedThreadPool(threadNum);// 建立线程池
			String[] paths = srcPath.split(",", -1);
			List<String> sampleList = new ArrayList<String>();
			for (String path : paths)
			{
				sampleList.addAll(LocalFile.getAllFiles(new File(path.trim()), "", 1));
			}
			sampleSize = sampleList.size();
			System.out.println("总共要参与计算的sample文件个数：【" + sampleSize + "】");
			System.out.println("开启的线程数：" + threadNum);
			if (sampleList.size() < 2000)
			{
				Thread.sleep(3000);// 停顿三秒
			}
			for (String samplePath : sampleList)
			{
				doSampleExec.submit(new DoSampleThread(table, samplePath));
			}
			doSampleExec.shutdown();
			doSampleExec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);// 等待子线程执行完毕在执行主线程
			System.out.println("处理完毕，即将开始写数据！");
			saveFile(gpsSavePath, table.getGpsOttCellGridMapValues());
			saveFile(wifiSavePath, table.getWifiOttCellGridMapValues());
			System.out.println("写数据完毕！");

			// long endTime = System.currentTimeMillis(); // 获取结束时间
			// System.out.println(endTime - startTime);
		}
		catch (Exception e)
		{
			OutLog.dosom(e);
		}
	}

	public void init()
	{
		// 读取参数
		readConfig();
		// 加载工参
		CellConfig.getInstance().loadLteCell(ltePath);
	}

	public void readConfig()
	{
		try
		{
			String time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
			SAXReader reader = new SAXReader();
			OutputFormat format = OutputFormat.createPrettyPrint();
			format.setEncoding("GBK");// 设置XML文件的编码格式

			String filePath = "conf/config.xml";
			File file = new File(filePath);
			if (file.exists())
			{
				Document doc = reader.read(file);// 读取XML文件
				{
					List<String> list = doc.selectNodes("//comm/ottSamplePath");
					Iterator iter = list.iterator();
					while (iter.hasNext())
					{
						Element element = (Element) iter.next();
						srcPath = element.getText();
						break;
					}

					List<String> list1 = doc.selectNodes("//comm/LteCellPath");
					iter = list1.iterator();
					while (iter.hasNext())
					{
						Element element = (Element) iter.next();
						ltePath = element.getText();
						break;
					}

					List<String> list2 = doc.selectNodes("//comm/OttCellGridSavePath");
					iter = list2.iterator();
					while (iter.hasNext())
					{
						Element element = (Element) iter.next();
						ottCellGridSavePath = element.getText();
						gpsSavePath = ottCellGridSavePath + "\\gpsOttCellGrid" + time + ".txt";
						wifiSavePath = ottCellGridSavePath + "\\wifiOttCellGrid" + time + ".txt";
						break;
					}

					List<String> list3 = doc.selectNodes("//comm/ThreadNum");
					iter = list3.iterator();
					while (iter.hasNext())
					{
						Element element = (Element) iter.next();
						threadNum = Integer.parseInt(element.getText());
						break;
					}
				}
			}
		}
		catch (Exception e)
		{
			OutLog.dosom(e);
		}
	}

	public void saveFile(String savePath, Collection<OTT_Cell_Grid> mapValues)
	{
		BufferedWriter bw = null;
		try
		{
			File tempFile = new File(savePath);
			if (!tempFile.getParentFile().exists())
			{
				tempFile.getParentFile().mkdirs();
			}
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(savePath, true)));
			for (OTT_Cell_Grid cellGrid : mapValues)
			{
				bw.write(cellGrid.toline());
				bw.newLine();
			}
			bw.close();
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			OutLog.dosom(e);
		}
		finally
		{
			if (bw != null)
			{
				try
				{
					bw.close();
				}
				catch (IOException e)
				{
					// TODO Auto-generated catch block
					OutLog.dosom(e);
				}
			}
		}
	}
}
