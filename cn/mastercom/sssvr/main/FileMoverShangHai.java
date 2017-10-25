package cn.mastercom.sssvr.main;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;

import cn.mastercom.sssvr.util.HadoopFSOperations;
import cn.mastercom.sssvr.util.LocalFile;

public class FileMoverShangHai extends Thread
{

	public static void main(String[] args)
	{
		FileMoverShangHai fm = new FileMoverShangHai();
		fm.start();
	}

	private static String NameNodeIp = "10.11.60.166";
	public static HadoopFSOperations hdfs = new HadoopFSOperations();
	private static String XdrPath = "B:/XDR";

	public static void Init()
	{
		if (!LocalFile.checkFileExist(XdrPath))
		{
			XdrPath = "E:/XDR";
		}
		System.out.println("FileMoverShangHai thread start!");
		readConfigInfo();
	}

	@Override
	public void run()
	{
		Init();
		try
		{
			Thread.sleep(10000);
		} catch (InterruptedException e1)
		{
		}

		while (!MainSvr.bExitFlag)
		{ // 循环解码/上传文件：循环调用多线程
			try
			{
				MoveXdrFilesTohdfs(XdrPath + "/S1-MME");
				MoveXdrFilesTohdfs(XdrPath + "/S1U");
				Thread.sleep(10000);
			} catch (Exception e)
			{
				System.out.println("Thread " + " error:" + e.getMessage());
			}
		}

		System.out.println("FileMoverShangHai thread end");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void readConfigInfo()
	{
		try
		{
			// XMLWriter writer = null;// 声明写XML的对象
			SAXReader reader = new SAXReader();

			OutputFormat format = OutputFormat.createPrettyPrint();
			format.setEncoding("GBK");// 设置XML文件的编码格式

			String filePath = "conf/config.xml";
			File file = new File(filePath);
			if (file.exists())
			{
				Document doc = reader.read(file);// 读取XML文件
				{
					List<String> list = doc.selectNodes("//comm/NameNodeIp");
					Iterator iter = list.iterator();
					while (iter.hasNext())
					{
						Element element = (Element) iter.next();
						NameNodeIp = element.getText();
						hdfs = new HadoopFSOperations("hdfs://" + NameNodeIp);
						break;
					}
				}
			}
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	final static class FileMoverCallable implements Callable<Object>
	{
		private List<String> FileNames = new ArrayList<String>();

		FileMoverCallable(List<String> fns)
		{
			this.FileNames.addAll(fns);
		}

		@Override
		public Object call()
		{// hdfs://192.168.1.31:9000/seq/news1mme_cut/20160923
			if (FileNames.size() == 0)
				return "";

			System.out.println("开始上传 " + FileNames.get(0));
			String FileName = FileNames.get(0);

			try
			{
				File file = new File(FileName);
				String type = "";
				if (FileName.contains("MME"))
				{
					type = "news1mme_cut";
				} else if (FileName.contains("S1U"))
				{
					type = "news1u_http_cut";
				} else
				{
					return "数据目录名错误";
				}
				String dateStr = file.getParentFile().getParentFile().getName();
				String hourStr = file.getParentFile().getName();

				// String filePath = FileName.substring(0, pos -
				// 1).replace("\\", "/");
				String destDir = "/seq/" + type + "/" + dateStr;
				// String filename = file.getParentFile().getName();

				if (NameNodeIp.trim().length() < 4 || hdfs.putMerge(FileNames, destDir, file.getName(), ""))
				{
					for (String fn : FileNames)
					{
						LocalFile.deleteFile(fn);
					}
				}

				/*
				 * String dateStr = file.getParentFile().getName(); String
				 * hourStr = file.getName();
				 * 
				 * //String filePath = FileName.substring(0, pos - 1).replace(
				 * "\\", "/"); String destDir = "hdfs://" + NameNodeIp +
				 * ":9000/seq/" +type+"/"+dateStr; String filename =
				 * dateStr+"_"+hourStr + ".csv";
				 * 
				 * if(NameNodeIp.trim().length()<4 || hdfs.putMerge(FileName,
				 * destDir, filename, "")) {
				 * LocalFile.deleteDirectory(FileName); }
				 */
				return file.getName() + " 上传完成";
			} catch (Exception e)
			{
				e.printStackTrace();
			}
			return FileName + " 上传fail";
		}
	}

	static Logger log = Logger.getLogger(FileMover.class.getName());

	/**
	 * 将数据移到hdfs上
	 * 
	 * @param filePath
	 */
	public static void MoveXdrFilesTohdfs(String filePath)
	{
		filePath = filePath.replace("\\", "/");
		List<String> files;
		HashMap<String, Integer> mapValue = new HashMap<String, Integer>();

		try
		{
			files = LocalFile.getAllFiles(new File(filePath), "", 1);
			if (files.size() > 0)
			{
				System.out.println("找到需要上传的文件 " + files.size() + "个");
			}
		} catch (Exception e)
		{
			e.printStackTrace();
			System.out.println(e.getMessage());
			return;
		}

		if (files.size() == 0)
			return;

		/*
		 * for (String fileName : files) { File file = new File(fileName); if
		 * (!mapValue.containsKey(file.getParent().toString())) {
		 * mapValue.put(file.getParent().toString(), 1); // 得到所有文件的上层文件夹 } }
		 */

		/*
		 * List<String> sortedList = new ArrayList<String>(); for (String
		 * fileName : mapValue.keySet()) { sortedList.add(fileName); }
		 * Collections.sort(sortedList);
		 */

		// 创建一个线程池
		ExecutorService pool = Executors.newFixedThreadPool(30);
		// 创建多个有返回值的任务
		List<Future> list = new ArrayList<Future>();
		List<String> fileList = new ArrayList<String>();

		for (String fileName : files)
		{
			File file = new File(fileName);
			if (MainSvr.bExitFlag == true)
			{
				break;
			}
			if (file.isFile())
			{
				fileList.add(fileName);
				if (fileList.size() >= 100)
				{
					Callable fm = new FileMoverCallable(fileList);
					// 执行任务并获取Future对象

					fileList = new ArrayList<String>();
					Future f = pool.submit(fm);
					list.add(f);
				}
			}
		}

		if (fileList.size() > 0)
		{
			Callable fm = new FileMoverCallable(fileList);
			// 执行任务并获取Future对象

			Future f = pool.submit(fm);
			list.add(f);
			fileList = new ArrayList<String>();
		}

		// 关闭线程池
		pool.shutdown();

		// 获取所有并发任务的运行结果
		try
		{
			for (Future f : list)
			{
				System.out.println(">>>" + f.get().toString());
			}
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
