package cn.mastercom.sssvr.main;

import java.io.File;
import java.util.ArrayList;
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

public class FileMoverBeiJing extends Thread
{

	public static void main(String[] args)
	{
		FileMoverBeiJing fm = new FileMoverBeiJing();
		fm.start();
	}

	private static String HdfsRoot = "192.168.2.80";// 10.188.44.7
	public static HadoopFSOperations hdfs = new HadoopFSOperations();
	private static String httpPath = "";// E:\DATA\XDRDATA\HTTP\20170425
	private static String mmePath = "";// E:\DATA\XDRDATA\MME\20170425
	private static String srcCommonPah = "";// E:\DATA\XDRDATA
	private static String XdrBkPath = "";// E:\DATA\XDRDATA//BKXDR
	public static int totalFileNum;
	public static int dealedFileNum;

	@Override
	public void run()
	{
		Init();
		if (!LocalFile.checkFileExist(httpPath) && !LocalFile.checkFileExist(mmePath))
		{
			System.out.println("Xdr文件路径不存在！结束运行！");
			return;
		}
		while (!MainSvr.bExitFlag)
		{ // 循环解码/上传文件：循环调用多线程
			try
			{
				totalFileNum = 0;
				dealedFileNum = 0;
				MoveXdrFilesTohdfs(httpPath, mmePath);
			}
			catch (Exception e)
			{
				hdfs = new HadoopFSOperations(HdfsRoot);
				System.out.println("Thread " + " error:" + e.getMessage());
			}
		}
	}

	public static void Init()
	{
		System.out.println("FileMover beijing thread start!");
		readConfigInfo();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void readConfigInfo()
	{
		try
		{
			SAXReader reader = new SAXReader();

			OutputFormat format = OutputFormat.createPrettyPrint();
			format.setEncoding("GBK");// 设置XML文件的编码格式

			String filePath = "conf/config.xml";
			File file = new File(filePath);
			if (file.exists())
			{
				Document doc = reader.read(file);// 读取XML文件
				{
					List<String> list = doc.selectNodes("//comm/HdfsRoot");
					Iterator iter = list.iterator();
					while (iter.hasNext())
					{
						Element element = (Element) iter.next();
						HdfsRoot = element.getText();
						hdfs = new HadoopFSOperations(HdfsRoot);
						break;
					}
					List<String> list1 = doc.selectNodes("//comm/httpPath");
					Iterator iter1 = list1.iterator();
					while (iter1.hasNext())
					{
						Element element = (Element) iter1.next();
						httpPath = element.getText();
						break;
					}
					List<String> list2 = doc.selectNodes("//comm/mmePath");
					Iterator iter2 = list2.iterator();
					while (iter2.hasNext())
					{
						Element element = (Element) iter2.next();
						mmePath = element.getText();
						break;
					}
					List<String> list3 = doc.selectNodes("//comm/xdrBckPath");
					Iterator iter3 = list3.iterator();
					while (iter3.hasNext())
					{
						Element element = (Element) iter3.next();
						XdrBkPath = element.getText();
						break;
					}
					List<String> list4 = doc.selectNodes("//comm/srcCommonPah");
					Iterator iter4 = list4.iterator();
					while (iter4.hasNext())
					{
						Element element = (Element) iter4.next();
						srcCommonPah = element.getText();
						break;
					}
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	static Logger log = Logger.getLogger(FileMover.class.getName());

	/**
	 * 将数据移到hdfs上
	 * 
	 * @param filePath
	 */
	public static void MoveXdrFilesTohdfs(String httpfilePath, String mmefilePath)
	{
		httpfilePath = httpfilePath.replace("\\", "/");
		mmefilePath = mmefilePath.replace("\\", "/");
		List<String> httpfiles;
		List<String> mmefiles;
		try
		{
			httpfiles = LocalFile.getAllFiles(new File(httpfilePath), "beijing103", 1);
			mmefiles = LocalFile.getAllFiles(new File(mmefilePath), "", 1);
			if (httpfiles.size() == 0 && mmefiles.size() == 0)
			{
				System.out.println("上传xdr文件结束，等待下一批xdr数据！");
				Thread.sleep(10000);
				return;
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			System.out.println(e.getMessage());
			return;
		}
		System.out.println("需要上传MME文件" + mmefiles.size() + "个");
		System.out.println("需要上传HTTP文件" + httpfiles.size() + "个");
		totalFileNum = mmefiles.size() + httpfiles.size();
		// 单独启动一个线程上传数据
		if (mmefiles.size() > 0)
		{
			uploadXdrFile(mmefiles);
		}
		if (httpfiles.size() > 0)
		{
			uploadXdrFile(httpfiles);
		}
	}

	public static void uploadXdrFile(final List<String> files)
	{
		// new Thread(new Runnable()
		// {
		// @Override
		// public void run()
		// {
		// 创建一个线程池
		ExecutorService pool = Executors.newFixedThreadPool(20);
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
				if (!file.getName().endsWith(".gz"))
				{
					System.out.println("删除文件" + file.getPath() + (LocalFile.deleteFile_xdr(file.getPath()) ? "成功" : "失败"));
					continue;
				}
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
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		// }
		// }).start();
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
		{
			if (FileNames.size() == 0)
				return "";

			System.out.println("开始上传 " + FileNames.get(0));
			String FileName = FileNames.get(0);

			try
			{
				File file = new File(FileName);
				String type = "";
				if (FileName.toUpperCase().contains("MME"))
				{
					type = "news1mme_cut";
				}
				else if (FileName.toUpperCase().contains("S1U") && FileName.contains("beijing103"))
				{
					type = "news1u_http_cut";
				}
				else
				{
					return "数据目录名错误";
				}
				String dateStr = file.getParentFile().getParentFile().getName();
				String destDir = "/seq/" + type + "/" + dateStr;

				if (hdfs.putMerge(FileNames, destDir, file.getName().replace(".gz", ""), "", srcCommonPah, XdrBkPath, totalFileNum, dealedFileNum))
				{
					// dealedFileNum += 100;
					// System.out.println("MME 和HTTP数据总数：【" + totalFileNum +
					// "】,已经上传完成【" + dealedFileNum + "】");
					// if (XdrBkPath.length() > 0)
					// {
					// for (String fn : FileNames)
					// {
					// System.out.println("备份文件" + fn + (LocalFile.bkFile(fn,
					// fn.replace(srcCommonPah, XdrBkPath)) ? "成功" : "失败"));
					// }
					// }
					// else
					// {
					// for (String fn : FileNames)
					// {
					// System.out.println("删除文件" + fn +
					// (LocalFile.deleteFile_xdr(fn) ? "成功" : "失败"));
					// }
					// }
				}
				else
				{
					System.out.println("上传失败！");
					return file.getName() + " 上传失败";
				}
				return file.getName() + " 上传完成";
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			return FileName + " 上传fail";
		}
	}
}
