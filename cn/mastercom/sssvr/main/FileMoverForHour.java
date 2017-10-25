package cn.mastercom.sssvr.main;
/**
 * 
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.net.ftp.FTPFile;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;

import cn.mastercom.sqlhp.DBHelper;
import cn.mastercom.sqlhp.JobHelper;
import cn.mastercom.sqlhp.JobStatus;
//import cn.mastercom.sqlhp.DBHelper;
import cn.mastercom.sssvr.main.MainSvr;
import cn.mastercom.sssvr.main.FileMoverShanXiToFtp.FileMoverCallable;
import cn.mastercom.sssvr.util.CalendarEx;
import cn.mastercom.sssvr.util.DatafileInfo;
import cn.mastercom.sssvr.util.FTPClientHelper;
import cn.mastercom.sssvr.util.GreepPlumHelper;
import cn.mastercom.sssvr.util.HadoopFSOperations;
import cn.mastercom.sssvr.util.HdfsExplorer;
import cn.mastercom.sssvr.util.LocalFile;
import cn.mastercom.sssvr.util.ReturnConfig;

/**
 * @author
 *
 */
@SuppressWarnings("unused")
public class FileMoverForHour extends Thread {

	public static String MrBcpBkPath = "";
	public static List<Integer> MrBcpBkHours = new ArrayList<Integer>();
	
	private static int iFileSeqNum = 0;
	private static Lock lock = new ReentrantLock();// 锁对象  
	
	public static int GetNextFileSeqNum()
	{
        lock.lock();// 得到锁  
        try {  
        	if(iFileSeqNum>=999999999)
        		iFileSeqNum = 0;
        	
             return iFileSeqNum++;
        } finally {  
            lock.unlock();// 释放锁  
        }       
	}
	
	// 内部线程类，多线程上传文件
	final static class FileMoverFromFtpCallable implements Callable<Object>
	{
		private String FileName;

		FileMoverFromFtpCallable(String FileName)
		{
			this.FileName = FileName;
		}

		public Object call()
		{	
			FTPClientHelper ftp = null;
			try {
				ftp = GetFtpHelper();	        

				File file = new File(FileName);
				int pos = FileName.indexOf("mt_wlyh");
				if (pos < 0)
					return "文件名错误:" + FileName;

				String filePath = FileName.substring(0, pos - 1).replace("\\", "/");
				String filename = file.getPath().replace("\\", "/").replace("/upload", "").replaceFirst(filePath, hdfs.HADOOP_URL);
				filename = filename.replace("/mt_wlyh/Data", HdfsDataPath);
				String suffix = ".MRO";
				if (FileName.contains("mre"))
					suffix = ".MRE";

				String destDir = filename.replace(file.getName(), "");
				if(hdfsRoot.trim().length()>4 && ftp.putMergeToHdfs(FileName, destDir, hdfs, file.getName()+suffix))
				{				
					ftp.deleteDir(FileName,false);
				}
				else
				{
					
				}
				ftp.disconnect();
				return file.getName() + " 上传完成";
			} catch (Exception e) {
				
				if(ftp!=null)
				{
					try {
						ftp.disconnect();
					} catch (Exception e1) {
					}
				}
				e.printStackTrace();
			}
			return FileName + " 上传fail";
		}
	}
		
	// 内部线程类，多线程上传文件
	final static class FileMoverCallable implements Callable<Object>
	{
		private String FileName;

		FileMoverCallable(String FileName)
		{
			this.FileName = FileName;
		}

		public Object call()
		{
			try {
				File file = new File(FileName);
				int pos = FileName.indexOf("mt_wlyh");
				if (pos < 0)
					return "文件名错误:" + FileName;

				String filePath = FileName.substring(0, pos - 1).replace("\\", "/");
				String filename = file.getPath().replace("\\", "/").replace("/upload", "").replaceFirst(filePath, hdfs.HADOOP_URL);
				String suffix = ".MRO";
				if (FileName.contains("mre"))
					suffix = ".MRE";

				String destDir = filename.replace(file.getName(), "");
				if(hdfsRoot.trim().length()<4 || hdfs.putMerge(FileName, destDir, file.getName()+suffix, ""))
				{
					String dir = file.getName();
					String[] vct = dir.split("_",-1);
					int hour = 0;
					if(vct.length>=2)
					{
						if(vct[1].length()>=10)
						{
							hour = Integer.parseInt(vct[1].substring(6, 8));
						}
					}
					
					//只备份规定时段
					if(FileName.toLowerCase().contains("mro") 
							&& MrBcpBkHours.contains(hour) 
							&& MrBcpBkPath.length()>0)
					{
						dir = MrBcpBkPath + "/" + file.getName();
						String tempName = dir;
						int seq = 1;
						while (LocalFile.checkLocakFileExist(tempName))
						{
							seq++;
							tempName = dir+seq;
						}
						dir = tempName;
						LocalFile.renameDirectory(FileName, dir);
						String fileName = dir + "/DECODE_STATUS.bcp";
						File repfile = new File(fileName);
						FileOutputStream repos = new FileOutputStream(repfile);
						repos.close();
					}
					else
					{
						LocalFile.deleteDirectory(FileName);
					}
				}
				return file.getName() + " 上传完成";
			} catch (Exception e) {
				e.printStackTrace();
			}
			return FileName + " 上传fail";
		}
	}
	
	// 内部线程类，多线程上传文件
	final static class FileMergeCallable implements Callable<Object>
	{
		private String FileName;
		private int moverid;
		private String outputPath;
		
		static String sxFtpIp = "";
		static int sxFtpPort;
		static String sxFtpUser = "";
		static String sxFtpPassword = "";
		static String sxFtpPath = "";
		
		FileMergeCallable(String FileName, int moverid, String outputPath )
		{
			this.FileName = FileName;
			this.moverid = moverid;
			this.outputPath = outputPath;
			
			if(sxFtpIp.length()==0)
			{
				sxFtpIp = ReturnConfig.returnconfig("conf/config.xml", "//comm//sxftp//serverip");
				sxFtpPort = Integer.parseInt(ReturnConfig.returnconfig("conf/config.xml", "//comm//sxftp//serverport"));
				sxFtpUser = ReturnConfig.returnconfig("conf/config.xml", "//comm//sxftp//username");
				sxFtpPassword = ReturnConfig.returnconfig("conf/config.xml", "//comm//sxftp//password");
				sxFtpPath = ReturnConfig.returnconfig("conf/config.xml", "//comm//sxftp//remotepath");
			}
		}

		public Object call()
		{
			String fn = "";
			try {
				File file = new File(FileName);
				String parentPath = file.getName();
				int pos = parentPath.indexOf("_");
				String dateStr = "20" + parentPath.substring(pos+1,pos+11)+ "00";
				int fileSeq = GetNextFileSeqNum();
				String destFileName = String.format("MR%03d%09d%s.txt", moverid, fileSeq, dateStr);
								
				if(LocalFile.MergeFile(FileName, outputPath, destFileName, "", false))
				{
					LocalFile.deleteDirectory(FileName);
				}
				FTPClientHelper ftp = new FTPClientHelper(sxFtpIp,sxFtpPort,sxFtpUser,sxFtpPassword);
		        
		        ftp.setBinaryTransfer(true);
		        ftp.setPassiveMode(true);
		        ftp.setEncoding("utf-8");	        
		        fn = outputPath + "/" + destFileName;
		        
		        try {		 
		        	System.out.println("Put to Ftp:" + destFileName+".tmp");
					ftp.put(sxFtpPath+ "/" + destFileName+".tmp", fn, true, false);
		        	System.out.println("Rname Temp file to:" + destFileName);
					ftp.rename(sxFtpPath+ "/"  + destFileName+".tmp", sxFtpPath+ "/"  + destFileName);
		        	System.out.println("Delete local file:" + fn);					
					LocalFile.deleteFile(fn);				
				} 
		        catch (Exception e1) {
					e1.printStackTrace();
					LocalFile.deleteFile(fn);	
				}
				return file.getName() + " 上传完成";
			} catch (Exception e) {
				e.printStackTrace();
				LocalFile.deleteFile(fn);
			}
			return FileName + " 上传fail";
		}
	}

	static Logger log = Logger.getLogger(FileMoverForHour.class.getName());
	private static String DealMre = "1";
	private static String DealMtMro = "0";
	private static String DealBcpGp = "0";
	private static String DealBcp = "0";
	private static String MroWorkSpace = "";
	private static String MreWorkSpace = "";
	private static int    MoverId = 0;
	private static int    CheckBcpDays = 7;
	static boolean bInit =false;
	
	public static void Init()
	{
		if(bInit)
			return;
		bInit= true;
		
		readConfigInfo();
		if(hdfsRoot.length()>0)
		{
			hdfs = new HadoopFSOperations(hdfsRoot);
		}
		System.out.println("DealBcpGp:" + DealBcpGp);

		if (DealBcp.equals("1"))
		{
			FileMoverForHour sampleMover = new FileMoverForHour(2);
			sampleMover.start();
			log.info("Begin sampleMover...");
		}
		/*if (DealBcpGp.equals("1"))
		{
			FileMover mroMover = new FileMover(3);
			mroMover.start();
			log.info("Begin Gp Bcp...");
		}*/
	}

	public static void main(String[] args)
	{ 
		/*List<String> sortedList = new ArrayList<String>();
		sortedList.add("D://mroDocode//mt_wlyh//Data//mre//170316//HUAWEI4_1703160645_1");
		sortedList.add("D://mroDocode//mt_wlyh//Data//mre//170315//HUAWEI4_1703150645_1");
		//ComparatorMap cop = new ComparatorMap();
		//Collections.sort(sortedList);
		for(String ss:sortedList)
		{
			System.out.println(ss);
		}*/

		FileMoverForHour.Init();
	/*	FileMover mv = new FileMover(1);
		CalendarEx curTime = new CalendarEx(2017,2,21);
		mv.GetMRSampleinfoFromHdfs(curTime, true, "FG" ,"IN" ,"");
		mv.GetMRSampleinfoFromHdfs(curTime, true, "FG" ,"OUT" ,"");
		mv.GetMRSampleinfoFromHdfs(curTime, true, "OTT" ,"IN" ,"");
		mv.GetMRSampleinfoFromHdfs(curTime, true, "OTT" ,"OUT" ,"DTEX");
		mv.GetMRSampleinfoFromHdfs(curTime, true, "OTT" ,"OUT" ,"DT");*/
		
	
	 /*	mv.GetFreqGridByimeiInfoFromHdfs(curTime, true , "", "DT" ,"", "LT");
		mv.GetFreqGridByimeiInfoFromHdfs(curTime, true , "", "DT" ,"10", "LT");
		mv.GetFreqGridByimeiInfoFromHdfs(curTime, true , "", "CQT" ,"", "LT");
		mv.GetFreqGridByimeiInfoFromHdfs(curTime, true , "", "CQT" ,"10", "LT");
		mv.GetFreqGridByimeiInfoFromHdfs(curTime, true , "FG", "DT" ,"", "LT");
		mv.GetFreqGridByimeiInfoFromHdfs(curTime, true , "FG", "DT" ,"10", "LT");
		mv.GetFreqGridByimeiInfoFromHdfs(curTime, true , "FG", "CQT" ,"", "LT");
		mv.GetFreqGridByimeiInfoFromHdfs(curTime, true , "FG", "CQT" ,"10", "LT");
		
		mv.GetFreqGridByimeiInfoFromHdfs(curTime, true , "", "DT" ,"10", "DX");
		mv.GetFreqGridByimeiInfoFromHdfs(curTime, true , "", "CQT" ,"10", "DX");
		mv.GetFreqGridByimeiInfoFromHdfs(curTime, true , "FG", "CQT" ,"10", "DX");
		mv.GetFreqGridByimeiInfoFromHdfs(curTime, true , "FG", "CQT" ,"10", "DX");
		
		//newcellgrid
		mv.GetCellgridInfoFromHdfs(curTime, true , "", "DT" ,"");
		mv.GetCellgridInfoFromHdfs(curTime, true , "", "DT" ,"10");
		mv.GetCellgridInfoFromHdfs(curTime, true , "", "CQT" ,"");
		mv.GetCellgridInfoFromHdfs(curTime, true , "", "CQT" ,"10");
		mv.GetCellgridInfoFromHdfs(curTime, true , "FG", "DT" ,"");
		mv.GetCellgridInfoFromHdfs(curTime, true , "FG", "DT" ,"10");
		mv.GetCellgridInfoFromHdfs(curTime, true , "FG", "CQT" ,"");
		mv.GetCellgridInfoFromHdfs(curTime, true , "FG", "CQT" ,"10");
		
		//cellbyimei
		mv.GetFreqCellByimeiInfoFromHdfs(curTime, true , "" ,"10", "LT");
		mv.GetFreqCellByimeiInfoFromHdfs(curTime, true , "" ,"10", "DX");
		mv.GetFreqCellByimeiInfoFromHdfs(curTime, true , "FG" ,"10", "LT");
		mv.GetFreqCellByimeiInfoFromHdfs(curTime, true , "FG" ,"10", "DX");
		
		mv.GetMRGridFromHdfs(curTime,true,"0TT_","","OUT","^TB_MODEL_MR_OTT_OUT_GRID","outgrid");
		mv.GetMRGridFromHdfs(curTime,true,"GPS_","","OUT","^TB_MODEL_MR_GPS_OUT_GRID","outgrid");
		mv.GetMRGridFromHdfs(curTime,true,"FG_","","OUT","^TB_MODEL_MR_FG_OUT_GRID","outgrid");
		mv.GetMRGridFromHdfs(curTime,true,"OTT_","","IN","^TB_MODEL_MR_OTT_IN_GRID","ingrid");
		mv.GetMRGridFromHdfs(curTime,true,"FG_","","IN","^TB_MODEL_MR_FG_IN_GRID","ingrid");
		
		mv.GetMRGridFromHdfs(curTime,true,"OTT_","DX_","OUT","^TB_MODEL_MR_DX_OTT_OUT_GRID","outgrid");
		mv.GetMRGridFromHdfs(curTime,true,"GPS_","DX_","OUT","^TB_MODEL_MR_DX_GPS_OUT_GRID","outgrid");
		mv.GetMRGridFromHdfs(curTime,true,"FG_","DX_","OUT","^TB_MODEL_MR_DX_FG_OUT_GRID","outgrid");
		mv.GetMRGridFromHdfs(curTime,true,"OTT_","DX_","IN","^TB_MODEL_MR_DX_OTT_IN_GRID","ingrid");
		mv.GetMRGridFromHdfs(curTime,true,"FG_","DX_","IN","^TB_MODEL_MR_DX_FG_IN_GRID","ingrid");
		mv.GetMRGridFromHdfs(curTime,true,"OTT_","LT_","OUT","^TB_MODEL_MR_LT_OTT_OUT_GRID","outgrid");
		mv.GetMRGridFromHdfs(curTime,true,"GPS_","LT_","OUT","^TB_MODEL_MR_LT_GPS_OUT_GRID","outgrid");
		mv.GetMRGridFromHdfs(curTime,true,"FG_","LT_","OUT","^TB_MODEL_MR_LT_FG_OUT_GRID","outgrid");
		mv.GetMRGridFromHdfs(curTime,true,"OTT_","LT_","IN","^TB_MODEL_MR_LT_OTT_IN_GRID","ingrid");
		mv.GetMRGridFromHdfs(curTime,true,"FG_","LT_","IN","^TB_MODEL_MR_LT_FG_IN_GRID","ingrid");
		
		mv.GetMRBuildFromHdfs(curTime,true,"OTT","","^TB_MODEL_MR_OTT_BUILD","build");
		mv.GetMRBuildFromHdfs(curTime,true,"FG","","^TB_MODEL_MR_FG_BUILD","build");
		mv.GetMRBuildFromHdfs(curTime,true,"OTT","DX_","^TB_MODEL_MR_DX_OTT_BUILD","build");
		mv.GetMRBuildFromHdfs(curTime,true,"FG","DX_","^TB_MODEL_MR_DX_FG_BUILD","build");
		mv.GetMRBuildFromHdfs(curTime,true,"OTT","LT_","^TB_MODEL_MR_LT_OTT_BUILD","build");
		mv.GetMRBuildFromHdfs(curTime,true,"FG","LT_","^TB_MODEL_MR_LT_FG_BUILD","build");
		
		mv.GetMRCELLFromHdfs(curTime,true,"^TB_MODEL_MR_CELL","cell");
		mv.GetMRCELLFromHdfs(curTime,true,"DX","^TB_MODEL_MR_DX_CELL","cell");
		mv.GetMRCELLFromHdfs(curTime,true,"LT","^TB_MODEL_MR_LT_CELL","cell");
		
		mv.GetMRCELLGridFromHdfs(curTime,true,"","OTT_","OUT","^TB_MODEL_MR_OTT_OUT_CELLGRID","outcellgrid");
		mv.GetMRCELLGridFromHdfs(curTime,true,"","GPS_","OUT","^TB_MODEL_MR_GPS_OUT_CELLGRID","outcellgrid");
		mv.GetMRCELLGridFromHdfs(curTime,true,"","FG_","OUT","^TB_MODEL_MR_FG_OUT_CELLGRID","outcellgrid");
		mv.GetMRCELLGridFromHdfs(curTime,true,"","OTT_","IN","^TB_MODEL_MR_OTT_IN_CELLGRID","incellgrid");
		mv.GetMRCELLGridFromHdfs(curTime,true,"","FG_","IN","^TB_MODEL_MR_FG_IN_CELLGRID","incellgrid");
						*/
									
		
		/*mv.GetFreqGridByImeiFromHdfs(curTime, true,"","10","LT");
		mv.GetFreqGridByImeiFromHdfs(curTime, true,"FG","10","LT");
		mv.GetFreqGridByImeiFromHdfs(curTime, true,"","10","DX");
		mv.GetFreqGridByImeiFromHdfs(curTime, true,"FG","10","DX");
		
		mv.GetFreqCellByImeiFromHdfs(curTime, true,"FG");
		mv.GetFreqCellByImeiFromHdfs(curTime, true,"");*/
		
		//mv.GetMRBuildFromHdfs(curTime,true,"","^TB_MR_BUILD","build");
		//FileMover.MoveMroFilesToFtp("D:/Data/MrDecode/");
		//FileMergeCallable fm = new FileMergeCallable("A:/NSN_1704130000000",1,"a:/mastercom/ftp");
		//fm.call();
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
					List<String> list = doc.selectNodes("//comm/HdfsRoot");
					Iterator iter = list.iterator();
					while (iter.hasNext()) {
						Element element = (Element) iter.next();
						hdfsRoot = element.getText();
						break;
					}
				}


				{
					List<String> list = doc.selectNodes("//comm/DEAL_BCP");
					if (list != null)
					{
						Iterator iter = list.iterator();
						while (iter.hasNext())
						{
							Element element = (Element) iter.next();
							DealBcp = element.getText();
							break;
						}
					}
				}

				// MoverId
				{
					List<String> list = doc.selectNodes("//comm/MoverId");
					if (list != null)
					{
						Iterator iter = list.iterator();
						while (iter.hasNext())
						{
							Element element = (Element) iter.next();
							MoverId = Integer.parseInt(element.getText());
							break;
						}
					}
				}
				
				{
					List<String> list = doc.selectNodes("//comm/SampleEventRoot");
					if (list != null)
					{
						Iterator iter = list.iterator();
						while (iter.hasNext())
						{
							Element element = (Element) iter.next();
							rootPath = element.getText();
							break;
						}
					}
				}	

				{
					List<String> list = doc.selectNodes("//comm/HdfsDataPath");
					if (list != null)
					{
						Iterator iter = list.iterator();
						while (iter.hasNext())
						{
							Element element = (Element) iter.next();
							HdfsDataPath = element.getText();
							break;
						}
					}
				}
				
				{
					List<String> list = doc.selectNodes("//comm/CheckBcpDays");
					if (list != null)
					{
						Iterator iter = list.iterator();
						while (iter.hasNext())
						{
							Element element = (Element) iter.next();
							CheckBcpDays = Integer.parseInt(element.getText());
							break;
						}
					}
				}
			}
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static HadoopFSOperations hdfs = new HadoopFSOperations();

	private static String hdfsRoot = "";

	private static String rootPath = "E:/mastercom";;
	
	private static String localRoot = "e:/mastercom/SampleEvent";

	private String localRoot2 = "e:/mastercom/SampleEvent2";

	private static String localRoot3 = "e:/mastercom/SampleEvent3";

	private int FileType = 0;
	
	private static String HdfsDataPath = "/mt_wlyh/Data"; 

	FTPClientHelper ftp = new FTPClientHelper("10.110.180.233", 21, "boco", "Bodal@798");
	FTPClientHelper ftpFault = new FTPClientHelper("10.110.180.139", 21, "dongzeng", "dongzeng@boco123");


	public FileMoverForHour(int fileType)
	{
		FileType = fileType;
	}

	private boolean CheckHadoopFinished(String hdfsOutputDir)
	{
		FileStatus fileStatus = hdfs.getFileStatus(hdfsOutputDir);
		if (fileStatus != null)
		{
			if (hdfs.checkFileExist(fileStatus.getPath().getParent().toString() + "/output/_SUCCESS"))
				return true;
		}
		return false;
	}

	private static boolean CanDownload(String hdfsEventDir, String localDir, String localBackupDir)
	{
		if (LocalFile.checkFileExist(localDir)
				|| LocalFile.checkFileExist(localDir.replace("SampleEvent", "SampleEvent2"))
				|| LocalFile.checkFileExist(localDir.replace("SampleEvent", "SampleEvent3"))
				|| LocalFile.checkFileExist(localBackupDir))
		{
			System.out.println("本地目录下已经存在：" + localDir);
			return false;
		}

		FileStatus fileStatus = hdfs.getFileStatus(hdfsEventDir);
		if (fileStatus != null)
		{
			if (hdfs.checkFileExist(fileStatus.getPath().getParent().toString() + "/output/_SUCCESS")
			  || hdfs.checkFileExist(fileStatus.getPath() + "/_SUCCESS"))
			{
				System.out.println("可以下载：" + localDir);
				return true;
			}
			
			if (!hdfsEventDir.contains("mt_wlyh"))
			{
				long lastDirModifyTime = fileStatus.getModificationTime() / 1000L;
				final CalendarEx cruTime = new CalendarEx(new Date());

				if (lastDirModifyTime + 1200 < cruTime._second)
				{
					System.out.println("可以下载：" + localDir);
					return true;
				}
				else
				{
					System.out.println("Should Wait More Time：" + localDir);
				}
			}
			else
			{
				System.out.println("没有检测到完成标志文件：" + fileStatus.getPath().getParent().toString() + "/output/_SUCCESS");
			}
		}
		else
		{
			System.out.println("没有检测到目录：" + hdfsEventDir);
		}
		return false;
	}

	public void changeDirectory(String filename, String oldpath, String newpath, boolean cover)
	{
		if (!oldpath.equals(newpath))
		{
			File oldfile = new File(oldpath + "/" + filename);
			File newfile = new File(newpath + "/" + filename);
			if (newfile.exists())
			{// 若在待转移目录下，已经存在待转移文件
				if (cover)// 覆盖
					oldfile.renameTo(newfile);
				else
					System.out.println("在新目录下已经存在：" + filename);
			} else
			{
				oldfile.renameTo(newfile);
			}
		}
	}

	static int CheckCompanyMrFiles(CalendarEx cruTime, String company, String type)
	{
		boolean bFullFlag = false;
		int flag = 2;
		for (int i = 1; i <= 6; i++)
		{
			flag = CheckMrFileFinished(cruTime, company + i, type);
			if (flag != 2)
			{
				if (flag == 1)
					bFullFlag = true;
			} 
			else
			{
				break;
			}
		}
		if (flag == 2)
			return 2;
		if (bFullFlag)
			return 1;
		return 0;
	}

	public static void CheckAllCompanyMrFiles(String type)
	{
		CalendarEx cruTime = new CalendarEx(new Date());

		if (cruTime.getHour() >= 1)
		{
			cruTime = cruTime.AddDays(-1);
			String fileNameSuccess = HdfsDataPath +"/" + type + "/"
					+ cruTime.getDateStr8().substring(2) + "/_SUCCESS";
			boolean retSuccess = hdfs.checkFileExist(fileNameSuccess);

			if (!retSuccess)
			{
				int flag_hw = CheckCompanyMrFiles(cruTime, "HUAWEI", type);
				int flag_eric = CheckCompanyMrFiles(cruTime, "ERICSSON", type);
				int flag_al = CheckCompanyMrFiles(cruTime, "ALCATEL", type);
				int flag_dt = CheckCompanyMrFiles(cruTime, "DATANG", type);
				int flag_zte = CheckCompanyMrFiles(cruTime, "ZTE", type);

				if ((flag_zte + flag_hw + flag_eric + flag_al + flag_dt) > 0 && flag_zte != 2 && flag_hw != 2
						&& flag_eric != 2 && flag_al != 2 && flag_dt != 2)
				{
					log.info("写完成标志:" + fileNameSuccess + ":flag_zte " + flag_zte + ",flag_al " + flag_al + ",flag_hw "
							+ flag_hw + ",flag_eric " + flag_eric + ",flag_dt" + flag_dt);
					hdfs.CreateEmptyFile(fileNameSuccess);
				} 
				else if (cruTime.getHour() >= 6)
				{
					log.info("强制写完成标志:" + fileNameSuccess + ":flag_zte " + flag_zte + ",flag_al " + flag_al
							+ ",flag_hw " + flag_hw + ",flag_eric " + flag_eric + ",flag_dt" + flag_dt);
					hdfs.CreateEmptyFile(fileNameSuccess);
				}
				else
				{
					log.info(type+"处理未完成:" + fileNameSuccess + ":flag_zte " + flag_zte + ",flag_al " + flag_al
							+ ",flag_hw " + flag_hw + ",flag_eric " + flag_eric + ",flag_dt" + flag_dt);
				}
			}
		}
	}

	static int CheckMrFileFinished(CalendarEx cal, String company, String suffix)
	{
		// hdfs://" + NameNodeIp +
		// ":9000/mt_wlyh/Data/mromt/151223/ERIC_15122308.mromt
		String fileName00 = HdfsDataPath +"/" + suffix + "/"
				+ cal.getDateStr8().substring(2) + "/" + company + "_" + cal.getDateStr8().substring(2) + "0045_0."
				+ suffix.toUpperCase().replace("MT", "");
		boolean ret00 = hdfs.checkFileExist(fileName00);

		String fileName12 = HdfsDataPath +"/" + suffix + "/"
				+ cal.getDateStr8().substring(2) + "/" + company + "_" + cal.getDateStr8().substring(2) + "1245_0."
				+ suffix.toUpperCase().replace("MT", "");
		boolean ret12 = hdfs.checkFileExist(fileName12);

		String fileName23 = HdfsDataPath +"/" + suffix + "/"
				+ cal.getDateStr8().substring(2) + "/" + company + "_" + cal.getDateStr8().substring(2) + "2345_0."
				+ suffix.toUpperCase().replace("MT", "");
		boolean ret23 = hdfs.checkFileExist(fileName23);

		if (ret23)
		{// 3个文件都齐了
			return 1;
		} else if (!ret00 && !ret12 && !ret23)
		{// 3个文件都没有
			return 0;
		}

		return 2;// 部分文件
	}

	public void Get23GEventinfoFromHdfs(CalendarEx cal, boolean bAutoDownload)
	{
		// final CalendarEx cal = new CalendarEx(theDate);
		// log.info("Begin GetEventinfoFromHdfs :" + cal.getDateStr8());
		String eventDir = "/TB_23G_CQTSIGNAL_EVENT_01_" + cal.getDateStr8().substring(2);
		String hdfsEventDir = HdfsDataPath +"/mroxdrmerge/xdr_loc/data_01_" + cal.getDateStr8().substring(2) + eventDir;
		if (bAutoDownload)
		{
			if (!CanDownload(hdfsEventDir, localRoot + eventDir, localRoot + "_done" + eventDir))
				return;
		}
		hdfs.readHdfsDirToLocal(hdfsEventDir, localRoot + eventDir, "event");

		eventDir = "/TB_23G_DTSIGNAL_EVENT_01_" + cal.getDateStr8().substring(2);
		hdfsEventDir = HdfsDataPath +"/mroxdrmerge/xdr_loc/data_01_" + cal.getDateStr8().substring(2) + eventDir;
		hdfs.readHdfsDirToLocal(hdfsEventDir, localRoot + eventDir, "event");

		eventDir = "/TB_23G_DTEXSIGNAL_EVENT_01_" + cal.getDateStr8().substring(2);
		hdfsEventDir = HdfsDataPath +"/mroxdrmerge/xdr_loc/data_01_" + cal.getDateStr8().substring(2) + eventDir;
		hdfs.readHdfsDirToLocal(hdfsEventDir, localRoot + eventDir, "event");

		log.info("Success Get23GEventinfoFromHdfs :" + cal.getDateStr8());
	}

	public void Get23GLocationInfoFromHdfs(CalendarEx cal, boolean bAutoDownload)
	{
		// log.info("Begin Get23GEvtinfoFromHdfs :" + cal.getDateStr8());
		{// 
			String hdfsGRIDDir = "/HanXin/4guser_23glocation/" + cal.getDateStr8() + "/";
			String dtGridDir = "/TB_23GSIGNAL_EVENT_01_" + cal.getDateStr8().substring(2);
			if (bAutoDownload)
			{
				if (!CanDownload(hdfsGRIDDir, localRoot + dtGridDir, localRoot + "_done" + dtGridDir))
					return;
			}
			// location_xdr_201511300000
			ArrayList<DatafileInfo> dirs = hdfs.listSubDirs(hdfsGRIDDir);
			boolean ret = false;
			if (dirs.size() == 1)
				ret = hdfs.getMerge(hdfsGRIDDir + dirs.get(0).filename, localRoot + dtGridDir, "",true);
			else
				ret = hdfs.getMerge(hdfsGRIDDir, localRoot + dtGridDir, "",true);
			if (ret)
				log.info("Success Get23GEvtinfoFromHdfs :" + cal.getDateStr8());
		}
	}

	public void Get2GCellGridinfoFromHdfs(CalendarEx cal, boolean bAutoDownload)
	{
		String cqtGridDir = "/TB_2G_CQTSIGNAL_CELLGRID_01_" + cal.getDateStr8().substring(2);
		String dtGridDir = "/TB_2G_DTSIGNAL_CELLGRID_01_" + cal.getDateStr8().substring(2);
		String GridDir = "/TB_2G_SIGNAL_CELLGRID_01_" + cal.getDateStr8().substring(2);

		{// total
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ GridDir;
			if (LocalFile.checkFileExist(localRoot + GridDir + "^TB_MODEl_2G_SIGNAL_CELLGRID"))
				return;
			if (LocalFile.checkFileExist(localRoot+ "_done" + GridDir + "^TB_MODEl_2G_SIGNAL_CELLGRID"))
				return;

			if (bAutoDownload)
			{
				if (!CanDownload(hdfsGRIDDir, localRoot + GridDir, localRoot + "_done" + GridDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot + GridDir + "^TB_MODEl_2G_SIGNAL_CELLGRID", "grid",true);
		}

		{// CQT
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ cqtGridDir;
			hdfs.getMerge(hdfsGRIDDir, localRoot + cqtGridDir + "^TB_MODEl_2G_SIGNAL_CELLGRID", "grid",true);
		}

		{// DT
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ dtGridDir;
			hdfs.getMerge(hdfsGRIDDir, localRoot + dtGridDir + "^TB_MODEl_2G_SIGNAL_CELLGRID", "grid",true);
		}
	}

	public void Get2GEventinfoFromHdfs(CalendarEx cal, boolean bAutoDownload)
	{
		// final CalendarEx cal = new CalendarEx(theDate);
		// log.info("Begin GetEventinfoFromHdfs :" + cal.getDateStr8());
		String eventDir = "/TB_2G_CQTSIGNAL_EVENT_01_" + cal.getDateStr8().substring(2);
		String hdfsEventDir = HdfsDataPath +"/mroxdrmerge/xdr_loc_23g/data_01_" + cal.getDateStr8().substring(2)
				+ eventDir;
		if (bAutoDownload)
		{
			if (!CanDownload(hdfsEventDir, localRoot + eventDir, localRoot + "_done" + eventDir))
				return;
		}
		hdfs.readHdfsDirToLocal(hdfsEventDir, localRoot + eventDir, "event");

		eventDir = "/TB_2G_DTSIGNAL_EVENT_01_" + cal.getDateStr8().substring(2);
		hdfsEventDir = HdfsDataPath +"/mroxdrmerge/xdr_loc_23g/data_01_" + cal.getDateStr8().substring(2) + eventDir;
		hdfs.readHdfsDirToLocal(hdfsEventDir, localRoot + eventDir, "event");

		eventDir = "/TB_2G_DTEXSIGNAL_EVENT_01_" + cal.getDateStr8().substring(2);
		hdfsEventDir = HdfsDataPath +"/mroxdrmerge/xdr_loc_23g/data_01_" + cal.getDateStr8().substring(2) + eventDir;
		hdfs.readHdfsDirToLocal(hdfsEventDir, localRoot + eventDir, "event");

		log.info("Success Get2GEventinfoFromHdfs :" + cal.getDateStr8());
	}

	public void Get2GridinfoFromHdfs(CalendarEx cal, boolean bAutoDownload)
	{// hdfs://" + NameNodeIp +
		// ":9000/mt_wlyh/Data/mroxdrmerge/mergestat/data_01_160327/TB_2G_SIGNAL_CELLGRID_01_160327
		String cqtGridDir = "/TB_2G_CQTSIGNAL_GRID_01_" + cal.getDateStr8().substring(2);
		String dtGridDir = "/TB_2G_DTSIGNAL_GRID_01_" + cal.getDateStr8().substring(2);
		String GridDir = "/TB_2G_SIGNAL_GRID_01_" + cal.getDateStr8().substring(2);

		{// total
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ GridDir;
			if (LocalFile.checkFileExist(localRoot + GridDir + "^TB_MODEL_2G_SIGNAL_GRID"))
				return;
			if (LocalFile.checkFileExist(localRoot + "_done" + GridDir + "^TB_MODEL_2G_SIGNAL_GRID"))
				return;
			if (bAutoDownload)
			{
				if (!CanDownload(hdfsGRIDDir, localRoot + GridDir, localRoot + "_done" + GridDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot + GridDir + "^TB_MODEL_2G_SIGNAL_GRID", "grid",true);
		}

		{// CQT
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ cqtGridDir;
			hdfs.getMerge(hdfsGRIDDir, localRoot + cqtGridDir + "^TB_MODEL_2G_SIGNAL_GRID", "grid",true);
		}

		{// DT
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ dtGridDir;
			hdfs.getMerge(hdfsGRIDDir, localRoot + dtGridDir + "^TB_MODEL_2G_SIGNAL_GRID", "grid",true);
		}
	}

	public void Get3GGridinfoFromHdfs(CalendarEx cal, boolean bAutoDownload)
	{
		String cqtGridDir = "/TB_3G_CQTSIGNAL_GRID_01_" + cal.getDateStr8().substring(2);
		String dtGridDir = "/TB_3G_DTSIGNAL_GRID_01_" + cal.getDateStr8().substring(2);
		String GridDir = "/TB_3G_SIGNAL_GRID_01_" + cal.getDateStr8().substring(2);
	
		{// total
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ GridDir;
			if (LocalFile.checkFileExist(localRoot + GridDir + "^TB_MODEL_3G_SIGNAL_GRID"))
				return;
			if (LocalFile.checkFileExist(localRoot + "_done" + GridDir + "^TB_MODEL_3G_SIGNAL_GRID"))
				return;
			if (bAutoDownload)
			{
				if (!CanDownload(hdfsGRIDDir, localRoot + GridDir, localRoot + "_done" + GridDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot + GridDir + "^TB_MODEL_3G_SIGNAL_GRID", "grid",true);
		}
	
		{// CQT
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ cqtGridDir;
			hdfs.getMerge(hdfsGRIDDir, localRoot + cqtGridDir + "^TB_MODEL_3G_SIGNAL_GRID", "grid",true);
		}
	
		{// DT
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ dtGridDir;
			hdfs.getMerge(hdfsGRIDDir, localRoot + dtGridDir + "^TB_MODEL_3G_SIGNAL_GRID", "grid",true);
		}
	}

	public void Get3GEventinfoFromHdfs(CalendarEx cal, boolean bAutoDownload)
	{
		// final CalendarEx cal = new CalendarEx(theDate);
		// log.info("Begin GetEventinfoFromHdfs :" + cal.getDateStr8());
		String eventDir = "/TB_3G_CQTSIGNAL_EVENT_01_" + cal.getDateStr8().substring(2);
		String hdfsEventDir = HdfsDataPath +"/mroxdrmerge/xdr_loc_23g/data_01_" + cal.getDateStr8().substring(2)
				+ eventDir;
		if (bAutoDownload)
		{
			if (!CanDownload(hdfsEventDir, localRoot + eventDir, localRoot + "_done" + eventDir))
				return;
		}
		hdfs.readHdfsDirToLocal(hdfsEventDir, localRoot + eventDir, "event");

		eventDir = "/TB_3G_DTSIGNAL_EVENT_01_" + cal.getDateStr8().substring(2);
		hdfsEventDir = HdfsDataPath +"/mroxdrmerge/xdr_loc_23g/data_01_" + cal.getDateStr8().substring(2) + eventDir;
		hdfs.readHdfsDirToLocal(hdfsEventDir, localRoot + eventDir, "event");

		eventDir = "/TB_3G_DTEXSIGNAL_EVENT_01_" + cal.getDateStr8().substring(2);
		hdfsEventDir = HdfsDataPath +"/mroxdrmerge/xdr_loc_23g/data_01_" + cal.getDateStr8().substring(2) + eventDir;
		hdfs.readHdfsDirToLocal(hdfsEventDir, localRoot + eventDir, "event");

		log.info("Success Get2GEventinfoFromHdfs :" + cal.getDateStr8());
	}

	public void GetVillageGridFromHdfs(CalendarEx cal, boolean bAutoDownload)
	{
		{// grid
			String GridDir = "/TB_SIGNAL_VILLAGE_GRID_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mro_village/data_01_" + cal.getDateStr8().substring(2)
					+ GridDir;
			if (LocalFile.checkFileExist(localRoot + GridDir))
				return;
			if (LocalFile.checkFileExist(localRoot + GridDir))
				return;
			if (LocalFile.checkFileExist(localRoot + GridDir))
				return;
			if (bAutoDownload)
			{
				if (!CanDownload(hdfsGRIDDir, localRoot + GridDir, localRoot + "_done" + GridDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot + GridDir, "grid",true);
		}

		/*
		 * {//sample String SampleDir = "/TB_SIGNAL_VILLAGE_SAMPLE_01_" +
		 * cal.getDateStr8().substring(2); String hdfsGRIDDir = hdfsRoot+
		 * "/mt_wlyh/Data/mroxdrmerge/mro_village/data_01_" + cal.getDateStr8().substring(2)
		 * + SampleDir; if(LocalFile.checkFileExist(localRoot+SampleDir))
		 * return; if(LocalFile.checkFileExist(localRoot+SampleDir)) return;
		 * if(LocalFile.checkFileExist(localRoot+SampleDir)) return;
		 * if(bAutoDownload) {
		 * if(!CanDownload(hdfsGRIDDir,localRoot+SampleDir,localRoot+"_done"+
		 * SampleDir)) return; }
		 * hdfs.getMerge(hdfsGRIDDir,localRoot+SampleDir,"sample"); }
		 */
	}

	public void GetCellgridInfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath,String tenMeterGrid)
	{
		if(truePath.contains("FG")){
			{// cell
				String CellDir = "/TB_"+truePath+"SIGNAL_CELLGRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat2/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				if (bAutoDownload)
				{
					if (LocalFile.checkFileExist(localRoot + CellDir + "^TB_MODEL_SIGNAL_CELLGRID"))
						return;
					if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + "^TB_MODEL_SIGNAL_CELLGRID"))
						return;
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + "^TB_MODEL_SIGNAL_CELLGRID", "cell",true);
			}
		
			{// cell
				String CellDir = "/TB_"+truePath+"CQTSIGNAL_CELLGRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat2/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				if (bAutoDownload)
				{
					if (LocalFile.checkFileExist(localRoot + CellDir + "^TB_MODEL_SIGNAL_CELLGRID"))
						return;
					if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + "^TB_MODEL_SIGNAL_CELLGRID"))
						return;
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + "^TB_MODEL_SIGNAL_CELLGRID", "cell",true);
			}
			
			{// cell
				String CellDir = "/TB_"+truePath+"DTSIGNAL_CELLGRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat2/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				if (bAutoDownload)
				{
					if (LocalFile.checkFileExist(localRoot + CellDir + "^TB_MODEL_SIGNAL_CELLGRID"))
						return;
					if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + "^TB_MODEL_SIGNAL_CELLGRID"))
						return;
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + "^TB_MODEL_SIGNAL_CELLGRID", "cell",true);
			}
		}else{
			{// cell
				String CellDir = "/TB_"+truePath+"SIGNAL_CELLGRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				if (bAutoDownload)
				{
					if (LocalFile.checkFileExist(localRoot + CellDir + "^TB_MODEL_SIGNAL_CELLGRID"))
						return;
					if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + "^TB_MODEL_SIGNAL_CELLGRID"))
						return;
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + "^TB_MODEL_SIGNAL_CELLGRID", "cell",true);
			}
		
			{// cell
				String CellDir = "/TB_"+truePath+"CQTSIGNAL_CELLGRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				if (bAutoDownload)
				{
					if (LocalFile.checkFileExist(localRoot + CellDir + "^TB_MODEL_SIGNAL_CELLGRID"))
						return;
					if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + "^TB_MODEL_SIGNAL_CELLGRID"))
						return;
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + "^TB_MODEL_SIGNAL_CELLGRID", "cell",true);
			}
			
			{// cell
				String CellDir = "/TB_"+truePath+"DTSIGNAL_CELLGRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				if (bAutoDownload)
				{
					if (LocalFile.checkFileExist(localRoot + CellDir + "^TB_MODEL_SIGNAL_CELLGRID"))
						return;
					if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + "^TB_MODEL_SIGNAL_CELLGRID"))
						return;
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + "^TB_MODEL_SIGNAL_CELLGRID", "cell",true);
			}
		}
		log.info("Success GetCellGridinfoFromHdfs :" + cal.getDateStr8());
		System.out.println("Success GetCellGridinfoFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetCellStatFromHdfs(CalendarEx cal, boolean bAutoDownload)
	{
		{// cell
			String CellDir = "/TB_SIGNAL_HOUR_CELL_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/cellstat/data_01_" + cal.getDateStr8().substring(2);
			if (bAutoDownload)
			{
				if (LocalFile.checkFileExist(localRoot + CellDir + "^TB_MODEL_SIGNAL_HOUR_CELL"))
					return;
				if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + "^TB_MODEL_SIGNAL_HOUR_CELL"))
					return;
				if (!CanDownload(hdfsGRIDDir+"23", localRoot + CellDir, localRoot + "_done" + CellDir))
					return;
			}
			for(int i=0; i<=23; i++)
			{
				String strHour = i+"";
				if(i<=9)
					strHour = "0" + strHour;
				hdfs.getMerge(hdfsGRIDDir+strHour, localRoot + CellDir + "^TB_MODEL_SIGNAL_HOUR_CELL", "cell"+strHour,true);
			}
		}
		log.info("Success GetCellStatFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetGsmNcellStatFromHdfs(CalendarEx cal, boolean bAutoDownload)
	{
		{// cell
			String CellDir = "/TB_SIGNAL_HOUR_GSMNCELL_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/gsmncellstat/data_01_" + cal.getDateStr8().substring(2);
			if (bAutoDownload)
			{
				if (LocalFile.checkFileExist(localRoot + CellDir + "^TB_MODEL_SIGNAL_HOUR_GSMNCELL"))
					return;
				if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + "^TB_MODEL_SIGNAL_HOUR_GSMNCELL"))
					return;
				if (!CanDownload(hdfsGRIDDir+"23", localRoot + CellDir, localRoot + "_done" + CellDir))
					return;
			}
			for(int i=0; i<=23; i++)
			{
				String strHour = i+"";
				if(i<=9)
					strHour = "0" + strHour;
				hdfs.getMerge(hdfsGRIDDir+strHour, localRoot + CellDir + "^TB_MODEL_SIGNAL_HOUR_GSMNCELL", "cell"+strHour,true);
			}
		}
		log.info("Success GetGsmNcellStatFromHdfs :" + cal.getDateStr8());
	}

	public static void GetHourUserFromHdfs(CalendarEx cal, boolean bAutoDownload ,String hour)
	{
		String tbmodel = "^TB_MODEL_SIGNAL_GRID_USER_HOUR#";
		{// cell
			String CellDir = "/TB_SIGNAL_GRID_USER_HOUR_01_" + cal.getDateStr8().substring(2) + hour;
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mroloc_backfill/data_01_" + cal.getDateStr8().substring(2) + hour
					+ CellDir;
			if (bAutoDownload)
			{
				if (!CanDownload(hdfsGRIDDir, localRoot + CellDir + tbmodel + hour, localRoot + "_done" + CellDir ))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + tbmodel + hour, "user",true);
		}
		log.info("Success GetHourUserFromHdfs :" + cal.getDateStr8() + hour);
	}

	public static void GetFreqGridFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath,String tenMeterGrid ,String hour)
	{
		String tbmodel = "^TB_MODEL_FREQ_SIGNAL_GRID#";
		if(truePath.contains("FG")){
			{// dt
				String CellDir = "/TB_"+truePath+"FREQ_DTSIGNAL_GRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2) + hour;
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mroloc_backfill/data_01_" + cal.getDateStr8().substring(2) + hour
						+ CellDir;
				if (bAutoDownload)
				{
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir + tbmodel + hour , localRoot + "_done" + CellDir ))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + tbmodel + hour, "grid",true);
			}

			{// cqt
				String CellDir = "/TB_"+truePath+"FREQ_CQTSIGNAL_GRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2) + hour;
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mroloc_backfill/data_01_" + cal.getDateStr8().substring(2) + hour
						+ CellDir;
				if (bAutoDownload)
				{
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir + tbmodel + hour, localRoot + "_done" + CellDir ))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + tbmodel + hour, "grid",true);
			}
		}else{
			{// dt
				String CellDir = "/TB_"+truePath+"FREQ_DTSIGNAL_GRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2) + hour;
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mroloc_backfill/data_01_" + cal.getDateStr8().substring(2) + hour
						+ CellDir;
				if (bAutoDownload)
				{
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir + tbmodel + hour , localRoot + "_done" + CellDir ))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + tbmodel + hour, "grid",true);
			}

			{// cqt
				String CellDir = "/TB_"+truePath+"FREQ_CQTSIGNAL_GRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2) + hour;
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mroloc_backfill/data_01_" + cal.getDateStr8().substring(2) + hour
						+ CellDir;
				if (bAutoDownload)
				{
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir + tbmodel + hour, localRoot + "_done" + CellDir ))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + tbmodel + hour, "grid",true);
			}
		}
		log.info("Success GetFreqInfoFromHdfs :" + cal.getDateStr8() + hour);
		System.out.println("Success GetFreqInfoFromHdfs :" + cal.getDateStr8() + hour);
	}
	
	public void GetFreqGridByImeiFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath,String tenMeterGrid,String ltdx)
	{
		String tbModel = "^TB_MODEL_FREQ_SIGNAL_GRID_BYIMEI";
		if(truePath.contains("FG")){
			{// dt
				String CellDir = "/TB_FREQ_"+truePath+"DTSIGNAL_"+ltdx+"_GRID"+tenMeterGrid+"_BYIMEI_01_" + cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat2/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				
				if (bAutoDownload)
				{
					if (LocalFile.checkFileExist(localRoot + CellDir + tbModel))
						return;
					if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + tbModel))
						return;
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + tbModel, "grid",true);
			}
			
			{// dt
				String CellDir = "/TB_FREQ_"+truePath+"CQTSIGNAL_"+ltdx+"_GRID"+tenMeterGrid+"_BYIMEI_01_" + cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat2/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				
				if (bAutoDownload)
				{
					if (LocalFile.checkFileExist(localRoot + CellDir + tbModel))
						return;
					if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + tbModel))
						return;
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + tbModel, "grid",true);
			}
		}else{
			{// dt
				String CellDir = "/TB_FREQ_"+truePath+"DTSIGNAL_"+ltdx+"_GRID"+tenMeterGrid+"_BYIMEI_01_" + cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				
				if (bAutoDownload)
				{
					if (LocalFile.checkFileExist(localRoot + CellDir + tbModel))
						return;
					if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + tbModel))
						return;
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + tbModel, "grid",true);
			}
			
			{// dt
				String CellDir = "/TB_FREQ_"+truePath+"CQTSIGNAL_"+ltdx+"_GRID"+tenMeterGrid+"_BYIMEI_01_" + cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				
				if (bAutoDownload)
				{
					if (LocalFile.checkFileExist(localRoot + CellDir + tbModel))
						return;
					if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + tbModel))
						return;
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + tbModel, "grid",true);
			}
		}
		log.info("Success GetFreqGridByImeiFromHdfs :" + cal.getDateStr8());
		System.out.println("Success GetFreqGridByImeiFromHdfs :" + cal.getDateStr8());
	}

	public static void GetFreqCellInfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath ,String hour)
	{
		String tbmodel = "^TB_MODEL_FREQ_CELL#";
		if(truePath.contains("FG")){
			{//
				String CellDir = "/TB_"+truePath+"FREQ_SIGNAL_CELL_01_" + cal.getDateStr8().substring(2) + hour;
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mroloc_backfill/data_01_" + cal.getDateStr8().substring(2) + hour
						+ CellDir;
				if (bAutoDownload)
				{
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir + tbmodel + hour, localRoot + "_done" + CellDir ))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + tbmodel + hour, "cell",true);
			}
		}else{
			{//
				String CellDir = "/TB_"+truePath+"FREQ_SIGNAL_CELL_01_" + cal.getDateStr8().substring(2) + hour;
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mroloc_backfill/data_01_" + cal.getDateStr8().substring(2) + hour
						+ CellDir;
				if (bAutoDownload)
				{
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir + tbmodel + hour, localRoot + "_done" + CellDir ))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + tbmodel + hour, "cell",true);
			}
		}
		log.info("Success GetFreqCellInfoFromHdfs :" + cal.getDateStr8() + hour);
		System.out.println("Success GetFreqCellInfoFromHdfs :" + cal.getDateStr8() + hour);
	}
	
	public void GetFreqCellByImeiFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath)
	{
		if(truePath.contains("FG")){
			{//
				String CellDir = "/TB_FREQ_"+truePath+"SIGNAL_LT_CELL_BYIMEI_01_" + cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat2/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				if (bAutoDownload)
				{
					if (LocalFile.checkFileExist(localRoot + CellDir + "^TB_MODEL_FREQ_CELL_BYIMEI"))
						return;
					if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + "^TB_MODEL_FREQ_CELL_BYIMEI"))
						return;
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + "^TB_MODEL_FREQ_CELL_BYIMEI", "cell",true);
			}
			
			{//
				String CellDir = "/TB_FREQ_"+truePath+"SIGNAL_DX_CELL_BYIMEI_01_" + cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat2/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				if (bAutoDownload)
				{
					if (LocalFile.checkFileExist(localRoot + CellDir + "^TB_MODEL_FREQ_CELL_BYIMEI"))
						return;
					if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + "^TB_MODEL_FREQ_CELL_BYIMEI"))
						return;
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + "^TB_MODEL_FREQ_CELL_BYIMEI", "cell",true);
			}
		}else{
			{//
				String CellDir = "/TB_FREQ_"+truePath+"SIGNAL_LT_CELL_BYIMEI_01_" + cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				if (bAutoDownload)
				{
					if (LocalFile.checkFileExist(localRoot + CellDir + "^TB_MODEL_FREQ_CELL_BYIMEI"))
						return;
					if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + "^TB_MODEL_FREQ_CELL_BYIMEI"))
						return;
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + "^TB_MODEL_FREQ_CELL_BYIMEI", "cell",true);
			}
			
			{//
				String CellDir = "/TB_FREQ_"+truePath+"SIGNAL_DX_CELL_BYIMEI_01_" + cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				if (bAutoDownload)
				{
					if (LocalFile.checkFileExist(localRoot + CellDir + "^TB_MODEL_FREQ_CELL_BYIMEI"))
						return;
					if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + "^TB_MODEL_FREQ_CELL_BYIMEI"))
						return;
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + "^TB_MODEL_FREQ_CELL_BYIMEI", "cell",true);
			}
		}
		log.info("Success GetFreqCellByImeiFromHdfs :" + cal.getDateStr8());
		System.out.println("Success GetFreqCellByImeiFromHdfs :" + cal.getDateStr8());
	}

	public static void GetCellinfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath ,String hour)
	{
		if(truePath.contains("FG")){
			{// cell
				String CellDir = "/TB_"+truePath+"SIGNAL_CELL_01_" + cal.getDateStr8().substring(2) + hour;
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mroloc_backfill/data_01_" + cal.getDateStr8().substring(2) + hour
						+ CellDir;
				if (bAutoDownload)
				{
					if (!CanDownload(hdfsGRIDDir, localRoot  + CellDir + "^TB_MODEL_SIGNAL_CELL#" + hour, localRoot + "_done" + CellDir ))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + "^TB_MODEL_SIGNAL_CELL#" + hour, "cell",true);
			}
		}else{
			{// cell
				String CellDir = "/TB_"+truePath+"SIGNAL_CELL_01_" + cal.getDateStr8().substring(2) + hour;
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mroloc_backfill/data_01_" + cal.getDateStr8().substring(2) + hour
						+ CellDir;
				if (bAutoDownload)
				{
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir + "^TB_MODEL_SIGNAL_CELL#" + hour, localRoot + "_done" + CellDir ))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + "^TB_MODEL_SIGNAL_CELL#" + hour, "cell",true);
			}
		}
		log.info("Success GetCellStatinfoFromHdfs :" + cal.getDateStr8() + hour);
		System.out.println("Success GetCellStatinfoFromHdfs :" + cal.getDateStr8() + hour);
	}
	
	public void GetFreqGridByimeiInfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath,String typePath ,String tenMeterGrid ,String flagPath)
	{
		String tbModel = "";
		if(truePath.contains("FG")){
			{
				String CellDir = "/TB_FREQ_"+truePath + typePath +"SIGNAL_" + flagPath + "_GRID"+tenMeterGrid+"_BYIMEI_" + cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat2/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				if (bAutoDownload)
				{
					if (LocalFile.checkFileExist(localRoot + CellDir + tbModel ))
						return;
					if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + tbModel))
						return;
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + tbModel, "cellgridbyImei",true);
			}
		}else{	
			{
				String CellDir = "/TB_FREQ_"+truePath + typePath +"SIGNAL_" + flagPath + "_GRID"+tenMeterGrid+"_BYIMEI_" + cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				if (bAutoDownload)
				{
					if (LocalFile.checkFileExist(localRoot + CellDir + tbModel ))
						return;
					if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + tbModel))
						return;
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + tbModel, "cellgridbyImei",true);
			}
		}
		log.info("Success GetFreqLtGridByimeiInfoFromHdfs :" + cal.getDateStr8());
		System.out.println("Success GetFreqLtGridByimeiInfoFromHdfs :" + cal.getDateStr8());
	}
	
	public static void GetCellgridInfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath ,String tenMeterGrid ,String hour)
	{
		String tbModel = "^TB_MODEL_SIGNAL_CELLGRID#";
		if(truePath.contains("FG")){
			{// cellgrid
				String CellDir = "/TB_"+truePath+"SIGNAL_CELLGRID"+tenMeterGrid +"_"+ cal.getDateStr8().substring(2) + hour;
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mroloc_backfill/data_01_" + cal.getDateStr8().substring(2) + hour
						+ CellDir;
				if (bAutoDownload)
				{
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir + tbModel + hour , localRoot + "_done" + CellDir ))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + tbModel + hour, "cellgrid",true);
			}
		}else{
			{// cellgrid
				String CellDir = "/TB_"+truePath+"SIGNAL_CELLGRID"+tenMeterGrid +"_"+ cal.getDateStr8().substring(2) + hour;
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mroloc_backfill/data_01_" + cal.getDateStr8().substring(2) + hour
						+ CellDir;
				if (bAutoDownload)
				{
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir + tbModel + hour , localRoot + "_done" + CellDir ))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + tbModel + hour, "cellgrid",true);
			}
		}
		log.info("Success GetCellgridInfoFromHdfs :" + cal.getDateStr8() + hour);
		System.out.println("Success GetCellgridInfoFromHdfs :" + cal.getDateStr8() + hour);
	}
	
	public void GetFreqCellByimeiInfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath ,String tenMeterGrid ,String flagPath)
	{
		//cell
		String tbModel = "";
		if(truePath.contains("FG")){
			{
				String CellDir = "/TB_FREQ_"+truePath +"SIGNAL_" + flagPath + "_CELL"+tenMeterGrid+"_BYIMEI_" + cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat2/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				if (bAutoDownload)
				{
					if (LocalFile.checkFileExist(localRoot + CellDir + tbModel ))
						return;
					if (LocalFile.checkFileExist(localRoot + "_done" + CellDir +  tbModel ))
						return;
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + tbModel , "cellbyImei",true);
			}
		}else{
			{
				String CellDir = "/TB_FREQ_"+truePath +"SIGNAL_" + flagPath + "_CELL"+tenMeterGrid+"_BYIMEI_" + cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				if (bAutoDownload)
				{
					if (LocalFile.checkFileExist(localRoot + CellDir + tbModel ))
						return;
					if (LocalFile.checkFileExist(localRoot + "_done" + CellDir +  tbModel ))
						return;
					if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + tbModel , "cellbyImei",true);
			}
		}
		log.info("Success GetFreqLtGridByimeiInfoFromHdfs :" + cal.getDateStr8());
		System.out.println("Success GetFreqLtGridByimeiInfoFromHdfs :" + cal.getDateStr8());
	}
	

	
	public void GetMRBuildFromHdfs(CalendarEx cal,boolean bAutoDownload,String truePath,String modelTable,String fileName){
		{// cell
			String CellDir ="/TB_MR_BUILD_"+truePath+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mro_loc/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{   //  "^TB_MODEL_MR_BUILD"      "MRBuild"
				if (LocalFile.checkFileExist(localRoot2 + CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot2 + CellDir, localRoot + "_done" + CellDir ))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot2 + CellDir + modelTable, fileName,true);
		}
		log.info("Success GetMRBuildFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMRCellFromHdfs(CalendarEx cal,boolean bAutoDownload){
		{// cell
			String CellDir ="/TB_STAT_MR_CELL_"+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mro_loc/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{
				if (LocalFile.checkFileExist(localRoot2 + CellDir + "^TB_STAT_MR_CELL"))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot2  + CellDir, localRoot + "_done" + CellDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot2 + CellDir + "^TB_STAT_MR_CELL", "MRCell",true);
		}
		log.info("Success GetMRCellFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMRIngridFromHdfs(CalendarEx cal,boolean bAutoDownload,String truePath,String modelTable,String fileName){
		{// cell   
			String CellDir ="/TB_MR_INGRID_"+truePath+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mro_loc/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{      //  "TB_MODEL_MR_INGRID"  mringrid
				if (LocalFile.checkFileExist(localRoot2 + CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot2 + CellDir, localRoot + "_done" + CellDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot2 + CellDir + modelTable, fileName,true);
		}
		log.info("Success GetMRIngridFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMROutGridFromHdfs(CalendarEx cal,boolean bAutoDownload,String truePath,String modelTable,String fileName){
		{// cell
			String CellDir ="/TB_MR_OUTGRID_"+truePath+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mro_loc/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{      //  "^TB_MODEL_MR_OUTGRID"   "outgrid"
				if (LocalFile.checkFileExist(localRoot2 + CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot2 + CellDir, localRoot + "_done" + CellDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot2 + CellDir + modelTable, fileName,true);
		}
		log.info("Success GetMROutGridFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMRGridFromHdfs(CalendarEx cal,boolean bAutoDownload,String truePath,String typePath,String inpath, String modelTable,String fileName){
		{// mrgrid
			String CellDir ="/TB_MR_"+typePath+truePath+inpath+"_GRID_"+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mergestat3/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{      //   "outgrid"
				if (LocalFile.checkFileExist(localRoot2 + CellDir + modelTable))
					return;
				if (LocalFile.checkFileExist(localRoot + "_done"+ CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot2 + CellDir, localRoot + "_done" + CellDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot2 + CellDir + modelTable, fileName,true);
		}
		log.info("Success GetMRGridFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMRCELLGridFromHdfs(CalendarEx cal,boolean bAutoDownload,String truePath,String typePath,String inpath, String modelTable,String fileName){
		{// mrgrid
			String CellDir ="/TB_MR_"+typePath+truePath+inpath+"_CELLGRID_"+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mergestat3/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{      //   "outgrid"
				if (LocalFile.checkFileExist(localRoot2 + CellDir + modelTable))
					return;
				if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot2 + CellDir, localRoot + "_done" + CellDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot2 + CellDir + modelTable, fileName,true);
		}
		log.info("Success GetMRGridFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMRBuildFromHdfs(CalendarEx cal,boolean bAutoDownload,String truePath,String typePath, String modelTable,String fileName){
		{// mrbuild
			String CellDir ="/TB_MR_"+typePath+truePath+"_BUILD_"+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mergestat3/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{      //   "outgrid"
				if (LocalFile.checkFileExist(localRoot2 + CellDir + modelTable))
					return;
				if (LocalFile.checkFileExist(localRoot + "_done "+ CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot2 + CellDir, localRoot + "_done" + CellDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot2 + CellDir + modelTable, fileName,true);
		}
		log.info("Success GetMRBuildFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMRCELLFromHdfs(CalendarEx cal,boolean bAutoDownload,String typePath, String modelTable,String fileName){
		{// mrbuild
			String CellDir ="/TB_MR_"+typePath+"_CELL_"+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mergestat3/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{      // 
				if (LocalFile.checkFileExist(localRoot2 + CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot2 + CellDir, localRoot + "_done" + CellDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot2 + CellDir + modelTable, fileName,true);
		}
		log.info("Success GetMRCELLFromHdfs :" + cal.getDateStr8());
	}
	public void GetMRCELLFromHdfs(CalendarEx cal,boolean bAutoDownload, String modelTable,String fileName){
		{// mrbuild
			String CellDir ="/TB_MR_CELL_"+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mergestat3/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{      // 
				if (LocalFile.checkFileExist(localRoot2 + CellDir + modelTable))
					return;
				if (LocalFile.checkFileExist(localRoot2 +"_done" + CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot2 + CellDir, localRoot + "_done" + CellDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot2 + CellDir + modelTable, fileName,true);
		}
		log.info("Success GetMRCELLFromHdfs :" + cal.getDateStr8());
	}

	public void Get2GCellinfoFromHdfs(CalendarEx cal, boolean bAutoDownload)
	{
		{// cell
			String CellDir = "/TB_2G_SIGNAL_CELL_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ CellDir;
			if (bAutoDownload)
			{
				if (LocalFile.checkFileExist(localRoot + CellDir + "^TB_MODEL_2G_SIGNAL_CELL"))
					return;
				if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + "^TB_MODEL_2G_SIGNAL_CELL"))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + "^TB_MODEL_2G_SIGNAL_CELL", "cell",true);
		}
		log.info("Success Get2GCellStatinfoFromHdfs :" + cal.getDateStr8());
	}

	public void Get3GCellinfoFromHdfs(CalendarEx cal, boolean bAutoDownload)
	{
		{// cell
			String CellDir = "/TB_3G_SIGNAL_CELL_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ CellDir;
			if (bAutoDownload)
			{
				if (LocalFile.checkFileExist(localRoot + CellDir + "^TB_MODEL_3G_SIGNAL_CELL"))
					return;
				if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + "^TB_MODEL_3G_SIGNAL_CELL"))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + "^TB_MODEL_3G_SIGNAL_CELL", "cell",true);
		}
		log.info("Success Get3GCellStatinfoFromHdfs :" + cal.getDateStr8());
	}

	public void Get3GCellGridinfoFromHdfs(CalendarEx cal, boolean bAutoDownload)
	{
		String cqtGridDir = "/TB_3G_CQTSIGNAL_CELLGRID_01_" + cal.getDateStr8().substring(2);
		String dtGridDir = "/TB_3G_DTSIGNAL_CELLGRID_01_" + cal.getDateStr8().substring(2);
		String GridDir = "/TB_3G_SIGNAL_CELLGRID_01_" + cal.getDateStr8().substring(2);

		{// total
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ GridDir;
			if (LocalFile.checkFileExist(localRoot + GridDir + "^TB_MODEl_3G_SIGNAL_CELLGRID"))
				return;
			if (LocalFile.checkFileExist(localRoot + "_done" + GridDir + "^TB_MODEl_3G_SIGNAL_CELLGRID"))
				return;

			if (bAutoDownload)
			{
				if (!CanDownload(hdfsGRIDDir, localRoot + GridDir, localRoot + "_done" + GridDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot + GridDir + "^TB_MODEl_3G_SIGNAL_CELLGRID", "grid",true);
		}

		{// CQT
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ cqtGridDir;
			hdfs.getMerge(hdfsGRIDDir, localRoot + cqtGridDir + "^TB_MODEl_3G_SIGNAL_CELLGRID", "grid",true);
		}

		{// DT
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ dtGridDir;
			hdfs.getMerge(hdfsGRIDDir, localRoot + dtGridDir + "^TB_MODEl_3G_SIGNAL_CELLGRID", "grid",true);
		}
	}

	public static void GetEventinfoFromHdfs(CalendarEx cal, boolean bAutoDownload ,String hour)
	{
		// final CalendarEx cal = new CalendarEx(theDate);
		String eventDir = "/TB_CQTSIGNAL_EVENT_01_" + cal.getDateStr8().substring(2) + hour ;
		String hdfsEventDir = HdfsDataPath +"/mroxdrmerge/xdr_loc/data_01_" + cal.getDateStr8().substring(2) + hour + eventDir;
		if (bAutoDownload)
		{
			if (!CanDownload(hdfsEventDir, localRoot3 + eventDir + "^TB_MODEL_SIGNAL_EVENT#" + hour, localRoot + "_done" + eventDir ))
				return;
		}
		log.info("Begin GetEventinfoFromHdfs :" + cal.getDateStr8() + hour);
		System.out.println("Begin GetEventinfoFromHdfs :" + cal.getDateStr8() +hour);
		hdfs.getMerge(hdfsEventDir, localRoot3 + eventDir + "^TB_MODEL_SIGNAL_EVENT#" + hour, "event", true);

		eventDir = "/TB_DTEXSIGNAL_EVENT_01_" + cal.getDateStr8().substring(2) + hour;
		hdfsEventDir = HdfsDataPath +"/mroxdrmerge/xdr_loc/data_01_" + cal.getDateStr8().substring(2) + hour + eventDir;
		hdfs.getMerge(hdfsEventDir, localRoot3 + eventDir + "^TB_MODEL_SIGNAL_EVENT#" + hour, "event", true);

		eventDir = "/TB_DTSIGNAL_EVENT_01_" + cal.getDateStr8().substring(2) + hour;
		hdfsEventDir = HdfsDataPath +"/mroxdrmerge/xdr_loc/data_01_" + cal.getDateStr8().substring(2) + hour + eventDir;
		hdfs.getMerge(hdfsEventDir, localRoot3 + eventDir + "^TB_MODEL_SIGNAL_EVENT#" + hour, "event", true);

		// eventDir = "/TB_ERRSIGNAL_EVENT_01_" +
		// cal.getDateStr8().substring(2);
		// hdfsEventDir = hdfsRoot+ HdfsDataPath +"/mroxdrmerge/xdr_loc/data_01_" +
		// cal.getDateStr8().substring(2)
		// + eventDir;
		// hdfs.readHdfsDirToLocal(hdfsEventDir,localRoot+eventDir+"^TB_MODEL_ERRSIGNAL_XDR","event");

		log.info("Success GetEventinfoFromHdfs :" + cal.getDateStr8() + hour);
		System.out.println("Success GetEventinfoFromHdfs :" + cal.getDateStr8() + hour);
	}

	public static void GetUserinfoFromHdfs(CalendarEx cal, boolean bAutoDownload ,String hour)
	{
		String tbmodel = "^TB_MODEL_SIGNAL_USERINFO#"; 
		{
			String eventDir = "/TB_SIGNAL_USERINFO_01_" + cal.getDateStr8().substring(2) + hour;
			String hdfsEventDir = HdfsDataPath +"/mroxdrmerge/xdr_loc/data_01_" + cal.getDateStr8().substring(2) + hour
					+ eventDir;
			if (bAutoDownload)
			{
				if (!CanDownload(hdfsEventDir, localRoot + eventDir + tbmodel + hour, localRoot + "_done" + eventDir ))
					return;
			}
			hdfs.readHdfsDirToLocal(hdfsEventDir, localRoot + eventDir + tbmodel + hour, "user");
		}

		log.info("Success GetUserinfoFromHdfs :" + cal.getDateStr8() + hour);
	}
	
	public void GetSRVCCFromHdfs(CalendarEx cal, boolean bAutoDownload)
	{
		log.info("begin GetSRVCCFromHdfs:" + cal.getDateStr8());
		{//hdfs://10.151.64.160:8020/HanXin/seqltecs/20170219
			String hdfsEventDir = "/HanXin/seqltecs/"+ cal.getDateStr8() ;
			String localeventDir = "/TB_SRVCC邻区四维精细核查_数据_双待机次数_"+ cal.getDateStr8();			
			//String hdfsEventDir  = "/test" + eventDir;
			
			log.info("begin GetSRVCCFromHdfs:" + hdfsEventDir);
			if (bAutoDownload)
			{
				if (LocalFile.checkFileExist(localRoot2 + localeventDir))
					return;
				
				if (!CanDownload(hdfsEventDir, localRoot2 + localeventDir, localRoot + "_done" + localeventDir))
					return;
			}
			hdfs.readHdfsDirToLocal(hdfsEventDir, localRoot2 + localeventDir, "");
		}

		log.info("Success GetSRVCCFromHdfs:" + cal.getDateStr8());
	}

	public static void GetGridinfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath,String tenMeterGrid ,String hour)
	{
		//
		String cqtGridDir = "/TB_"+truePath+"CQTSIGNAL_GRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2) + hour;
		String dtGridDir  = "/TB_"+truePath+"DTSIGNAL_GRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2) + hour;
		String GridDir    = "/TB_"+truePath+"SIGNAL_GRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2) + hour;

		if(truePath.contains("FG")){
			{// total
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mroloc_backfill/data_01_" + cal.getDateStr8().substring(2) + hour
						+ GridDir;
				if (bAutoDownload)
				{
					if (!CanDownload(hdfsGRIDDir, localRoot + GridDir + "^TB_MODEL_SIGNAL_GRID#" + hour, localRoot + "_done" + GridDir ))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + GridDir + "^TB_MODEL_SIGNAL_GRID#" + hour, "grid",true);
			}
	
			{// CQT
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mroloc_backfill/data_01_" + cal.getDateStr8().substring(2) + hour
						+ cqtGridDir;
				hdfs.getMerge(hdfsGRIDDir, localRoot + cqtGridDir + "^TB_MODEL_SIGNAL_GRID#" + hour, "grid",true);
			}
	
			{// DT
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mroloc_backfill/data_01_" + cal.getDateStr8().substring(2) + hour
						+ dtGridDir;
				hdfs.getMerge(hdfsGRIDDir, localRoot + dtGridDir + "^TB_MODEL_SIGNAL_GRID#" + hour, "grid",true);
			}
		}else{
			{// total
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mroloc_backfill/data_01_" + cal.getDateStr8().substring(2) + hour
						+ GridDir;
				if (bAutoDownload)
				{
					if (!CanDownload(hdfsGRIDDir, localRoot + GridDir + "^TB_MODEL_SIGNAL_GRID#" + hour, localRoot + "_done" + GridDir + "^TB_MODEL_SIGNAL_GRID#" + hour))
						return;
				}
				hdfs.getMerge(hdfsGRIDDir, localRoot + GridDir + "^TB_MODEL_SIGNAL_GRID#" + hour, "grid",true);
			}
	
			{// CQT
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mroloc_backfill/data_01_" + cal.getDateStr8().substring(2) + hour
						+ cqtGridDir;
				hdfs.getMerge(hdfsGRIDDir, localRoot + cqtGridDir + "^TB_MODEL_SIGNAL_GRID#" + hour, "grid",true);
			}
	
			{// DT
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mroloc_backfill/data_01_" + cal.getDateStr8().substring(2) + hour
						+ dtGridDir;
				hdfs.getMerge(hdfsGRIDDir, localRoot + dtGridDir + "^TB_MODEL_SIGNAL_GRID#" + hour, "grid",true);
			}
		}
		log.info("Success GetGridinfoFromHdfs :" + cal.getDateStr8() + hour);
		System.out.println("Success GetGridinfoFromHdfs :" + cal.getDateStr8() + hour);
		GetCellinfoFromHdfs(cal, bAutoDownload,truePath , hour);
	}
	
	public void GetMDTGridinfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath,String tenMeterGrid)
	{
		String tbModel = "^TB_MODEL_SIGNAL_GRID";
		//
		String cqtGridDir = "/TB_"+truePath+"_CQTSIGNAL_GRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
		String dtGridDir  = "/TB_"+truePath+"_DTSIGNAL_GRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
		String GridDir    = "/TB_"+truePath+"_SIGNAL_GRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);

		
		{// total
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ GridDir;
			if (bAutoDownload)
			{	
				if (LocalFile.checkFileExist(localRoot + GridDir + tbModel))
					return;
				if (LocalFile.checkFileExist(localRoot + "_done" + GridDir + tbModel))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot + GridDir, localRoot + "_done" + GridDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot + GridDir + tbModel, "grid",true);
		}
	
		{// CQT
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ cqtGridDir;
			hdfs.getMerge(hdfsGRIDDir, localRoot + cqtGridDir + tbModel, "grid",true);
		}
	
		{// DT
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ dtGridDir;
			hdfs.getMerge(hdfsGRIDDir, localRoot + dtGridDir + tbModel, "grid",true);
		}
		
		log.info("Success GetMDTGridinfoFromHdfs :" + cal.getDateStr8());
		System.out.println("Success GetMDTGridinfoFromHdfs :" + cal.getDateStr8());
		
	}
	
	public void GetMDTCellgridInfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath,String tenMeterGrid)
	{
		String tbModel = "^TB_MODEl_SIGNAL_CELLGRID";
		{// totall
			String CellDir = "/TB_"+truePath+"_SIGNAL_CELLGRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ CellDir;
			if (bAutoDownload)
			{
				if (LocalFile.checkFileExist(localRoot + CellDir + tbModel ))
					return;
				if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + tbModel))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + tbModel , "cellgrid",true);
		}
	
		{// cqt
			String CellDir = "/TB_"+truePath+"_CQTSIGNAL_CELLGRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ CellDir;
			if (bAutoDownload)
			{
				if (LocalFile.checkFileExist(localRoot + CellDir + tbModel))
					return;
				if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + tbModel ))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir  + tbModel, "cellgrid",true);
		}
		
		{// dt
			String CellDir = "/TB_"+truePath+"_DTSIGNAL_CELLGRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ CellDir;
			if (bAutoDownload)
			{
				if (LocalFile.checkFileExist(localRoot + CellDir + tbModel))
					return;
				if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + tbModel))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot + CellDir, localRoot + "_done" + CellDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + tbModel , "cellgrid",true);
		}
		log.info("Success GetMDTCellGridinfoFromHdfs :" + cal.getDateStr8());
		System.out.println("Success GetMDTCellGridinfoFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMDTCellinfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath)
	{	
		String tbModel = "^TB_MODEL_SIGNAL_CELL";
		{// cell
			String CellDir = "/TB_"+truePath+"_SIGNAL_CELL_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ CellDir;
			if (bAutoDownload)
			{
				if (LocalFile.checkFileExist(localRoot + CellDir + tbModel))
					return;
				if (LocalFile.checkFileExist(localRoot  + "_done" + CellDir + tbModel ))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot + "_done" + CellDir , localRoot + "_done" + CellDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir +tbModel, "cell",true);
		}
	
	log.info("Success GetMDTCellStatinfoFromHdfs :" + cal.getDateStr8());
	System.out.println("Success GetMDTCellStatinfoFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMDTSampleinfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath)
	{
		// final CalendarEx cal = new CalendarEx(theDate);
		String tbModel = "^TB_MODEL_SIGNAL_SAMPLE";
		
		String SAMPLEDir = "/TB_"+truePath+"_CQTSIGNAL_SAMPLE_01_" + cal.getDateStr8().substring(2);
		String hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/mro_loc/data_01_" + cal.getDateStr8().substring(2) + SAMPLEDir;
		if (bAutoDownload)
		{
			if (LocalFile.checkFileExist(localRoot3 + SAMPLEDir + tbModel))
				return;
			if (LocalFile.checkFileExist(localRoot  + "_done" + SAMPLEDir + tbModel ))
				return;
			if (!CanDownload(hdfsSAMPLEDir, localRoot + SAMPLEDir, localRoot + "_done" + SAMPLEDir))
				return;
		}
		log.info("Begin GetSampleinfoFromHdfs :" + cal.getDateStr8());
		hdfs.getMerge(hdfsSAMPLEDir, localRoot3 + SAMPLEDir + tbModel, "sample", true);

		SAMPLEDir = "/TB_"+truePath+"_DTEXSIGNAL_SAMPLE_01_" + cal.getDateStr8().substring(2);
		hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/mro_loc/data_01_" + cal.getDateStr8().substring(2) + SAMPLEDir;
		hdfs.getMerge(hdfsSAMPLEDir, localRoot3 + SAMPLEDir + tbModel, "sample", true);

		SAMPLEDir = "/TB_"+truePath+"_DTSIGNAL_SAMPLE_01_" + cal.getDateStr8().substring(2);
		hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/mro_loc/data_01_" + cal.getDateStr8().substring(2) + SAMPLEDir;
		hdfs.getMerge(hdfsSAMPLEDir, localRoot3 + SAMPLEDir + tbModel, "sample", true);

		log.info("Success GetMDTSampleinfoFromHdfs :" + cal.getDateStr8());
		System.out.println("Success GetMDTSampleinfoFromHdfs :" + cal.getDateStr8());

	}
	
	public void GetMRSceneCELLFromHdfs(CalendarEx cal,boolean bAutoDownload,String typePath, String modelTable,String fileName){
		{// mrbuild
			String CellDir ="/TB_MR_"+typePath+"_CELL_"+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mergestat3/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{      // 
				if (LocalFile.checkFileExist(localRoot2 + CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot2 + CellDir, localRoot + "_done" + CellDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot2 + CellDir + modelTable, fileName,true);
		}
		log.info("Success GetMRSceneCELLFromHdfs :" + cal.getDateStr8());
		System.out.println("Success GetMRSceneCELLFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMRSceneGridFromHdfs(CalendarEx cal,boolean bAutoDownload,String truePath, String modelTable,String fileName){
		{// mrgrid
			String CellDir ="/TB_MR_" + truePath +"_GRID_"+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mergestat3/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{      //   "outgrid"
				if (LocalFile.checkFileExist(localRoot2 + CellDir + modelTable))
					return;
				if (LocalFile.checkFileExist(localRoot + "_done"+ CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot2 + CellDir, localRoot + "_done" + CellDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot2 + CellDir + modelTable, fileName,true);
		}
		log.info("Success GetMRSceneGridFromHdfs :" + cal.getDateStr8());
		System.out.println("Success GetMRSceneGridFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMRSceneCELLGridFromHdfs(CalendarEx cal,boolean bAutoDownload,String truePath, String modelTable,String fileName){
		{// mrgrid
			String CellDir ="/TB_MR_"+truePath+"_CELLGRID_"+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mergestat3/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{      //   "outgrid"
				if (LocalFile.checkFileExist(localRoot2 + CellDir + modelTable))
					return;
				if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot2 + CellDir, localRoot + "_done" + CellDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot2 + CellDir + modelTable, fileName,true);
		}
		log.info("Success GetMRSceneCELLGridFromHdfs :" + cal.getDateStr8());
		log.info("Success GetMRSceneCELLGridFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMRSceneFromHdfs(CalendarEx cal,boolean bAutoDownload,String truePath, String modelTable,String fileName){
		{// mrgrid
			String CellDir ="/TB_MR_"+truePath+"_"+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mergestat3/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{      //   "outgrid"
				if (LocalFile.checkFileExist(localRoot2 + CellDir + modelTable))
					return;
				if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot2 + CellDir, localRoot + "_done" + CellDir))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot2 + CellDir + modelTable, fileName,true);
		}
		log.info("Success GetMRSceneCELLGridFromHdfs :" + cal.getDateStr8());
		System.out.println("Success GetMRSceneCELLGridFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMRSampleinfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath,String typepath,String dttype){
		
		String SAMPLEDir = "/TB_MR_"+truePath+ "_" + typepath +"_"+ dttype + "SAMPLE_" + cal.getDateStr8().substring(2);
		String hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/mro_loc/fg_ott_data_01_" + cal.getDateStr8().substring(2) + SAMPLEDir;
		if (bAutoDownload)
		{
			if (!CanDownload(hdfsSAMPLEDir, localRoot3 + SAMPLEDir, localRoot + "_done" + SAMPLEDir))
				return;
		}
		log.info("Begin GetMRSampleinfoFromHdfs :" + cal.getDateStr8());
		System.out.println("Begin GetMRSampleinfoFromHdfs :" + cal.getDateStr8());
		hdfs.getMerge(hdfsSAMPLEDir, localRoot3 + SAMPLEDir, "sample", true);
	}

	private boolean getSiteFaultFiles()
	{
		try
		{
			ArrayList<FTPFile> list = new ArrayList<FTPFile>();
			FTPFile[] files = ftpFault.listFiles("/", true);

			for (int i = 0; i < files.length; i++)
			{
				if (files[i].isDirectory() || files[i].getSize() == 0)
					continue;
				TimeZone tz = files[i].getTimestamp().getTimeZone();
				CalendarEx lastModifyTime = new CalendarEx(files[i].getTimestamp().getTime());
				final CalendarEx cal = new CalendarEx(new Date());

				if (!files[i].getName().startsWith("fault_") || !files[i].getName().endsWith(".dat")
						|| files[i].getName().contains("["))
				{
					continue;
				}

				if ((lastModifyTime._second + 8 * 3600 + 900) > cal._second)
				{
					continue;
				}

				FTPFile file = files[i];
				String fileDate = file.getName().replaceFirst("fault_", "").substring(0, 8).replace("-", "");
				String path = "D:\\mastercom\\SampleEvent\\tb_sitefault_" + fileDate;
				makeDir(path);
				File thefile = new File(path + "/" + file.getName());
				FileOutputStream os = new FileOutputStream(thefile);

				if (os != null)
				{
					try
					{
						log.info("Begin Move SiteFault file: " + file.getName());
						if (ftpFault.get("/" + file.getName(), os))
						{
							os.close();
							// hdfs.movefile("hdfs://" + NameNodeIp +
							// ":9000/mt_wlyh/Data/mro/"+ fileDate + "/" +
							// file.getName()+".tmp", "hdfs://" + NameNodeIp +
							// ":9000/mt_wlyh/Data/mro/"+ fileDate + "/" +
							// file.getName());
							String fileName = file.getName() + "[已取]";
							File repfile = new File(fileName);
							FileOutputStream repos = new FileOutputStream(repfile);
							repos.close();
							ftpFault.delete("/" + file.getName());
							// ftpFault.put("/"+fileName,
							// repfile.getAbsolutePath());
							LocalFile.deleteFile(repfile.getAbsolutePath());
							log.info("Success Move SiteFault :" + file.getName());
						}
					} catch (Exception e)
					{
						log.info("getSiteFaultFiles error:" + e.getMessage());
						// e.printStackTrace();
					}
				}
				log.info(file.getName() + " 下载完成");
				// return file.getName() + " 下载完成";
			}
			return true;
		} catch (Exception e)
		{
			e.printStackTrace();
			log.info("getSiteFaultFiles error:" + e.getMessage());
		}

		return false;
	}

	private ArrayList<FTPFile> getMroFiles()
	{
		try
		{
			ArrayList<FTPFile> list = new ArrayList<FTPFile>();
			FTPFile[] files = ftp.listFiles("/mro_data", true);

			for (int i = 0; i < files.length; i++)
			{
				if (files[i].isDirectory() || files[i].getSize() == 0)
					continue;
				TimeZone tz = files[i].getTimestamp().getTimeZone();
				CalendarEx lastModifyTime = new CalendarEx(files[i].getTimestamp().getTime());
				final CalendarEx cal = new CalendarEx(new Date());

				if (!files[i].getName().startsWith("tdl_mro_basetable_ori_") || !files[i].getName().endsWith(".txt")
						|| files[i].getName().contains("["))
				{
					continue;
				}

				/*
				 * if((lastModifyTime._second + 8*3600+900) >cal._second) {
				 * continue; }
				 */
				list.add(files[i]);
			}
			return list;
		} catch (Exception e)
		{
			e.printStackTrace();
		}

		return null;
	}

	public static void GetSampleinfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath ,String hour)
	{
		// final CalendarEx cal = new CalendarEx(theDate);

		String SAMPLEDir = "/TB_"+truePath+"CQTSIGNAL_SAMPLE_01_" + cal.getDateStr8().substring(2) + hour;
		String hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/mroloc_backfill/data_01_" + cal.getDateStr8().substring(2) + hour + SAMPLEDir;
		if (bAutoDownload)
		{
			if (!CanDownload(hdfsSAMPLEDir, localRoot3 + SAMPLEDir + "^TB_MODEL_SIGNAL_SAMPLE#" + hour, localRoot + "_done" + SAMPLEDir ))
				return;
		}
		log.info("Begin GetSampleinfoFromHdfs :" + cal.getDateStr8() + hour);
		System.out.println("Begin GetSampleinfoFromHdfs :" + cal.getDateStr8() + hour);
		hdfs.getMerge(hdfsSAMPLEDir, localRoot3 + SAMPLEDir + "^TB_MODEL_SIGNAL_SAMPLE#" + hour, "sample", true);

		SAMPLEDir = "/TB_"+truePath+"DTEXSIGNAL_SAMPLE_01_" + cal.getDateStr8().substring(2) + hour;
		hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/mroloc_backfill/data_01_" + cal.getDateStr8().substring(2) + hour + SAMPLEDir;
		hdfs.getMerge(hdfsSAMPLEDir, localRoot3 + SAMPLEDir + "^TB_MODEL_SIGNAL_SAMPLE#" + hour, "sample", true);
		log.info("Success GetSampleinfoFromHdfs :" + cal.getDateStr8() + hour);
		System.out.println("Success GetSampleinfoFromHdfs :" + cal.getDateStr8() + hour);

		SAMPLEDir = "/TB_"+truePath+"DTSIGNAL_SAMPLE_01_" + cal.getDateStr8().substring(2) + hour;
		hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/mroloc_backfill/data_01_" + cal.getDateStr8().substring(2) + hour + SAMPLEDir;
		hdfs.getMerge(hdfsSAMPLEDir, localRoot3 + SAMPLEDir + "^TB_MODEL_SIGNAL_SAMPLE#" + hour, "sample", true);

		log.info("Success GetSampleinfoFromHdfs :" + cal.getDateStr8() + hour);
		System.out.println("Success GetSampleinfoFromHdfs :" + cal.getDateStr8() + hour);

	}
	
	public void GetVilSampleinfoFromHdfs(CalendarEx cal, boolean bAutoDownload)
	{
		// final CalendarEx cal = new CalendarEx(theDate);

		String SAMPLEDir = "/TB_SIGNAL_VILLAGE_SAMPLE_01_" + cal.getDateStr8().substring(2);
		String hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/mro_village/data_01_" + cal.getDateStr8().substring(2) + SAMPLEDir;
		if (bAutoDownload)
		{
			if (!CanDownload(hdfsSAMPLEDir, localRoot + SAMPLEDir, localRoot + "_done" + SAMPLEDir))
				return;
		}
		log.info("Begin GetViilSampleinfoFromHdfs :" + cal.getDateStr8());
		hdfs.getMerge(hdfsSAMPLEDir, localRoot3 + SAMPLEDir, "sample", true);

		log.info("Success GetViilSampleinfoFromHdfs :" + cal.getDateStr8());

	}
	
	public void GetMmeXdrinfoFromHdfs(CalendarEx cal, boolean bAutoDownload)
	{
		// final CalendarEx cal = new CalendarEx(theDate);

		String SAMPLEDir = "/TB_SIGNAL_MMEXDR_01_" + cal.getDateStr8().substring(2);
		String hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/xdr_loc/data_01_" + cal.getDateStr8().substring(2) + SAMPLEDir;
		if (bAutoDownload)
		{
			if (!CanDownload(hdfsSAMPLEDir, localRoot + SAMPLEDir, localRoot + "_done" + SAMPLEDir))
				return;
		}
		log.info("Begin GetMmeXdrinfoFromHdfs :" + cal.getDateStr8());
		hdfs.getMerge(hdfsSAMPLEDir, localRoot3 + SAMPLEDir, "mme", true);

		log.info("Success GetMmeXdrinfoFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetS1uXdrinfoFromHdfs(CalendarEx cal, boolean bAutoDownload)
	{
		// final CalendarEx cal = new CalendarEx(theDate);
		String SAMPLEDir = "/TB_SIGNAL_HTTPXDR_01_" + cal.getDateStr8().substring(2);
		String hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/xdr_loc/data_01_" + cal.getDateStr8().substring(2)
				+ SAMPLEDir;
		
		if (bAutoDownload)
		{
			if (!CanDownload(hdfsSAMPLEDir, localRoot + SAMPLEDir, localRoot + "_done" + SAMPLEDir))
				return;
		}
		log.info("Begin GetS1uXdrinfoFromHdfs :" + cal.getDateStr8());
		hdfs.getMerge(hdfsSAMPLEDir, localRoot3 + SAMPLEDir, "xdr", true);
		log.info("Success GetS1uXdrinfoFromHdfs :" + cal.getDateStr8());
	}

	public int BcpSampleinfoToGp(String EvtType, CalendarEx cal, boolean bAutoDownload)
	{
		String SAMPLEDir = "/TB_" + EvtType + "SIGNAL_SAMPLE_01_" + cal.getDateStr8().substring(2);
		String hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/mro_loc/data_01_" + cal.getDateStr8().substring(2) + SAMPLEDir;
		if (bAutoDownload)
		{
			if (!CheckHadoopFinished(hdfsSAMPLEDir))
				return -2;
		}

		if (GreepPlumHelper.IsTableExists(SAMPLEDir.replace("/", "")))
		{
			return 1;
		}
		
		log.info("Begin BcpSampleinfoToGp " + EvtType + ":" + cal.getDateStr8());
		return GreepPlumHelper.ImportSample(EvtType, cal);
	}

	public int BcpVilSampleinfoToGp(CalendarEx cal, boolean bAutoDownload)
	{
		String SAMPLEDir = "/TB_SIGNAL_VILLAGE_SAMPLE_01_" + cal.getDateStr8().substring(2);
		String hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/mro_village/data_01_" + cal.getDateStr8().substring(2)
				+ SAMPLEDir;
		if (bAutoDownload)
		{
			if (!CheckHadoopFinished(hdfsSAMPLEDir))
				return -2;
		}

		if (GreepPlumHelper.IsTableExists(SAMPLEDir.replace("/", "")))
		{
			return 1;
		}

		log.info("Begin BcpVilSampleinfoToGp :" + cal.getDateStr8());
		return GreepPlumHelper.ImportVilSample(cal);
	}
	
	public int BcpS1MmeXdrInfoToGp(CalendarEx cal, boolean bAutoDownload)
	{//hdfs://10.139.6.169:9000/mt_wlyh/Data/mroxdrmerge/xdr_loc/data_01_160905/TB_SIGNAL_MMEXDR_01_160905
		String SAMPLEDir = "/TB_SIGNAL_MMEXDR_01_" + cal.getDateStr8().substring(2);
		String hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/xdr_loc/data_01_" + cal.getDateStr8().substring(2)
				+ SAMPLEDir;
		
		if (bAutoDownload)
		{
			if (!CheckHadoopFinished(hdfsSAMPLEDir))
				return -2;
		}

		if (GreepPlumHelper.IsTableExists(SAMPLEDir.replace("/", "")))
		{
			return 1;
		}

		log.info("Begin BcpS1MmeXdrInfoToGp :" + cal.getDateStr8());
		return GreepPlumHelper.ImportS1MMEXDR(cal);
	}
	
	public int BcpS1uXdrInfoToGp(CalendarEx cal, boolean bAutoDownload)
	{
		String SAMPLEDir = "/TB_SIGNAL_HTTPXDR_01_" + cal.getDateStr8().substring(2);
		String hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/xdr_loc/data_01_" + cal.getDateStr8().substring(2)
				+ SAMPLEDir;
		if (bAutoDownload)
		{
			if (!CheckHadoopFinished(hdfsSAMPLEDir))
				return -2;
		}

		if (GreepPlumHelper.IsTableExists(SAMPLEDir.replace("/", "")))
		{
			return 1;
		}

		log.info("Begin BcpVilSampleinfoToGp :" + cal.getDateStr8());
		return GreepPlumHelper.ImportS1UXDR(cal);
	}

	public int BcpEventinfoToGp(String EvtType, CalendarEx cal, boolean bAutoDownload)
	{
		String SAMPLEDir = "/TB_" + EvtType + "SIGNAL_EVENT_01_" + cal.getDateStr8().substring(2);
		String hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/xdr_loc/data_01_" + cal.getDateStr8().substring(2) + SAMPLEDir;
		if (bAutoDownload)
		{
			if (!CheckHadoopFinished(hdfsSAMPLEDir))
				return -2;
		}

		if (GreepPlumHelper.IsTableExists(SAMPLEDir.replace("/", "")))
		{
			return 1;
		}

		log.info("Begin BcpEventinfoToGp " + EvtType + ":" + cal.getDateStr8());
		return GreepPlumHelper.ImportEvent(EvtType, cal);
	}

	public int Bcp23GEventinfoToGp(String EvtType, CalendarEx cal, boolean bAutoDownload)
	{
		String SAMPLEDir = "/TB_" + EvtType + "SIGNAL_EVENT_01_" + cal.getDateStr8().substring(2);
		String hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/xdr_loc_23g/data_01_" + cal.getDateStr8().substring(2)
				+ SAMPLEDir;
		if (bAutoDownload)
		{
			if (!CheckHadoopFinished(hdfsSAMPLEDir))
				return -2;
		}

		if (GreepPlumHelper.IsTableExists(SAMPLEDir.replace("/", "")))
		{
			return 1;
		}

		log.info("Begin Bcp23GEventinfoToGp " + EvtType + ":" + cal.getDateStr8());
		return GreepPlumHelper.Import23GEvent(EvtType.toLowerCase(), cal);
	}

	public boolean makeDir(String dirName)
	{
		File file = new File(dirName);
		// 如果文件夹不存在则创建
		if (!file.exists() && !file.isDirectory())
		{
			// System.out.println("//不存在");
			file.mkdir();
		}
		return true;
	}

	void MoveDaoliuInfo(String srcPath, String DestPath)
	{
		try
		{
			File root = new File(srcPath);
			File[] dirs = root.listFiles();
			if (dirs.length == 0)
				return;

			for (File dir : dirs)
			{
				if (!dir.isDirectory())
					continue;
				long lastDirModifyTime = dir.lastModified() / 1000L;
				final CalendarEx cal = new CalendarEx(new Date());

				if (lastDirModifyTime + 360 > cal._second)
				{
					continue;
				}

				File[] files = dir.listFiles();
				if (files.length == 0)
				{
					continue;
				}

				String dirName = dir.getName();
				if (!dirName.startsWith("daoliuresult_20") || dirName.length() != "daoliuresult_20160105".length())
				{
					continue;
				}
				String subDirName = DestPath + "\\TB_23G_FLOW_01_" + dirName.substring(15, 21) + "^TB_MODEL_23G_FLOW";
				HadoopFSOperations.makeDir(subDirName);

				for (File file : files)
				{
					String type = "";
					if (file.length() == 0 || file.getName().toLowerCase().contains(".crc"))
					{
						continue;
					}

					changeDirectory(file.getName(), file.getParent(), subDirName, true);
				}

				LocalFile.deleteDirectory(dir.getAbsolutePath());
				log.info("Success CopyUserInfo:" + dir.getName());
			}
		} catch (Exception e)
		{
			// log.info("MoveMreFilesTohdfs Error:" +company + " | "+ filePath);
			// e.printStackTrace();
		}
	}

	/**
	 * 将数据移到hdfs上
	 * 
	 * @param filePath
	 */
	@SuppressWarnings("unchecked")
	public static void MoveMroFilesTohdfs(String filePath)
	{
		if(hdfsRoot.trim().length()>4 && !hdfs.ReCoconnect())
			return;
		
		if(MrBcpBkPath.length()==0 && hdfsRoot.trim().length()<4)
		{
			System.out.println("解码后文件无需备份和上传HDFS，停止解码文件搬移。");
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
			}
			return;
		}
		
		filePath = filePath.replace("\\", "/");
		List<String> files;
		
		HashMap<String, Integer> mapValue = new HashMap<String, Integer>();

		try
		{
			files = LocalFile.getAllDirs(new File(filePath), "_", 10, 4);
		} 
		catch (Exception e)
		{
			e.printStackTrace();
			System.out.println(e.getMessage());
			return;
		}

		if (files.size() == 0)
			return;

		for (String fileName : files)
		{
			File file = new File(fileName);
			if(fileName.toLowerCase().contains("gsm"))
				continue;
			
			if (!mapValue.containsKey(file.getAbsolutePath().toString()))
			{
				mapValue.put(file.getAbsolutePath().toString(), 1); // 得到所有文件的上层文件夹
			}
		}
		
		List<String> sortedList = new ArrayList<String>();
		for (String fileName : mapValue.keySet())
		{
			sortedList.add(fileName);
		}
		Collections.sort(sortedList, new ComparatorMapForHour());

		// 创建一个线程池
		ExecutorService pool = Executors.newFixedThreadPool(10);
		// 创建多个有返回值的任务
		List<Future> list = new ArrayList<Future>();

		for (String fileName : sortedList)
		{
			File file = new File(fileName);
			if (MainSvr.bExitFlag == true)
			{
				break;
			}
			if (file.isDirectory())
			{
				long lastModifyTime = file.lastModified() / 1000L;
				final CalendarEx cal = new CalendarEx(new Date());
				try {
					if (lastModifyTime + 300 > cal._second)
					{
						continue;
					}
					String dstFileName = fileName;
					if(!fileName.toLowerCase().contains("upload"))
					{
						dstFileName = file.getParent() + "/upload/" + file.getName();
						LocalFile.makeDir(file.getParent() + "/upload/");
						while(LocalFile.checkFileExist(dstFileName))
						{
							dstFileName += "x";
						}
						file.renameTo(new File(dstFileName));
					}
					else if (lastModifyTime + 1800 > cal._second)
					{//已经搬移到了upload,应该是上次上次失败了。
						continue;
					}
					
					Callable fm = new FileMoverCallable(dstFileName);
					// 执行任务并获取Future对象

					Future f = pool.submit(fm);
					list.add(f);
				} catch (Exception e) {
					
					e.printStackTrace();
				}
			}
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
	
	/**
	 * 山西要求将数据上传到ftp,本程序将文件放置到本地目录，等待上传
	 * 
	 * @param filePath
	 */
	@SuppressWarnings("unchecked")
	public static void MoveMroFilesToFtp(String filePath)
	{
		//if(hdfsRoot.trim().length()>4 && !hdfs.ReCoconnect())
		//	return;
		System.out.println("MoveMroFilesToFtp. Begin Scan Files:" + filePath);
		
		filePath = filePath.replace("\\", "/");
		List<String> files;
		HashMap<String, Integer> mapValue = new HashMap<String, Integer>();

		try
		{
			files = LocalFile.getAllDirs(new File(filePath), "_", 10, 4);
		} 
		catch (Exception e)
		{
			e.printStackTrace();
			System.out.println(e.getMessage());
			return;
		}

		if (files.size() == 0)
			return;
		System.out.println("MoveMroFilesToFtp. Find Directorys num:" + files.size());

		for (String fileName : files)
		{
			File file = new File(fileName);
			if(fileName.toLowerCase().contains("gsm"))
				continue;
			
			if (!mapValue.containsKey(file.getAbsolutePath().toString()))
			{
				mapValue.put(file.getAbsolutePath().toString(), 1); // 得到所有文件的上层文件夹
			}
		}
		
		List<String> sortedList = new ArrayList<String>();
		for (String fileName : mapValue.keySet())
		{
			sortedList.add(fileName);
		}
		Collections.sort(sortedList, new ComparatorMapForHour());

		// 创建一个线程池
		ExecutorService pool = Executors.newFixedThreadPool(4);
		// 创建多个有返回值的任务
		List<Future> list = new ArrayList<Future>();

		for (String fileName : sortedList)
		{
			File file = new File(fileName);
			if (MainSvr.bExitFlag == true)
			{
				break;
			}
			if (file.isDirectory())
			{
				long lastModifyTime = file.lastModified() / 1000L;
				final CalendarEx cal = new CalendarEx(new Date());
				try {
					if (lastModifyTime + 300 > cal._second)
					{
						continue;
					}
					String dstFileName = fileName;
					if(!fileName.toLowerCase().contains("upload"))
					{
						dstFileName = file.getParent() + "/upload/" + file.getName();
						LocalFile.makeDir(file.getParent() + "/upload/");
						while(LocalFile.checkFileExist(dstFileName))
						{
							dstFileName += "x";
						}
						file.renameTo(new File(dstFileName));
					}
					else if (lastModifyTime + 1800 > cal._second)
					{//已经搬移到了upload,应该是上次上次失败了。
						continue;
					}
					
					Callable fm = new FileMergeCallable(dstFileName, MoverId, "A:/mastercom/ftp");
					// 执行任务并获取Future对象

					Future f = pool.submit(fm);
					list.add(f);
				} catch (Exception e) {
					
					e.printStackTrace();
				}
			}
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

	public static void MoveMroFilesTohdfsFromFtp(String filePath)
	{
		if(hdfsRoot.trim().length()>4 && !hdfs.ReCoconnect())
			return;
		
		if(MrBcpBkPath.length()==0 && hdfsRoot.trim().length()<4)
		{
			System.out.println("解码后文件无需备份和上传HDFS，停止解码文件搬移。");
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
			}
			return;
		}	        
		filePath = filePath.replace("\\", "/");
		List<String> files;
		
		HashMap<String, Integer> mapValue = new HashMap<String, Integer>();
		FTPClientHelper ftp = GetFtpHelper();
		
		try
		{		
			files = ftp.listFileNamesAll(filePath, "_", 5);

		} 
		catch (Exception e)
		{
			e.printStackTrace();
			System.out.println(e.getMessage());
			return;
		}

		if (files.size() == 0)
			return;

		for (String fileName : files)
		{
			File file = new File(fileName);
			if(fileName.toLowerCase().contains("gsm"))
				continue;
			
			if (!mapValue.containsKey(file.getParent()))
			{
				mapValue.put(file.getParent(), 1); // 得到所有文件的上层文件夹
			}
		}
		
		List<String> sortedList = new ArrayList<String>();
		for (String fileName : mapValue.keySet())
		{
			sortedList.add(fileName);
		}
		Collections.sort(sortedList, new ComparatorMapForHour());

		// 创建一个线程池
		ExecutorService pool = Executors.newFixedThreadPool(1);
		// 创建多个有返回值的任务
		List<Future> list = new ArrayList<Future>();

		for (String fileName : sortedList)
		{		
			try {
				long lastModifyTime = ftp.getModificationTime(fileName)/ 1000L;
				final CalendarEx cal = new CalendarEx(new Date());
				if (lastModifyTime + 600 > cal._second)
				{
					continue;
				}
				Callable fm = new FileMoverFromFtpCallable(fileName);
				// 执行任务并获取Future对象
				Future f = pool.submit(fm);
				list.add(f);
			} catch (Exception e) {
				
				e.printStackTrace();
			}
		}
		// 关闭线程池
		pool.shutdown();
		
		try {
			ftp.disconnect();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
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
	
	public static FTPClientHelper GetFtpHelper() {
		String sxFtpIp = ReturnConfig.returnconfig("conf/config.xml", "//comm//sxftp//serverip");
		int sxFtpPort = Integer.parseInt(ReturnConfig.returnconfig("conf/config.xml", "//comm//sxftp//serverport"));
		String sxFtpUser = ReturnConfig.returnconfig("conf/config.xml", "//comm//sxftp//username");
		String sxFtpPassword = ReturnConfig.returnconfig("conf/config.xml", "//comm//sxftp//password");
		String sxFtpPath = ReturnConfig.returnconfig("conf/config.xml", "//comm//sxftp//remotepath");
		FTPClientHelper ftp = new FTPClientHelper(sxFtpIp,sxFtpPort,sxFtpUser,sxFtpPassword);      
        ftp.setBinaryTransfer(true);
        ftp.setPassiveMode(true);
        ftp.setEncoding("utf-8");
		return ftp;
	}
	

	// D:\MRE采集程序\decode_mro\WorkPlace\ERIC\Complete
	void MoveMrFilesTohdfsFromFtp1(String filePath, String company, String suffix,FTPClientHelper ftp)
	{
		try
		{
			// log.info("MoveMreFilesTohdfs:" +company + " | "+ filePath);
			File root = new File(filePath);
			if (!root.exists())
			{
				return;
			}
			File[] dirs = root.listFiles();

			for (File dir : dirs)
			{
				if (!dir.isDirectory())
					continue;
				long lastDirModifyTime = dir.lastModified() / 1000L;
				final CalendarEx cal = new CalendarEx(new Date());

				if (!dir.getName().startsWith("bcp_"))
				{
					continue;
				}

				File statusFile = new File(dir.getPath() + "/DECODE_STATUS.bcp");
				if (!statusFile.exists())
				{
					continue;
				}

				File[] files = dir.listFiles();
				if (files.length == 0)
				{
					if (lastDirModifyTime + 360 > cal._second)
					{
						continue;
					} else
					{
						LocalFile.deleteFile(dir.getPath());
					}
				}

				String dirName = dir.getName().substring(6, 12);
				String temp = company;
				if (MoverId > 0)
				{
					temp += MoverId;
				}
				String fileName = temp + "_" + dir.getName().substring(6, 14) + "." + suffix;

				if (hdfs.putMerge(dir.getPath(),
						HdfsDataPath +"/" + suffix + "/" + dirName, fileName,
						suffix.toLowerCase().replace("mt", "")))
				{
					log.info("Success MoveMreFilesTohdfs:" + company + " | " + filePath);
					LocalFile.deleteFile(dir.getPath());
				}
			}
			// log.info("Success MoveMreFilesTohdfs:" +company + " | "+
			// filePath);
		} catch (Exception e)
		{
			log.info("MoveMreFilesTohdfs Error:" + company + " | " + filePath);
			e.printStackTrace();
		}
	}
	void MoveUserInfo(String srcPath, String DestPath)
	{
		try
		{
			File root = new File(srcPath);
			File[] dirs = root.listFiles();
			if (dirs.length == 0)
				return;
	
			for (File dir : dirs)
			{
				if (!dir.isDirectory())
					continue;
				long lastDirModifyTime = dir.lastModified() / 1000L;
				final CalendarEx cal = new CalendarEx(new Date());
	
				if (lastDirModifyTime + 360 > cal._second)
				{
					continue;
				}
	
				File[] files = dir.listFiles();
				if (files.length == 0)
				{
					continue;
				}
	
				String dirName = dir.getName();
				String subDirName = DestPath + "\\TB_USER_PLANE_01_" + dirName.substring(2, 8) + "^TB_MODEL_USER_PLANE";
				HadoopFSOperations.makeDir(subDirName);
	
				File outfile = new File(subDirName + "/userplane.txt");
				if (outfile.exists())
					continue;
	
				FileWriter os = new FileWriter(outfile);
	
				for (File file : files)
				{
					String type = "";
					if (file.getName().contains("上机用户 "))
					{
						type = "shangji";
					} else if (file.getName().contains("下机用户 "))
					{
						type = "xiaji";
					} else
					{
						continue;
					}
	
					try
					{
						BufferedReader br = new BufferedReader(
								new InputStreamReader(new FileInputStream(file.getAbsolutePath())));
						String data = null;
						while ((data = br.readLine()) != null)
						{
							if (!data.contains("imsi"))
							{
								os.write(data.trim() + "\t" + type + "\r\n");
							}
						}
						br.close();
					} catch (Exception e)
					{
						e.printStackTrace();
					}
				}
				os.close();
				LocalFile.deleteFile(dir.getAbsolutePath());
				log.info("Success CopyUserInfo:" + dir.getName());
			}
		} catch (Exception e)
		{
			// log.info("MoveMreFilesTohdfs Error:" +company + " | "+ filePath);
			e.printStackTrace();
		}
	}

	// D:\MRE采集程序\decode_mro\WorkPlace\ERIC\Complete
	void MoveMtMrFilesTohdfs(String filePath, String company, String suffix)
	{
		try
		{
			// log.info("MoveMreFilesTohdfs:" +company + " | "+ filePath);
			File root = new File(filePath);
			if (!root.exists())
			{
				return;
			}
			File[] dirs = root.listFiles();

			for (File dir : dirs)
			{
				if (!dir.isDirectory())
					continue;
				long lastDirModifyTime = dir.lastModified() / 1000L;
				final CalendarEx cal = new CalendarEx(new Date());

				if (!dir.getName().startsWith("bcp_"))
				{
					continue;
				}

				File statusFile = new File(dir.getPath() + "/DECODE_STATUS.bcp");
				if (!statusFile.exists())
				{
					continue;
				}

				File[] files = dir.listFiles();
				if (files.length == 0)
				{
					if (lastDirModifyTime + 360 > cal._second)
					{
						continue;
					} else
					{
						LocalFile.deleteFile(dir.getPath());
					}
				}

				String dirName = dir.getName().substring(6, 12);
				String temp = company;
				if (MoverId > 0)
				{
					temp += MoverId;
				}
				String fileName = temp + "_" + dir.getName().substring(6, 14) + "." + suffix;

				if (hdfs.putMerge(dir.getPath(),
						HdfsDataPath +"/" + suffix + "/" + dirName, fileName,
						suffix.toLowerCase().replace("mt", "")))
				{
					log.info("Success MoveMreFilesTohdfs:" + company + " | " + filePath);
					LocalFile.deleteFile(dir.getPath());
				}
			}
			// log.info("Success MoveMreFilesTohdfs:" +company + " | "+
			// filePath);
		} catch (Exception e)
		{
			log.info("MoveMreFilesTohdfs Error:" + company + " | " + filePath);
			e.printStackTrace();
		}
	}

	@Override
	public void run()
	{
		File root = new File(rootPath);
		if (!root.exists())
		{
			rootPath = "D:/mastercom";
			root = new File(rootPath);
			if (!root.exists())
			{
				rootPath = "C:/mastercom";
			}
		}
		
		localRoot = rootPath + "/SampleEvent";
		localRoot2 = rootPath + "/SampleEvent2";
		localRoot3 = rootPath + "/SampleEvent3";
		LocalFile.makeDir(localRoot);
		
		log.info("hdfsRoot:" + hdfsRoot);
		log.info("rootPath:" + rootPath);
		log.info("localRoot:" + localRoot);
		log.info("localRoot2:" + localRoot2);
		log.info("localRoot3:" + localRoot3);		
		
		while (!MainSvr.bExitFlag)
		{
			try
			{
				Thread.sleep(1000);
				hdfs.ReCoconnect();

				if (FileType == 2)
				{
					CalendarEx curTime = new CalendarEx(new Date());
					CalendarEx beginCal = curTime.AddDays(-CheckBcpDays);
					curTime = curTime.AddDays(-1);
					
					String name = ManagementFactory.getRuntimeMXBean().getName();       
					String pid = name.split("@")[0];    
					System.out.println("PID is:" + pid);  
					CheckAllCompanyMrFiles("mre");
					CheckAllCompanyMrFiles("mromt");		
											
					while (beginCal._second < curTime._second)
					{
						log.info("Deal Bcp:" + curTime.getDateStr8());
						System.out.println("Deal Bcp:" + curTime.getDateStr8());
						String hour = "00";
						for(int i = 0 ;i < 24 ; i ++){
							if(i < 10 ){
								hour = "0" + String.valueOf(i);
								doloadDate(curTime,hour);
							}else{
								hour = String.valueOf(i);
								doloadDate(curTime,hour);
							}
						
						}
						curTime = curTime.AddDays(-1);
					}
					Thread.sleep(600000);	
				} 
			} 
			catch (InterruptedException e)
			{
				log.info(e.getMessage());
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	public static void doloadDate( CalendarEx curTime,String hour){
		
		String eventDir = "/TB_CQTSIGNAL_EVENT_01_" + curTime.getDateStr8().substring(2) + hour;
		String sampleDir = "/TB_CQTSIGNAL_SAMPLE_01_" + curTime.getDateStr8().substring(2) + hour;
		String fgSampleDir = "/TB_FGCQTSIGNAL_SAMPLE_01_" + curTime.getDateStr8().substring(2) + hour;
		String hdfsEventDir  = HdfsDataPath +"/mroxdrmerge/xdr_loc/data_01_" + curTime.getDateStr8().substring(2) + hour + eventDir;
		String hdfsSampleDir   = HdfsDataPath +"/mroxdrmerge/mro_loc/data_01_" + curTime.getDateStr8().substring(2) + hour + sampleDir;
		String hdfsFgSampleDir = HdfsDataPath +"/mroxdrmerge/mro_loc/data_01_" + curTime.getDateStr8().substring(2) + hour + fgSampleDir;
		
		
		if(hdfs.checkFileExist(hdfsEventDir)
				|| hdfs.checkFileExist(hdfsSampleDir)
				|| hdfs.checkFileExist(hdfsFgSampleDir))
		{		
			if (ReturnConfig.returnconfig("conf/config.xml", "//comm//DEAL_BCP_SAMPLE").equals("1")
					||ReturnConfig.returnconfig("conf/config.xml", "//comm//DEAL_BCP_SAMPLE").equals("") ) 
			{
				//sample
				GetEventinfoFromHdfs(curTime, true ,hour);							
				GetSampleinfoFromHdfs(curTime, true, "" ,hour);
			}

			if (ReturnConfig.returnconfig("conf/config.xml", "//comm//DEAL_BCP_FGSAMPLE").equals("1")) 
			{
				GetSampleinfoFromHdfs(curTime, true,"FG" ,hour);
			}
			
		/*	if(ReturnConfig.returnconfig("conf/config.xml", "//comm//DEAL_BCP_MRSAMPLE").equals("1")){
				GetMRSampleinfoFromHdfs(curTime, true, "FG" ,"IN" ,"");
				GetMRSampleinfoFromHdfs(curTime, true, "FG" ,"OUT" ,"");
				GetMRSampleinfoFromHdfs(curTime, true, "OTT" ,"IN" ,"");
				GetMRSampleinfoFromHdfs(curTime, true, "OTT" ,"OUT" ,"DTEX");
				GetMRSampleinfoFromHdfs(curTime, true, "OTT" ,"OUT" ,"DT");
				
			}  */
				
			if (ReturnConfig.returnconfig("conf/config.xml", "//comm//DEAL_BCP_STAT").equals("1")
					||ReturnConfig.returnconfig("conf/config.xml", "//comm//DEAL_BCP_STAT").equals("")) 
			{//stat							
				//空字符串默认是40米栅格  "10"表示10米栅格
				GetGridinfoFromHdfs(curTime, true,"FG","" ,hour);
				GetGridinfoFromHdfs(curTime, true,"FG","10" ,hour);
				GetGridinfoFromHdfs(curTime, true,"","" ,hour);
				GetGridinfoFromHdfs(curTime, true,"","10" ,hour);
				
				GetCellinfoFromHdfs(curTime, true,"" ,hour);
				GetCellinfoFromHdfs(curTime, true,"FG" ,hour);
				GetCellgridInfoFromHdfs(curTime, true,"FG","",hour );
				GetCellgridInfoFromHdfs(curTime, true,"FG","10" ,hour);
				GetCellgridInfoFromHdfs(curTime, true,"","" ,hour);
				GetCellgridInfoFromHdfs(curTime, true,"","10" ,hour);
				
	/*			GetFreqGridByImeiFromHdfs(curTime, true,"","10","LT");
				GetFreqGridByImeiFromHdfs(curTime, true,"FG","10","LT");
				GetFreqGridByImeiFromHdfs(curTime, true,"","10","DX");
				GetFreqGridByImeiFromHdfs(curTime, true,"FG","10","DX");					
				GetFreqCellByImeiFromHdfs(curTime, true,"FG");
				GetFreqCellByImeiFromHdfs(curTime, true,"");
	*/			
				GetFreqGridFromHdfs(curTime, true,"FG","" ,hour);
				GetFreqGridFromHdfs(curTime, true,"FG","10" ,hour);
				GetFreqGridFromHdfs(curTime, true,"","" ,hour);
				GetFreqGridFromHdfs(curTime, true,"","10" ,hour);
				GetFreqCellInfoFromHdfs(curTime, true,"FG" ,hour);
				GetFreqCellInfoFromHdfs(curTime, true,"" ,hour);
				
				//gridbyimei
	/*			GetFreqGridByimeiInfoFromHdfs(curTime, true , "", "DT" ,"", "LT");
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "", "DT" ,"10", "LT");
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "", "CQT" ,"", "LT");
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "", "CQT" ,"10", "LT");
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "FG", "DT" ,"", "LT");
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "FG", "DT" ,"10", "LT");
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "FG", "CQT" ,"", "LT");
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "FG", "CQT" ,"10", "LT");
				
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "", "DT" ,"10", "DX");
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "", "CQT" ,"10", "DX");
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "FG", "CQT" ,"10", "DX");
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "FG", "CQT" ,"10", "DX");
				
				//newcellgrid
				GetCellgridInfoFromHdfs(curTime, true , "", "DT" ,"");
				GetCellgridInfoFromHdfs(curTime, true , "", "DT" ,"10");
				GetCellgridInfoFromHdfs(curTime, true , "", "CQT" ,"");
				GetCellgridInfoFromHdfs(curTime, true , "", "CQT" ,"10");
				GetCellgridInfoFromHdfs(curTime, true , "FG", "DT" ,"");
				GetCellgridInfoFromHdfs(curTime, true , "FG", "DT" ,"10");
				GetCellgridInfoFromHdfs(curTime, true , "FG", "CQT" ,"");
				GetCellgridInfoFromHdfs(curTime, true , "FG", "CQT" ,"10");
				
				//cellbyimei
				GetFreqCellByimeiInfoFromHdfs(curTime, true , "" ,"10", "LT");
				GetFreqCellByimeiInfoFromHdfs(curTime, true , "" ,"10", "DX");
				GetFreqCellByimeiInfoFromHdfs(curTime, true , "FG" ,"10", "LT");
				GetFreqCellByimeiInfoFromHdfs(curTime, true , "FG" ,"10", "DX");
				
				//mdt 
				GetMDTCellinfoFromHdfs(curTime, true,"MDT");
				GetMDTGridinfoFromHdfs(curTime, true,"MDT","");
				GetMDTGridinfoFromHdfs(curTime, true,"MDT","10");
				GetMDTCellgridInfoFromHdfs(curTime, true,"MDT","");
				GetMDTCellgridInfoFromHdfs(curTime, true,"MDT","10");
				GetMDTSampleinfoFromHdfs(curTime, true,"MDT");
	*/				
			}
			GetUserinfoFromHdfs(curTime, true ,hour);
			GetHourUserFromHdfs(curTime, true , hour);
			
			//=======================MR文件下载===============//  //还没修改
	/*		GetMRBuildFromHdfs(curTime,true,"","^TB_MR_BUILD","build");
			GetMRBuildFromHdfs(curTime,true,"CELL_NC_","^TB_MR_BUILD_CELL_NC","build_cellnc");
			GetMRBuildFromHdfs(curTime,true,"CELL_","^TB_MR_BUILD_CELL","buildcell");
			
			GetMRCellFromHdfs(curTime,true);
			
			GetMRIngridFromHdfs(curTime,true,"","^TB_MR_INGRID","ingrid");
			GetMRIngridFromHdfs(curTime,true,"CELL_NC_","^TB_MR_INGRID_CELL_NC","ingrid_cellnc");
			GetMRIngridFromHdfs(curTime,true,"CELL_","^TB_MR_INGRID_CELL","ingridcell");
			
			GetMROutGridFromHdfs(curTime,true,"","^TB_MR_OUTGRID","outgrid");
			GetMROutGridFromHdfs(curTime,true,"CELL_NC_","^TB_MR_OUTGRID_CELL_NC","outgrid_cellnc");
			GetMROutGridFromHdfs(curTime,true,"CELL_","^TB_MR_OUTGRID_CELL","outgridcell");		
			
			//mergestat3
			GetMRGridFromHdfs(curTime,true,"0TT_","","OUT","^TB_MODEL_MR_OTT_OUT_GRID","outgrid");
			GetMRGridFromHdfs(curTime,true,"GPS_","","OUT","^TB_MODEL_MR_GPS_OUT_GRID","outgrid");
			GetMRGridFromHdfs(curTime,true,"FG_","","OUT","^TB_MODEL_MR_FG_OUT_GRID","outgrid");
			GetMRGridFromHdfs(curTime,true,"OTT_","","IN","^TB_MODEL_MR_OTT_IN_GRID","ingrid");
			GetMRGridFromHdfs(curTime,true,"FG_","","IN","^TB_MODEL_MR_FG_IN_GRID","ingrid");
			
			GetMRGridFromHdfs(curTime,true,"OTT_","DX_","OUT","^TB_MODEL_MR_DX_OTT_OUT_GRID","outgrid");
			GetMRGridFromHdfs(curTime,true,"GPS_","DX_","OUT","^TB_MODEL_MR_DX_GPS_OUT_GRID","outgrid");
			GetMRGridFromHdfs(curTime,true,"FG_","DX_","OUT","^TB_MODEL_MR_DX_FG_OUT_GRID","outgrid");
			GetMRGridFromHdfs(curTime,true,"OTT_","DX_","IN","^TB_MODEL_MR_DX_OTT_IN_GRID","ingrid");
			GetMRGridFromHdfs(curTime,true,"FG_","DX_","IN","^TB_MODEL_MR_DX_FG_IN_GRID","ingrid");
			GetMRGridFromHdfs(curTime,true,"OTT_","LT_","OUT","^TB_MODEL_MR_LT_OTT_OUT_GRID","outgrid");
			GetMRGridFromHdfs(curTime,true,"GPS_","LT_","OUT","^TB_MODEL_MR_LT_GPS_OUT_GRID","outgrid");
			GetMRGridFromHdfs(curTime,true,"FG_","LT_","OUT","^TB_MODEL_MR_LT_FG_OUT_GRID","outgrid");
			GetMRGridFromHdfs(curTime,true,"OTT_","LT_","IN","^TB_MODEL_MR_LT_OTT_IN_GRID","ingrid");
			GetMRGridFromHdfs(curTime,true,"FG_","LT_","IN","^TB_MODEL_MR_LT_FG_IN_GRID","ingrid");
			
			GetMRBuildFromHdfs(curTime,true,"OTT","","^TB_MODEL_MR_OTT_BUILD","build");
			GetMRBuildFromHdfs(curTime,true,"FG","","^TB_MODEL_MR_FG_BUILD","build");
			GetMRBuildFromHdfs(curTime,true,"OTT","DX_","^TB_MODEL_MR_DX_OTT_BUILD","build");
			GetMRBuildFromHdfs(curTime,true,"FG","DX_","^TB_MODEL_MR_DX_FG_BUILD","build");
			GetMRBuildFromHdfs(curTime,true,"OTT","LT_","^TB_MODEL_MR_LT_OTT_BUILD","build");
			GetMRBuildFromHdfs(curTime,true,"FG","LT_","^TB_MODEL_MR_LT_FG_BUILD","build");
			
			GetMRCELLFromHdfs(curTime,true,"^TB_MODEL_MR_CELL","cell");
			GetMRCELLFromHdfs(curTime,true,"DX","^TB_MODEL_MR_DX_CELL","cell");
			GetMRCELLFromHdfs(curTime,true,"LT","^TB_MODEL_MR_LT_CELL","cell");
			
			GetMRCELLGridFromHdfs(curTime,true,"","OTT_","OUT","^TB_MODEL_MR_OTT_OUT_CELLGRID","outcellgrid");
			GetMRCELLGridFromHdfs(curTime,true,"","GPS_","OUT","^TB_MODEL_MR_GPS_OUT_CELLGRID","outcellgrid");
			GetMRCELLGridFromHdfs(curTime,true,"","FG_","OUT","^TB_MODEL_MR_FG_OUT_CELLGRID","outcellgrid");
			GetMRCELLGridFromHdfs(curTime,true,"","OTT_","IN","^TB_MODEL_MR_OTT_IN_CELLGRID","incellgrid");
			GetMRCELLGridFromHdfs(curTime,true,"","FG_","IN","^TB_MODEL_MR_FG_IN_CELLGRID","incellgrid");
			
			//gaotie
			GetMRSceneCELLFromHdfs(curTime,true, "SCENE" ,"^TB_MODEL_MR_SCENE_CELL", "cell");
			GetMRSceneGridFromHdfs(curTime,true, "SCENE" ,"^TB_MODEL_MR_SCENE_GRID", "grid");
			GetMRSceneCELLGridFromHdfs(curTime,true, "SCENE" ,"^TB_MODEL_MR_SCENE_CELLGRID", "cellgrid");
			GetMRSceneFromHdfs(curTime,true, "SCENE" ,"^TB_MODEL_MR_SCENE", "scene");
			
	*/								
		}
	}
}
	

class ComparatorMapForHour implements Comparator{  
	  
    public int compare(Object arg0, Object arg1) {  
      
        String dir1 = arg0.toString();  
        String dir2 = arg1.toString();  
		File file1 = new File(dir1);
		File file2 = new File(dir2);
		String filename1 = file1.getName().substring(file1.getName().indexOf("_"));
		String filename2 = file2.getName().substring(file2.getName().indexOf("_"));
      
        return (filename1.toLowerCase()).compareTo(filename2.toLowerCase());  
    }  
}  

