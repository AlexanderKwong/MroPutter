package cn.mastercom.sssvr.main;
/**
 * 
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.net.ftp.FTPFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;

import cn.mastercom.sssvr.util.CalendarEx;
import cn.mastercom.sssvr.util.DatafileInfo;
import cn.mastercom.sssvr.util.FTPClientHelper;
import cn.mastercom.sssvr.util.GreepPlumHelper;
import cn.mastercom.sssvr.util.HadoopFSOperations;
import cn.mastercom.sssvr.util.LocalFile;
import cn.mastercom.sssvr.util.ReturnConfig;

/**
 * @author
 *
 */
@SuppressWarnings("unused")
public class FileMoverShanXiToFtp extends Thread {

	public static String MrBcpBkPath = "";
	public static List<Integer> MrBcpBkHours = new ArrayList<Integer>();
	
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
				String filename = file.getPath().replace("\\", "/").replaceFirst(filePath, hdfs.HADOOP_URL);
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

	static Logger log = Logger.getLogger(FileMoverShanXiToFtp.class.getName());
	private static String DealMre = "1";
	private static String DealMtMro = "0";
	private static String SparkDealBcp = "0";
	private static String MroWorkSpace = "";
	private static String MreWorkSpace = "";
	private static int    MoverId = 0;
	private static int    CheckBcpDays = 7;
	
	
	public static boolean PutDirFromFtp(String ip, int port, String name,String pwd,String ftpPath, String hdfsPath,String hdfsRoot)
	{
		FTPClientHelper ftpHelper = new FTPClientHelper( ip , port, name , pwd );
		ftpHelper.setBinaryTransfer(true);
		ftpHelper.setPassiveMode(true);
		ftpHelper.setEncoding("utf-8");
		
		HadoopFSOperations hdfs = null;
		try {
			if(hdfsRoot.length()>0)
			{
				hdfs = new HadoopFSOperations(hdfsRoot);
			}
			else
			{
				Configuration conf = new Configuration();
				hdfs = new HadoopFSOperations(conf);
			}
			ftpHelper.putDirToHdfs(ftpPath, hdfsPath, hdfs);
			
		} catch (Exception e1) {		
			e1.printStackTrace();
			return false;
		}
		return true;
	}

	public static void Init()
	{
		readConfigInfo();
		hdfs = new HadoopFSOperations(hdfsRoot);
		Configuration conf = new Configuration();
		try {
			hdfs = new HadoopFSOperations(conf);			
		} catch (Exception e1) {		
			e1.printStackTrace();
		}
		System.out.println("DEAL_BCP:" + SparkDealBcp);

		if (SparkDealBcp.equals("1"))
		{
			FileMoverShanXiToFtp sampleMover = new FileMoverShanXiToFtp(2);
			sampleMover.start();
			System.out.println("Begin sampleMover...");
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
		ComparatorMap cop = new ComparatorMap();
		Collections.sort(sortedList, cop);
		for(String ss:sortedList)
		{
			System.out.println(ss);
		}*/
		if(args.length>1)
		{		
			if(args.length !=6)
			{
				System.out.println("useage: ip port user pwd ftppath hdfspath");
			}
			else
			{
				PutDirFromFtp(args[0], Integer.parseInt(args[1]),args[2],args[3], args[4],args[5],"");
			}
		}
		else
		{
			FileMoverShanXiToFtp.Init();
		}
		/*FileMoverShanXiToFtpSpark sampleMover = new FileMoverShanXiToFtpSpark(2);
		SimpleDateFormat simFormat = new SimpleDateFormat("yyyyMMdd HHmmss");
		try {
			Date dtBeg = simFormat.parse("20170511 010000");
			CalendarEx curTime = new CalendarEx(dtBeg);
	//		sampleMover.GetCellinfoFromHdfs();
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		*/
		
		
		//
	/*	HadoopFSOperations hftp = null ;
		FileMoverShanXiToFtp mv = new FileMoverShanXiToFtp(1);
		CalendarEx cal = new CalendarEx(new Date(2017,2,6));
	//	mv.GetFreqInfoFromHdfs(cal, true,"FG","10" ,time);
		mv.GetGridinfo(cal, true,"FG","",hftp);*/
		
		
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
							SparkDealBcp = element.getText();
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

	public static HadoopFSOperations hdfs = null;

	private static String hdfsRoot = "";

	private static String rootPath = "E:/mastercom";;
	
	private String localRoot = "e:/mastercom/SampleEvent";

	private String localRoot2 = "e:/mastercom/SampleEvent2";

	private String localRoot3 = "e:/mastercom/SampleEvent3";

	private int FileType = 0;
	
	private static String HdfsDataPath = "/mt_wlyh/Data"; 
	
	private static String ftpOutPath = "/data/";
	
	private static String ftpIp = "10.228.222.19";
	
	private static String ftpName = "mroputter";
	
	private static String ftpPwd = "MasterCom@168";
		
	private static int ftpPort = 21 ;
	


//	FTPClientHelper ftp = new FTPClientHelper("10.231.20.40", 21, "sample", "sample");
// 天津	FTPClientHelper ftp = new FTPClientHelper( ftpIp , ftpPort, ftpName , ftpPwd );
	FTPClientHelper ftp = new FTPClientHelper("10.231.20.153", 21, "swxt", "swxt@123");
	
	FTPClientHelper ftpFault = new FTPClientHelper("10.110.180.139", 21, "dongzeng", "dongzeng@boco123");


	public FileMoverShanXiToFtp(int fileType)
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

	private boolean CanDownload(String hdfsEventDir, String localDir, String localBackupDir)
	{ 	
		if (ftp.checkFileExist(localDir)
				|| ftp.checkFileExist(localDir.replace("SampleEvent", "SampleEvent2"))
				|| ftp.checkFileExist(localDir.replace("SampleEvent", "SampleEvent3"))
				|| ftp.checkFileExist(localBackupDir))
		{
			System.out.println("ftp目录下已经存在：" + localDir);
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

		if (cruTime.getHour() >= 3)
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

	public void GetCellgridInfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath, String typePath ,String tenMeterGrid ,HadoopFSOperations hftp)
	{
		if(truePath.contains("FG")){
			String tbModel = "^TB_MODEL_SIGNAL_CELLGRID";
			{// cellgrid
				String CellDir = "/TB_"+truePath+typePath+"SIGNAL_CELLGRID"+tenMeterGrid +"_"+ cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat2/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				if (bAutoDownload)
				{
					if (LocalFile.checkFileExist(ftpOutPath+"SampleEvent" + CellDir + tbModel))
						return;
					if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent" + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir))
						return;
				}
				hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent" + CellDir + tbModel, "cellgrid",ftp);
			}
		}else{
			String tbModel = "^TB_MODEL_SIGNAL_CELLGRID";
			{// cellgrid
				String CellDir = "/TB_"+truePath+typePath+"SIGNAL_CELLGRID"+tenMeterGrid +"_"+ cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				if (bAutoDownload)
				{
					if (LocalFile.checkFileExist(ftpOutPath+"SampleEvent" + CellDir + tbModel))
						return;
					if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent" + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir))
						return;
				}
				hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent" + CellDir + tbModel, "cellgrid",ftp);
			}
		}
		System.out.println("Success GetCellgridInfoFromHdfs :" + cal.getDateStr8());
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

	public void GetHourUserFromHdfs(CalendarEx cal, boolean bAutoDownload ,String time)
	{
		{// cell
			String CellDir = "/TB_SIGNAL_GRID_USER_HOUR_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/"+cal.getDateStr8().substring(2)+ "_" + time +"/gridstat"
					+ CellDir;
			if (bAutoDownload)
			{
				if (LocalFile.checkFileExist(localRoot + CellDir + "#" +time + "^TB_MODEL_SIGNAL_GRID_USER_HOUR"))
					return;
				if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + "#" +time + "^TB_MODEL_SIGNAL_GRID_USER_HOUR"))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot + CellDir + "#" +time, localRoot + "_done" + CellDir + "#" +time))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + "#" +time + "^TB_MODEL_SIGNAL_GRID_USER_HOUR", "user",true);
		}
		log.info("Success GetHourUserFromHdfs :" + cal.getDateStr8());
	}

	public void GetFreqInfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath,String tenMeterGrid,String time)
	{
		{// dt
			String CellDir = "/TB_"+truePath+"FREQ_DTSIGNAL_GRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/"+cal.getDateStr8().substring(2)+"_" + time +"/gridstat"
					+ CellDir;
			if (bAutoDownload)
			{
				if (LocalFile.checkFileExist(localRoot + CellDir + "#" +time + "^TB_MODEL_FREQ_SIGNAL_GRID"))
					return;
				if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + "#" +time + "^TB_MODEL_FREQ_SIGNAL_GRID"))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot + CellDir + "#" +time, localRoot + "_done" + CellDir + "#" +time))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + "#" +time + "^TB_MODEL_FREQ_SIGNAL_GRID", "grid",true);
		}

		{// cqt
			String CellDir = "/TB_"+truePath+"FREQ_CQTSIGNAL_GRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/"+cal.getDateStr8().substring(2)+"_" + time +"/gridstat"
					+ CellDir;
			if (bAutoDownload)
			{
				if (LocalFile.checkFileExist(localRoot + CellDir + "#" +time + "^TB_MODEL_FREQ_SIGNAL_GRID"))
					return;
				if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + "#" +time + "^TB_MODEL_FREQ_SIGNAL_GRID"))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot + CellDir + "#" +time, localRoot + "_done" + CellDir + "#" +time))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + "#" +time + "^TB_MODEL_FREQ_SIGNAL_GRID", "grid",true);
		}
		log.info("Success GetFreqInfoFromHdfs :" + cal.getDateStr8());
	}

	public void GetFreqCellInfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath ,String time)
	{
		{//
			String CellDir = "/TB_"+truePath+"FREQ_SIGNAL_CELL_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/"+cal.getDateStr8().substring(2)+"_" + time +"/gridstat"
					+ CellDir;
			if (bAutoDownload)
			{
				if (LocalFile.checkFileExist(localRoot + CellDir + "#" +time + "^TB_MODEL_FREQ_CELL"))
					return;
				if (LocalFile.checkFileExist(localRoot + "_done" + CellDir + "#" +time  + "^TB_MODEL_FREQ_CELL"))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot + CellDir + "#" +time , localRoot + "_done" + CellDir + "#" +time ))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + "#" +time  + "^TB_MODEL_FREQ_CELL", "cell",true);
		}

		log.info("Success GetFreqCellInfoFromHdfs :" + cal.getDateStr8());
	}

	public void GetCellinfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath,String time)
	{
		{// cell
			String CellDir = "/TB_"+truePath+"SIGNAL_CELL_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/"+cal.getDateStr8().substring(2)+"_" + time +"/gridstat"
					+ CellDir;
			if (bAutoDownload)
			{
				if (LocalFile.checkFileExist(localRoot + CellDir + "#" + time + "^TB_MODEL_SIGNAL_CELL"))
					return;
				if (!CanDownload(hdfsGRIDDir, localRoot + "_done" + CellDir + "#" + time, localRoot + "_done" + CellDir + "#" + time))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot + CellDir + "#" + time + "^TB_MODEL_SIGNAL_CELL", "cell",true);
		}
		log.info("Success GetCellStatinfoFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMRBuildFromHdfs(CalendarEx cal,boolean bAutoDownload,String truePath,String modelTable,HadoopFSOperations hftp ){
		{// cell
			String CellDir ="/TB_MR_BUILD_"+truePath+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mro_loc/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{   //  "^TB_MODEL_MR_BUILD"      "MRBuild"
				if (ftp.checkFileExist(ftpOutPath + "SampleEvent2" + CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent2" + CellDir , ftpOutPath+"SampleEvent" + "_done" + CellDir  ))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent2"+ CellDir + modelTable, "",ftp);
		}
		System.out.println("Success GetMRBuildFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMRCellFromHdfs(CalendarEx cal,boolean bAutoDownload, HadoopFSOperations hftp){
		{// cell
			String CellDir ="/TB_STAT_MR_CELL_"+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mro_loc/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{
				if (ftp.checkFileExist(ftpOutPath+"SampleEvent2" + CellDir + "^TB_STAT_MR_CELL"))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent2" + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir ))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent2"+CellDir + "^TB_STAT_MR_CELL", "",ftp);
		}
		System.out.println("Success GetMRCellFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMRIngridFromHdfs(CalendarEx cal,boolean bAutoDownload,String truePath,String modelTable, String fileName ,HadoopFSOperations hftp){
		{// cell   
			String CellDir ="/TB_MR_INGRID_"+truePath+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mro_loc/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{      //  "TB_MODEL_MR_INGRID"  mringrid
				if (ftp.checkFileExist(ftpOutPath+"SampleEvent2" + CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent2" + CellDir , ftpOutPath+"SampleEvent" + "_done" + CellDir ))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent2"+ CellDir + modelTable, "", ftp);
		}
		System.out.println("Success GetMRIngridFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMROutGridFromHdfs(CalendarEx cal,boolean bAutoDownload,String truePath,String modelTable,String fileName ,HadoopFSOperations hftp){
		{// cell
			String CellDir ="/TB_MR_OUTGRID_"+truePath+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mro_loc/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{      //  "^TB_MODEL_MR_OUTGRID"   "outgrid"
				if (LocalFile.checkFileExist(ftpOutPath+"SampleEvent2" + CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent2" + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent2"+ CellDir + modelTable, "", ftp);
		}
		System.out.println("Success GetMROutGridFromHdfs :" + cal.getDateStr8());
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

	public void GetEventinfoFromHdfs(CalendarEx cal, boolean bAutoDownload,HadoopFSOperations hftp)
	{

		String eventDir = "/TB_CQTSIGNAL_EVENT_01_" + cal.getDateStr8().substring(2);	
		String hdfsEventDir = HdfsDataPath +"/mroxdrmerge/xdr_loc/data_01_" + cal.getDateStr8().substring(2) + eventDir;
	
		if (bAutoDownload)
		{
			if (!CanDownload(hdfsEventDir, ftpOutPath+"SampleEvent3" + eventDir , ftpOutPath+"SampleEvent" + "_done" + eventDir ))
				return;
		}
		System.out.println("Begin GetEventinfoFromHdfs :" + cal.getDateStr8());		
		hftp.readHdfsDirToftp(hdfsEventDir, ftpOutPath+"SampleEvent3"+eventDir, "",ftp);
		
		eventDir = "/TB_DTEXSIGNAL_EVENT_01_" + cal.getDateStr8().substring(2);
		hdfsEventDir = HdfsDataPath +"/mroxdrmerge/xdr_loc/data_01_" + cal.getDateStr8().substring(2) + eventDir;

		hftp.readHdfsDirToftp(hdfsEventDir, ftpOutPath+"SampleEvent3"+eventDir, "",ftp);

		eventDir = "/TB_DTSIGNAL_EVENT_01_" + cal.getDateStr8().substring(2);
		hdfsEventDir = HdfsDataPath +"/mroxdrmerge/xdr_loc/data_01_" + cal.getDateStr8().substring(2) + eventDir;

		hftp.readHdfsDirToftp(hdfsEventDir, ftpOutPath+"SampleEvent3"+eventDir, "",ftp);

		// eventDir = "/TB_ERRSIGNAL_EVENT_01_" +
		// cal.getDateStr8().substring(2);
		// hdfsEventDir = hdfsRoot+ HdfsDataPath +"/mroxdrmerge/xdr_loc/data_01_" +
		// cal.getDateStr8().substring(2)
		// + eventDir;
		// hdfs.readHdfsDirToLocal(hdfsEventDir,localRoot+eventDir+"^TB_MODEL_ERRSIGNAL_XDR","event");

		//log.info("Success GetEventinfoFromHdfs :" + cal.getDateStr8());
		System.out.println("Success GetEventinfoFromHdfs :" + cal.getDateStr8());
	}

	public void GetUserinfoFromHdfs(CalendarEx cal, boolean bAutoDownload ,String time)
	{
		{
			String eventDir = "/TB_SIGNAL_USERINFO_01_" + cal.getDateStr8().substring(2);
			String hdfsEventDir = HdfsDataPath +"/"+cal.getDateStr8().substring(2)+"_"+time+"/xdr_loc" 
					+ eventDir;
			if (bAutoDownload)
			{
				if (LocalFile.checkFileExist(localRoot + eventDir +"#"+ time + "^TB_MODEL_SIGNAL_USERINFO"))
					return;
				if (LocalFile.checkFileExist(localRoot + "_done" + eventDir  +"#"+ time + "^TB_MODEL_SIGNAL_USERINFO"))
					return;
				if (!CanDownload(hdfsEventDir, localRoot + eventDir +"#"+ time, localRoot + "_done" + eventDir +"#"+ time))
					return;
			}
			hdfs.readHdfsDirToLocal(hdfsEventDir, localRoot + eventDir +"#"+ time + "^TB_MODEL_SIGNAL_USERINFO", "user");
		}

		log.info("Success GetUserinfoFromHdfs :" + cal.getDateStr8());
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

	public void GetGridinfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath,String tenMeterGrid,String time)
	{
		//
		String cqtGridDir = "/TB_"+truePath+"CQTSIGNAL_GRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
		String dtGridDir  = "/TB_"+truePath+"DTSIGNAL_GRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
		String GridDir    = "/TB_"+truePath+"SIGNAL_GRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);

		{// total
		//	HdfsDataPath +"/mroxdrmerge/"+curTime.getDateStr8().substring(2)+"_"+time+"/mro_loc/"+fgSampleDir
			String hdfsGRIDDir = HdfsDataPath +"/"+cal.getDateStr8().substring(2)+"_" + time +"/gridstat"
					+ GridDir;
			if (bAutoDownload)
			{
				if (!CanDownload(hdfsGRIDDir, localRoot + GridDir +"#"+ time, localRoot + "_done" + GridDir +"#"+ time))
					return;
			}
			hdfs.getMerge(hdfsGRIDDir, localRoot + GridDir +"#"+ time, "grid",true);
		}

		{// CQT
			String hdfsGRIDDir = HdfsDataPath +"/"+cal.getDateStr8().substring(2)+"_" + time +"/gridstat"
					+ cqtGridDir;
			hdfs.getMerge(hdfsGRIDDir, localRoot + cqtGridDir+"#"+ time, "grid",true);
		}

		{// DT
			String hdfsGRIDDir = HdfsDataPath +"/"+cal.getDateStr8().substring(2)+"_" + time +"/gridstat"
					+ dtGridDir;
			hdfs.getMerge(hdfsGRIDDir, localRoot + dtGridDir+"#"+ time, "grid",true);
		}
		log.info("Success GetGridinfoFromHdfs :" + cal.getDateStr8());
		GetCellinfoFromHdfs(cal, bAutoDownload,truePath ,time);
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

	public void GetSampleinfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath,HadoopFSOperations hftp)
	{
		// final CalendarEx cal = new CalendarEx(theDate);

		String SAMPLEDir = "/TB_"+truePath+"CQTSIGNAL_SAMPLE_01_" + cal.getDateStr8().substring(2);
		String hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/mro_loc/data_01_" + cal.getDateStr8().substring(2) + SAMPLEDir;
		if (bAutoDownload)
		{
			if (!CanDownload(hdfsSAMPLEDir, ftpOutPath+"SampleEvent3"+SAMPLEDir,ftpOutPath+"SampleEvent" + "_done" + SAMPLEDir))
				return;
		}
		System.out.println("Begin GetSampleinfoFromHdfs :" + ftpOutPath+"SampleEvent3"+SAMPLEDir);
		
		hftp.readHdfsDirToftp(hdfsSAMPLEDir, ftpOutPath+"SampleEvent3"+SAMPLEDir, "",ftp);

		SAMPLEDir = "/TB_"+truePath+"DTEXSIGNAL_SAMPLE_01_" + cal.getDateStr8().substring(2);
		hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/mro_loc/data_01_" + cal.getDateStr8().substring(2) + SAMPLEDir;
	
		hftp.readHdfsDirToftp(hdfsSAMPLEDir, ftpOutPath+"SampleEvent3"+SAMPLEDir, "",ftp);
		
		System.out.println("Success GetSampleinfoFromHdfs :" + cal.getDateStr8());

		SAMPLEDir = "/TB_"+truePath+"DTSIGNAL_SAMPLE_01_" + cal.getDateStr8().substring(2);
		hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/mro_loc/data_01_" + cal.getDateStr8().substring(2) + SAMPLEDir;
		//hdfs.getMerge(hdfsSAMPLEDir, localRoot3 + SAMPLEDir+"#"+time, "sample", true);
		hftp.readHdfsDirToftp(hdfsSAMPLEDir, ftpOutPath+"SampleEvent3"+SAMPLEDir, "",ftp);

		System.out.println("Success GetSampleinfoFromHdfs :" + cal.getDateStr8());

	}
	
	public void GetVilSampleinfoFromHdfs(CalendarEx cal, boolean bAutoDownload)
	{
		// final CalendarEx cal = new CalendarEx(theDate);

		String SAMPLEDir = "/TB_SIGNAL_VILLAGE_SAMPLE_01_" + cal.getDateStr8().substring(2);
		String hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/mro_village/data_01_" + cal.getDateStr8().substring(2) + SAMPLEDir;
		if (bAutoDownload)
		{
			if (!CanDownload(hdfsSAMPLEDir, localRoot3 + SAMPLEDir, localRoot + "_done" + SAMPLEDir))
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
			if (!CanDownload(hdfsSAMPLEDir, localRoot3 + SAMPLEDir, localRoot + "_done" + SAMPLEDir))
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
			if (!CanDownload(hdfsSAMPLEDir, localRoot3 + SAMPLEDir, localRoot + "_done" + SAMPLEDir))
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
		
		filePath = filePath.replace("\\", "/");
		List<String> files;
		HashMap<String, Integer> mapValue = new HashMap<String, Integer>();

		try
		{
			files = LocalFile.getAllFiles(new File(filePath), "MR", 10);
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
			if (!mapValue.containsKey(file.getParent().toString()))
			{
				mapValue.put(file.getParent().toString(), 1); // 得到所有文件的上层文件夹
			}
		}
		
		List<String> sortedList = new ArrayList<String>();
		for (String fileName : mapValue.keySet())
		{
			sortedList.add(fileName);
		}
		Collections.sort(sortedList, new ComparatorMapps());

		// 创建一个线程池
		ExecutorService pool = Executors.newFixedThreadPool(30);
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

				if (lastModifyTime + 900 > cal._second)
				{
					continue;
				}

				String dstFileName = filePath + "/" + file.getName();
				while(LocalFile.checkFileExist(dstFileName))
				{
					dstFileName += "x";
				}
				
				
				// if(file.getName().contains("processing"))
				// continue;

				try {
					file.renameTo(new File(dstFileName));
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
	public void GetGridinfo(CalendarEx cal,boolean bAutoDownload,String truePath ,String tenMeterGrid, HadoopFSOperations hftp){
		
		String cqtGridDir = "/TB_"+truePath+"CQTSIGNAL_GRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
		String dtGridDir  = "/TB_"+truePath+"DTSIGNAL_GRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
		String GridDir    = "/TB_"+truePath+"SIGNAL_GRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
		
		if(truePath.contains("FG")){
			{String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat2/data_01_" + cal.getDateStr8().substring(2)
					+ GridDir;
			if (bAutoDownload)
			{
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent"+GridDir, ftpOutPath+"SampleEvent" + "_done" + GridDir))
					return;
			}
			
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+GridDir, "",ftp);
			}
			//---------cqt--------
			{
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat2/data_01_" + cal.getDateStr8().substring(2)
					+ cqtGridDir;
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+cqtGridDir, "",ftp);
			}
			//---------dt--------
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat2/data_01_" + cal.getDateStr8().substring(2)
					+ dtGridDir;
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+dtGridDir, "",ftp);
		}else{
		//---------grid----
			{String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ GridDir;
			if (bAutoDownload)
			{
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent"+GridDir, ftpOutPath+"SampleEvent" + "_done" + GridDir))
					return;
			}
			
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+GridDir, "",ftp);
			}
			//---------cqt--------
			{
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ cqtGridDir;
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+cqtGridDir, "",ftp);
			}
			//---------dt--------
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ dtGridDir;
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+dtGridDir, "",ftp);
		}		
		System.out.println("Success GetGridinfoFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetCellinfo(CalendarEx cal,boolean bAutoDownload,String truePath  ,HadoopFSOperations hftp){
		
		if(truePath.contains("FG")){
			String CellDir = "/TB_"+truePath+"SIGNAL_CELL_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat2/data_01_" + cal.getDateStr8().substring(2)
					+ CellDir;
			if (bAutoDownload)
			{
				if (ftp.checkFileExist(ftpOutPath+"SampleEvent" + CellDir + "^TB_MODEL_SIGNAL_CELL"))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent"  + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+ CellDir +"^TB_MODEL_SIGNAL_CELL", "",ftp);
		}else{
			String CellDir = "/TB_"+truePath+"SIGNAL_CELL_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ CellDir;
			if (bAutoDownload)
			{
				if (ftp.checkFileExist(ftpOutPath+"SampleEvent" + CellDir + "^TB_MODEL_SIGNAL_CELL"))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent"  + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+ CellDir +"^TB_MODEL_SIGNAL_CELL", "",ftp);
		}
		System.out.println("Success GetCellStatinfoFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetCellgridInfo(CalendarEx cal ,boolean bAutoDownload ,String truePath ,String tenMeterGrid ,HadoopFSOperations hftp){
		if(truePath.contains("FG")){
			String CellDir = "/TB_"+truePath+"SIGNAL_CELLGRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat2/data_01_" + cal.getDateStr8().substring(2)
					+ CellDir;
			if (bAutoDownload)
			{
				if (ftp.checkFileExist(ftpOutPath+"SampleEvent" + CellDir + "^TB_MODEL_SIGNAL_CELLGRID"))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent" + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+CellDir + "^TB_MODEL_SIGNAL_CELLGRID", "",ftp);
		}else{
			String CellDir = "/TB_"+truePath+"SIGNAL_CELLGRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ CellDir;
			if (bAutoDownload)
			{
				if (ftp.checkFileExist(ftpOutPath+"SampleEvent" + CellDir + "^TB_MODEL_SIGNAL_CELLGRID"))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent" + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+CellDir + "^TB_MODEL_SIGNAL_CELLGRID", "",ftp);
		}
		System.out.println("Success GetCellGridinfoFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetFreqinfo(CalendarEx cal,boolean bAutoDownload,String truePath ,String tenMeterGrid ,HadoopFSOperations hftp){
		if(truePath.contains("FG")){
			// dt
			{String CellDir = "/TB_"+truePath+"FREQ_DTSIGNAL_GRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat2/data_01_" + cal.getDateStr8().substring(2)
					+ CellDir;
			if (bAutoDownload)
			{
				if (ftp.checkFileExist(ftpOutPath+"SampleEvent" + CellDir + "^TB_MODEL_FREQ_SIGNAL_GRID"))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent" + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir))
					return;
			}
			
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+CellDir + "^TB_MODEL_FREQ_SIGNAL_GRID", "",ftp);
			}
			
			// cqt
			{
			String CellDir = "/TB_"+truePath+"FREQ_CQTSIGNAL_GRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat2/data_01_" + cal.getDateStr8().substring(2)
					+ CellDir;
			if (bAutoDownload)
			{
				if (ftp.checkFileExist(ftpOutPath+"SampleEvent" + CellDir + "^TB_MODEL_FREQ_SIGNAL_GRID"))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent" + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+ CellDir + "^TB_MODEL_FREQ_SIGNAL_GRID", "",ftp);
			}
		}else{
			// dt
			{String CellDir = "/TB_"+truePath+"FREQ_DTSIGNAL_GRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ CellDir;
			if (bAutoDownload)
			{
				if (ftp.checkFileExist(ftpOutPath+"SampleEvent" + CellDir + "^TB_MODEL_FREQ_SIGNAL_GRID"))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent" + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir))
					return;
			}
			
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent" + CellDir + "^TB_MODEL_FREQ_SIGNAL_GRID", "",ftp);
			}
			
			// cqt
			{
			String CellDir = "/TB_"+truePath+"FREQ_CQTSIGNAL_GRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ CellDir;
			if (bAutoDownload)
			{
				if (ftp.checkFileExist(ftpOutPath+"SampleEvent" + CellDir + "^TB_MODEL_FREQ_SIGNAL_GRID"))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent" + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+CellDir + "^TB_MODEL_FREQ_SIGNAL_GRID", "",ftp);
			}	
		}
		System.out.println("Success GetFreqInfoFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetFreqCellInfo(CalendarEx cal, boolean bAutoDownload, String truePath ,HadoopFSOperations hftp){
		if(truePath.contains("FG")){
			String CellDir = "/TB_"+truePath+"FREQ_SIGNAL_CELL_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat2/data_01_" + cal.getDateStr8().substring(2)
					+ CellDir;
			if (bAutoDownload)
			{
				if (ftp.checkFileExist(ftpOutPath+"SampleEvent" + CellDir + "^TB_MODEL_FREQ_CELL"))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent" + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+ CellDir + "^TB_MODEL_FREQ_CELL" , "",ftp);
		}else{
			String CellDir = "/TB_"+truePath+"FREQ_SIGNAL_CELL_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ CellDir;
			if (bAutoDownload)
			{
				if (ftp.checkFileExist(ftpOutPath+"SampleEvent" + CellDir + "^TB_MODEL_FREQ_CELL"))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent" + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent" + CellDir + "^TB_MODEL_FREQ_CELL", "",ftp);
		}
		System.out.println("Success GetFreqCellInfoFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetUserinfo(CalendarEx cal , boolean bAutoDownload  ,HadoopFSOperations hftp){
		String eventDir = "/TB_SIGNAL_USERINFO_01_" + cal.getDateStr8().substring(2);
		String hdfsEventDir = HdfsDataPath +"/mroxdrmerge/xdr_loc/data_01_" + cal.getDateStr8().substring(2)
				+ eventDir;
		if (bAutoDownload)
		{
			if (ftp.checkFileExist(ftpOutPath+"SampleEvent" + eventDir + "^TB_MODEL_SIGNAL_USERINFO"))
				return;
			if (!CanDownload(hdfsEventDir, ftpOutPath+"SampleEvent" + eventDir, ftpOutPath+"SampleEvent" + "_done" + eventDir))
				return;
		}
		hftp.readHdfsDirToftp(hdfsEventDir, ftpOutPath+"SampleEvent"+eventDir + "^TB_MODEL_SIGNAL_USERINFO", "",ftp);
		System.out.println("Success GetUserinfoFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetHourUser(CalendarEx cal ,  boolean bAutoDownload ,HadoopFSOperations hftp){	
	   {// cell
		String CellDir = "/TB_SIGNAL_GRID_USER_HOUR_01_" + cal.getDateStr8().substring(2);
		String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
				+ CellDir;
		if (bAutoDownload)
		{
			if (ftp.checkFileExist(ftpOutPath+"SampleEvent" + CellDir + "^TB_MODEL_SIGNAL_GRID_USER_HOUR"))
				return;
			if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent" + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir))
				return;
		}
		hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+CellDir + "^TB_MODEL_SIGNAL_GRID_USER_HOUR" , "",ftp);
	   }
	   System.out.println("Success GetHourUserFromHdfs :" + cal.getDateStr8());
	}
	

	
	public void GetFreqGridByimeiInfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath,String typePath ,String tenMeterGrid ,String flagPath,HadoopFSOperations hftp)
	{
		if(truePath.contains("FG")){
			String tbModel = "^TB_MODEL_FREQ_SIGNAL_CELLGRID_BYIMEI";
			{
				String CellDir = "/TB_FREQ_"+truePath + typePath +"SIGNAL_" + flagPath + "_GRID"+tenMeterGrid+"_BYIMEI_" + cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				if (bAutoDownload)
				{
					if (ftp.checkFileExist(ftpOutPath+"SampleEvent" + CellDir + tbModel ))
						return;
					if (!CanDownload(hdfsGRIDDir, ftpOutPath +"SampleEvent" + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir))
						return;
				}
				hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent" + CellDir + tbModel, "",ftp);
			}
		}else{
			String tbModel = "^TB_MODEL_FREQ_SIGNAL_CELLGRID_BYIMEI";
			{
				String CellDir = "/TB_FREQ_"+truePath + typePath +"SIGNAL_" + flagPath + "_GRID"+tenMeterGrid+"_BYIMEI_" + cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				if (bAutoDownload)
				{
					if (ftp.checkFileExist(ftpOutPath+"SampleEvent" + CellDir + tbModel ))
						return;
					if (!CanDownload(hdfsGRIDDir, ftpOutPath +"SampleEvent" + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir))
						return;
				}
				hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent" + CellDir + tbModel, "",ftp);
			}
		}
		System.out.println("Success GetFreqLtGridByimeiInfoFromHdfs :" + cal.getDateStr8());
	}
	
	
	public void GetFreqCellByimeiInfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath ,String tenMeterGrid ,String flagPath ,HadoopFSOperations hftp)
	{
		if(truePath.contains("FG")){
			//cell
			String tbModel = "^TB_MODEL_FREQ_SIGNAL_CELL_BYIMEI";
			{
				String CellDir = "/TB_FREQ_"+truePath +"SIGNAL_" + flagPath + "_CELL"+tenMeterGrid+"_BYIMEI_" + cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat2/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				if (bAutoDownload)
				{
					if (ftp.checkFileExist(ftpOutPath+"SampleEvent" + CellDir + tbModel ))
						return;
					if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent" + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir))
						return;
				}
				hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+ CellDir + tbModel , "",ftp);
			}
		}else{
			//cell
			String tbModel = "^TB_MODEL_FREQ_SIGNAL_CELL_BYIMEI";
			{
				String CellDir = "/TB_FREQ_"+truePath +"SIGNAL_" + flagPath + "_CELL"+tenMeterGrid+"_BYIMEI_" + cal.getDateStr8().substring(2);
				String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
						+ CellDir;
				if (bAutoDownload)
				{
					if (ftp.checkFileExist(ftpOutPath+"SampleEvent" + CellDir + tbModel ))
						return;
					if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent" + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir))
						return;
				}
				hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+ CellDir + tbModel , "",ftp);
			}
		}
		System.out.println("Success GetFreqLtGridByimeiInfoFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMRGridFromHdfs(CalendarEx cal,boolean bAutoDownload,String truePath,String typePath,String inpath, String modelTable,String fileName ,HadoopFSOperations hftp){
		{// mrgrid
			String CellDir ="/TB_MR_"+typePath+truePath+inpath+"_GRID_"+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mergestat3/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{      //   "outgrid"
				if (ftp.checkFileExist(ftpOutPath+"SampleEvent2" + CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent2" + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir))
					return;
			}			
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent2"+ CellDir + modelTable , "",ftp);
		}
		System.out.println("Success GetMRGridFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMRBuildFromHdfs(CalendarEx cal,boolean bAutoDownload,String truePath,String typePath, String modelTable,String fileName ,HadoopFSOperations hftp){
		{// mrbuild
			String CellDir ="/TB_MR_"+typePath+truePath+"_BUILD_"+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mergestat3/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{      //   "outgrid"
				if (ftp.checkFileExist(ftpOutPath+"SampleEvent2" + CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent2" + CellDir , ftpOutPath+"SampleEvent" + "_done" + CellDir))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent2"+ CellDir + modelTable , "",ftp);
		}
		System.out.println("Success GetMRBuildFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMRCELLFromHdfs(CalendarEx cal,boolean bAutoDownload, String modelTable,String fileName ,HadoopFSOperations hftp){
		{// mrbuild
			String CellDir ="/TB_MR_CELL_"+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mergestat3/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{      // 
				if (ftp.checkFileExist(ftpOutPath+"SampleEvent2" + CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent2" + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent2"+ CellDir + modelTable , "",ftp);
		}
		System.out.println("Success GetMRCELLFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMRCELLFromHdfs(CalendarEx cal,boolean bAutoDownload,String typePath, String modelTable,String fileName ,HadoopFSOperations hftp){
		{// mrbuild
			String CellDir ="/TB_MR_"+typePath+"_CELL_"+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mergestat3/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{      // 
				if (ftp.checkFileExist(ftpOutPath+"SampleEvent2" + CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent2" + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent2"+ CellDir + modelTable , "",ftp);
		}
		log.info("Success GetMRCELLFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMRCELLGridFromHdfs(CalendarEx cal,boolean bAutoDownload,String truePath,String typePath,String inpath, String modelTable,String fileName ,HadoopFSOperations hftp){
		{// mrgrid
			String CellDir ="/TB_MR_"+typePath+truePath+inpath+"_CELLGRID_"+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mergestat3/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{      //   "outgrid"
				if (ftp.checkFileExist(ftpOutPath+"SampleEvent2" + CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+"SampleEvent2" + CellDir, ftpOutPath+"SampleEvent" + "_done" + CellDir))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent2"+ CellDir + modelTable , "",ftp);
		}
		System.out.println("Success GetMRGridFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMDTGridinfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath,String tenMeterGrid ,HadoopFSOperations hftp)
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
				if (ftp.checkFileExist(ftpOutPath + "SampleEvent" + GridDir + tbModel))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath + "SampleEvent" + GridDir, ftpOutPath + "SampleEvent" + "_done" + GridDir))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+ GridDir + tbModel , "",ftp);
		}
	
		{// CQT
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ cqtGridDir;
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+ cqtGridDir + tbModel , "",ftp);
		}
	
		{// DT
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ dtGridDir;
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+ dtGridDir + tbModel , "",ftp);
		}
		
		log.info("Success GetMDTGridinfoFromHdfs :" + cal.getDateStr8());
		System.out.println("Success GetMDTGridinfoFromHdfs :" + cal.getDateStr8());
		
	}
	
	public void GetMDTCellgridInfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath,String tenMeterGrid ,HadoopFSOperations hftp)
	{
		String tbModel = "^TB_MODEl_SIGNAL_CELLGRID";
		{// totall
			String CellDir = "/TB_"+truePath+"_SIGNAL_CELLGRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ CellDir;
			if (bAutoDownload)
			{
				if (LocalFile.checkFileExist(ftpOutPath+ "SampleEvent" + CellDir + tbModel ))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+ "SampleEvent" + CellDir, ftpOutPath+ "SampleEvent" + "_done" + CellDir))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+ CellDir + tbModel , "",ftp);
		}
	
		{// cqt
			String CellDir = "/TB_"+truePath+"_CQTSIGNAL_CELLGRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ CellDir;
			if (bAutoDownload)
			{
				if (LocalFile.checkFileExist(ftpOutPath+ "SampleEvent" + CellDir + tbModel ))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+ "SampleEvent" + CellDir, ftpOutPath+ "SampleEvent" + "_done" + CellDir))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+ CellDir + tbModel , "",ftp);
		}
		
		{// dt
			String CellDir = "/TB_"+truePath+"_DTSIGNAL_CELLGRID"+tenMeterGrid+"_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ CellDir;
			if (bAutoDownload)
			{
				if (LocalFile.checkFileExist(ftpOutPath+ "SampleEvent" + CellDir + tbModel ))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath+ "SampleEvent" + CellDir, ftpOutPath+ "SampleEvent" + "_done" + CellDir))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+"SampleEvent"+ CellDir + tbModel , "",ftp);
		}
		log.info("Success GetMDTCellGridinfoFromHdfs :" + cal.getDateStr8());
		System.out.println("Success GetMDTCellGridinfoFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMDTCellinfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath ,HadoopFSOperations hftp)
	{	
		String tbModel = "^TB_MODEL_SIGNAL_CELL";
		{// cell
			String CellDir = "/TB_"+truePath+"_SIGNAL_CELL_01_" + cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath +"/mroxdrmerge/mergestat/data_01_" + cal.getDateStr8().substring(2)
					+ CellDir;
			if (bAutoDownload)
			{
				if (hftp.checkFileExist(ftpOutPath + "SampleEvent" + CellDir + tbModel))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath + "SampleEvent" + CellDir , ftpOutPath + "SampleEvent" + "_done" + CellDir))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+ "SampleEvent" + CellDir + tbModel , "",ftp);
		}
	
	log.info("Success GetMDTCellStatinfoFromHdfs :" + cal.getDateStr8());
	System.out.println("Success GetMDTCellStatinfoFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMDTSampleinfoFromHdfs(CalendarEx cal, boolean bAutoDownload,String truePath ,HadoopFSOperations hftp)
	{
		// final CalendarEx cal = new CalendarEx(theDate);
		String tbModel = "^TB_MODEL_SIGNAL_SAMPLE";
		
		String SAMPLEDir = "/TB_"+truePath+"_CQTSIGNAL_SAMPLE_01_" + cal.getDateStr8().substring(2);
		String hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/mro_loc/data_01_" + cal.getDateStr8().substring(2) + SAMPLEDir;
		if (bAutoDownload)
		{
			if (LocalFile.checkFileExist(ftpOutPath + "SampleEvent3" + SAMPLEDir + tbModel))
				return;
			if (!CanDownload(hdfsSAMPLEDir, ftpOutPath + "SampleEvent3" + SAMPLEDir, ftpOutPath + "SampleEvent" + "_done" + SAMPLEDir))
				return;
		}
		log.info("Begin GetSampleinfoFromHdfs :" + cal.getDateStr8());
		hftp.readHdfsDirToftp(hdfsSAMPLEDir, ftpOutPath+ "SampleEvent3" + SAMPLEDir + tbModel , "",ftp);

		SAMPLEDir = "/TB_"+truePath+"_DTEXSIGNAL_SAMPLE_01_" + cal.getDateStr8().substring(2);
		hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/mro_loc/data_01_" + cal.getDateStr8().substring(2) + SAMPLEDir;
		hftp.readHdfsDirToftp(hdfsSAMPLEDir, ftpOutPath+ "SampleEvent3" + SAMPLEDir + tbModel , "",ftp);

		SAMPLEDir = "/TB_"+truePath+"_DTSIGNAL_SAMPLE_01_" + cal.getDateStr8().substring(2);
		hdfsSAMPLEDir = HdfsDataPath +"/mroxdrmerge/mro_loc/data_01_" + cal.getDateStr8().substring(2) + SAMPLEDir;
		hftp.readHdfsDirToftp(hdfsSAMPLEDir, ftpOutPath+ "SampleEvent3" + SAMPLEDir + tbModel , "",ftp);

		log.info("Success GetMDTSampleinfoFromHdfs :" + cal.getDateStr8());
		System.out.println("Success GetMDTSampleinfoFromHdfs :" + cal.getDateStr8());

	}
	
	public void GetMRSceneCELLFromHdfs(CalendarEx cal,boolean bAutoDownload,String typePath, String modelTable,String fileName ,HadoopFSOperations hftp){
		{// mrbuild
			String CellDir ="/TB_MR_"+typePath+"_CELL_"+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mergestat3/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{      // 
				if (ftp.checkFileExist(ftpOutPath + "SampleEvent2" + CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath + "SampleEvent2" + CellDir, ftpOutPath + "SampleEvent" + "_done" + CellDir))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+ "SampleEvent2" + CellDir + modelTable , "",ftp);
		}
		log.info("Success GetMRSceneCELLFromHdfs :" + cal.getDateStr8());
		System.out.println("Success GetMRSceneCELLFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMRSceneGridFromHdfs(CalendarEx cal,boolean bAutoDownload,String truePath, String modelTable,String fileName ,HadoopFSOperations hftp){
		{// mrgrid
			String CellDir ="/TB_MR_" + truePath +"_GRID_"+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mergestat3/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{      //   "outgrid"
				if (ftp.checkFileExist(ftpOutPath + "SampleEvent2" + CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath + "SampleEvent2" + CellDir, ftpOutPath + "SampleEvent" + "_done" + CellDir))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+ "SampleEvent2" + CellDir + modelTable , "",ftp);
		}
		log.info("Success GetMRSceneGridFromHdfs :" + cal.getDateStr8());
		System.out.println("Success GetMRSceneGridFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMRSceneCELLGridFromHdfs(CalendarEx cal,boolean bAutoDownload,String truePath, String modelTable,String fileName ,HadoopFSOperations hftp){
		{// mrgrid
			String CellDir ="/TB_MR_"+truePath+"_CELLGRID_"+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mergestat3/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{      //   "outgrid"
				if (ftp.checkFileExist(ftpOutPath + "SampleEvent2" + CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath + "SampleEvent2" + CellDir, ftpOutPath + "SampleEvent" + "_done" + CellDir))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+ "SampleEvent2" + CellDir + modelTable , "",ftp);
		}
		log.info("Success GetMRSceneCELLGridFromHdfs :" + cal.getDateStr8());
		log.info("Success GetMRSceneCELLGridFromHdfs :" + cal.getDateStr8());
	}
	
	public void GetMRSceneFromHdfs(CalendarEx cal,boolean bAutoDownload,String truePath, String modelTable,String fileName ,HadoopFSOperations hftp){
		{// mrgrid
			String CellDir ="/TB_MR_"+truePath+"_"+cal.getDateStr8().substring(2);
			String hdfsGRIDDir = HdfsDataPath+"/mroxdrmerge/mergestat3/data_01_"+cal.getDateStr8().substring(2)
					+CellDir;
			if (bAutoDownload)
			{      //   "outgrid"
				if (ftp.checkFileExist(ftpOutPath + "SampleEvent2" + CellDir + modelTable))
					return;
				if (!CanDownload(hdfsGRIDDir, ftpOutPath + "SampleEvent2" + CellDir, ftpOutPath + "SampleEvent" + "_done" + CellDir))
					return;
			}
			hftp.readHdfsDirToftp(hdfsGRIDDir, ftpOutPath+ "SampleEvent2" + CellDir + modelTable , "",ftp);
		}
		log.info("Success GetMRSceneCELLGridFromHdfs :" + cal.getDateStr8());
		System.out.println("Success GetMRSceneCELLGridFromHdfs :" + cal.getDateStr8());
	}
	
	
	public void downloadHDFS(CalendarEx curTime ,HadoopFSOperations hftp){
		
		
		System.out.println("Deal Bcp:" + curTime.getDateStr8());
		String eventDir = "/TB_CQTSIGNAL_EVENT_01_" + curTime.getDateStr8().substring(2);
		String sampleDir = "/TB_CQTSIGNAL_SAMPLE_01_" + curTime.getDateStr8().substring(2);
		String fgSampleDir = "/TB_FGCQTSIGNAL_SAMPLE_01_" + curTime.getDateStr8().substring(2);
												
		String hdfsEventDir  = HdfsDataPath +"/mroxdrmerge/xdr_loc/data_01_" + curTime.getDateStr8().substring(2) + eventDir;
		String hdfsSampleDir = HdfsDataPath +"/mroxdrmerge/mro_loc/data_01_" + curTime.getDateStr8().substring(2) + sampleDir;
		String hdfsFgSampleDir = HdfsDataPath +"/mroxdrmerge/mro_loc/data_01_" + curTime.getDateStr8().substring(2) + fgSampleDir;
		

		if(hdfs.checkFileExist(hdfsEventDir)
				|| hdfs.checkFileExist(hdfsSampleDir)
				|| hdfs.checkFileExist(hdfsFgSampleDir))
		{	
			if (ReturnConfig.returnconfig("conf/config.xml", "//comm//DEAL_BCP_SAMPLE").equals("1")
				||ReturnConfig.returnconfig("conf/config.xml", "//comm//DEAL_BCP_SAMPLE").equals("") ) 			
			{
				//sample
				GetEventinfoFromHdfs(curTime, true, hftp);				
				GetSampleinfoFromHdfs(curTime, true,"",hftp);
				
			}	
			if (ReturnConfig.returnconfig("conf/config.xml", "//comm//DEAL_BCP_FGSAMPLE").equals("1")) 
			{
			    GetSampleinfoFromHdfs(curTime, true,"FG", hftp);
			}
			if (ReturnConfig.returnconfig("conf/config.xml", "//comm//DEAL_BCP_STAT").equals("1")
					||ReturnConfig.returnconfig("conf/config.xml", "//comm//DEAL_BCP_STAT").equals("")) 
			{//stat							
				//空字符串默认是40米栅格  "10"表示10米栅格			
				GetGridinfo(curTime, true,"FG","",hftp);
				GetGridinfo(curTime, true,"FG","10", hftp);
				GetGridinfo(curTime, true,"","", hftp);
				GetGridinfo(curTime, true,"","10",hftp);
							
				GetCellinfo(curTime, true,"FG", hftp);
				GetCellinfo(curTime, true, "",  hftp);
				
				
				GetCellgridInfo(curTime,true, "FG", "", hftp);
				GetCellgridInfo(curTime,true, "FG", "10", hftp);
				GetCellgridInfo(curTime, true,"", "",  hftp);
				GetCellgridInfo(curTime,true, "", "10",  hftp);
				
				GetFreqinfo(curTime,true,  "FG", "", hftp);				
				GetFreqinfo(curTime,true, "FG", "10",  hftp);				
				GetFreqinfo(curTime, true , "", "", hftp);
				GetFreqinfo(curTime, true, "", "10",  hftp);
							
				
				GetFreqCellInfo(curTime,true, "FG",  hftp);
				GetFreqCellInfo(curTime,true, "",  hftp);
				
				//gridbyimei
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "", "DT" ,"", "LT" ,hftp);
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "", "DT" ,"10", "LT" ,hftp);
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "", "CQT" ,"", "LT" ,hftp);
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "", "CQT" ,"10", "LT" ,hftp);
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "FG", "DT" ,"", "LT" ,hftp);
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "FG", "DT" ,"10", "LT" ,hftp);
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "FG", "CQT" ,"", "LT" ,hftp);
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "FG", "CQT" ,"10", "LT" ,hftp);
				
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "", "DT" ,"10", "DX" ,hftp);
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "", "CQT" ,"10", "DX" ,hftp);
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "FG", "CQT" ,"10", "DX" ,hftp);
				GetFreqGridByimeiInfoFromHdfs(curTime, true , "FG", "CQT" ,"10", "DX" ,hftp);
				
				//newcellgrid
				GetCellgridInfoFromHdfs(curTime, true , "", "DT" ,"" ,hftp);
				GetCellgridInfoFromHdfs(curTime, true , "", "DT" ,"10" ,hftp);
				GetCellgridInfoFromHdfs(curTime, true , "", "CQT" ,"" ,hftp);
				GetCellgridInfoFromHdfs(curTime, true , "", "CQT" ,"10" ,hftp);
				GetCellgridInfoFromHdfs(curTime, true , "FG", "DT" ,"" ,hftp);
				GetCellgridInfoFromHdfs(curTime, true , "FG", "DT" ,"10" ,hftp);
				GetCellgridInfoFromHdfs(curTime, true , "FG", "CQT" ,"" ,hftp);
				GetCellgridInfoFromHdfs(curTime, true , "FG", "CQT" ,"10" ,hftp);
				
				//cellbyimei
				GetFreqCellByimeiInfoFromHdfs(curTime, true , "" ,"10", "LT" ,hftp);
				GetFreqCellByimeiInfoFromHdfs(curTime, true , "" ,"10", "DX" ,hftp);
				GetFreqCellByimeiInfoFromHdfs(curTime, true , "FG" ,"10", "LT" ,hftp);
				GetFreqCellByimeiInfoFromHdfs(curTime, true , "FG" ,"10", "DX" ,hftp);
				
				//mdt 
				GetMDTCellinfoFromHdfs(curTime, true,"MDT" , hftp);
				GetMDTGridinfoFromHdfs(curTime, true,"MDT","" ,hftp);
				GetMDTGridinfoFromHdfs(curTime, true,"MDT","10" ,hftp);
				GetMDTCellgridInfoFromHdfs(curTime, true,"MDT","" ,hftp);
				GetMDTCellgridInfoFromHdfs(curTime, true,"MDT","10" ,hftp);
				GetMDTSampleinfoFromHdfs(curTime, true,"MDT" ,hftp);
				
			}
			
	/*		GetUserinfo(curTime, true, hftp);
			GetHourUser(curTime, true, hftp);
			
			//=======================MR文件下载===============//
			GetMRBuildFromHdfs(curTime,true,"","^TB_MR_BUILD",hftp);
			GetMRBuildFromHdfs(curTime,true,"CELL_NC_","^TB_MR_BUILD_CELL_NC",hftp);
			GetMRBuildFromHdfs(curTime,true,"CELL_","^TB_MR_BUILD_CELL",hftp);
			
			GetMRCellFromHdfs(curTime,true,hftp);		
			GetMRIngridFromHdfs(curTime,true,"","^TB_MODEL_MR_INGRID" ,"ingrid" ,hftp);			
			GetMRIngridFromHdfs(curTime,true,"CELLPARE_","^TB_MODEL_MR_INGRID_CELLPARE","ingridcellpare" ,hftp);			
			GetMRIngridFromHdfs(curTime,true,"CELL_","^TB_MODEL_MR_INGRID","ingridcell" ,hftp);
			
			
			GetMROutGridFromHdfs(curTime,true,"","TB_MODEL_MR_OUTGRID","outgrid",hftp);
			GetMROutGridFromHdfs(curTime,true,"CELLPARE_","TB_MODEL_MR_OUTGRID_CELLPARE","outgridcellpare",hftp);
			GetMROutGridFromHdfs(curTime,true,"CELL_","TB_MODEL_MR_OUTGRID_CELL","outgridcell",hftp);
			
			
			GetMRGridFromHdfs(curTime,true,"0TT_","","OUT","^TB_MODEL_MR_OTT_OUT_GRID","outgrid" ,hftp);
			GetMRGridFromHdfs(curTime,true,"GPS_","","OUT","^TB_MODEL_MR_GPS_OUT_GRID","outgrid" ,hftp);
			GetMRGridFromHdfs(curTime,true,"FG_","","OUT","^TB_MODEL_MR_FG_OUT_GRID","outgrid" ,hftp);
			GetMRGridFromHdfs(curTime,true,"OTT_","","IN","^TB_MODEL_MR_OTT_IN_GRID","ingrid" ,hftp);
			GetMRGridFromHdfs(curTime,true,"FG_","","IN","^TB_MODEL_MR_FG_IN_GRID","ingrid" ,hftp);
			
			GetMRGridFromHdfs(curTime,true,"OTT_","DX_","OUT","^TB_MODEL_MR_DX_OTT_OUT_GRID","outgrid" ,hftp);
			GetMRGridFromHdfs(curTime,true,"GPS_","DX_","OUT","^TB_MODEL_MR_DX_GPS_OUT_GRID","outgrid" ,hftp);
			GetMRGridFromHdfs(curTime,true,"FG_","DX_","OUT","^TB_MODEL_MR_DX_FG_OUT_GRID","outgrid" ,hftp);
			GetMRGridFromHdfs(curTime,true,"OTT_","DX_","IN","^TB_MODEL_MR_DX_OTT_IN_GRID","ingrid" ,hftp);
			GetMRGridFromHdfs(curTime,true,"FG_","DX_","IN","^TB_MODEL_MR_DX_FG_IN_GRID","ingrid" ,hftp);
			GetMRGridFromHdfs(curTime,true,"OTT_","LT_","OUT","^TB_MODEL_MR_LT_OTT_OUT_GRID","outgrid" ,hftp);
			GetMRGridFromHdfs(curTime,true,"GPS_","LT_","OUT","^TB_MODEL_MR_LT_GPS_OUT_GRID","outgrid" ,hftp);
			GetMRGridFromHdfs(curTime,true,"FG_","LT_","OUT","^TB_MODEL_MR_LT_FG_OUT_GRID","outgrid" ,hftp);
			GetMRGridFromHdfs(curTime,true,"OTT_","LT_","IN","^TB_MODEL_MR_LT_OTT_IN_GRID","ingrid" ,hftp);
			GetMRGridFromHdfs(curTime,true,"FG_","LT_","IN","^TB_MODEL_MR_LT_FG_IN_GRID","ingrid" ,hftp);
			
			GetMRBuildFromHdfs(curTime,true,"OTT","","^TB_MODEL_MR_OTT_BUILD","build" ,hftp);
			GetMRBuildFromHdfs(curTime,true,"FG","","^TB_MODEL_MR_FG_BUILD","build" ,hftp);
			GetMRBuildFromHdfs(curTime,true,"OTT","DX_","^TB_MODEL_MR_DX_OTT_BUILD","build" ,hftp);
			GetMRBuildFromHdfs(curTime,true,"FG","DX_","^TB_MODEL_MR_DX_FG_BUILD","build" ,hftp);
			GetMRBuildFromHdfs(curTime,true,"OTT","LT_","^TB_MODEL_MR_LT_OTT_BUILD","build" ,hftp);
			GetMRBuildFromHdfs(curTime,true,"FG","LT_","^TB_MODEL_MR_LT_FG_BUILD","build" ,hftp);
			
			GetMRCELLFromHdfs(curTime,true,"^TB_MODEL_MR_CELL","cell" ,hftp );
			GetMRCELLFromHdfs(curTime,true,"DX","^TB_MODEL_MR_DX_CELL","cell" ,hftp);
			GetMRCELLFromHdfs(curTime,true,"LT","^TB_MODEL_MR_LT_CELL","cell" ,hftp);
			
			GetMRCELLGridFromHdfs(curTime,true,"","OTT_","OUT","^TB_MODEL_MR_OTT_OUT_CELLGRID","outcellgrid" ,hftp);
			GetMRCELLGridFromHdfs(curTime,true,"","GPS_","OUT","^TB_MODEL_MR_GPS_OUT_CELLGRID","outcellgrid" ,hftp);
			GetMRCELLGridFromHdfs(curTime,true,"","FG_","OUT","^TB_MODEL_MR_FG_OUT_CELLGRID","outcellgrid",hftp);
			GetMRCELLGridFromHdfs(curTime,true,"","OTT_","IN","^TB_MODEL_MR_OTT_IN_CELLGRID","incellgrid" ,hftp);
			GetMRCELLGridFromHdfs(curTime,true,"","FG_","IN","^TB_MODEL_MR_FG_IN_CELLGRID","incellgrid" ,hftp);
			
			//gaotie
			GetMRSceneCELLFromHdfs(curTime,true, "SCENE" ,"^TB_MODEL_MR_SCENE_CELL", "cell" ,hftp);
			GetMRSceneGridFromHdfs(curTime,true, "SCENE" ,"^TB_MODEL_MR_SCENE_GRID", "grid" ,hftp);
			GetMRSceneCELLGridFromHdfs(curTime,true, "SCENE" ,"^TB_MODEL_MR_SCENE_CELLGRID", "cellgrid" ,hftp);
			GetMRSceneFromHdfs(curTime,true, "SCENE" ,"^TB_MODEL_MR_SCENE", "scene" ,hftp); */
		}
		
	}
	

	@Override
	public void run()
	{			
		ftp.setBinaryTransfer(true);
	    ftp.setPassiveMode(true);
	    ftp.setEncoding("utf-8");	
		
		while (!MainSvr.bExitFlag)
		{	
			int i = 0 ;
			try
			{
				Thread.sleep(1000);
			//	hdfs.ReCoconnect();

				if (FileType == 2)
				{
					CalendarEx curTime = new CalendarEx(new Date());
					CalendarEx beginCal = curTime.AddDays(-CheckBcpDays);
					curTime = curTime.AddDays(-1);
															
					while (beginCal._second < curTime._second)
					{
						downloadHDFS(curTime,hdfs);						
						curTime = curTime.AddDays(-1);
					}
					Thread.sleep(60000);	
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
	public void setDir(){
		String hdfsEventDir  = HdfsDataPath +"/";
	}
}

class ComparatorMapps implements Comparator{  
	  
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
