package cn.mastercom.sssvr.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import cn.mastercom.sssvr.util.FTPClientHelper;
import cn.mastercom.sssvr.util.FTPRuleHelper;
import cn.mastercom.sssvr.util.MyFTPFile;
import cn.mastercom.sssvr.util.SftpClientHelper;
import cn.mastercom.sssvr.util.SftpFile;
import cn.mastercom.sssvr.util.SftpRuleHelper;

@SuppressWarnings("unused")
public class DownloadFtpFile extends Thread {

	protected static final Log LOG = LogFactory.getLog(DownloadFtpFile.class);

	private static int FtpServerPort = 21;
	private static String ValidString = "";
	private static String INValidString = "";
	private static String AutoDeleteZipFiles = "false";
	private static String FtpServerIp = "";
	private static String LocalPath = "";
	private static String RemotePath = "";
	private static int DealDays = 1;
	private static String FtpUserName = "";
	private static String FtpPassword = "";
	private static String Pathrule = "";
	private static String Encode = "";
	private static int DownloadThreadsize = 1;
	private static boolean bExitFlag = false;
	private static String Passmodel = "";
	private static String PathStructure = "";
	private static String SFTP = "";

	private static int m_ThreadCnt = 0;
	private static int m_FileCnt = 0;

	SimpleDateFormat m_dateFormat = new SimpleDateFormat("yyyyMMdd");

	public MainThreadArgs args;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void readConfigInfo() {
		try {
			// XMLWriter writer = null;// 声明写XML的对象
			SAXReader reader = new SAXReader();

			OutputFormat format = OutputFormat.createPrettyPrint();
			format.setEncoding("GBK");// 设置XML文件的编码格式

			String filePath = "conf/config_ftpdownloadfile.xml";
			File file = new File(filePath);
			if (file.exists()) {
				Document doc = reader.read(file);// 读取XML文件

				{
					List<String> list = doc.selectNodes("//comm/FtpServerPort");
					Iterator iter = list.iterator();
					while (iter.hasNext()) {
						Element element = (Element) iter.next();
						FtpServerPort = Integer.parseInt(element.getText());
						break;
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/FtpServerIp");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							FtpServerIp = element.getText();
							break;
						}
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/Passmodel");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							Passmodel = element.getText();
							break;
						}
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/Encode");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							Encode = element.getText();
							break;
						}
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/LocalPath");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							LocalPath = element.getText();
							break;
						}
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/RemotePath");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							RemotePath = element.getText();
							break;
						}
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/DealDays");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							DealDays = Integer.parseInt(element.getText());
							break;
						}
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/FtpUserName");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							FtpUserName = element.getText();
							break;
						}
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/FtpPassword");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							FtpPassword = element.getText();
							break;
						}
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/Pathrule");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							Pathrule = element.getText();
							break;
						}
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/PathStructure");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							PathStructure = element.getText();
							break;
						}
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/DownloadThreadsize");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							DownloadThreadsize = Integer.parseInt(element.getText());
							break;
						}
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/ValidString");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							ValidString = element.getText();
							break;
						}
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/InvalidString");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							INValidString = element.getText();
							break;
						}
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/SFTP");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							SFTP = element.getText();
							break;
						}
					}
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void init() {

		PropertyConfigurator.configure("conf/log4jHE.properties");
		LOG.info(" init download program ...");
		readConfigInfo();
		String[] splitIp = FtpServerIp.split("\\^");
		String[] splitUserName = FtpUserName.split("\\^");
		String[] splitPassword = FtpPassword.split("\\^");
		int nServerNum = splitIp.length;
		if (nServerNum >= 1) {
			for (int i = 0; i < nServerNum; i++) {

				MainThreadArgs args = new MainThreadArgs();
				args.FtpServerIp = splitIp[i];

				if (splitUserName.length == nServerNum) {
					args.UserName = splitUserName[i];
				} else {
					args.UserName = splitUserName[0];
				}

				if (splitPassword.length == nServerNum) {
					args.Password = splitPassword[i];
				} else {
					args.Password = splitPassword[0];
				}

				DownloadFtpFile dff = new DownloadFtpFile();
				dff.args = args;
				dff.start();
			}

		}

	}

	public void run() {
		Date lastScanTime = new Date(Long.MIN_VALUE);
		Date dateNow = new Date();
		while (!bExitFlag) {
			try {
				downLoadDataFile(this.args);
			} catch (Exception e) {
				System.out.println(e.toString());
			}
			try {
				Thread.sleep(1000 * 60);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}
	public SftpClientHelper initSftpClientHelper(SftpClientHelper sftphelp,String FtpServerIp,String UserName,String Password){
		sftphelp = new SftpClientHelper(FtpServerIp, UserName, Password, FtpServerPort, 1000);
		sftphelp.setEncoding(Encode);
		return sftphelp;
	}
	
	public FTPClientHelper initFTPClientHelper(FTPClientHelper ftpClient,String FtpServerIp,String UserName,String Password){
		ftpClient = new FTPClientHelper(FtpServerIp, FtpServerPort, UserName, Password);

		// 设置ftp属性
		ftpClient.setPassiveMode(Boolean.parseBoolean(Passmodel));
		ftpClient.setEncoding(Encode);
		ftpClient.setBinaryTransfer(true);
		return ftpClient;
	}
	
	public void readFile(Calendar calendar,BufferedReader br,Map<String, Long> dicFilemap){
		for (int day = 0; day < DealDays; day++) {
			try {
				String dateFlag = m_dateFormat.format(calendar.getTime());
				File file = new File(args.RecordFileName() + "." + dateFlag + ".txt");
				if (file.exists()) {

					br = new BufferedReader(new FileReader(file.getAbsolutePath()));
					String fname = null;
					while ((fname = br.readLine()) != null) {
						// 本地文件|FTP文件|文件大小
						String[] arrs = fname.split("\\|");
						if (arrs.length == 3) {
							dicFilemap.put(arrs[1], Long.parseLong(arrs[2]));
						}
					}
					br.close();
				}
			} catch (Exception e) {
				if (br != null) {
					try {
						br.close();
					} catch (Exception ex) {
					}
				}
				System.out.println(getDateNow() + "[" + args.FtpServerIp + "] 捕捉到异常" + e.getMessage());
			}finally {
				if (br != null) {
					try {
						br.close();
					} catch (Exception ex) {
					}
				}
			}
			
			calendar.add(Calendar.DAY_OF_YEAR, -1);
		}
	}
	
	public void checkFileExist(List<MyFTPFile> ftpfileLists,List<SftpFile> sftpfileLists){
		if ("true".equals(SFTP)) {
			if (sftpfileLists.size() == 0) {
				System.out.println(getDateNow() + "[" + args.FtpServerIp + "] " + "sftp文件没有得到或者文件不存在");
				try {
					Thread.sleep(60000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				System.out.println(getDateNow() + "[" + args.FtpServerIp + "] sftp获取到文件的个数: " + sftpfileLists.size());
			}
		} else {
			if (ftpfileLists.size() == 0) {
				System.out.println(getDateNow() + "[" + args.FtpServerIp + "] " + "ftp文件没有得到或者文件不存在");
				try {
					Thread.sleep(60000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				System.out.println(getDateNow() + "[" + args.FtpServerIp + "] ftp获取到文件的个数: " + ftpfileLists.size());
			}
		}
	}
	
	public List<MyFTPFile> getFtpList(List<MyFTPFile> ftpfileLists,FTPClientHelper ftpClient){
		FTPRuleHelper frh = new FTPRuleHelper();
		ftpfileLists = new ArrayList<MyFTPFile>();
		if (Pathrule.contains("$TIME")) {
			int days = DealDays;
			if (new Date().getHours() < 6) {
				days++;
			}
			try {
				for (int i = 0; i < days; i++) {
					SimpleDateFormat dft = new SimpleDateFormat("yyyy-MM-dd hhmmss");
					Date beginTime = new Date();
					Calendar date = Calendar.getInstance();
					date.setTime(beginTime);
					date.set(Calendar.DATE, date.get(Calendar.DATE) - i);
					Date endDate = dft.parse(dft.format(date.getTime()));
					List<MyFTPFile> tempList = frh.ListFiles(ftpClient, Pathrule, endDate);
					for (MyFTPFile file : tempList) {
						ftpfileLists.add(file);
					}
					tempList.clear();
				}
			} catch (Exception e2) {
				System.out.println(getDateNow() + "[" + args.FtpServerIp + "] " + "ftpfileLists获取异常");
			}
			System.out.println(getDateNow() + "[" + args.FtpServerIp + "] " + "loop end.....");
		} else {
			try {
				Date beginTime = new Date();
				List<MyFTPFile> tempList = frh.ListFiles(ftpClient, Pathrule, beginTime);
				for (MyFTPFile file : tempList) {
					ftpfileLists.add(file);
				}
				tempList.clear();
			} catch (Exception e2) {
				System.out.println(getDateNow() + "[" + args.FtpServerIp + "] " + "ftpfileLists获取异常");
			}
			System.out.println(getDateNow() + "[" + args.FtpServerIp + "] " + "loop end....");
		}	
		return ftpfileLists;
	}
	
	public List<SftpFile> getSftpList(List<SftpFile> sftpfileLists,SftpClientHelper sftphelp){
		SftpRuleHelper srh = new SftpRuleHelper();
		sftpfileLists = new ArrayList<SftpFile>();
		if (Pathrule.contains("$TIME")) {
			int days = DealDays;
			if (new Date().getHours() < 6) {
				days++;
			}
			try {
				for (int i = 0; i < days; i++) {
					SimpleDateFormat dft = new SimpleDateFormat("yyyy-MM-dd hhmmss");
					Date beginTime = new Date();
					Calendar date = Calendar.getInstance();
					date.setTime(beginTime);
					date.set(Calendar.DATE, date.get(Calendar.DATE) - i);
					Date endDate = dft.parse(dft.format(date.getTime()));
					List<SftpFile> tempList = srh.ListFiles(sftphelp, Pathrule, endDate);
					System.out.println(
							getDateNow() + "[" + args.FtpServerIp + "] " + "sftptempList:" + tempList.size());
					for (SftpFile file : tempList) {
						sftpfileLists.add(file);
					}
					tempList.clear();
				}
			} catch (Exception e2) {
				System.out.println(getDateNow() + "[" + args.FtpServerIp + "] " + "sftpfileLists获取异常");
			}
			System.out.println(getDateNow() + "[" + args.FtpServerIp + "] " + "sftp loop end....");
		} else {
			try {
				Date beginTime = new Date();
				List<SftpFile> tempList = srh.ListFiles(sftphelp, Pathrule, beginTime);
				for (SftpFile file : tempList) {
					sftpfileLists.add(file);
				}
				tempList.clear();
			} catch (Exception e2) {
				System.out.println(getDateNow() + "[" + args.FtpServerIp + "] " + "sftpfileLists获取异常");
			}
			System.out.println(getDateNow() + "[" + args.FtpServerIp + "] " + "sftp loop end....");
		}
		return sftpfileLists;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void downLoadDataFile(MainThreadArgs args) {
		Map<String, Long> dicFilemap = new HashMap<String, Long>();
		BufferedReader br = null;
		FTPClientHelper ftpClient = null;
		SftpClientHelper sftphelp = null;
	
		Date end_Date = new Date();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(end_Date);		
		readFile(calendar, br, dicFilemap);		
		calendar = null;
		System.out.println(
				getDateNow() + "[" + args.FtpServerIp + "] DownFileList.text已经有:" + dicFilemap.size() + "个文件。");
		
		// 添加处理sftp
		if ("true".equals(SFTP)) {
			sftphelp = initSftpClientHelper(sftphelp, args.FtpServerIp, args.UserName, args.Password);
		} else {
			ftpClient = initFTPClientHelper(ftpClient,args.FtpServerIp, args.UserName, args.Password);
		}

		/**
		 * 得到ftp目录下文件 传入路径规则
		 * 
		 */
		List<MyFTPFile> ftpfileLists = null;
		List<SftpFile> sftpfileLists = null;
		
		if ("true".equals(SFTP)) {
			sftpfileLists = getSftpList(sftpfileLists, sftphelp);			
		} else {
			ftpfileLists = getFtpList(ftpfileLists,ftpClient);			
		}
		checkFileExist(ftpfileLists, sftpfileLists);		
		ArrayList<File> fileList = new ArrayList<File>();
		// 测试代码
		fileList = changeList(fileList,ftpfileLists,sftpfileLists);		
		/**
		 * 对文件目录先排序，在分组 
		 */
		groupOneFile(fileList, ftpClient, sftphelp, dicFilemap, end_Date);			
	}
	
	public void groupOneFile(ArrayList<File> fileList,FTPClientHelper ftpClient,SftpClientHelper sftphelp,Map<String, Long> dicFilemap
			,Date end_Date){
		if (fileList.size() > 0) {
			int nNum = 0;
			System.out.println(getDateNow() + "[" + args.FtpServerIp + "] 数据开始排序......");
			Collections.sort(fileList, new CompratorByLastModified());
			System.out.println(getDateNow() + "[" + args.FtpServerIp + "] 排序完成......");
			int nMaxThreadNum = 6;
			if (DownloadThreadsize > 0 && DownloadThreadsize < 10) {
				nMaxThreadNum = DownloadThreadsize;
			}
			boolean successFlag = false;
			List<TaskInfo> taskList = new ArrayList<TaskInfo>();
			ArrayList<String> sbsoluteList = new ArrayList<String>();
			ArrayList<String> downFileList = new ArrayList<String>();
			int count = 0;
			long nTotalLen = 0;
			// 分组
			for (File file : fileList) {
				String path = file.getPath();
				int filesize = 0;
				if ("true".equals(SFTP)) {
					filesize = (int) sftphelp.getFilesize(path);
				} else {
					filesize = (int) ftpClient.getFilesize(path);

				}
				if (dicFilemap.containsKey(path) && dicFilemap.get(path) == filesize) {
					continue;
				}
				
				if(filterINValidFile(path))
					continue;
							
				if(filterValidFile(path))
					continue;

				nNum++;
				nTotalLen += 1;
				String localPath = LocalPath + "/" + file.getName();
				String togetherPath = localPath + "|" + path + "|" + filesize;
				downFileList.add(togetherPath);
				sbsoluteList.add(path);
				
				groupFile(nTotalLen,downFileList,sbsoluteList,taskList,count);
			}
			
			lastFile(nTotalLen,downFileList,sbsoluteList,taskList);
			
			
			if (dicFilemap.size() + nNum >= fileList.size()) {
				successFlag = true;
			}
			
			fileList.clear();
			dicFilemap.clear();
			
			System.out.println(getDateNow() + "[" + args.FtpServerIp + "] 待处理的文件个数：" + nNum);
			if (nNum == 0) {
				try {
					Thread.sleep(60000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			disConnect(ftpClient,sftphelp);		
			
			mkdirThreadPool(nMaxThreadNum,taskList,end_Date,successFlag);
		}
	}
	
	
	public void lastFile(long nTotalLen,ArrayList<String> downFileList,ArrayList<String> sbsoluteList,List<TaskInfo> taskList){
		if (nTotalLen > 0) {
			TaskInfo ti = new TaskInfo();
			ti.args = args;
			ti.downFileList = downFileList;
			ti.outpath = LocalPath;
			ti.fileLst = sbsoluteList;
			taskList.add(ti);
		}
	}
	
	public void disConnect(FTPClientHelper ftpClient,SftpClientHelper sftphelp){
		try {
			if ("true".equals(SFTP)) {
				sftphelp.disconnect();
			} else {
				ftpClient.disconnect();
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}	
	}
	
	@SuppressWarnings("unchecked")
	public void groupFile(long nTotalLen,ArrayList<String> downFileList,ArrayList<String> sbsoluteList,List<TaskInfo> taskList
			,int count){
		if (nTotalLen > 50) {
			TaskInfo ti = new TaskInfo();
			ArrayList tempArys = new ArrayList<String>();
			for (int i = 0; i < downFileList.size(); i++) {
				tempArys.add(downFileList.get(i));
			}
			ti.downFileList = tempArys;
			ti.args = args;

			ArrayList tempArys1 = new ArrayList<String>();
			for (int i = 0; i < sbsoluteList.size(); i++) {
				tempArys1.add(sbsoluteList.get(i));
			}
			ti.fileLst = tempArys1;
			ti.outpath = LocalPath;
			taskList.add(ti);
			count++;
			nTotalLen = 0;
			downFileList.clear();
			sbsoluteList.clear();
		}
	}
	
	public ArrayList<File> changeList(ArrayList<File> fileList,List<MyFTPFile> ftpfileLists , List<SftpFile> sftpfileLists){
		if ("true".equals(SFTP)) {
			try {
				for (SftpFile sftpFile : sftpfileLists) {
					File file = new File(sftpFile.GetFullPath());
					fileList.add(file);
				}
				sftpfileLists.clear();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} else {
			try {
				for (MyFTPFile ftpFile : ftpfileLists) {
					File file = new File(ftpFile.GetFullPath());
					fileList.add(file);
				}
				ftpfileLists.clear();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		return fileList;
	}
	
	@SuppressWarnings("rawtypes")
	public void mkdirThreadPool(int nMaxThreadNum,List<TaskInfo> taskList,Date end_Date,boolean successFlag){
		ExecutorService pool = Executors.newFixedThreadPool(nMaxThreadNum);

		List<Future> futureList = new ArrayList<Future>();
		for (TaskInfo taskinfo : taskList) {
			Callable fm = new FileDownloadCallable(taskinfo, end_Date);
			// 执行任务并获取Future对象
			Future f = pool.submit(fm);
			futureList.add(f);
		}
		taskList.clear();

		int failureNum = 0;
		// 获取所有并发任务的运行结果
		try {
			for (Future f : futureList) {
				System.out.println(f.get().toString());
				Object obj = f.get();
				if (obj.toString().contains("Error") || obj.toString().contains("下载失败")) {
					failureNum++;
				}
			}
			futureList.clear();
			// 关闭线程池
			pool.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(getDateNow() + "[" + args.FtpServerIp + "] 下载失败个数:" + failureNum);
		if (!successFlag || failureNum > 0) {
			downLoadDataFile(args);
		} else {
			System.out.println(getDateNow() + "[" + args.FtpServerIp + "] 数据下载完成！");
		}
	}
	
	public boolean filterINValidFile(String path){
		if (INValidString.trim().length() > 0) {
			String[] split = INValidString.trim().split(" ");
			boolean bFind = false;
			for (String str : split) {
				if (path.toLowerCase().contains(str.toLowerCase())) {
					bFind = true;
					break;
				}
			}
			if (bFind) {
				return true;
			}
		}
		return false;
	}
	
	public boolean filterValidFile(String path){
		if (ValidString.trim().length() > 0) {
			String[] split = ValidString.trim().split(" ");
			boolean bFind = false;
			for (String str : split) {
				if (path.toLowerCase().contains(str.toLowerCase())) {
					bFind = true;
					break;
				}
			}
			if (!bFind) {
				return true;
			}
		}
		return false;
	}

	public static void main(String[] args) {
		DownloadFtpFile.init();
	}

	public static class MainThreadArgs {
		public String FtpServerIp = "";
		public String UserName = "";
		public String Password = "";

		public String RecordFileName() {
			File file = new File(this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
			return file.getParent() + System.getProperty("file.separator") + "DownFileList."
					+ FtpServerIp.replace(".", "_");
		}

	}

	// 检测剩余空间
	public long checkDiskFreeSpace(String diskPath) {
		File diskPartition = new File(diskPath);
		long freeSpace = (diskPartition.getFreeSpace()) / (1024 * 1024); // MB
		return freeSpace;
	}

	public String getDateNow() {
		Date date = new Date();
		SimpleDateFormat timeFormat = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss] ");
		String formatDate = timeFormat.format(date);
		return formatDate;
	}

	public void createDirectory(String path) {
		if (StringUtils.isEmpty(path)) {
			return;
		}
		try {
			File f = new File(path);
			if (!f.exists()) {
				f.mkdirs();
			}
		} catch (Exception e) {
			System.out.println("目录创建失败.....");
		}
	}

	class TaskInfo {
		public List<String> fileLst = new ArrayList<String>();// 待处理文件完整路径组成的list
		public List<String> downFileList = new ArrayList<String>();
		public MainThreadArgs args;
		public String outpath;
	}

	// 根据文件修改时间进行比较的内部类
	class CompratorByLastModified implements Comparator<File> {

		public int compare(File f1, File f2) {
			long diff = 0;
			diff = f1.lastModified() - f2.lastModified();
			if (diff > 0) {
				return 1;
			} else if (diff == 0) {
				return 0;
			} else {
				return -1;
			}
		}
	}

	static ReentrantLock m_ReentrantLock = new ReentrantLock(true);

	class FileDownloadCallable implements Callable<Object> {
		private TaskInfo task;

		List<Date> Dates = new ArrayList<Date>();

		FTPClientHelper ftpClient = null;
		SftpClientHelper sftphelp = null;
		String diskpath = null;
		String logFileName = null;

		FileDownloadCallable(TaskInfo taskInfo, Date end_Date) {
			this.task = taskInfo;

			Calendar cals = Calendar.getInstance();
			cals.setTime(end_Date);
			
			for (int i = 0; i < DealDays; i++) {
				Dates.add(cals.getTime());
				cals.add(Calendar.DAY_OF_YEAR, -1);
			}

			diskpath = task.outpath;
			File file = new File(this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
			logFileName = file.getParent() + System.getProperty("file.separator") + "DownFileList."
					+ task.args.FtpServerIp.replace(".", "_") + ".";
		}

		@Override
		public Object call() {
			try {
				m_ReentrantLock.lock();
				m_ThreadCnt++;
				m_ReentrantLock.unlock();

				return work();
			} catch (Exception e) {
				System.out.println(getDateNow() + "[" + task.args.FtpServerIp + "]" + "线程错误！");
				return getDateNow() + "[" + task.args.FtpServerIp + "] " + "失败！";
			} finally {
				System.out.println(getDateNow() + "[" + task.args.FtpServerIp + "] " + "ThreadCnt:" + m_ThreadCnt);
				m_ReentrantLock.lock();
				m_ThreadCnt--;
				m_ReentrantLock.unlock();
			}

		}

		private void workOne(String ftpFilename, String downloadListInfo) throws Exception {
			FileOutputStream os = null;
			m_ReentrantLock.lock();
			m_FileCnt++;
			m_ReentrantLock.unlock();
			boolean ok = false;

			try {
				File file = new File(ftpFilename);
				// 保持目录结构
				int length = RemotePath.length();
				String path = task.outpath;
				if (Boolean.parseBoolean(PathStructure)) {
					path = path.replace("\\", System.getProperty("file.separator")).replace("/",
							System.getProperty("file.separator")) + file.getParent().substring(length + 1);
				}

				createDirectory(path);
				System.out.println(getDateNow() + "[" + task.args.FtpServerIp + "] 已打开文件数：" + m_FileCnt + " 开始下载:"
						+ file.getName() + " >> " + path);

				os = new FileOutputStream(path + System.getProperty("file.separator") + file.getName() + "");
				ok = true;

				boolean flag = false;

				if ("true".equals(SFTP)) {
					sftphelp.get(ftpFilename, os);
					os.close();
					System.out.println(getDateNow() + "[" + task.args.FtpServerIp + "] SFTP " + ftpFilename + "下载完毕！");

				} else {
					ftpClient.get(ftpFilename, os);
					os.close();
					System.out.println(getDateNow() + "[" + task.args.FtpServerIp + "] FTP " + ftpFilename + " 下载完毕！");
				}

				os.close();
				ok = false;
				// 写成功标志文件。
				writeLog(path, file, downloadListInfo);

			} catch (Exception e) {

				if (os != null) {
					os.close();

					// 如果之前没关闭，则关闭
					if (ok) {
						m_ReentrantLock.lock();
						m_FileCnt--;
						m_ReentrantLock.unlock();
					}

					try {
						Thread.sleep(500);
					} catch (Exception ex) {

					}
					throw e;
				}
			} finally {

				if (!ok) {
					m_ReentrantLock.lock();
					m_FileCnt--;
					m_ReentrantLock.unlock();
				}

			}

		}

		private String getLogPath(File file, String path) {
			String fileName = file.getName();

			for (Date date : Dates) {
				String str = m_dateFormat.format(date);
				if (fileName.indexOf(str) > 0) {
					return logFileName + str + ".txt";
				}
			}

			return null;
		}

		private void writeLog(String path, File file, String downloadListInfo) {
			String fileLog = getLogPath(file, path);
			if (fileLog == null)
				return;

			OutputStreamWriter out = null;
			BufferedWriter bufferedWriter = null;
			try {
				out = new OutputStreamWriter(new FileOutputStream(fileLog, true));
				try {
					bufferedWriter = new BufferedWriter(out);
					bufferedWriter.write(downloadListInfo + "\r\n");
					bufferedWriter.flush();
				} finally {
					try {
						if (bufferedWriter != null)
							bufferedWriter.close();
					} catch (Exception e) {

					}
				}

			} catch (Exception e) {
				System.out.println(getDateNow() + "[" + task.args.FtpServerIp + "] 写日志出错！");
			} finally {
				try {
					if (out != null)
						out.close();
				} catch (Exception e) {

				}

			}
		}

		private Object work() throws Exception {

			if ("true".equals(SFTP)) {
				System.out.println(getDateNow() + "[" + task.args.FtpServerIp + "] download SFTP file");
				sftphelp = initSftpClientHelper(sftphelp,task.args.FtpServerIp, task.args.UserName, task.args.Password);
			} else {
				System.out.println(getDateNow() + "[" + task.args.FtpServerIp + "] download FTP file");
				ftpClient = initFTPClientHelper(ftpClient, task.args.FtpServerIp, task.args.UserName, task.args.Password);
			}

			if (diskpath.contains(":")) {
				diskpath = task.outpath.substring(0, 2);
			}

			if (checkDiskFreeSpace(diskpath) < 1) { // 空间小于1MB
				System.out.println(
						getDateNow() + "[" + task.args.FtpServerIp + "] " + "剩余空间大小：" + checkDiskFreeSpace(diskpath));
				System.out.println(getDateNow() + "[" + task.args.FtpServerIp + "] " + " 空间不足。。。。等待一分钟。。。");
				Thread.sleep(60000);
			}

			for (int i = 0; i < task.fileLst.size(); i++) {
				try {
					workOne(task.fileLst.get(i), task.downFileList.get(i));
				} catch (Exception e) {
					System.out.println(getDateNow() + "[" + task.args.FtpServerIp + "] " + e.toString());
				}
			}
			task.fileLst.clear();
			task.downFileList.clear();

			if (ftpClient != null)
				ftpClient.disconnect();
			if (sftphelp != null)
				sftphelp.disconnect();

			return getDateNow() + "[" + task.args.FtpServerIp + "]" + " Task Exec Success";
		}
	}

}
