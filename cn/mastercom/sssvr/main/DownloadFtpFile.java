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
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.I;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

import cn.mastercom.sssvr.util.CalendarEx;
import cn.mastercom.sssvr.util.FTPClientHelper;
import cn.mastercom.sssvr.util.FTPRuleHelper;
import cn.mastercom.sssvr.util.MyFTPFile;
import cn.mastercom.sssvr.util.ReturnConfig;
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
				e.getMessage();
			}
		}

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void downLoadDataFile(MainThreadArgs args) {
		Map<String, Long> dicFilemap = new HashMap<String, Long>();
		BufferedReader br = null;
		CalendarEx addDays = null;
		CalendarEx cals = null;

		for (int day = 0; day < DealDays; day++) {
			try {
				cals = new CalendarEx();
				addDays = cals.AddDays(-day);
				if (!new File(args.RecordFileName() + "." + addDays.getDateStr8() + ".txt").exists()) {
					continue;
				}
				br = new BufferedReader(new FileReader(args.RecordFileName() + "." + addDays.getDateStr8() + ".txt"));
				String fname = null;
				while ((fname = br.readLine()) != null) {
					// 本地文件|FTP文件|文件大小
					String[] arrs = fname.split("\\|");
					if (arrs.length == 3) {
						dicFilemap.put(arrs[1], Long.parseLong(arrs[2]));
					}
				}
				br.close();
			} catch (Exception e) {
				System.out.println(getDateNow() + "[" + args.FtpServerIp + "] 捕捉到异常" + e.getMessage());
			}

		}
		System.out.println(
				getDateNow() + "[" + args.FtpServerIp + "] DownFileList.text已经有:" + dicFilemap.size() + "个文件。");

		FTPClientHelper ftpClient = null;
		SftpClientHelper sftphelp = null;
		// 添加处理sftp
		if (ReturnConfig.returnconfig("conf/config_ftpdownloadfile.xml", "//comm//SFTP").equals("true")) {
			sftphelp = new SftpClientHelper(args.FtpServerIp, args.UserName, args.Password, 22, 1000);
			sftphelp.setEncoding(Encode);
		} else {
			ftpClient = new FTPClientHelper(args.FtpServerIp, FtpServerPort, args.UserName, args.Password);

			// 设置ftp属性
			ftpClient.setPassiveMode(Boolean.parseBoolean(Passmodel));
			ftpClient.setEncoding(Encode);
			ftpClient.setBinaryTransfer(true);
		}

		/**
		 * 得到ftp目录下文件 传入路径规则
		 * 
		 */
		List<MyFTPFile> ftpfileLists = null;
		List<SftpFile> sftpfileLists = null;
		if (ReturnConfig.returnconfig("conf/config_ftpdownloadfile.xml", "//comm//SFTP").equals("true")) {
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
						System.out
								.println(getDateNow() + "[" + args.FtpServerIp + "] " + "tempList1:" + tempList.size());
						for (SftpFile file : tempList) {
							sftpfileLists.add(file);
						}
					}
				} catch (Exception e2) {
					System.out.println(getDateNow() + "[" + args.FtpServerIp + "] " + "sftpfileLists获取异常");
				}
				System.out.println(getDateNow() + "[" + args.FtpServerIp + "] " + "loop end....");
			} else {
				try {
					Date beginTime = new Date();
					List<SftpFile> tempList = srh.ListFiles(sftphelp, Pathrule, beginTime);
					for (SftpFile file : tempList) {
						sftpfileLists.add(file);
					}
				} catch (Exception e2) {
					System.out.println(getDateNow() + "[" + args.FtpServerIp + "] " + "sftpfileLists获取异常");
				}
				System.out.println(getDateNow() + "[" + args.FtpServerIp + "] " + "loop end....");
			}
		} else {
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
				} catch (Exception e2) {
					System.out.println(getDateNow() + "[" + args.FtpServerIp + "] " + "ftpfileLists获取异常");
				}
				System.out.println(getDateNow() + "[" + args.FtpServerIp + "] " + "loop end....");
			}
		}

		if (ReturnConfig.returnconfig("conf/config_ftpdownloadfile.xml", "//comm//SFTP").equals("true")) {
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

		ArrayList<File> fileList = new ArrayList<File>();
		// 测试代码
		if (ReturnConfig.returnconfig("conf/config_ftpdownloadfile.xml", "//comm//SFTP").equals("true")) {
			try {
				for (SftpFile sftpFile : sftpfileLists) {
					File file = new File(sftpFile.GetFullPath());
					fileList.add(file);
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} else {
			try {
				for (MyFTPFile ftpFile : ftpfileLists) {
					File file = new File(ftpFile.GetFullPath());
					fileList.add(file);
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		/**
		 * 对文件目录先排序，在分组
		 * 
		 */

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
				if (ReturnConfig.returnconfig("conf/config_ftpdownloadfile.xml", "//comm//SFTP").equals("true")) {
					filesize = (int) sftphelp.getFilesize(path);
				} else {
					filesize = (int) ftpClient.getFilesize(path);

				}
				if (dicFilemap.containsKey(path) && dicFilemap.get(path) == filesize) {
					continue;
				}

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
						continue;
					}
				}

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
						continue;
					}
				}

				nNum++;
				nTotalLen += 1;
				String localPath = LocalPath + "/" + file.getName();
				String togetherPath = localPath + "|" + path + "|" + filesize;
				downFileList.add(togetherPath);
				sbsoluteList.add(path);
				if (nTotalLen > 100) {
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

			if (nTotalLen > 0) {
				TaskInfo ti = new TaskInfo();
				ti.args = args;
				ti.downFileList = downFileList;
				ti.outpath = LocalPath;
				ti.fileLst = sbsoluteList;
				taskList.add(ti);
			}
			if (dicFilemap.size() + nNum >= fileList.size()) {
				successFlag = true;
			}
			System.out.println(getDateNow() + "[" + args.FtpServerIp + "] 待处理的文件个数：" + nNum);
			if (nNum == 0) {
				try {
					Thread.sleep(60000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			try {
				if (ReturnConfig.returnconfig("conf/config_ftpdownloadfile.xml", "//comm//SFTP").equals("true")) {
					sftphelp.disconnect();
				} else {
					ftpClient.disconnect();
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			ExecutorService pool = Executors.newFixedThreadPool(nMaxThreadNum);
			List<Future> futureList = new ArrayList<Future>();
			for (TaskInfo taskinfo : taskList) {
				Callable fm = new FileDownloadCallable(taskinfo, cals);
				// 执行任务并获取Future对象
				Future f = pool.submit(fm);
				futureList.add(f);
			}

			// 关闭线程池
			pool.shutdown();
			int failureNum = 0;
			// 获取所有并发任务的运行结果
			try {
				for (Future f : futureList) {
					System.out.println(f.get().toString());
					if (f.get().toString().contains("Error") || f.get().toString().contains("下载失败")) {
						failureNum++;
					}
				}
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

	class FileDownloadCallable implements Callable<Object> {
		private TaskInfo task;
		private CalendarEx addDays;

		FileDownloadCallable(TaskInfo taskInfo, CalendarEx addDays) {
			this.task = taskInfo;
			this.addDays = addDays;
		}

		@SuppressWarnings({ "resource" })
		@Override
		public Object call() throws Exception {
			if (task != null) {
				FileOutputStream os = null;
				try {
					FTPClientHelper ftpClient = null;
					SftpClientHelper sftphelp = null;
					if (ReturnConfig.returnconfig("conf/config_ftpdownloadfile.xml", "//comm//SFTP").equals("true")) {
						sftphelp = new SftpClientHelper(args.FtpServerIp, args.UserName, args.Password, 22, 1000);
						sftphelp.setEncoding(Encode);

					} else {
						ftpClient = new FTPClientHelper(task.args.FtpServerIp, FtpServerPort, task.args.UserName,
								task.args.Password);
						// 设置ftp属性
						ftpClient.setPassiveMode(Boolean.parseBoolean(Passmodel));
						ftpClient.setEncoding(Encode);
						ftpClient.setBinaryTransfer(true);
					}
					String diskpath = task.outpath;
					if (diskpath.contains(":")) {
						diskpath = task.outpath.substring(0, 2);
					}
					if (checkDiskFreeSpace(diskpath) < 1) { // 空间小于1MB
						System.out.println(getDateNow() + "[" + task.args.FtpServerIp + "] " + "剩余空间大小："
								+ checkDiskFreeSpace(diskpath));
						System.out.println(getDateNow() + "[" + task.args.FtpServerIp + "] " + " 空间不足。。。。等待一分钟。。。");
						Thread.sleep(60000);
					}
					for (int i = 0; i < task.fileLst.size(); i++) {
						String ftpFilename = task.fileLst.get(i);
						File file = new File(ftpFilename);
						os = new FileOutputStream(task.outpath + "/" + file.getName() + ".tmp");
						boolean flag = false;

						System.out.println(getDateNow() + "[" + task.args.FtpServerIp + "] " + "开始下载:" + file.getName()
								+ " >> " + task.outpath);

						if (ReturnConfig.returnconfig("conf/config_ftpdownloadfile.xml", "//comm//SFTP")
								.equals("true")) {
							try {
								flag = sftphelp.get( ftpFilename, os, false, false);
							} catch (Exception e) {
								if (os != null) {
									os.close();
								}
								System.out.println(
										getDateNow() + "[" + task.args.FtpServerIp + "] " + "下载异常:" + file.getName());
								break;
							}
						} else {
							try {
								flag = ftpClient.get(ftpFilename, os);
							} catch (Exception e) {
								if (os != null) {
									os.close();
								}
								System.out.println(
										getDateNow() + "[" + task.args.FtpServerIp + "] " + "下载异常:" + file.getName());
								break;
							}
						}
						os.close();
						// 写成功标志文件。
						if (flag) {
							File oldfile = new File(task.outpath + "/" + file.getName() + ".tmp");
							File newfile = new File(task.outpath + "/" + file.getName());
							if (newfile.exists()) {
								try {
									newfile.delete();
								} catch (Exception e) {
									oldfile.delete();
									continue;
								}
							}
							boolean renameTo = oldfile.renameTo(newfile);
							CalendarEx cals = new CalendarEx();
							String recordFileName = null;
							for (int j = 0; j < DealDays; j++) {
								if (file.getName().contains(cals.AddDays(-j).toString(2))
										|| file.getName().contains(cals.AddDays(-j).toString(4))) {
									recordFileName = task.args.RecordFileName() + "." + cals.AddDays(-j).getDateStr8()
											+ ".txt";
									if (new File(recordFileName).exists()) {
										// 标志文件处理（重新下载）
										StringBuffer sb = new StringBuffer();
										BufferedReader br = new BufferedReader(new FileReader(recordFileName));
										String fileStr = null;
										while ((fileStr = br.readLine()) != null) {
											if (!fileStr.contains(file.getName())) {
												sb.append(fileStr + "\r\n");
											}
										}
										OutputStreamWriter out1 = new OutputStreamWriter(
												new FileOutputStream(recordFileName));
										BufferedWriter bufferedWriter1 = new BufferedWriter(out1);
										bufferedWriter1.write(sb.toString());
										bufferedWriter1.flush();
										out1.close();
										bufferedWriter1.close();
									}
									OutputStreamWriter out = new OutputStreamWriter(
											new FileOutputStream(recordFileName, true));
									BufferedWriter bufferedWriter = new BufferedWriter(out);
									String str = task.downFileList.get(i);
									bufferedWriter.write(str + "\r\n");
									bufferedWriter.flush();
									out.close();
									bufferedWriter.close();
								} else {
									recordFileName = task.args.RecordFileName() + "." + cals.getDateStr8() + ".txt";
									if (new File(recordFileName).exists()) {
										// 标志文件处理（重新下载）
										StringBuffer sb = new StringBuffer();
										BufferedReader br = new BufferedReader(new FileReader(recordFileName));
										String fileStr = null;
										while ((fileStr = br.readLine()) != null) {
											if (!fileStr.contains(file.getName())) {
												sb.append(fileStr + "\r\n");
											}
										}
										OutputStreamWriter out1 = new OutputStreamWriter(
												new FileOutputStream(recordFileName));
										BufferedWriter bufferedWriter1 = new BufferedWriter(out1);
										bufferedWriter1.write(sb.toString());
										bufferedWriter1.flush();
										out1.close();
										bufferedWriter1.close();
									}
									OutputStreamWriter out = new OutputStreamWriter(
											new FileOutputStream(recordFileName, true));
									BufferedWriter bufferedWriter = new BufferedWriter(out);
									String str = task.downFileList.get(i);
									bufferedWriter.write(str + "\r\n");
									bufferedWriter.flush();
									out.close();
									bufferedWriter.close();
								}
							}

						}
						if (ReturnConfig.returnconfig("conf/config_ftpdownloadfile.xml", "//comm//SFTP")
								.equals("true")) {
							sftphelp.disconnect();
						} else {
							ftpClient.disconnect();
						}
					}
				} catch (Exception e) {
					System.out.println(getDateNow() + "[" + task.args.FtpServerIp + "] 下载失败。。。。。" + e.getMessage());
					String diskpath = task.outpath;
					if (diskpath.contains(":")) {
						diskpath = task.outpath.substring(0, 2);
					}

					if (checkDiskFreeSpace(diskpath) < 1) {
						System.out.println(getDateNow() + "[" + task.args.FtpServerIp + "] " + "剩余空间大小："
								+ checkDiskFreeSpace(diskpath));
						System.out.println(getDateNow() + "[" + task.args.FtpServerIp + "] " + " 空间不足。。。。等待一分钟。。。");
						Thread.sleep(60000);
					}
					System.out.println(
							getDateNow() + "[" + task.args.FtpServerIp + "]" + " Task Exec Error: " + e.getMessage());
					return getDateNow() + "[" + task.args.FtpServerIp + "]" + " Task Exec Error: " + e.getMessage();
				}
			}
			return getDateNow() + "[" + task.args.FtpServerIp + "]" + " Task Exec Success";
		}
	}

}
