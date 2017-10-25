package cn.mastercom.sssvr.main;
/**
 * 
 */

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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
import java.util.Scanner;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPInputStream;

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
public class Import extends Thread {

	static Logger log = Logger.getLogger(Import.class.getName());

	private static String DealBcpGp = "1";

	static String exePath = "";
	static String PORT = "5432";
	static String user = "dtauser";
	static String password = "dtauser";
	static String HOST = "192.168.1.31";
	static String LOCAL_HOSTNAME = "master";
	static String DATABASE = "HAERBIN";
	static String datapath = "D:\\mastercom\\SampleEvent3";
	static String donePath = "D:\\mastercom\\SampleEvent_done";
	static String doloadsize = "1";
	static String doloadOne = "1";
	static String binPath = "E:\\工具\\Import2Db\\";

	static boolean bInit = false;

	public static void Init() {
		if (bInit)
			return;
		bInit = true;

		readConfigInfo();

		System.out.println("ImportGp:" + DealBcpGp);

		if (DealBcpGp.equals("1")) {
			Import sampleMover = new Import();
			sampleMover.start();
			log.info("Begin ImportGp...");
		}
	}

	public static void main(String[] args) {
		//Import.Init();
		// unGzipFile("D:\\mastercom\\SampleEvent3\\TB_FGCQTSIGNAL_SAMPLE_01_170813\\sample.gz");
		// readConfigInfo();
		// copyDirectory("D:\\mastercom\\SampleEvent3"+"\\TB_CQTSIGNAL_SAMPLE_01_170813","D:\\mastercom\\SampleEvent_done"+"\\TB_CQTSIGNAL_SAMPLE_01_170813");
		//GenerateYamlFile(null , true);
	}

	@Override
	public void run() {

		while (!MainSvr.bExitFlag) {
			try {
				Thread.sleep(1000);

		//		GenerateYamlFile();

				Thread.sleep(600000);

			} catch (InterruptedException e) {

				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}

	public static void GenerateYamlFile(List<String> fileNameList,String targetDbTable, boolean flag) {
		String templateTbName = null;
		//String targetDbTable = null;
		System.out.println("INFO：制作模板表");
		String strExePath = binPath + "YAML/";
		File file2 = new File(targetDbTable);
		int count = 0 ;
		if (file2.exists()) 
		{
			targetDbTable = file2.getName();
			{				
				if (targetDbTable.contains("SAMPLE")) {
					templateTbName = "TB_MODEL_SIGNAL_SAMPLE";
				} else {
					templateTbName = "TB_MODEL_SIGNAL_EVENT";
				}

				String cal = targetDbTable.split("_")[4];
				templateTbName += ".yaml";

				InputStream in = null;

				File file = new File(strExePath + templateTbName);
				File files = new File(strExePath + targetDbTable + ".yaml");
				
				ArrayList<String> filelist = new ArrayList<String>();
								

				if (!file.exists()) {
					System.out.println("INFO：not find model file" + strExePath + templateTbName);

				} else {
					String content = read(file);
					content = content.replace("%DATABASE%", DATABASE);
					content = content.replace("%PORT%", PORT);
					content = content.replace("%USER%", user);
					content = content.replace("%PASSWORD%", password);
					content = content.replace("%HOST%", HOST);
					content = content.replace("%LOCAL_HOSTNAME%", LOCAL_HOSTNAME);
					content = content.replace("%FILE%", datapath + "/" + targetDbTable);
					content = content.replace("%TABLE%", targetDbTable);
					write(content, files);
					if (targetDbTable.contains("CQT")) {
						GreepPlumHelper.ImportFGSampleorEvent("CQT", cal, templateTbName);
					} else {
						GreepPlumHelper.ImportFGSampleorEvent("DT", cal, templateTbName);
					}					

					try {
						Process pro = Runtime.getRuntime().exec(" gpload -f " + strExePath + targetDbTable + ".yaml");

						pro.waitFor();

						in = pro.getInputStream();
						BufferedReader read = new BufferedReader(new InputStreamReader(in));

						String line = null;
						while ((line = read.readLine()) != null) {
					//		System.out.println("INFO:" + line);
							if (line.contains("gpload succeeded")) {
								System.out.println("INFO:" + "Data warehousing is successful！");
								// copyDirectory(datapath + "/" + targetDbTable,
								// donePath + "/" + targetDbTable);
								File df = new File(datapath + "/" + targetDbTable);
								if (df.isDirectory()) {
									File[] listFiles = df.listFiles();
									for (File file3 : listFiles) {
										file3.delete();
									}
									System.out.println("INFO:" + datapath + "/" + targetDbTable +"/*delete！");
								}                          
								String donePath = "";
								if(datapath.contains(":")){
									donePath = datapath.replace("SampleEvent3", "SampleEvent_done") + "\\" + targetDbTable;
								}else{
									donePath = datapath.replace("SampleEvent3", "SampleEvent_done") + "/" + targetDbTable;
								}
								
								writerFileName( fileNameList, donePath ,flag);
							}
						}

					} catch (Exception e) {
						// TODO Auto-generated catch block
						System.out.println("INFO:" + "Data warehousing failure！");
					} finally {
						if(flag){
							files.delete();
							System.out.println("INFO:" + targetDbTable + ".yaml" + " delete success！");
						}
					}
				}
			}
		}

	}

	public static void unGzipFile(String sourcedir) {

		GZIPInputStream gzi;
		BufferedOutputStream bos;
		File file = null;
		try {
			file = new File(sourcedir);

			gzi = new GZIPInputStream(new FileInputStream(sourcedir));

			int to = sourcedir.lastIndexOf('.');

			String toFileName = sourcedir.substring(0, to);
			bos = new BufferedOutputStream(new FileOutputStream(toFileName));

			int b;
			byte[] d = new byte[1024];

			while ((b = gzi.read(d)) > 0) {
				bos.write(d, 0, b);
			}

			gzi.close();
			bos.close();
		} catch (Exception e) {
			e.getMessage();
		} finally {
			file.delete();
		}

	}

	private static boolean copyDirectory(String srcPath, String destDir) {
		boolean flag = false;

		File srcFile = new File(srcPath);
		if (!srcFile.exists()) { // 源文件夹不存在
			System.out.println("INFO:" + "源文件夹不存在");
			return false;
		}
		// 目标文件夹的完整路径
		String destPath = destDir;
		// System.out.println("目标文件夹的完整路径为：" + destPath);

		if (destPath.equals(srcPath)) {
			System.out.println("INFO:" + "目标文件夹与源文件夹重复");
			return false;
		}
		File destDirFile = new File(destPath);
		if (destDirFile.exists()) { // 目标位置有一个同名文件夹
			System.out.println("INFO:" + "目标位置已有同名文件夹!");
			return false;
		}
		destDirFile.mkdirs(); // 生成目录

		File[] fileList = srcFile.listFiles(); // 获取源文件夹下的子文件和子文件夹
		if (fileList.length == 0) { // 如果源文件夹为空目录则直接设置flag为true，这一步非常隐蔽，debug了很久
			flag = true;
		} else {
			for (File temp : fileList) {
				if (temp.isFile()) { // 文件
					flag = copyFile(temp.getAbsolutePath(), destPath);

				} else if (temp.isDirectory()) { // 文件夹
					flag = copyDirectory(temp.getAbsolutePath(), destPath);
				}
				if (!flag) {
					break;
				}
			}
		}

		if (flag) {
			System.out.println("INFO:" + "复制文件夹成功!");
			if (flag) {
				deleteDir(srcFile);
			}
		}

		return flag;
	}

	private static boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			// 递归删除目录中的子目录下
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) {
					return false;
				}
			}
		}
		// 目录此时为空，可以删除
		return dir.delete();
	}

	private static boolean copyFile(String srcPath, String destDir) {
		boolean flag = false;

		File srcFile = new File(srcPath);
		if (!srcFile.exists()) { // 源文件不存在
			System.out.println("INFO:" + "源文件不存在");
			return false;
		}
		// 获取待复制文件的文件名
		String fileName = srcPath.substring(srcPath.lastIndexOf(File.separator));
		String destPath = destDir + fileName;
		if (destPath.equals(srcPath)) { // 源文件路径和目标文件路径重复
			System.out.println("INFO:" + "源文件路径和目标文件路径重复!");
			return false;
		}
		File destFile = new File(destPath);
		if (destFile.exists() && destFile.isFile()) { // 该路径下已经有一个同名文件
			System.out.println("INFO:" + "目标目录下已有同名文件!");
			return false;
		}

		File destFileDir = new File(destDir);
		destFileDir.mkdirs();
		try {
			FileInputStream fis = new FileInputStream(srcPath);
			FileOutputStream fos = new FileOutputStream(destFile);
			byte[] buf = new byte[1024];
			int c;
			while ((c = fis.read(buf)) != -1) {
				fos.write(buf, 0, c);
			}
			fis.close();
			fos.close();

			flag = true;
		} catch (IOException e) {
			//
		}

		if (flag) {
			System.out.println("INFO:" + "复制文件成功!");
		}
		return flag;
	}

	public static String read(File src) {
		StringBuffer res = new StringBuffer();
		String line = null;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(src));
			while ((line = reader.readLine()) != null) {
				res.append(line + "\n");
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();

		}
		return res.toString();
	}

	public static boolean write(String cont, File dist) {
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(dist));
			writer.write(cont);
			writer.flush();
			writer.close();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public static void writerFileName(List<String> fileNameList ,String fileputpath ,boolean flag){
		BufferedWriter bw = null; 
		BufferedWriter dbw = null; 
		FileWriter output = null;
		String done = "";
		String donesuccess = "";
		FileWriter doneput = null ;
		if(fileputpath.contains(":")){
			done = fileputpath + "\\filelist.txt";
			donesuccess = fileputpath + "\\_SUCCESS";
		}else{
			done = fileputpath + "/filelist.txt";
			donesuccess = fileputpath + "/_SUCCESS";
		}
		System.out.println("INFO:" + done);
		try{
	//	output = new FileWriter(done);
		
		
		OutputStreamWriter out = new OutputStreamWriter(                        
                new FileOutputStream(done, true));
		
		bw = new BufferedWriter(out); 
		dbw = new BufferedWriter(doneput); 
		for (String str : fileNameList) {
			bw.write(str + "\r\n");
			bw.flush();		
		}
		
		if(flag){
			doneput = new FileWriter(donesuccess);
			dbw.write("");
			dbw.flush();
			System.out.println("INFO:" + done+  "success flag _SUCCESS");
		}
		
		}catch (Exception e){
		//	e.getMessage();
		}finally {
			try {
				bw.close();
				output.close();
				dbw.close();
				doneput.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				//System.out.println("INFO:" + "java.lang.NullPointerException");
			}
		}
	}

	@SuppressWarnings("unchecked")
	public static void readConfigInfo() {
		try {
			// XMLWriter writer = null;// 声明写XML的对象
			SAXReader reader = new SAXReader();

			OutputFormat format = OutputFormat.createPrettyPrint();
			format.setEncoding("GBK");// 设置XML文件的编码格式

			String filePath = "conf/config_gp.xml";
			File file = new File(filePath);
			if (file.exists()) {
				Document doc = reader.read(file);// 读取XML文件

				{
					List<String> list = doc.selectNodes("//comm/PORT");
					Iterator iter = list.iterator();
					while (iter.hasNext()) {
						Element element = (Element) iter.next();
						PORT = element.getText();
						break;
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/user");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							user = element.getText();
							break;
						}
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/DATABASE");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							DATABASE = element.getText();
							break;
						}
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/HOST");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							HOST = element.getText();
							break;
						}
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/LOCAL_HOSTNAME");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							LOCAL_HOSTNAME = element.getText();
							break;
						}
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/dataPath");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							datapath = element.getText();
							break;
						}
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/binPath");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							binPath = element.getText();
							break;
						}
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/donePath");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							donePath = element.getText();
							break;
						}
					}
				}
				
				{
					List<String> list = doc.selectNodes("//comm/doloadsize");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							doloadsize = element.getText();
							break;
						}
					}
				}
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static String getdoloadSize(){
		return doloadsize;
	}
}
