package cn.mastercom.sssvr.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.log4j.Logger;

import cn.mastercom.sssvr.main.DownloadMR;
import cn.mastercom.sssvr.main.Import;
import cn.mastercom.sssvr.main.MainSvr;

public class HadoopFSOperation {
	static Logger log = Logger.getLogger(HadoopFSOperation.class.getName());
	public static final String FS_DEFAULT_NAME_KEY = "fs.defaultFS";

	private static Configuration conf = new Configuration();
	public String HADOOP_URL = "";
	String[] HADOOP_URL_List = null;
	public static FileSystem fs;
	private String hadoopHost;
	private int hadoopPort;

	private static DistributedFileSystem hdfs;

	public HadoopFSOperation() {
		// SetHadoopRoot();

	}

	public class TaskInfo {
		public List<String> fileLst = new ArrayList<String>();// 待处理文件完整路径组成的list
		public String outputLocalName;
		public boolean Compress = true;
		public List<String> fileName = new ArrayList<String>();
	}

	public HadoopFSOperation(Configuration conf) throws Exception {
		try {
			this.conf = conf;
			
			fs = FileSystem.get(conf);
			hdfs = (DistributedFileSystem) fs;

			String hdfsUrl = conf.get(FS_DEFAULT_NAME_KEY);
			String tmStr = hdfsUrl.replace("hdfs://", "");
			if (tmStr.indexOf(":") > 0) {
				this.hadoopHost = tmStr.substring(0, tmStr.indexOf(":"));
				this.hadoopPort = Integer.parseInt(tmStr.substring(tmStr.indexOf(":") + 1));
			} else {
				this.hadoopHost = tmStr;
				this.hadoopPort = 8020;
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	public class FileDownloadCallable implements Callable<Object> {
		public TaskInfo task;

		public FileDownloadCallable(TaskInfo taskInfo) {
			this.task = taskInfo;
		}

		@SuppressWarnings("unused")
		@Override
		public Object call() throws Exception {

			if (task != null) {

				try {
					FileOutputStream os = null;
					GZIPOutputStream gfs = null;

					int fileSeq = 1;
					long nTotalLen = 0;
					String fileName = task.outputLocalName;

					if (task.Compress) {
						gfs = new GZIPOutputStream(new FileOutputStream(fileName + ".gz"));
						os = null;
					} else {
						os = new FileOutputStream(fileName);
						gfs = null;
					}

					for (int i = 0; i < task.fileLst.size(); i++) {
						String hdfsFilename = task.fileLst.get(i);
						Path f = new Path(hdfsFilename);
						FSDataInputStream dis = fs.open(f);

						byte[] buffer = new byte[1024000];
						int length = 0;
						long nTotalLength = 0;
						int nCount = 0;
						while (MainSvr.bExitFlag != true && (length = dis.read(buffer)) > 0) {
							nCount++;
							if (gfs != null) {
								gfs.write(buffer, 0, length);
							} else {
								os.write(buffer, 0, length);
							}
							nTotalLength += length;
						}

						dis.close();
					}

					if (gfs != null) {
						// gfs.flush();
						gfs.finish();
						gfs.close();
						// System.out.println("Close File " +
						// task.outputLocalName);
					} else {
						os.flush();
						os.close();
					}

				} catch (Exception e) {
					e.printStackTrace();
					System.out.println((new Date()).toString() + " Task Exec Error:" + task.outputLocalName + "\r\n"
							+ e.getMessage());
					System.out.println(e.getStackTrace());

					return "Task Exec Error:" + task.outputLocalName + "\r\n" + e.getMessage();
				} finally {

				}
			}
			return "Task Exec Success:" + task.outputLocalName;
		}
	}
	
	static ReentrantLock m_ReentrantLock = new ReentrantLock(true);
	public int m_ThreadCnt;

	
	public class FileDownloadCallables implements Callable<Object> {
		public TaskInfo task;

		public FileDownloadCallables(TaskInfo taskInfo) {
			this.task = taskInfo;
		}
		@Override
		public Object call() throws Exception {
			FileOutputStream os = null;
			if (task != null) {
				try {
					m_ReentrantLock.lock();
					m_ThreadCnt++;
					m_ReentrantLock.unlock();
					
					for (int i = 0; i < task.fileLst.size(); i++) {												
						workWrite(task.fileName.get(i),task.fileLst.get(i),os);						
					}													
				} catch (Exception e) {
					
					Thread.sleep(1000);
					System.out.println( getDateNow()+ "下载异常,休息一分钟：" + e.getMessage());					
					return "Task Exec Fail....";
				}
			}
			return "Task Exec Success.....";
		}
	}
	
	public void workWrite(String fileName,String hdfsName,FileOutputStream os){
		FSDataInputStream dis = null ;
		try {
			m_ReentrantLock.lock();
			m_ReentrantLock.unlock();
			
			os = new FileOutputStream(fileName);
			String hdfsFilename = hdfsName;
			Path f = new Path(hdfsFilename);
			dis = fs.open(f);

			byte[] buffer = new byte[1024000];
			int length = 0;
			long nTotalLength = 0;
			int nCount = 0;
			while ((length = dis.read(buffer)) > 0) {
				nCount++;
				os.write(buffer, 0, length);							
				nTotalLength += length;
			}	
			System.out.println( getDateNow() + "下载成功:"+fileName);
			dis.close();
			os.close();	
			os.flush();	
		} catch (Exception e) {
			try {
				if(os != null ){				
					os.close();					
				}
				if(dis != null){
					dis.close();					
				}
			} catch (IOException e1) {
				
			}
		}finally{
			try {
				if(os != null ){				
					os.close();					
				}
				if(dis != null){
					dis.close();					
				}
			} catch (IOException e1) {
				
			}
		}
		
	}
	
	
	public HadoopFSOperation(String hdfsRoot) {
		Configuration conf = new Configuration();
		
		try {
			fs = FileSystem.get(conf);
			if (fs.getClass().equals(DistributedFileSystem.class)) {
				hdfs = (DistributedFileSystem) fs;
				return;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (hdfsRoot.trim().length() == 0)
			return;

		HADOOP_URL_List = hdfsRoot.split(";");
		ProcessStandby();
	}

	private void ProcessStandby() {
		for (int i = 0; i < HADOOP_URL_List.length; i++) {
			HADOOP_URL = HADOOP_URL_List[i];
			SetHadoopRoot();
			if (!checkStandbyException("/"))
				break;
		}
	}

	public boolean ReCoconnect() {
		try {
			FileSystem.closeAll();
			SetHadoopRoot();
			if (checkStandbyException("/")) {
				ProcessStandby();
			}
			return checkFileExist("/");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	private void SetHadoopRoot() {
		try {
			if (HADOOP_URL.length() < 6)
				return;
			System.out.println("SetHadoopRoot： " + HADOOP_URL);
			FileSystem.setDefaultUri(conf, HADOOP_URL);
			fs = FileSystem.get(conf);
			hdfs = (DistributedFileSystem) fs;
		} catch (Exception e) {
			System.out.println("SetHadoopRoot error： " + e.getMessage());
			e.printStackTrace();
		}
	}

	@SuppressWarnings("deprecation")
	public void moveSmallFilesToParent(String parentDir) {
		System.out.println("MoveSmallFilesToParent： " + parentDir);
		FileStatus fileStatus[];
		try {
			fileStatus = fs.listStatus(new Path(parentDir));
			int listlength = fileStatus.length;

			for (int i = 0; i < listlength; i++) {
				if (fileStatus[i].isDirectory() == true) {
					String pathName = fileStatus[i].getPath().getName().toLowerCase();
					/*
					 * if (!pathName.contains("sample") &&
					 * !pathName.contains("event") &&
					 * !pathName.contains("grid")) { continue; }
					 */

					FileStatus childStatus[] = fs.listStatus(new Path(parentDir + "/" + pathName));

					int childListlength = childStatus.length;
					boolean bShouldMove = false;
					try {
						for (int j = 0; j < childListlength; j++) {
							if (childStatus[j].isDirectory() == false) {
								String childName = childStatus[j].getPath().getName().toLowerCase();
								if (!childName.contains("sample") && !childName.contains("event")
										&& !childName.contains("grid")) {
									continue;
								}
								movefile(parentDir + "/" + pathName + "/" + childName, parentDir);
								bShouldMove = true;
							}
						}
						if (bShouldMove) {
							fs.delete(new Path(parentDir + "/" + pathName));
							System.out.println("删除文件夹成功: " + parentDir + "/" + pathName);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public FileStatus[] listStatus(String path) {
		try {
			if (!path.startsWith("hdfs"))
				path = HADOOP_URL + path;
			FileSystem myfs = FileSystem.get(conf);
			return myfs.listStatus(new Path(path));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;

	}

	/**
	 * 得到文件夹下的文件，元素是DatafileInfo包括文件名、文件大小、文件修改时间
	 */
	public ArrayList<DatafileInfo> listFiles(String path)
			throws FileNotFoundException, IllegalArgumentException, IOException {
		FileStatus fileStatus[] = fs.listStatus(new Path(path));
		int listlength = fileStatus.length;
		ArrayList<DatafileInfo> fileList = new ArrayList<DatafileInfo>();
		for (int i = 0; i < listlength; i++) {
			if (fileStatus[i].isDirectory() == false) {
				if (fileStatus[i].getLen() > 0) {
					long modificationTime = fileStatus[i].getModificationTime();
					fileList.add(new DatafileInfo(fileStatus[i].getPath().getName(), fileStatus[i].getLen(),
							modificationTime));
				}
			}
		}
		return fileList;
	}

	/**
	 * 得到文件夹下的文件夹，元素是DatafileInfo包括文件夹名、文件夹大小、文件夹修改时间
	 */
	public ArrayList<DatafileInfo> listSubDirs(String path) {
		ArrayList<DatafileInfo> fileList = new ArrayList<DatafileInfo>();
		try {
			FileStatus fileStatus[] = fs.listStatus(new Path(path));
			int listlength = fileStatus.length;
			for (int i = 0; i < listlength; i++) {
				if (fileStatus[i].isDirectory() == true) {
					long modificationTime = fileStatus[i].getModificationTime();
					fileList.add(new DatafileInfo(fileStatus[i].getPath().getName(), fileStatus[i].getLen(),
							modificationTime));
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fileList;
	}

	/**
	 * 列出所有DataNode的名字信息
	 */
	public void listDataNodeInfo() {
		try {
			DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
			String[] names = new String[dataNodeStats.length];
			System.out.println("List of all the datanode in the HDFS cluster:");

			for (int i = 0; i < names.length; i++) {
				names[i] = dataNodeStats[i].getHostName();
				System.out.println(names[i]);
			}
			System.out.println(hdfs.getUri().toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public boolean movefile(String src, String dst) {
		try {
			Path p1 = new Path(src);
			Path p2 = new Path(dst);
			hdfs.rename(p1, p2);

			System.out.println("重命名文件夹或文件成功: " + src + " --> " + dst);
			return true;
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public boolean delete(String src) throws Exception {
		Path p1 = new Path(src);
		if (hdfs.isDirectory(p1)) {
			// System.out.println("删除文件夹成功: " + src);
			return hdfs.delete(p1, true);
		} else if (hdfs.isFile(p1)) {
			// System.out.println("删除文件成功: " + src);
			return hdfs.delete(p1, false);
		}
		return true;
	}

	/**
	 * 查看文件是否存在
	 * 
	 * @throws Exception
	 */
	public boolean checkFileExist(String filename) {
		try {
			Path f = new Path(filename);
			return hdfs.exists(f);
		} catch (org.apache.hadoop.ipc.RemoteException e) {
			if (e.getClassName().equals("org.apache.hadoop.ipc.StandbyException")) {
				ProcessStandby();
			}
		} catch (Exception e) {

		}
		return false;
	}

	/**
	 * 查看文件是否存在
	 * 
	 * @throws Exception
	 */
	public boolean checkStandbyException(String filename) {
		try {
			Path f = new Path(filename);
			hdfs.exists(f);
			return false;
		} catch (org.apache.hadoop.ipc.RemoteException e) {
			if (e.getClassName().equals("org.apache.hadoop.ipc.StandbyException")) {
				return true;
			}
		} catch (Exception e) {

		}
		return true;
	}

	public boolean mkdir(String dirName) {
		try {
			if (checkFileExist(dirName))
				return true;
			Path f = new Path(dirName);
			System.out.println("Create and Write :" + dirName + " to hdfs");
			return hdfs.mkdirs(f);
		} catch (Exception e) {
			System.out.println("mkdir Fail:" + e.getMessage());
			e.printStackTrace();
		}

		return false;
	}

	/**
	 * 读取本地文件到HDFS系统<br>
	 * 请保证文件格式一直是UTF-8，从本地->HDFS
	 */
	@SuppressWarnings("deprecation")
	public boolean getMergeST(String hdfsDir, String localDir, String filter, boolean Compress) {
		try {
			ArrayList<DatafileInfo> fileLst = listFiles(hdfsDir);
			if (fileLst.isEmpty())
				return false;
			makeDir(localDir);
			// deleteFile(localDir + "/" + destFileName);
			// File file = new File(localDir + "/" + destFileName);
			FileOutputStream os = null;
			GZIPOutputStream gfs = null;

			int fileSeq = 1;
			long nTotalLen = 0;
			String fileName = localDir + "/" + filter + "." + fileSeq;
			if (Compress) {
				gfs = new GZIPOutputStream(new FileOutputStream(fileName + ".gz"));
				os = null;
			} else {
				os = new FileOutputStream(fileName);
				gfs = null;
			}

			for (int i = 0; i < fileLst.size(); i++) {
				if (!fileLst.get(i).filename.contains(filter)) {
					continue;
				}
				nTotalLen += fileLst.get(i).filesize;
				if (nTotalLen > 1024 * 1000 * 1000) {
					nTotalLen = 0;
					fileSeq++;
					if (gfs != null) {
						gfs.flush();
						gfs.close();
					} else {
						os.flush();
						os.close();
					}
					fileName = localDir + "/" + filter + "." + fileSeq;
					if (Compress) {
						gfs = new GZIPOutputStream(new FileOutputStream(fileName + ".gz"));
						os = null;
					} else {
						os = new FileOutputStream(fileName);
						gfs = null;
					}
				}

				String hdfsFilename = hdfsDir + "/" + fileLst.get(i).filename;
				Path f = new Path(hdfsFilename);
				FSDataInputStream dis = fs.open(f);

				byte[] buffer = new byte[1024000];
				int length = 0;
				long nTotalLength = 0;
				int nCount = 0;
				while (MainSvr.bExitFlag != true && (length = dis.read(buffer)) > 0) {
					nCount++;
					if (gfs != null) {
						gfs.write(buffer, 0, length);
					} else {
						os.write(buffer, 0, length);
					}
					nTotalLength += length;

					/*
					 * if(nCount%100 == 0) { StringBuilder stringBuilder = new
					 * StringBuilder(); stringBuilder.append((new
					 * Date()).toLocaleString()); stringBuilder.append(
					 * ": Have move ");
					 * stringBuilder.append((nTotalLength/1024000));
					 * stringBuilder.append(" MB");
					 * System.out.println(stringBuilder.toString()); }
					 */
				}

				dis.close();
			}
			if (gfs != null) {
				gfs.flush();
				gfs.close();
			} else {
				os.flush();
				os.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return true;
	}

	/**
	 * 读取本地文件到HDFS系统<br>
	 * 请保证文件格式一直是UTF-8，从本地->HDFS
	 */
	@SuppressWarnings("deprecation")
	public boolean getMerge(String hdfsDir, String localDir, String filter, boolean Compress) {

		FileOutputStream os = null;
		GZIPOutputStream gfs = null;
		int fileSeq = 1;
		long nTotalLen = 0;
		int filecount = 0;
		boolean flag = false;
		ExecutorService pool;

		HashMap<String, String> doneMap = new HashMap<String, String>();
		List<TaskInfo> taskList = new ArrayList<TaskInfo>();
		List<String> fileList = new ArrayList<String>();
		List<String> fileNameList = new ArrayList<String>();

		boolean bComplete = true;
		try {
			ArrayList<DatafileInfo> fileLst = listFiles(hdfsDir);
			String locaDir_done = localDir.replace("SampleEvent3", "SampleEvent_done");
			makeDir(locaDir_done);

			File locaDir_done_file = new File(locaDir_done);

			File locaDir_file = new File(localDir);

			if (locaDir_done_file.exists()) {
				File[] listFiles = locaDir_done_file.listFiles();
				for (File path : listFiles) {
					String line = null;
					BufferedReader reader = new BufferedReader(new FileReader(path.toString()));
					while ((line = reader.readLine()) != null) {
						doneMap.put(line, "");
		//				 System.out.println("文件名:" + line);
					}
					reader.close();
				}
			}

			if (fileLst.isEmpty())
				return false;
			makeDir(localDir);

			FileStatus fileStatus[] = fs.listStatus(new Path(hdfsDir));
			int listlength = fileStatus.length;
			
			Import.readConfigInfo();
			String getdoloadSize = Import.getdoloadSize();
	//		getdoloadSize = "2";

			for (int i = 0; i < fileLst.size(); i++) {

				if (!doneMap.containsKey(fileLst.get(i).filename)) {
					nTotalLen += fileLst.get(i).filesize;
					// System.out.println(fileLst.get(i).filename);
					fileList.add(hdfsDir + "/" + fileLst.get(i).filename);
					fileNameList.add(fileLst.get(i).filename);
					filecount++;
					int doub = Integer.valueOf(getdoloadSize);

					if (nTotalLen > 1024 * 1024 * 1000) {
						TaskInfo ti = new TaskInfo();
						ti.fileLst = fileList;
						ti.outputLocalName = localDir + "/" + filter + "." + fileSeq;
						ti.Compress = Compress;
						taskList.add(ti);
						fileList = new ArrayList<String>();
						fileSeq++;
						nTotalLen = 0;
					}

					if (fileSeq > doub) {
						bComplete = false;
						break;
					}
				}
			}

			if (nTotalLen > 0) {
				TaskInfo ti = new TaskInfo();
				ti.fileLst = fileList;
				ti.outputLocalName = localDir + "/" + filter + "." + fileSeq;
				ti.Compress = Compress;
				taskList.add(ti);
			}
			
			
			if (filecount + doneMap.size() >= listlength) {
				flag = true;
			}
			
			System.out.println( "filecount=" + filecount +"doneMap.size():"+ doneMap.size() + "listlength:"+ listlength + "flag:"+ flag);
         
			// 创建一个线程池
			pool = Executors.newFixedThreadPool(5);
			// 创建多个有返回值的任务
			@SuppressWarnings("rawtypes")
			List<Future> list = new ArrayList<Future>();

			for (TaskInfo taskinfo : taskList) {
				if (MainSvr.bExitFlag == true) {
					break;
				}
				@SuppressWarnings("rawtypes")
				Callable fm = new FileDownloadCallable(taskinfo);

				// 执行任务并获取Future对象
				@SuppressWarnings("rawtypes")
				Future f = pool.submit(fm);

				list.add(f);
			}
			// 关闭线程池
			pool.shutdown();

			for (Future f : list) {
				try {
					f.get(); // 线程执行完，在执行其它的。
				} catch (Exception e) {
			//		e.printStackTrace();
				}
			}

			System.out.println("INFO:" + "Download Complete");

			// 获取所有并发任务的运行结果
			Import.readConfigInfo();
			Import.GenerateYamlFile(fileNameList, localDir, flag);
			if (!bComplete) {
				getMerge(hdfsDir, localDir, filter, Compress);
			}

		} catch (Exception e) {
			// e.printStackTrace();
			System.out.println("INFO:" + "file not exist。");
		}

		return true;
	}

	public FSDataOutputStream GetOutputStream(String destFileName) {
		try {
			if (this.checkFileExist(destFileName)) {
				delete(destFileName);
			}
			Path f = new Path(destFileName);
			FSDataOutputStream os = fs.create(f, true);
			return os;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public FSDataInputStream GetInputStream(String destFileName) {
		try {
			if (!this.checkFileExist(destFileName)) {
				return null;
			}
			Path f = new Path(destFileName);

			FSDataInputStream is = fs.open(f);
			return is;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 读取本地文件到HDFS系统<br>
	 * 请保证文件格式一直是UTF-8，从本地->HDFS
	 * 
	 */
	/**
	 * 
	 * @param localDirname
	 *            源文件所在位置
	 * @param hdfsPath要放在服务器的位置
	 * @param destFileName要合并成的文件名称
	 * @param filter
	 * @return
	 */
	public boolean putMerge(String localDirname, String hdfsPath, String destFileName, String filter) {
		try {
			File dir = new File(localDirname);
			if (!dir.isDirectory()) {
				System.out.println(localDirname + "不是目录 ");
				return false;
			}

			File[] files = dir.listFiles();
			if (files.length == 0)
				return false;

			System.out.println("Begin move " + localDirname + " to " + hdfsPath);

			while (checkFileExist(hdfsPath + "/" + destFileName)) {
				if (destFileName.contains(".x"))
					destFileName += "x";
				else
					destFileName += ".x";
			}

			mkdir(hdfsPath);

			Path f = new Path(hdfsPath + "/" + destFileName);
			FSDataOutputStream os = fs.create(f, true);
			byte[] buffer = new byte[10240000];

			for (int i = 0; i < files.length; i++) {
				if (MainSvr.bExitFlag == true)
					break;

				File file = files[i];
				if (!file.getName().toLowerCase().contains(filter.toLowerCase()))
					continue;
				if (file.getName().toLowerCase().contains(".processing"))
					continue;
				FileInputStream is = new FileInputStream(file);
				GZIPInputStream gis = null;

				try {
					if (file.getName().toLowerCase().endsWith("gz"))
						gis = new GZIPInputStream(is);

					while (MainSvr.bExitFlag != true) {
						int bytesRead = 0;
						if (gis == null)
							bytesRead = is.read(buffer);
						else
							bytesRead = gis.read(buffer, 0, buffer.length);
						if (bytesRead >= 0) {
							os.write(buffer, 0, bytesRead);
						} else {
							break;
						}
					}
				} catch (Exception e) {

				}
				if (gis != null)
					gis.close();
				is.close();
			}
			os.close();
			if (MainSvr.bExitFlag)
				return false;
			System.out.println("Success move " + localDirname + " to " + hdfsPath);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			log.info("putMerge error:" + e.getMessage());
		}
		return false;
	}

	/**
	 * 读取本地文件到HDFS系统<br>
	 * 请保证文件格式一直是UTF-8，从本地->HDFS
	 * 
	 */
	/**
	 * 
	 * @param localDirname
	 *            源文件所在位置
	 * @param hdfsPath要放在服务器的位置
	 * @param destFileName要合并成的文件名称
	 * @param filter
	 * @return
	 */
	public boolean putMerge(List<String> files, String hdfsPath, String destFileName, String filter) {
		try {
			System.out.println("Begin move files to " + hdfsPath);

			while (checkFileExist(hdfsPath + "/" + destFileName)) {
				if (destFileName.contains(".x"))
					destFileName += "x";
				else
					destFileName += ".x";
			}

			mkdir(hdfsPath);
			Path f = new Path(hdfsPath + "/" + destFileName);
			FSDataOutputStream os = fs.create(f, true);
			byte[] buffer = new byte[10240000];

			for (int i = 0; i < files.size(); i++) {
				if (MainSvr.bExitFlag == true)
					break;

				File file = new File(files.get(i));
				if (!file.exists())
					continue;
				if (!file.getName().toLowerCase().contains(filter.toLowerCase()))
					continue;
				FileInputStream is = new FileInputStream(file);
				GZIPInputStream gis = null;
				if (file.getName().toLowerCase().endsWith("gz"))
					gis = new GZIPInputStream(is);

				while (MainSvr.bExitFlag != true) {
					int bytesRead = 0;
					if (gis == null)
						bytesRead = is.read(buffer);
					else
						bytesRead = gis.read(buffer, 0, buffer.length);
					if (bytesRead >= 0) {
						os.write(buffer, 0, bytesRead);
					} else {
						break;
					}
				}
				if (gis != null)
					gis.close();
				is.close();
			}
			os.close();
			if (MainSvr.bExitFlag)
				return false;
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			log.info(e.getStackTrace());
		}
		return false;
	}

	public boolean putMerge(List<String> files, String hdfsPath, String destFileName, String filter,
			String srcCommonPah, String XdrBkPath, int totalNum, int dealNum) {
		File file = null;
		FSDataOutputStream os = null;
		FileInputStream is = null;
		GZIPInputStream gis = null;
		try {
			System.out.println("Begin move files to " + hdfsPath);

			while (checkFileExist(hdfsPath + "/" + destFileName)) {
				if (destFileName.contains(".x"))
					destFileName += "x";
				else
					destFileName += ".x";
			}

			mkdir(hdfsPath);
			Path f = new Path(hdfsPath + "/" + destFileName);
			os = fs.create(f, true);
			byte[] buffer = new byte[10240000];

			for (int i = 0; i < files.size(); i++) {
				try {
					if (MainSvr.bExitFlag == true)
						break;

					file = new File(files.get(i));
					if (!file.exists())
						continue;
					if (!file.getName().toLowerCase().contains(filter.toLowerCase()))
						continue;
					is = new FileInputStream(file);
					gis = null;
					if (file.getName().toLowerCase().endsWith("gz"))
						gis = new GZIPInputStream(is);

					while (MainSvr.bExitFlag != true) {
						int bytesRead = 0;
						if (gis == null)
							bytesRead = is.read(buffer);
						else
							bytesRead = gis.read(buffer, 0, buffer.length);
						if (bytesRead >= 0) {
							os.write(buffer, 0, bytesRead);
						} else {
							break;
						}
					}
					System.out.println("上传文件" + file.getAbsolutePath() + "完成！");
					if (gis != null)
						gis.close();
					is.close();
					dealNum++;
					if (dealNum % 100 == 0) {
						System.out.println("MME和HTTP文件个数总共" + totalNum + "个，已上传完成" + dealNum + "个!");
					}
					if (XdrBkPath.length() > 0) {
						System.out
								.println("备份文件" + file.getAbsolutePath()
										+ (LocalFile.bkFile(file.getAbsolutePath(),
												file.getAbsolutePath().replace(srcCommonPah, XdrBkPath)) ? "成功"
														: "失败"));
					} else {
						System.out.println("删除文件" + file.getAbsolutePath() + (file.delete() ? "成功" : "失败"));
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					if (gis != null)
						gis.close();
					is.close();
					System.out.println("删除文件" + file.getAbsolutePath() + (file.delete() ? "成功" : "失败"));
					continue;
				}
			}
			os.close();
			if (MainSvr.bExitFlag)
				return false;
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			log.info(e.getStackTrace());
		} finally {
			try {
				if (gis != null)
					gis.close();
				is.close();
				os.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return false;
	}

	public boolean CopyDirTohdfs(String localPath, String hdfsPath, boolean bOverRrite) {
		try {
			File root = new File(localPath);
			File[] files = root.listFiles();

			for (File file : files) {
				if (file.isFile()) {
					if (!copyFileToHDFS(file.getPath().toString(), hdfsPath, bOverRrite)) {
					}
				} else if (file.isDirectory()) {
					CopyDirTohdfs(localPath + "/" + file.getName(), hdfsPath + "/" + file.getName(), bOverRrite);
				}
			}
			return true;
		} catch (Exception e) {
			log.info(e.getStackTrace());
		}
		return false;
	}

	/**
	 * 创建一个空文件
	 * 
	 * @param filename
	 * @return
	 */
	public boolean CreateEmptyFile(String filename) {
		try {
			Path f = new Path(filename);
			FSDataOutputStream os = fs.create(f, true);
			os.close();
			return true;
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 读取本地文件到HDFS系统<br>
	 * 请保证文件格式一直是UTF-8，从本地->HDFS
	 */
	@SuppressWarnings("deprecation")
	public boolean unzipFileToHDFS(String localFilename, String hdfsPath) {
		try {
			System.out.println("Begin move " + localFilename + " to " + hdfsPath);
			int pos = hdfsPath.lastIndexOf("/");
			if (pos < 0)
				return false;
			String path = hdfsPath.substring(0, pos);
			mkdir(path);

			File file = new File(localFilename);
			FileInputStream is = new FileInputStream(file);
			String finalName = file.getName().replace(".gz", "");

			while (checkFileExist(path + "/" + finalName)) {
				if (finalName.contains(".x"))
					finalName += "x";
				else
					finalName += ".x";
			}

			Path f = new Path(path + "/" + finalName);

			FSDataOutputStream os = fs.create(f, false);
			GZIPInputStream gis = null;
			if (file.getName().toLowerCase().contains(".gz"))
				gis = new GZIPInputStream(is);

			byte[] buffer = new byte[1024000];
			int nCount = 0;
			while (MainSvr.bExitFlag != true) {
				int bytesRead = 0;
				try {
					if (gis != null)
						bytesRead = gis.read(buffer);
					else
						bytesRead = is.read(buffer);
				} catch (Exception e) {
					System.out.println(e.getMessage());
					bytesRead = -1;
				}
				if (bytesRead >= 0) {
					os.write(buffer, 0, bytesRead);
					nCount++;
					// if(nCount%(100) == 0)
					// System.out.println((new Date()).toLocaleString() + ":
					// Have move " + nCount + " blocks");
				} else {
					break;
				}
			}

			is.close();
			os.close();
			if (gis != null)
				gis.close();
			System.out.println((new Date()).toLocaleString() + ": Write content of file " + file.getName()
					+ " to hdfs file " + f.getName() + " success");

			if (MainSvr.bExitFlag)
				return false;
			return true;
		} catch (Exception e) {
			log.info(e.getMessage());
		}
		return false;
	}

	/**
	 * 读取本地文件到HDFS系统<br>
	 * 请保证文件格式一直是UTF-8，从本地->HDFS
	 */
	@SuppressWarnings("deprecation")
	public boolean copyFileToHDFS(String localFilename, String hdfsPath, boolean bOverRrite) {
		try {
			System.out.println("Begin move " + localFilename + " to " + hdfsPath);
			mkdir(hdfsPath);

			File file = new File(localFilename);
			FileInputStream is = new FileInputStream(file);

			if (this.checkFileExist(hdfsPath + "/" + file.getName())) {
				if (!bOverRrite)
					return true;
				else
					delete(hdfsPath + "/" + file.getName());
			}

			Path f = new Path(hdfsPath + "/" + file.getName());

			FSDataOutputStream os = fs.create(f, false);
			byte[] buffer = new byte[10240000];
			int nCount = 0;
			while (MainSvr.bExitFlag != true) {
				int bytesRead = is.read(buffer);
				if (bytesRead >= 0) {
					os.write(buffer, 0, bytesRead);
					nCount++;
					// if (nCount % (100) == 0)
					// System.out.println((new Date()).toLocaleString() + ":
					// Have move " + nCount + " blocks");
				} else {
					break;
				}
			}

			is.close();
			os.close();
			System.out.println((new Date()).toLocaleString() + ": Write content of file " + file.getName()
					+ " to hdfs file " + f.getName() + " success");

			if (MainSvr.bExitFlag)
				return false;
			return true;
		} catch (Exception e) {
			System.out.println(e.getMessage());
			log.info(e.getMessage());
		}
		return false;
	}

	/**
	 * 取得文件块所在的位置..
	 */
	public void getLocation() {
		try {
			Path f = new Path("/user/xxx/input02/file01");
			FileStatus fileStatus = fs.getFileStatus(f);

			BlockLocation[] blkLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
			for (BlockLocation currentLocation : blkLocations) {
				String[] hosts = currentLocation.getHosts();
				for (String host : hosts) {
					System.out.println(host);
				}
			}

			// 取得最后修改时间
			long modifyTime = fileStatus.getModificationTime();
			Date d = new Date(modifyTime);
			System.out.println(d);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public boolean deleteFile(String sPath) {
		boolean flag = false;
		File file = new File(sPath);
		// 路径为文件且不为空则进行删除
		if (file.isFile() && file.exists()) {
			file.delete();
			flag = true;
		} else if (!file.exists()) {
			flag = true;
		}
		return flag;
	}

	public static boolean makeDir(String dirName) {
		File file = new File(dirName);
		// 如果文件夹不存在则创建
		if (!file.exists() && !file.isDirectory()) {
			// System.out.println("//不存在");
			file.mkdir();
		}
		return true;
	}

	public boolean downloadHdfsDir(String hdfsDir, String ftpDir, String filter, FTPClientHelper ftp) {
		if (ftp == null) {
			return readHdfsDirToLocal(hdfsDir, ftpDir, filter);
		}
		return readHdfsDirToftp(hdfsDir, ftpDir, filter, ftp);
	}

	/**
	 * 将hdfs上的文件读到本地
	 * 
	 * @param hdfsDir
	 * @param localDir
	 * @param filter
	 * @return
	 */
	public boolean readHdfsDirToLocal(String hdfsDir, String localDir, String filter) {
		try {
			ArrayList<DatafileInfo> fileLst = listFiles(hdfsDir);
			if (fileLst.size() == 0)
				return false;
			for (int i = 0; i < fileLst.size(); i++) {
				final CalendarEx cal = new CalendarEx(new Date());

				if (!fileLst.get(i).filename.contains(filter)) {
					continue;
				}

				/*
				 * if(fileLst.get(i).modificationTime/1000 + 360 >cal._second) {
				 * continue; }
				 */
				makeDir(localDir);
				String hdfsFilename = hdfsDir + "/" + fileLst.get(i).filename;
				String localFilename = localDir + "/" + fileLst.get(i).filename;
				deleteFile(localFilename);

				if (!readFileFromHdfs(hdfsFilename, localDir, -1))// 读文件到本地
				{
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return true;
	}

	public boolean readHdfsDirToftp(String hdfsDir, String ftpDir, String filter, FTPClientHelper ftp) {
		try {
			ArrayList<DatafileInfo> fileLst = listFiles(hdfsDir);
			if (fileLst.size() == 0)
				return false;
			for (int i = 0; i < fileLst.size(); i++) {
				final CalendarEx cal = new CalendarEx(new Date());

				if (!fileLst.get(i).filename.contains(filter)) {
					continue;
				}

				/*
				 * if(fileLst.get(i).modificationTime/1000 + 360 >cal._second) {
				 * continue; }
				 */
				ftp.mkdir(ftpDir);
				String hdfsFilename = hdfsDir + "/" + fileLst.get(i).filename;
				String ftpFilename = ftpDir + "/" + fileLst.get(i).filename;
				ftp.delete(ftpFilename);

				if (!ftp.put(ftpFilename, GetInputStream(hdfsFilename)))// 读文件到ftp
				{
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return true;
	}

	public boolean putHdfsDirFromftp(String hdfsDir, String ftpDir, String filter, FTPClientHelper ftp) {
		try {
			ArrayList<DatafileInfo> fileLst = listFiles(hdfsDir);
			if (fileLst.size() == 0)
				return false;
			for (int i = 0; i < fileLst.size(); i++) {
				final CalendarEx cal = new CalendarEx(new Date());

				if (!fileLst.get(i).filename.contains(filter)) {
					continue;
				}

				/*
				 * if(fileLst.get(i).modificationTime/1000 + 360 >cal._second) {
				 * continue; }
				 */
				ftp.mkdir(ftpDir);
				String hdfsFilename = hdfsDir + "/" + fileLst.get(i).filename;
				String ftpFilename = ftpDir + "/" + fileLst.get(i).filename;
				ftp.delete(ftpFilename);

				if (!ftp.put(ftpFilename, GetInputStream(hdfsFilename)))// 读文件到ftp
				{
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return true;
	}

	/**
	 * 读取hdfs中的文件内容到本地
	 */
	@SuppressWarnings("deprecation")
	public boolean readFileFromHdfs(String hdfsFilename, String localPath, int nMaxSize) {
		try {
			Path f = new Path(hdfsFilename);

			FSDataInputStream dis = fs.open(f);
			File file = new File(localPath + "/" + f.getName());
			FileOutputStream os = new FileOutputStream(file);

			byte[] buffer = new byte[1024000];
			int length = 0;
			long nTotalLength = 0;
			int nCount = 0;
			while (MainSvr.bExitFlag != true && (length = dis.read(buffer)) > 0) {
				nCount++;
				os.write(buffer, 0, length);
				nTotalLength += length;
				if (nMaxSize > 0 && nTotalLength > nMaxSize)// ?
					break;

				/*
				 * if(nCount%100 == 0) { StringBuilder stringBuilder = new
				 * StringBuilder(); stringBuilder.append((new
				 * Date()).toLocaleString()); stringBuilder.append(
				 * ": Have move ");
				 * stringBuilder.append((nTotalLength/1024000));
				 * stringBuilder.append(" MB");
				 * System.out.println(stringBuilder.toString()); }
				 */
			}

			os.close();
			dis.close();
			if (MainSvr.bExitFlag != true)
				return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	public String viewFileFromHdfs(String hdfsFilename, int nMaxSize) {
		String str = "";
		if (nMaxSize > 1024000)
			nMaxSize = 1024000;// 最大1M
		try {
			Path f = new Path(hdfsFilename);

			FSDataInputStream dis = fs.open(f);
			byte[] buffer = new byte[1024];
			int length = 0;
			long nTotalLength = 0;
			while (MainSvr.bExitFlag != true && (length = dis.read(buffer)) > 0) {
				str += new String(buffer, 0, length, "UTF-8");
				nTotalLength += length;

				if (nMaxSize > 0 && nTotalLength > nMaxSize)
					break;
			}
			dis.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return str;
	}

	public FileStatus getFileStatus(String path) {
		System.out.println("getFileStatus：" + path);
		try {
			Path curPath = new Path(path);
			if (hdfs.exists(curPath) && (!curPath.isRoot())) {
				FileStatus fileStatus[] = fs.listStatus(curPath.getParent());
				int listlength = fileStatus.length;
				for (int i = 0; i < listlength; i++) {
					if (fileStatus[i].getPath().toString().contains(curPath.toString())) {
						System.out.println("getFileStatus：Success");
						return fileStatus[i];
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * list all file/directory
	 * 
	 * @param args
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws FileNotFoundException
	 */
	public void listFileStatus(String path) throws FileNotFoundException, IllegalArgumentException, IOException {
		FileStatus fileStatus[] = fs.listStatus(new Path(path));
		int listlength = fileStatus.length;
		for (int i = 0; i < listlength; i++) {
			if (fileStatus[i].isDirectory() == false) {
				System.out
						.println("filename:" + fileStatus[i].getPath().getName() + "\tsize:" + fileStatus[i].getLen());
			} else {
				String newpath = fileStatus[i].getPath().toString();
				listFileStatus(newpath);
			}
		}
	}

	public boolean ImportFileToSqlDb(String dbName, String hdfsDirName, String delichar, String dbURL, String userName,
			String userPwd) {
		try {
			if (!this.checkFileExist(hdfsDirName)) {
				return false;
			}

			FileStatus[] fileStatus = fs.listStatus(new Path(hdfsDirName));
			int listlength = fileStatus.length;

			if (listlength == 0)
				return false;

			List<String> columnNames = new ArrayList<String>();
			List<String> columnTypes = new ArrayList<String>();

			String driverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
			try {

				Class.forName(driverName);

				Connection connection = DriverManager.getConnection(dbURL, userName, userPwd);
				System.out.println("连接数据库成功");
				DatabaseMetaData dbmd = connection.getMetaData();
				ResultSet colRet = dbmd.getColumns(null, "%", dbName, "%");
				String sql = "insert into " + dbName + "(";

				while (colRet.next()) {
					String columnName = colRet.getString("COLUMN_NAME");
					sql += columnName + ",";
					String columnType = colRet.getString("TYPE_NAME");
					columnNames.add(columnName);
					columnTypes.add(columnType);
				}
				colRet.close();
				sql = sql.substring(0, sql.length() - 1);
				sql += ") values(";
				for (int i = 0; i < columnNames.size(); i++) {
					sql += "?,";
				}
				sql = sql.substring(0, sql.length() - 1);
				sql += ")";

				for (int i = 0; i < listlength; i++) {
					if (fileStatus[i].isFile() == true) {
						System.out.println("开始入库文件：" + fileStatus[i].getPath().getName());
						FSDataInputStream dis = fs.open(fileStatus[i].getPath());
						InputStreamReader isr = new InputStreamReader(dis, "utf-8");
						BufferedReader br = new BufferedReader(isr);

						String str = "";
						PreparedStatement ps = connection.prepareStatement(sql);
						int nRows = 0;
						while ((str = br.readLine()) != null) {
							String[] vct = str.split(delichar);
							if (vct.length != columnNames.size() && (vct.length != columnNames.size() + 1)) {
								break;
							}

							boolean bHasError = false;
							for (int j = 0; j < columnNames.size(); j++) {
								int jj = j;
								if (vct.length == (columnNames.size() + 1)) {
									jj = j + 1;
								}

								try {
									if (columnTypes.get(j).toLowerCase().contains("varchar")) {
										// System.out.println(vct[jj]);
										ps.setString(j + 1, vct[jj]);
									} else if (columnTypes.get(j).toLowerCase().equals("int")) {
										try {
											ps.setInt(j + 1, Integer.parseInt(vct[jj]));
										} catch (Exception e) {
											ps.setInt(j + 1, 0);
										}
									} else if (columnTypes.get(j).toLowerCase().contains("float")) {
										try {
											ps.setDouble(j + 1, Double.parseDouble(vct[jj]));
										} catch (Exception e) {
											ps.setDouble(j + 1, 0);
										}
									} else if (columnTypes.get(j).toLowerCase().equals("smallint")) {
										try {
											ps.setShort(j + 1, Short.parseShort(vct[jj]));
										} catch (Exception e) {
											ps.setShort(j + 1, Short.parseShort("0"));
										}
									} else if (columnTypes.get(j).toLowerCase().contains("tinyint")) {
										ps.setByte(j + 1, Byte.parseByte("0"));
									} else if (columnTypes.get(j).toLowerCase().contains("bigint")) {
										try {
											ps.setLong(j + 1, Long.parseLong(vct[jj]));
										} catch (Exception e) {
											ps.setLong(j + 1, 0L);
										}
									} else if (columnTypes.get(j).toLowerCase().contains("datetime")) {
										try {
											ps.setTimestamp(j + 1, java.sql.Timestamp.valueOf(vct[j]));
										} catch (Exception e) {
											ps.setTimestamp(j + 1, java.sql.Timestamp.valueOf("1970-01-01 08:00：00"));
										}
									} else {
										ps.clearParameters();
										bHasError = true;
										break;
									}

								} catch (Exception e) {
									e.printStackTrace();
									ps.clearParameters();
									bHasError = true;
									break;
								}

							}
							if (bHasError != true) {
								nRows++;
								ps.addBatch();
								if (nRows > 1000) {
									ps.executeBatch();
									nRows = 0;
								}
							}
						}
						if (nRows > 0) {
							ps.executeBatch();
							connection.commit();
							nRows = 0;
						}
						br.close();
						isr.close();
						dis.close();
						System.out.println("完成入库文件：" + fileStatus[i].getPath().getName());
					}
				}
				System.out.println("导入完成");
			}

			catch (Exception e) {
				e.printStackTrace();
				System.out.print("连接失败");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return true;
	}

	/**
	 *
	 * @param srcfile
	 *            复制的起始目录
	 * @param desfile
	 *            复制的最终目录，包括文件名
	 * @return
	 */
	public boolean hdfsCopyUtils(String srcfile, String desfile) {

		Path src = new Path(srcfile);
		Path dst = new Path(desfile);
		try {
			FileUtil.copy(src.getFileSystem(conf), src, dst.getFileSystem(conf), dst, false, conf);
		} catch (IOException e) {
			return false;
		}
		return true;
	}

	// 得到整个目录大小
	public long GetFileLen(Path path) {
		// SetHadoopRoot();
		long i = 0;
		if (fs == null) {
			return 0;
		} else {
			try {
				i = fs.getContentSummary(path).getLength();
			} catch (Exception e) {
				// TODO: handle exception
				return 0;
			}
		}
		return i;
	}

	public void deleteFiles(String dpath) {

		try {
			FileStatus fileStatus[] = fs.listStatus(new Path(dpath));
			for (int i = 0; i < fileStatus.length; i++) {
				if (!fileStatus[i].getPath().getName().startsWith("part-")) {
					hdfs.delete(fileStatus[i].getPath(), true);
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block

		}
	}
	
	public String getDateNow() {
		Date date = new Date();
		SimpleDateFormat timeFormat = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss] ");
		String formatDate = timeFormat.format(date);
		return formatDate;
	}

	public static void main(String[] args) {
		try {
			System.out.println("usage:HdfsPuter localFilename hdfsPath");
			HadoopFSOperation hdfs = new HadoopFSOperation("hdfs://10.151.64.160:8020;hdfs://10.151.64.150:8020");
			System.out.println(hdfs.checkFileExist("/mt_wlyh"));
			// hdfs.getMerge("/mt_wlyh/Data/mroxdrmerge/mro_loc/data_01_161209/TB_DTSIGNAL_SAMPLE_01_161209","d:/zf","sample",true);

			// System.out.println(hdfs.checkFileExist("hdfs://192.168.1.31:9000/mt_wlyh"));
			// ArrayList<DatafileInfo> fileList = hdfs.listSubDirs("/");
			// hdfs.ReCoconnect();

			// fileList = hdfs.listSubDirs("/");
			// System.out.println(fileList.size());
		} catch (Exception e) {
			e.printStackTrace();
		}
		// hdfs.putMerge("E:/调试/MRE/ERIC/decode",
		// "hdfs://10.139.6.169:9000/mt_wlyh/input/20151022","hw_1023.mre",".bcp");
		// hdfs.getMerge("E:/调试/MRE/ERIC/decode",
		// "hdfs://10.139.6.169:9000/mt_wlyh/Data/mroxdrmerge/gridstat/cqtgrid_01_151014/TB_SIGNAL_GRID_01_151014","hw_1023.grid","grid");
		// readFileFromHdfs("hdfs://10.139.6.169:9000/mt_wlyh/Data/mro/151014/test.txt","d:/");
	}
}
