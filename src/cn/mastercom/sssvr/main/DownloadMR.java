package cn.mastercom.sssvr.main;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;

import cn.mastercom.sssvr.util.HadoopFSOperation;



public class DownloadMR extends Thread{
	
	private static String HdfsRoot = "";
	private static String LocalPath = "";
	private static String RemotePath = "";
	private static String SelectDate = "";
	private static String FilterKey = "";
	private static int ThreadSize = 0;
	
	private static HadoopFSOperation hdfsoper = null;
	private static String separator = System.getProperty("file.separator");
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void readConfigInfo() {
		try {
			// XMLWriter writer = null;// 声明写XML的对象
			SAXReader reader = new SAXReader();

			OutputFormat format = OutputFormat.createPrettyPrint();
			format.setEncoding("GBK");// 设置XML文件的编码格式

			String filePath = "conf/config_downloadMr.xml";
			File file = new File(filePath);
			if (file.exists()) {
				Document doc = reader.read(file);// 读取XML文件

				{
					List<String> list = doc.selectNodes("//comm/HdfsRoot");
					Iterator iter = list.iterator();
					while (iter.hasNext()) {
						Element element = (Element) iter.next();
						HdfsRoot = element.getText();
						break;
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/SelectDate");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							SelectDate = element.getText();
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
					List<String> list = doc.selectNodes("//comm/FilterKey");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							FilterKey = element.getText();
							break;
						}
					}
				}
				
				{
					List<String> list = doc.selectNodes("//comm/ThreadSize");
					if (list != null) {
						Iterator iter = list.iterator();
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							ThreadSize = Integer.parseInt(element.getText());
							break;
						}
					}
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void init(){
		System.out.println(getDateNow() +"程序初始化............");
		readConfigInfo();
		Configuration conf = null ;
		try{
			conf = new Configuration();
			conf.set("hadoop.security.bdoc.access.id", "5612833cb2e74cb84659");
			conf.set("hadoop.security.bdoc.access.key", "e622412492ee28fd803094633feef0faeee73f51");
	//		conf.set("mapreduce.job.queuename", "root.bdoc.renter_1.renter_33.renter_36.dev_54");
		}catch( Exception e){
			System.out.println(getDateNow() + "configurarion 初始化失败."+ e.getMessage());
		}	
		try {
			hdfsoper = new HadoopFSOperation(conf);
//			hdfsoper = new HadoopFSOperation(HdfsRoot);
		} catch (Exception e) {
			System.out.println(getDateNow() + "hdfsoper 初始化失败.."+ e.getMessage());
		}
		
		DownloadMR down = new DownloadMR();
		down.start();
	}
	
	public String getDateNow() {
		Date date = new Date();
		SimpleDateFormat timeFormat = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss] ");
		String formatDate = timeFormat.format(date);
		return formatDate;
	}
	
	public void run(){
		try{
			System.out.println(getDateNow() +"开始计算文件个数.......");
			getMRFile();
		}catch(Exception e){
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		}
	}
	
	public void getMRFile(){
		String hdfsDir = RemotePath + "/" + SelectDate;
		List<String> fileList = new ArrayList<String>();
		getMRFile(hdfsDir ,fileList);
		getFileToLocal(fileList);

	}		
	
	public void getMRFile(String path,List<String> fileList){		
		FileStatus[] fileStatus = hdfsoper.listStatus(path);
		System.out.println(getDateNow() + "扫描到的文件数量为: "+ fileStatus.length);
		for (int i = 0; i< fileStatus.length; i++ ) {		
			if(fileStatus[i].isDirectory()){
				getMRFile(fileStatus[i].getPath().toString(),fileList);
			}else{
				fileList.add(fileStatus[i].getPath().toString());
			}
		}
	}
	
	List<HadoopFSOperation.TaskInfo> taskList = new ArrayList<HadoopFSOperation.TaskInfo>();
	List<String> fileLst = new ArrayList<String>();
	List<String> fileNameList = new ArrayList<String>();
	
	@SuppressWarnings({ "static-access" })
	public void getFileToLocal(List<String> fileList){
		try
		{
			if(fileList.size() == 0){
				return;
			}
			hdfsoper.makeDir(LocalPath);
			int count = 0;	
			String fileName = null;
			File file = null;
			for (int i = 0; i < fileList.size(); i++)
			{
				fileName = fileList.get(i);
				file = new File(fileName);
				
				if(filterFile(fileName)){
					continue;
				}else{
					fileLst.add(fileName);
					fileNameList.add(LocalPath + separator + file.getName());
				}
				
				if (count > 30)
				{
					HadoopFSOperation.TaskInfo ti = hdfsoper.new TaskInfo();
					ti.fileLst = fileList;
					ti.fileName = fileNameList;
					taskList.add(ti);					
					fileLst = new ArrayList<String>();
					count = 0;
				}
				count++;
			}
			if ( count > 0)
			{
				collection();
			}
			mkdirThreadPool(taskList);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void collection(){
		HadoopFSOperation.TaskInfo ti = hdfsoper.new TaskInfo();
		ti.fileLst = fileLst;
		ti.fileName = fileNameList;
		taskList.add(ti);
	}
	
	public boolean filterFile(String path){		
		if (FilterKey.trim().length() > 0) {
			String[] split = FilterKey.trim().split(",");
			boolean bFind = false;
			for (String str : split) {
				if (path.contains(str)) {
					bFind = true;
					break;
				}			
			}
			if(bFind){
				return true;
			}
		}
		return false;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void mkdirThreadPool(List<HadoopFSOperation.TaskInfo> taskList){
		// 创建一个线程池
		ExecutorService pool = Executors.newFixedThreadPool(ThreadSize);
		// 创建多个有返回值的任务
		List<Future> list = new ArrayList<Future>();
		for (HadoopFSOperation.TaskInfo taskinfo : taskList)
		{
			Callable fm = hdfsoper.new FileDownloadCallables(taskinfo);

			// 执行任务并获取Future对象
			Future f = pool.submit(fm);
			list.add(f);
		}		
		// 获取所有并发任务的运行结果
		try
		{
			for (Future f : list)
			{
				System.out.println(getDateNow() + ">>>" + f.get().toString());
			}			
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		// 关闭线程池
		pool.shutdown();
		System.out.println(getDateNow() + "数据下载完成！");
		
	}	
	
	public static void main(String[] args) {
		DownloadMR down = new DownloadMR();
		down.init();
	}
	
	
}
