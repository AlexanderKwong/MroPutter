package cn.mastercom.sssvr.sparktask;

import java.io.File;

import org.apache.log4j.Logger;

import cn.mastercom.sssvr.util.HdfsExplorer;






public class MainModel {
	protected Logger LOG = Logger.getLogger(MainModel.class);
	private static MainModel instance;
	
	public static MainModel GetInstance(){
		if(instance == null)
    	{
    		instance = new MainModel();
    	}
    	return instance;
	}
	
	private MainModel()
    {
    	File file = new File(HdfsExplorer.class.getProtectionDomain().getCodeSource().getLocation().getPath());
    	if(file.isDirectory())
    	{
//    		exePath = file.getPath();
    		exePath = file.getParent();
    	}
    	else 
    	{
    		exePath = file.getParent();
    	}
    	
    }
	
	private String exePath = "";	
	public final String GetRootPath()
	{
		return exePath;
	}
	
	private SparkTaskConfig sparkConfig;
	public final SparkTaskConfig GetSparkConfig()
	{
		return sparkConfig;
	}
	
	public void loadConfig()
	{		
		LOG.info("程序启动路径是： " + exePath);
//			System.out.println("程序启动路径是： " + exePath);
			sparkConfig = new SparkTaskConfig(GetRootPath()+ "\\conf\\SparkConfig.xml");
			sparkConfig.loadConfigure();	
			//假如没有，要新生成
			//dcconfig.saveConfigure();
		}
}
