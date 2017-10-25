package cn.mastercom.sssvr.main;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CreateEnbidFigure
{ 
	public static String ip;
	public static String database;
	public static String user;
	public static String passwd;

	public CreateEnbidFigure(String ip, String database, String user, String passwd)
	{
		CreateEnbidFigure.ip = ip;
		CreateEnbidFigure.database = database;
		CreateEnbidFigure.user = user;
		CreateEnbidFigure.passwd = passwd;
	}

	public static void CreatFigureByFile(final String[] simuFile, final String enbidSimuFile)
	{
		new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				ExecutorService exec = Executors.newFixedThreadPool(simuFile.length);
				System.out.println("要处理的文件有" + simuFile.length + "个");
				for (int i = 0; i < simuFile.length; i++)
				{
					exec.submit(new DealFigureFileByReadFile(simuFile[i], i, enbidSimuFile));
					System.out.println("正在处理" + simuFile[i]);
				}
				try
				{
					exec.shutdown();
					exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
					System.out.println("按enbid生成指纹库完毕！");
				} catch (InterruptedException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}).start();
	}

	public void CreatFigureBySqlDB(final String[] simuFile, final File enbidSimuFile)
	{
		new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				ExecutorService exec = Executors.newFixedThreadPool(simuFile.length);
				for (int i = 0; i < simuFile.length; i++)
				{
					// Future futurn =
					exec.submit(new DealFigureFile(simuFile[i], user, passwd, ip, database, i, enbidSimuFile));
					System.out.println("正在处理" + simuFile[i]);
					// try
					// {
					// futurn.get();
					// } catch (InterruptedException e)
					// {
					// // TODO Auto-generated catch block
					// e.printStackTrace();
					// } catch (ExecutionException e)
					// {
					// // TODO Auto-generated catch block
					// e.printStackTrace();
					// }
				}
				exec.shutdown();
			}
		}).start();
	}
}
