package cn.mastercom.sssvr.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MergersFigureKu
{
	private File figureKuPaths[];
	private File commonFilePath;
	private File savePath;

	public MergersFigureKu(File figureKuPaths[], File commonFilePath, File savePath)
	{
		this.figureKuPaths = figureKuPaths;
		this.commonFilePath = commonFilePath;
		this.savePath = savePath;
	}

	public File[] getFigureKuPaths()
	{
		return figureKuPaths;
	}

	public File getCommonFilePath()
	{
		return commonFilePath;
	}

	public HashMap<Integer, PubParticipation> returngongcanMap()
	{
		HashMap<Integer, PubParticipation> gongcanMap = new HashMap<Integer, PubParticipation>();
		BufferedReader dr = null;
		try
		{
			dr = new BufferedReader(new InputStreamReader(new FileInputStream(commonFilePath)));
		} catch (FileNotFoundException e1)
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} // 读取公参文件
		String line = "";
		int gongcanKey;
		try
		{
			while ((line = dr.readLine()) != null)
			{
				String[] temp = line.split(",|\t", -1);
				PubParticipation pp = null;
				try
				{
					pp = new PubParticipation(temp);
				} catch (Exception e)
				{
					continue;
				}
				gongcanKey = pp.getEnodebId() * 256 + pp.getCellid();// 根据enbid和cellid组装eci作为key
				if (!gongcanMap.containsKey(gongcanKey))
				{
					gongcanMap.put(gongcanKey, pp);
				}
			}
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try
		{
			dr.close();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return gongcanMap;
	}

	/**
	 * 生成新的供mapreduce程序用的指纹库文件 返回生成文件的路径
	 */
	public void createFigureForHadoop()
	{
		new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				// TODO Auto-generated method stub
				ExecutorService exec = Executors.newFixedThreadPool(figureKuPaths.length);
				System.out.println("要处理的文件有" + figureKuPaths.length + "个");
				HashMap<Integer, PubParticipation> gongcanMap = returngongcanMap();
				for (int i = 0; i < figureKuPaths.length; i++)
				{
					exec.submit(new HadoopFigureCreate(figureKuPaths[i], gongcanMap, savePath));
					System.out.println("正在处理" + figureKuPaths[i]);
				}
				exec.shutdown();
				try
				{
					exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
					System.out.println("生成hadoop版指纹库完成，位于" + savePath.getPath());
				} catch (InterruptedException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}).start();
	}
}
