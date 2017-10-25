package cn.mastercom.sssvr.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;

public class FingerFilter
{
	private File[] FingerFilePaths;
	private File FingerFilePath;

	public FingerFilter(File[] fingerFilepaths, File FilePaths)
	{
		FingerFilePath = FilePaths;
		FingerFilePaths = fingerFilepaths;
	}

	/**
	 * 将10*10的指纹库组成map
	 * 
	 * @return
	 */

	public HashMap<GridKey, Integer> mapFilter()
	{
		long num = 0;
		HashMap<GridKey, Integer> FigureMap = new HashMap<GridKey, Integer>();

		FileInputStream fs;
		BufferedReader bf = null;
		String line = "";
		for (int i = 0; i < FingerFilePaths.length; i++)
		{
			try
			{
				fs = new FileInputStream(FingerFilePaths[i]);
				bf = new BufferedReader(new InputStreamReader(fs));

				while ((line = bf.readLine()) != null)
				{
					num++;
					String[] temp = line.split(",|\t", -1);
					GridKey gk = null;
					gk = new GridKey(Integer.parseInt(temp[0]), Integer.parseInt(temp[1]) / 4000 * 4000,
							Integer.parseInt(temp[2]) / 3600 * 3600, -1);
					if (!FigureMap.containsKey(gk))
					{
						FigureMap.put(gk, 1);
					}
					if (num % 10000 == 0)
					{
						System.out.println("10*10的指纹库已读取" + num + "行");
					}
				}
			} catch (Exception e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally
			{
				try
				{
					bf.close();
				} catch (IOException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return FigureMap;
	}

	public void compareFinger()
	{

		new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				// TODO Auto-generated method stub
				BufferedWriter bw = null;
				BufferedReader bfs = null;
				long num = 0;
				try
				{
					bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
							FingerFilePath.getParent() + "\\Finished_" + FingerFilePath.getName() , true)));
				} catch (FileNotFoundException e1)
				{
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				try
				{
					FileInputStream fis = new FileInputStream(FingerFilePath);
					bfs = new BufferedReader(new InputStreamReader(fis));
					String line = "";
					HashMap<GridKey, Integer> mapFilter = mapFilter();
					while ((line = bfs.readLine()) != null)
					{
						num++;
						if (num % 10000 == 0)
						{
							System.out.println("40*40的指纹库读取：" + num + "行");
						}
						String[] temp = line.split(",|\t", -1);
						GridKey gk = null;
						gk = new GridKey(Integer.parseInt(temp[0]), Integer.parseInt(temp[1]) / 4000 * 4000,
								Integer.parseInt(temp[2]) / 3600 * 3600, -1);
						if (!mapFilter.containsKey(gk))
						{
							bw.write(line);
							bw.newLine();
						}
					}
					System.out.println("去重后的40*40指纹库文件位于：" + FingerFilePath.getParent() + "\\Finished_"
							+ FingerFilePath.getName() );

				} catch (Exception e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally
				{
					try
					{
						bfs.close();
						bw.close();
					} catch (IOException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
			}

		}).start();

	}
}
