package cn.mastercom.sssvr.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import cn.mastercom.sssvr.util.GridTimeKey;
import cn.mastercom.sssvr.util.Sample_4G;

public class CreatCellGridFile
{ 
	public static void creatCellGridFile(ArrayList<File> localFiles, File savePath)
	{
		BufferedReader bf = null;
		BufferedWriter bw = null;
		HashMap<GridTimeKey, FigureCell> simuMap = new HashMap<GridTimeKey, FigureCell>();// 存放cellGrid
		String line = "";
		String temp[] = null;
		GridTimeKey key = null;
		FigureCell figurecell = null;
		double rsrp;

		try
		{
			for (int i = 0; i < localFiles.size(); i++)
			{
				bf = new BufferedReader(new InputStreamReader(new FileInputStream(localFiles.get(i))));
				while ((line = bf.readLine()) != null)
				{
					temp = line.split("\\t", -1);
					Sample_4G sample = new Sample_4G();
					sample.returnSample(temp);
					key = new GridTimeKey(sample.ilongitude, sample.ilatitude, 0, (int) sample.Eci);
					rsrp = sample.LteScRSRP;
					if (sample.LteScRSRP > 0)
					{
						rsrp = rsrp - 141;
					}
					if (!simuMap.containsKey(key))
					{
						figurecell = new FigureCell((int) sample.Eci, sample.ilongitude, sample.ilatitude, rsrp);
						figurecell.num = 1;
						simuMap.put(key, figurecell);
					}
					else
					{
						simuMap.get(key).num++;
						simuMap.get(key).rsrp = simuMap.get(key).rsrp + rsrp;
					}
				}
			}

			if (simuMap.size() > 0)
			{
				File file = new File(savePath.getPath() + "//Cell_Grid_" + new SimpleDateFormat("yyyyMMddhhmm").format(new Date()) + ".txt");
				bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
				for (GridTimeKey s : simuMap.keySet())
				{
					simuMap.get(s).rsrp = simuMap.get(s).rsrp / simuMap.get(s).num;
					bw.write(simuMap.get(s).getFigureCell());
					bw.newLine();
				}
			}
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
