package cn.mastercom.sssvr.main;

import java.io.File;
import java.util.concurrent.Callable;

public class DealFigureFileByReadFile implements Callable<String>
{
	private String figureFile;
	private int ThreadNum;
	private String enbidSimuFile;

	public DealFigureFileByReadFile(String figureFileName, int num, String enbidSimuFile)
	{
		this.figureFile = figureFileName;
		ThreadNum = num;
		this.enbidSimuFile = enbidSimuFile;
	}

	public String call() throws Exception
	{
		Thread lowFigureThread = null;
		Thread tenCoverfaceFigureThread = null;
		Thread fortyCoverfaceFigureThread = null;
		Thread tenBuildingFigureThread = null;
		Thread fortyBuildingFigureThread = null;
		if (figureFile.contains("coverface") && (figureFile.contains("40m") || figureFile.contains("40M")))
		{
			lowFigureThread = new Thread(new DealNoMergerFigure("fortySimuFile_low", new File(figureFile), ThreadNum,
					enbidSimuFile, "coverface", 40));
			lowFigureThread.start();
		}
		else if (figureFile.contains("coverface"))
		{
			tenCoverfaceFigureThread = new Thread(new DealNoMergerFigure("tenSimuFile", new File(figureFile), ThreadNum,
					enbidSimuFile, "coverface", 10));
			tenCoverfaceFigureThread.start();
			fortyCoverfaceFigureThread = new Thread(
					new DealMergeGrid("fortySimuFile", new File(figureFile), ThreadNum, enbidSimuFile, "coverface"));
			fortyCoverfaceFigureThread.start();
		}
		else if (figureFile.contains("building"))
		{
			tenBuildingFigureThread = new Thread(new DealNoMergerFigure("tenSimuFile", new File(figureFile), ThreadNum,
					enbidSimuFile, "building", 10));
			tenBuildingFigureThread.start();
			fortyBuildingFigureThread = new Thread(
					new DealMergeGrid("fortySimuFile", new File(figureFile), ThreadNum, enbidSimuFile, "building"));
			fortyBuildingFigureThread.start();
		}

		if (lowFigureThread != null)
		{
			lowFigureThread.join();
		}
		if (tenCoverfaceFigureThread != null)
		{
			tenCoverfaceFigureThread.join();
		}
		if (fortyCoverfaceFigureThread != null)
		{
			fortyCoverfaceFigureThread.join();
		}
		if (tenBuildingFigureThread != null)
		{
			tenBuildingFigureThread.join();
		}
		if (fortyBuildingFigureThread != null)
		{
			fortyBuildingFigureThread.join();
		}
		return "finish";
	}

}
