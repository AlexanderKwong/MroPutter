package cn.mastercom.sssvr.main;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.Callable;

public class HadoopFigureCreate implements Callable<String>
{

	public File figureFile;
	public HashMap<Integer, PubParticipation> gongcanMap;
	public File resultFolder;

	public HadoopFigureCreate(File figureFile, HashMap<Integer, PubParticipation> gongcanMap, File resultFolder)
	{
		super();
		this.figureFile = figureFile;
		this.gongcanMap = gongcanMap;
		this.resultFolder = resultFolder;
	}

	@Override
	public String call() throws Exception
	{
		Thread lowFortyFigure = null;
		Thread tenFigure = null;
		Thread FortyFigure = null;
		if (figureFile.getPath().contains("coverface")
				&& (figureFile.getPath().contains("40m") || figureFile.getPath().contains("40M")))
		{
			File outFile = new File(resultFolder.getPath() + "\\lowForty_" + figureFile.getName());
			lowFortyFigure = new Thread(new HadoopFigureDealNoMergeGrid(figureFile, gongcanMap, outFile));
			lowFortyFigure.start();
		} else
		{
			File tenoutFile = new File(resultFolder.getPath() + "\\ten_" + figureFile.getName());
			File fortyoutFile = new File(resultFolder.getPath() + "\\forty_" + figureFile.getName());
			tenFigure = new Thread(new HadoopFigureDealNoMergeGrid(figureFile, gongcanMap, tenoutFile));
			tenFigure.start();
			FortyFigure = new Thread(new HadoopFigureDealMergeGrid(figureFile, gongcanMap, fortyoutFile));
			FortyFigure.start();
		}
		if (lowFortyFigure != null)
		{
			lowFortyFigure.join();
		}
		if (tenFigure != null)
		{
			tenFigure.join();
		}
		if (FortyFigure != null)
		{
			FortyFigure.join();
		}
		return "finish";
	}

}
