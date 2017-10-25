package cn.mastercom.sssvr.main;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;

public class ThreadWriteDTandCQT extends Thread
{
	private ArrayList<String> sampleClassList;
	private BufferedWriter Dw;

	public ThreadWriteDTandCQT(ArrayList<String> sampleClassList, BufferedWriter cqtDw)
	{
		super();
		this.sampleClassList = sampleClassList;
		this.Dw = cqtDw;
	}

	public ArrayList<String> getSampleClassList()
	{
		return sampleClassList;
	}

	public BufferedWriter getDw()
	{
		return Dw;
	}

	@Override
	public void run()
	{
		for (String sampleClass : sampleClassList)
		{
			try
			{
				if (sampleClass.split("\t", -1).length != 167)
				{
					continue;
				}
				Dw.write(sampleClass + "\r\n");
			} catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

}
