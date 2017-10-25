package cn.mastercom.simu.adjust;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class OutLog
{
	public static void dosom(Exception e)
	{
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		PrintStream printStream = new PrintStream(byteArrayOutputStream);
		e.printStackTrace(printStream);
		String strlog = "MESSAGE：" + e.getMessage() + "\n" + byteArrayOutputStream.toString();
		System.out.println(strlog);
		printStream.close();
	}

}
