package cn.mastercom.sssvr.test;

import java.io.File;

public class test11 {
	public static void main(String[] args) {
		
		String dataFile = "d:/bcp_20160928dfd/MRO_1231234_1834.bcp.gz";
		String[] filename = new File(dataFile).getName().split("_");
		String date = "";
		if(filename.length >=7)
		{
			date = filename[6].substring(2, 8);
		}
		else
		{
			String parentPath = new File(dataFile).getParentFile().getName();
			if(parentPath.startsWith("bcp"))
			{
				date = parentPath.substring(6,12);
			}
		}
		
		byte[] spl = new byte[1];
		spl[0] =		0x01;
		String sp2 = new String(spl);;
		
		String sss = "12455" + sp2 + "ddddd";
		String[] vct = sss.split(sp2,-1);
		if(vct.length>0)
		{
			System.out.println(vct[0]);
		}
	}
}
