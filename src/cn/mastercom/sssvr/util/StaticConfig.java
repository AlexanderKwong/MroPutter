package cn.mastercom.sssvr.util;

public class StaticConfig 
{
	public final static String DataSlipter = "	";
	public final static String DataSliper2 = "		";
	
    //data type
	public final static int DataType_SIGNAL_MR_All = 1;
	public final static int DataType_SIGNAL_XDR = 2;
	
	//user type
	public final static int TestType_DT = 1;
	public final static int TestType_CQT = 2;
	public final static int TestType_DT_EX = 3;
	public final static int TestType_CPE = 4;
	public final static int TestType_OTHER = 100;
	public final static int TestType_ERROR = 101;
	
	//hive split
	public final static char HiveSplit = 0x01;
	
	//code type
	public final static String UTFCode = "UTF-8";
	
	//input data 
	public final static String InputPath_NoData = "NODATA";
	
	//abnormal value define
	public final static double Double_Abnormal = -1000000;
	public final static long Long_Abnormal = -1000000;
	public final static float Float_Abnormal = -1000000;
	public final static int Int_Abnormal = -1000000;
	public final static short Short_Abnormal = -255;
	public final static short TinyInt_Abnormal = 255;
	public final static String String_Abnormal = "";
	public final static int Natural_Abnormal = -1;
	
	
}


