package cn.mastercom.sssvr.util;

public class Stat_Sample_4G
{
	public long RSRP_nTotal; // 总数
	public long RSRP_nSum; // 总和
	public long[] RSRP_nCount; // [-141,-110),[-110,-95),[-95,-80),[-80,-65),[-65,-50),[-50,)

	public long SINR_nTotal; // 总数
	public long SINR_nSum; // 总和
	public long[] SINR_nCount; // [-20,0),[0,5),[5,10),[10,15),[15,20),[20,25),[25,50),[50,)

	public long RSRP100_SINR0; // RSRP>-100 and SINR>0
	public long RSRP105_SINR0; // RSRP>=-105 and SINR>=0
	public long RSRP110_SINR3; // RSRP>-110 and SINR>3
	public long RSRP110_SINR0; // RSRP>-110 and SINR>=0

	public long UpLen;
	public long DwLen;
	public long DurationU;
	public long DurationD;
	public float AvgUpSpeed;
	public float MaxUpSpeed;
	public float AvgDwSpeed;
	public float MaxDwSpeed;

	public long UpLen_1M;
	public long DwLen_1M;
	public long DurationU_1M;
	public long DurationD_1M;
	public float AvgUpSpeed_1M;
	public float MaxUpSpeed_1M;
	public float AvgDwSpeed_1M;
	public float MaxDwSpeed_1M;

	public Stat_Sample_4G()
	{
		Clear();
	}

	public void Clear()
	{
		RSRP_nCount = new long[6];
		SINR_nCount = new long[8];

	};

}
