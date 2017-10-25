package cn.mastercom.simu.adjust;

public class OTT_Cell_Grid
{
	public long ieci;
	public int ilongitude;
	public int ilatitude;
	public float rsrpavg;
	public float rsrpmax;
	public float rsrpmin;
	public int samplecnt;
	public int pci;
	public int fcn;
	public float totalrsrp;
	public static final String spliter = "\t";

	public OTT_Cell_Grid(long ieci, int ilongitude, int ilatitude, int pci, int fcn)
	{
		this.ieci = ieci;
		this.ilongitude = ilongitude;
		this.ilatitude = ilatitude;
		this.pci = pci;
		this.fcn = fcn;
	}

	public void deal(int rsrp)
	{
		if (rsrp > -150 && rsrp <= -30)
		{
			if (rsrpmax == 0 || rsrpmax < rsrp)
			{
				rsrpmax = rsrp;
			}
			if (rsrpmin == 0 || rsrpmin > rsrp)
			{
				rsrpmin = rsrp;
			}
			samplecnt++;
			totalrsrp += rsrp;
		}
	}

	public String toline()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(ieci);
		bf.append(spliter);
		bf.append(ilongitude);
		bf.append(spliter);
		bf.append(ilatitude);
		bf.append(spliter);
		if (samplecnt > 0)
		{
			bf.append(totalrsrp / samplecnt);
		}
		else
		{
			bf.append(0);
		}
		bf.append(spliter);
		bf.append(rsrpmax);
		bf.append(spliter);
		bf.append(rsrpmin);
		bf.append(spliter);
		bf.append(samplecnt);
		bf.append(spliter);
		bf.append(pci);
		bf.append(spliter);
		bf.append(fcn);
		return bf.toString();
	}

}