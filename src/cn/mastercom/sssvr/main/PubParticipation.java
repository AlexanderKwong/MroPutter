package cn.mastercom.sssvr.main;

public class PubParticipation
{
	private static final String Spliter = "\t";
	private int enodebId;
	private int cellid;
	private int cityid;
	private double temp;
	private double ilongitude;
	private double ilatitud;
	private int fPoint;
	private int pci;
	private String cellName;
	private String city;
	private int indoor;
	private int angle;

	public PubParticipation(String pubtemp[]) throws Exception
	{
		this.enodebId = Integer.parseInt(pubtemp[0]);
		this.cellid = Integer.parseInt(pubtemp[1]);
		this.cityid = Integer.parseInt(pubtemp[2]);
		this.temp = Double.parseDouble(pubtemp[3]);
		this.ilongitude = Double.parseDouble(pubtemp[4]);
		this.ilatitud = Double.parseDouble(pubtemp[5]);
		this.fPoint = Integer.parseInt(pubtemp[6]);
		this.pci = Integer.parseInt(pubtemp[7]);
		this.cellName = pubtemp[8];
		this.city = pubtemp[9];
		this.indoor = Integer.parseInt(pubtemp[10]);
		this.angle = Integer.parseInt(pubtemp[11]);
	}

	public int getEnodebId()
	{
		return enodebId;
	}

	public void setEnodebId(int enodebId)
	{
		this.enodebId = enodebId;
	}

	public int getCellid()
	{
		return cellid;
	}

	public void setCellid(int cellid)
	{
		this.cellid = cellid;
	}

	public int getCityid()
	{
		return cityid;
	}

	public void setCityid(int cityid)
	{
		this.cityid = cityid;
	}

	public double getTemp()
	{
		return temp;
	}

	public void setTemp(double temp)
	{
		this.temp = temp;
	}

	public double getIlongitude()
	{
		return ilongitude;
	}

	public void setIlongitude(double ilongitude)
	{
		this.ilongitude = ilongitude;
	}

	public double getIlatitud()
	{
		return ilatitud;
	}

	public void setIlatitud(double ilatitud)
	{
		this.ilatitud = ilatitud;
	}

	public int getfPoint()
	{
		return fPoint;
	}

	public void setfPoint(int fPoint)
	{
		this.fPoint = fPoint;
	}

	public int getPci()
	{
		return pci;
	}

	public void setPci(int pci)
	{
		this.pci = pci;
	}

	public String getCellName()
	{
		return cellName;
	}

	public void setCellName(String cellName)
	{
		this.cellName = cellName;
	}

	public String getCity()
	{
		return city;
	}

	public void setCity(String city)
	{
		this.city = city;
	}

	public int getIndoor()
	{
		return indoor;
	}

	public void setIndoor(int indoor)
	{
		this.indoor = indoor;
	}

	public int getAngle()
	{
		return angle;
	}

	public void setAngle(int angle)
	{
		this.angle = angle;
	}

	public String getPPToString()
	{
		StringBuffer sb = new StringBuffer();
		sb.append(enodebId);
		sb.append(Spliter);
		sb.append(cellid);
		sb.append(Spliter);
		sb.append(cityid);
		sb.append(Spliter);
		sb.append(temp);
		sb.append(Spliter);
		sb.append(ilongitude);
		sb.append(Spliter);
		sb.append(ilatitud);
		sb.append(Spliter);
		sb.append(fPoint);
		sb.append(Spliter);
		sb.append(pci);
		sb.append(Spliter);
		sb.append(cellName);
		sb.append(Spliter);
		sb.append(city);
		sb.append(Spliter);
		sb.append(indoor);
		sb.append(Spliter);
		sb.append(angle);
		return sb.toString();
	}

}
