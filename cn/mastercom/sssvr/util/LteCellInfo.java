package cn.mastercom.sssvr.util;


public class LteCellInfo
{
    public int enbid;
    public int cellid;
    public int cityid;
	public int radius;
	public int ilongitude;
	public int ilatitude;
	public int fcn;
	public int pci;
	public String cellName;
	public String cityName;
	public int indoor;
	public int angle;
	
	public long eci;
	public int high;

	
    public static LteCellInfo FillData(String[] values)
    {
    	LteCellInfo item = new LteCellInfo();
    	int i = 0;
    	item.enbid = DataGeter.GetInt(values[i++]);
    	item.cellid = DataGeter.GetInt(values[i++]);
    	item.cityid = DataGeter.GetInt(values[i++]);
    	item.radius = (int)DataGeter.GetDouble(values[i++]);
    	item.ilongitude = (int)(DataGeter.GetDouble(values[i++])*10000000);
    	item.ilatitude = (int)(DataGeter.GetDouble(values[i++])*10000000);
    	item.fcn = DataGeter.GetInt(values[i++]);
    	item.pci = DataGeter.GetInt(values[i++]);
    	item.cellName = DataGeter.GetString(values[i++]);
    	item.cityName = DataGeter.GetString(values[i++]);
    	item.indoor = DataGeter.GetInt(values[i++]);
    	item.angle = DataGeter.GetInt(values[i++]);
    	
    	item.eci = item.enbid * 256 + item.cellid;
    	return item;
    }
    
    public static LteCellInfo FillData2(String[] values)
    {
    	LteCellInfo item = new LteCellInfo();
    	int i = 0;
    	item.enbid = Integer.parseInt(values[i++]);
    	item.cellid = Integer.parseInt(values[i++]);
    	item.cityid = Integer.parseInt(values[i++]);
    	item.radius = (int)Double.parseDouble(values[i++]);
    	item.ilongitude = (int)(Double.parseDouble(values[i++])*10000000);
    	item.ilatitude = (int)(Double.parseDouble(values[i++])*10000000);
    	item.fcn =  Integer.parseInt(values[i++]);
    	item.pci =  Integer.parseInt(values[i++]);
    	item.cellName = values[i++];
    	item.cityName = values[i++];
    	item.indoor =  Integer.parseInt(values[i++]);
    	item.angle =  Integer.parseInt(values[i++]);
    	
    	item.eci = item.enbid * 256 + item.cellid;
    	return item;
    }
    
}
