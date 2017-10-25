package cn.mastercom.sssvr.main;

import java.util.ArrayList;

import cn.mastercom.sssvr.util.GridTimeKey;

public class FigureCell
{
	public static final String Split = ",";
	public int buildingid = -1;
	public int ieci;
	public int ilongitude;
	public int ilatitude;
	public int level = -1;
	public double rsrp;
	public int num;// 生成cell_grid时候，记录相同key值得cell有多少个

	public GridTimeKey key()
	{
		return new GridTimeKey(ilongitude, ilatitude, level);
	}

	public ArrayList<Integer> location()
	{
		ArrayList<Integer> location = new ArrayList<Integer>();
		location.add(ilongitude);
		location.add(ilatitude);
		location.add(level);
		location.add(buildingid);
		return location;
	}

	public FigureCell(String[] values) throws Exception
	{
		if (values.length == 6)
		{
			if (values[0] != null || !"".equals(values[0]))
			{
				this.buildingid = Integer.parseInt(values[0]);
			}
			this.ieci = Integer.parseInt(values[1]);
			this.ilongitude = Integer.parseInt(values[2]);
			this.ilatitude = Integer.parseInt(values[3]);
			this.level = Integer.parseInt(values[4]);
			this.rsrp = Double.parseDouble(values[5]);
		}
		else if (values.length == 4)
		{
			this.buildingid = -1;
			this.ieci = Integer.parseInt(values[0]);
			this.ilongitude = Integer.parseInt(values[1]);
			this.ilatitude = Integer.parseInt(values[2]);
			this.rsrp = Double.parseDouble(values[3]);
		}
	}

	public FigureCell(String[] values, int size) throws Exception
	{
		if (size == 10)
		{
			if (values.length == 6)
			{
				if (values[0] != null || !"".equals(values[0]))
				{
					this.buildingid = Integer.parseInt(values[0]);
				}
				this.ieci = Integer.parseInt(values[1]);
				this.ilongitude = Integer.parseInt(values[2]);
				this.ilatitude = Integer.parseInt(values[3]);
				this.level = Integer.parseInt(values[4]);
				this.rsrp = Double.parseDouble(values[5]);
			}
			else if (values.length == 4)
			{
				this.buildingid = -1;
				this.ieci = Integer.parseInt(values[0]);
				this.ilongitude = Integer.parseInt(values[1]);
				this.ilatitude = Integer.parseInt(values[2]);
				this.rsrp = Double.parseDouble(values[3]);
			}
		}
		else if (size == 40)
		{
			if (values.length == 6)
			{
				if (values[0] != null || !"".equals(values[0]))
				{
					this.buildingid = Integer.parseInt(values[0]);
				}
				this.ieci = Integer.parseInt(values[1]);
				this.ilongitude = (Integer.parseInt(values[2]) / 4000) * 4000 + 2000;
				this.ilatitude = (Integer.parseInt(values[3]) / 3600) * 3600 + 1800;
				this.level = Integer.parseInt(values[4]);
				this.rsrp = Double.parseDouble(values[5]);
			}
			else if (values.length == 4)
			{
				this.buildingid = -1;
				this.ieci = Integer.parseInt(values[0]);
				this.ilongitude = (Integer.parseInt(values[1]) / 4000) * 4000 + 2000;
				this.ilatitude = (Integer.parseInt(values[2]) / 3600) * 3600 + 1800;
				this.rsrp = Double.parseDouble(values[3]);
			}
		}

	}

	public FigureCell(int eci, int ilongitude, int ilatitude, double rsrp)
	{
		this.ieci = eci;
		this.ilongitude = ilongitude;
		this.ilatitude = ilatitude;
		this.rsrp = rsrp;
	}

	public FigureCell(int buildingid, int ieci, int ilongitude, int ilatitude, int level, double rsrp, int num)
	{
		this.buildingid = buildingid;
		this.ieci = ieci;
		this.ilongitude = ilongitude;
		this.ilatitude = ilatitude;
		this.level = level;
		this.rsrp = rsrp;
		this.num = num;
	}

	public FigureCell()
	{
	}

	public FigureCell(FigureCell temp)
	{
		this.buildingid = temp.buildingid;
		this.ieci = temp.ieci;
		this.ilongitude = temp.ilongitude;
		this.ilatitude = temp.ilatitude;
		this.level = temp.level;
		this.rsrp = temp.rsrp;
		this.num = temp.num;
	}

	public String getFigureCell()
	{
		StringBuffer sb = new StringBuffer();
		if (buildingid != -1)
		{
			sb.append(buildingid);
			sb.append(Split);
		}
		sb.append(ieci);
		sb.append(Split);
		sb.append(ilongitude);
		sb.append(Split);
		sb.append(ilatitude);
		sb.append(Split);
		if (level != -1)
		{
			sb.append(level);
			sb.append(Split);
		}
		sb.append(rsrp);
		return sb.toString();
	}

	/**
	 * 不论是building还是coverface都返回统一格式6位
	 * 
	 * @return
	 */
	public String returnFigureAll()
	{
		StringBuffer sb = new StringBuffer();
		sb.append(buildingid);
		sb.append(Split);
		sb.append(ieci);
		sb.append(Split);
		sb.append(ilongitude);
		sb.append(Split);
		sb.append(ilatitude);
		sb.append(Split);
		sb.append(level);
		sb.append(Split);
		sb.append(rsrp);
		return sb.toString();
	}

}
