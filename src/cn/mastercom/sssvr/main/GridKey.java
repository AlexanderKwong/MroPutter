package cn.mastercom.sssvr.main;

public class GridKey
{
	public int longitude = 0;
	public int latitude = 0;
	public int level = -1;
	public int eci = 0;

	public GridKey()
	{
	}

	public GridKey(String value, int type)
	{
		String[] values = value.split(",|\t", -1);
		if (type == 10)
		{
			if (values.length == 4)
			{
				longitude = Integer.parseInt(values[1]);
				latitude = Integer.parseInt(values[2]);
			} else if (values.length == 6)
			{
				longitude = Integer.parseInt(values[2]);
				latitude = Integer.parseInt(values[3]);
				level = Integer.parseInt(values[4]);
			}
		} else if (type == 40)
		{
			if (values.length == 4)
			{
				longitude = (Integer.parseInt(values[1]) / 4000) * 4000 + 2000;
				latitude = (Integer.parseInt(values[2]) / 3600) * 3600 + 1800;
			} else if (values.length == 6)
			{
				longitude = (Integer.parseInt(values[2]) / 4000) * 4000 + 2000;
				latitude = (Integer.parseInt(values[3]) / 3600) * 3600 + 1800;
				level = Integer.parseInt(values[4]);
			}
		}
	}

	public void CreateKey(String value, int type)
	{
		String[] values = value.split(",|\t", -1);
		if (type == 10)
		{
			if (values.length == 4)
			{
				longitude = Integer.parseInt(values[1]);
				latitude = Integer.parseInt(values[2]);
			} else if (values.length == 6)
			{
				longitude = Integer.parseInt(values[2]);
				latitude = Integer.parseInt(values[3]);
				level = Integer.parseInt(values[4]);
			}
		} else if (type == 40)
		{
			if (values.length == 4)
			{
				longitude = (Integer.parseInt(values[1]) / 4000) * 4000 + 2000;
				latitude = (Integer.parseInt(values[2]) / 3600) * 3600 + 1800;
			} else if (values.length == 6)
			{
				longitude = (Integer.parseInt(values[2]) / 4000) * 4000 + 2000;
				latitude = (Integer.parseInt(values[3]) / 3600) * 3600 + 1800;
				level = Integer.parseInt(values[4]);
			}
		}

	}

	public GridKey(int eci, int longitude, int latitude, int level)
	{
		this.eci = eci;
		this.longitude = longitude;
		this.latitude = latitude;
		this.level = level;
	}

	public GridKey(FigureCell figurecell, int type, boolean merge)
	{
		if (merge)
		{
			this.eci = figurecell.ieci;
		}
		if (type == 10)
		{
			this.latitude = figurecell.ilatitude;
			this.longitude = figurecell.ilongitude;
		} else if (type == 40)
		{
			this.latitude = (figurecell.ilatitude / 3600) * 3600 + 1800;
			this.longitude = (figurecell.ilongitude / 4000) * 4000 + 2000;
		}
		this.level = figurecell.level;
	}

	public void setGridKey(FigureCell figurecell, int type)
	{
		if (type == 10)
		{
			this.latitude = figurecell.ilatitude;
			this.longitude = figurecell.ilongitude;
		} else if (type == 40)
		{
			this.latitude = (figurecell.ilatitude / 3600) * 3600 + 1800;
			this.longitude = (figurecell.ilongitude / 4000) * 4000 + 2000;
		}
		this.level = figurecell.level;
	}

	public void setGridKey(GridKey key)
	{
		this.level = key.level;
		this.longitude = key.longitude;
		this.latitude = key.latitude;
		this.level = key.level;
		this.eci = key.eci;

	}

	@Override
	public String toString()
	{
		return eci + "_" + longitude + "_" + latitude + "_" + level;
	}

	@Override
	public int hashCode()
	{
		// TODO Auto-generated method stub
		return toString().hashCode();
	}

	@Override
	public boolean equals(Object obj)
	{
		// TODO Auto-generated method stub
		if (obj == null)
		{
			return false;
		}
		if (this == obj)
		{
			return true;
		}
		if (obj instanceof GridKey)
		{
			GridKey s = (GridKey) obj;
			return eci == s.eci && longitude == s.longitude && latitude == s.latitude && level == s.level;
		} else
		{
			return false;
		}
	}

}
