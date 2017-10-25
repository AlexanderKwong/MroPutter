package cn.mastercom.sssvr.util;

public class GridItem
{
	private int cityid;
	private int tllongitude;
	private int tllatitude;
	private int brlongitude;
	private int brlatitude;

	private GridItem(int cityid, int tllongitude, int tllatitude, int brlongitude, int brlatitude)
	{
		this.cityid = cityid;
		this.tllongitude = tllongitude;
		this.tllatitude = tllatitude;
		this.brlongitude = tllongitude;
		this.brlatitude = tllatitude;
	}

	public static GridItem GetGridItem(int cityid, int longitude, int latitude, int gridSize)
	{
		int tllongitude = 0;
		int tllatitude = 0;
		int brlongitude = 0;
		int brlatitude = 0;
		if (gridSize == 10)
		{
			tllongitude = longitude / 1000 * 1000;
			tllatitude = latitude / 900 * 900 + 900;
			brlongitude = tllongitude + 1000;
			brlatitude = tllatitude - 900;
		} else if (gridSize == 40)
		{
			tllongitude = longitude / 4000 * 4000;
			tllatitude = latitude / 3600 * 3600 + 3600;
			brlongitude = tllongitude + 4000;
			brlatitude = tllatitude - 3600;
		}
		GridItem girdItem = new GridItem(cityid, tllongitude, tllatitude, brlongitude, brlatitude);
		return girdItem;
	}

	public int getCityid()
	{
		return cityid;
	}

	public int getTLLongitude()
	{
		return tllongitude;
	}

	public int getTLLatitude()
	{
		return tllatitude;
	}

	public int getBRLongitude()
	{
		return brlongitude;
	}

	public int getBRLatitude()
	{
		return brlatitude;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		GridItem item = (GridItem) o;

		if (cityid == item.cityid && tllongitude == item.getTLLongitude() && tllatitude == item.getTLLatitude())
			return true;

		return false;
	}

	@Override
	public int hashCode()
	{
		return toString().hashCode();
	}

	@Override
	public String toString()
	{
		return cityid + "," + tllongitude + "," + tllatitude;
	}

}
