package cn.mastercom.simu.adjust;

public class CellGridKey
{

	public int longtitude;
	public int latitude;
	public long eci;

	public CellGridKey returnTenCellGridKey(int longtitude, int latitude, long eci)
	{
		CellGridKey tenGridKey = new CellGridKey();
		tenGridKey.longtitude = (longtitude / 1000) * 1000 + 500;
		tenGridKey.latitude = (latitude / 900) * 900 + 450;
		tenGridKey.eci = eci;
		return tenGridKey;
	}

	public CellGridKey returnFortyCellGridKey(int longtitude, int latitude, long eci)
	{
		CellGridKey tenGridKey = new CellGridKey();
		tenGridKey.longtitude = (longtitude / 4000) * 4000 + 2000;
		tenGridKey.latitude = (latitude / 3600) * 3600 + 1800;
		tenGridKey.eci = eci;
		return tenGridKey;
	}

	@Override
	public String toString()
	{
		return longtitude + "_" + latitude + "_" + eci;
	}

	@Override
	public int hashCode()
	{
		return toString().hashCode();
	}

	@Override
	public boolean equals(Object obj)
	{
		CellGridKey o = (CellGridKey) obj;
		return this.longtitude == o.longtitude && this.latitude == o.latitude && this.eci == o.eci;
	}
}
