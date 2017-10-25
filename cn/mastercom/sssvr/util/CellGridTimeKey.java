package cn.mastercom.sssvr.util;

public class CellGridTimeKey
{
	private long eci = 0;
	private int itllongitude = 0;
	private int itllatitude = 0;
	private int ibrlongitude = 0;
	private int ibrlatitude = 0;

	public CellGridTimeKey()
	{
	}

	public CellGridTimeKey(long eci, int ilongitude, int ilatitude, int size)
	{
		this.eci = eci;
		if (size == 40)
		{
			this.itllongitude = ilongitude / 4000 * 4000;
			this.itllatitude = ilatitude / 3600 * 3600 + 3600;
			this.ibrlongitude = (itllongitude + 4000);
			this.ibrlatitude = (itllatitude - 3600);
		} else if (size == 10)
		{
			this.itllongitude = ilongitude / 1000 * 1000;
			this.itllatitude = ilatitude / 900 * 900 + 900;
			this.ibrlongitude = (itllongitude + 1000);
			this.ibrlatitude = (itllatitude - 900);
		}
	}

	public long getEci()
	{
		return eci;
	}

	public int getItllongitude()
	{
		return itllongitude;
	}

	public int getItllatitude()
	{
		return itllatitude;
	}

	@Override
	public int hashCode()
	{
		return toString().hashCode();
	}

	@Override
	public String toString()
	{
		return eci + "_" + itllongitude + "_" + itllatitude;
	}

	public int compareTo(CellGridTimeKey o)
	{
		if (eci > o.getEci())
		{
			return 1;
		} else if (eci < o.getEci())
		{
			return -1;
		} else
		{
			if (itllongitude > o.getItllongitude())
			{
				return 1;
			} else if (itllongitude < o.getItllongitude())
			{
				return -1;
			} else
			{
				return itllatitude - o.getItllatitude();
			}
		}
	}

	public boolean equals(Object obj)
	{
		if (obj == null)
		{
			return false;
		}
		if (this == obj)
		{
			return true;
		}

		if (obj instanceof CellGridTimeKey)
		{
			CellGridTimeKey s = (CellGridTimeKey) obj;
			return eci == s.getEci() && itllongitude == s.getItllongitude() && itllatitude == s.getItllatitude();
		} else
		{
			return false;
		}
	}

	public int getIbrlongitude()
	{
		return ibrlongitude;
	}

	public void setIbrlongitude(int ibrlongitude)
	{
		this.ibrlongitude = ibrlongitude;
	}

	public int getIbrlatitude()
	{
		return ibrlatitude;
	}

	public void setIbrlatitude(int ibrlatitude)
	{
		this.ibrlatitude = ibrlatitude;
	}
}
