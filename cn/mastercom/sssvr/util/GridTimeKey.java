package cn.mastercom.sssvr.util;

public class GridTimeKey implements Comparable<GridTimeKey>
{
	private int cityid = 0;
	private int tllongitude = 0;
	private int tllatitude = 0;
	private int time = 0;
	private int level = -1;
	private int eci = 0;

	// 要写????默认构??函数，否则MapReduce的反射机制，无法创建该类报错
	public GridTimeKey()
	{
	}

	public GridTimeKey(String[] values) throws Exception
	{
		if (values.length == 6)
		{
			this.tllongitude = Integer.parseInt(values[2]);
			this.tllatitude = Integer.parseInt(values[3]);
			this.level = Integer.parseInt(values[4]);
		} else if (values.length == 4)
		{
			this.tllongitude = Integer.parseInt(values[1]);
			this.tllatitude = Integer.parseInt(values[2]);
		}
	}

	/**
	 *
	 * @param eci
	 * @param timeSpan
	 */
	public GridTimeKey(int cityid, int tllongitude, int tllatitude, int time, int level)
	{
		this.cityid = cityid;
		this.tllongitude = tllongitude;
		this.tllatitude = tllatitude;
		this.time = time;
		this.level = level;
	}

	/**
	 * 指纹库中一个栅格中同一个eci只有一个小区与之相对应
	 * 
	 * @param tllongitude
	 * @param tllatitude
	 * @param level
	 * @param eci
	 */
	public GridTimeKey(int tllongitude, int tllatitude, int level, int eci)
	{
		this.tllongitude = tllongitude;
		this.tllatitude = tllatitude;
		this.level = level;
		this.eci = eci;
	}

	public void gridTimeKey(int cityid, int time, int itllongitude, int itllatitude)
	{
		this.cityid = cityid;
		this.time = time;
		this.tllongitude = itllongitude;
		this.tllatitude = itllatitude;
	}

	public GridTimeKey(int tllongitude, int tllatitude, int level)
	{
		this.tllongitude = tllongitude;
		this.tllatitude = tllatitude;
		this.level = level;
	}

	public GridTimeKey(int tllongitude, int tllatitude)
	{
		this.tllongitude = tllongitude;
		this.tllatitude = tllatitude;
		this.level = -1;
	}

	public int getCityid()
	{
		return cityid;
	}

	public int getTllatitude()
	{
		return tllatitude;
	}

	public int getTllongitude()
	{
		return tllongitude;
	}

	public int getTime()
	{
		return time;
	}

	public int getLevel()
	{
		return level;
	}

	public int getEci()
	{
		return eci;
	}

	// 这个方法????Overrride
	@Override
	public int hashCode()
	{
		return toString().hashCode();
	}

	@Override
	public String toString()
	{
		return time + "_" + cityid + "_" + tllongitude + "_" + tllatitude + "_" + level + "_" + eci;
	}

	@Override
	public int compareTo(GridTimeKey o)
	{
		if (time > o.getTime())
		{
			return 1;
		} else if (time < o.getTime())
		{
			return -1;
		} else
		{
			if (cityid > o.getCityid())
			{
				return 1;
			} else if (cityid < o.getCityid())
			{
				return -1;
			} else
			{
				if (tllongitude > o.getTllongitude())
				{
					return 1;
				} else if (tllongitude < o.getTllongitude())
				{
					return -1;
				} else
				{
					if (tllatitude > o.getTllatitude())
					{
						return 1;
					} else if (tllatitude < o.getTllatitude())
					{
						return -1;
					} else
					{
						if (level > o.getLevel())
						{
							return 1;
						} else if (level < o.getLevel())
						{
							return -1;
						} else
						{
							if (eci > o.eci)
							{
								return 1;
							} else if (eci < o.eci)
							{
								return -1;
							}

						}
					}
					return 0;
				}
			}
		}

	}

	// 这个方法，写不写都不会影响的，至少我测的是这?? @Override
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

		if (obj instanceof GridTimeKey)
		{
			GridTimeKey s = (GridTimeKey) obj;

			return cityid == s.cityid && tllongitude == s.getTllongitude() && tllatitude == s.getTllatitude()
					&& time == s.getTime() && level == s.level && eci == s.eci;
		} else
		{
			return false;
		}
	}

}
