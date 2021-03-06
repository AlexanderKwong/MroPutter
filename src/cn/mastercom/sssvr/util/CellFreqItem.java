package cn.mastercom.sssvr.util;

public class CellFreqItem
{
	private long eci;
	private int freq;

	public CellFreqItem(long eci, int freq)
	{
		this.eci = eci;
		this.freq = freq;
	}

	public long getEci()
	{
		return eci;
	}

	public int getFreq()
	{
		return freq;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		CellFreqItem item = (CellFreqItem) o;

		if (eci == item.getEci() && freq == item.getFreq())
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
		return eci + "," + freq;
	}

}
