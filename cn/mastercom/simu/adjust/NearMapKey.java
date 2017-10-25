package cn.mastercom.simu.adjust;

public class NearMapKey
{
	public long ieci;
	public int fcn;
	public int pci;

	public NearMapKey(long ieci, int fcn, int pci)
	{
		this.ieci = ieci;
		this.fcn = fcn;
		this.pci = pci;
	}

	@Override
	public boolean equals(Object obj)
	{
		NearMapKey o = (NearMapKey) obj;
		return this.ieci == o.ieci && this.fcn == o.fcn && this.pci == o.pci;
	}

	@Override
	public String toString()
	{
		return this.ieci + "_" + this.fcn + "_" + this.pci;
	}

	@Override
	public int hashCode()
	{
		return toString().hashCode();
	}

}
