package cn.mastercom.sssvr.main;

import java.util.Comparator;

public class FigureCellComparator implements Comparator<FigureCell>
{
	@Override
	public int compare(FigureCell o1, FigureCell o2)
	{
		// TODO Auto-generated method stub
		if (o1.rsrp > o2.rsrp)
		{
			return -1;
		} else if (o1.rsrp < o2.rsrp)
		{
			return 1;
		}
		return 0;
	}
}
