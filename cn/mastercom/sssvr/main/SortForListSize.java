package cn.mastercom.sssvr.main;

import java.util.Comparator;

public class SortForListSize implements Comparator<OneGridResult>
{

	@Override
	public int compare(OneGridResult o1, OneGridResult o2)
	{
		// TODO Auto-generated method stub
		return o1.getOneresult().size() - o2.getOneresult().size();
	}
}
