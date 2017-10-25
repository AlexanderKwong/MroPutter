package cn.mastercom.sssvr.main;

import java.util.HashMap;

import cn.mastercom.sssvr.util.GridTimeKey;

public class GridClass
{
	private GridTimeKey gridkey;

	private HashMap<Integer, FigureCell> eci_map = new HashMap<Integer, FigureCell>();
	private HashMap<Integer, FigureCell> earfcn_pci_map = new HashMap<Integer, FigureCell>();

	public GridClass(GridTimeKey gridkey)
	{
		this.gridkey = gridkey;
	}

	public HashMap<Integer, FigureCell> getEci_map()
	{
		return eci_map;
	}

	public void setEci_map(HashMap<Integer, FigureCell> eci_map)
	{
		this.eci_map = eci_map;
	}

	public HashMap<Integer, FigureCell> getEarfcn_pci_map()
	{
		return earfcn_pci_map;
	}

	public void setEarfcn_pci_map(HashMap<Integer, FigureCell> earfcn_pci_map)
	{
		this.earfcn_pci_map = earfcn_pci_map;
	}

	public GridTimeKey getGridkey()
	{
		return gridkey;
	}

}
