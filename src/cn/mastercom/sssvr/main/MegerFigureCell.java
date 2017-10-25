package cn.mastercom.sssvr.main;

public class MegerFigureCell
{
	private FigureCell figurecell;
	private int num;

	public MegerFigureCell()
	{
	}

	public MegerFigureCell(FigureCell figurecell, int num)
	{
		this.figurecell = figurecell;
		this.num = num;
	}

	public FigureCell getFigurecell()
	{
		return figurecell;
	}

	public void setFigurecell(FigureCell figurecell)
	{
		this.figurecell = figurecell;
	}

	public int getNum()
	{
		return num;
	}

	public void setNum(int num)
	{
		this.num = num;
	}

}
