package cn.mastercom.sssvr.main;

import java.io.BufferedWriter;
import java.io.File;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import cn.mastercom.sqlhp.DBHelper;

class DealFigureFile implements Callable<String>
{
	private String figureFileName;
	private String user;
	private String passwd;
	private String ip;
	private String database;
	private int ThreadNum;
	private File enbidSimuFile;

	public DealFigureFile(String figureFileName, String user, String passwd, String ip, String database, int num,
			File enbidSimuFile)
	{
		this.figureFileName = figureFileName;
		this.user = user;
		this.passwd = passwd;
		this.ip = ip;
		this.database = database;
		ThreadNum = num;
		this.enbidSimuFile = enbidSimuFile;
	}

	/** 
	 * 将数据库中的每条指纹库装进stringbuffer中
	 * 
	 * @param bf
	 * @param columnCount
	 * @param rs
	 * @return
	 * @throws Exception
	 */
	public static FigureCell returnStringBuffer(int columnCount, ResultSet rs, int type) throws Exception
	{
		FigureCell figurecell = new FigureCell();
		if (columnCount == 4)
		{
			if (type == 10)
			{
				figurecell.ieci = Integer.parseInt(rs.getString(1));
				figurecell.ilongitude = Integer.parseInt(rs.getString(2));
				figurecell.ilatitude = Integer.parseInt(rs.getString(3));
				figurecell.rsrp = Double.parseDouble(rs.getString(4));
			} else if (type == 40)
			{
				figurecell.ieci = Integer.parseInt(rs.getString(1));
				figurecell.ilongitude = (Integer.parseInt(rs.getString(2)) / 4000) * 4000 + 2000;
				figurecell.ilatitude = (Integer.parseInt(rs.getString(3)) / 3600) * 3600 + 1800;
				figurecell.rsrp = Double.parseDouble(rs.getString(4));
			}
		} else if (columnCount == 6)
		{
			if (type == 10)
			{
				if (rs.getString(1) != null || !"".equals(rs.getString(1)))
				{
					figurecell.buildingid = Integer.parseInt(rs.getString(1));
				}
				figurecell.ieci = Integer.parseInt(rs.getString(2));
				figurecell.ilongitude = Integer.parseInt(rs.getString(3));
				figurecell.ilatitude = Integer.parseInt(rs.getString(4));
				figurecell.level = Integer.parseInt(rs.getString(5));
				figurecell.rsrp = Double.parseDouble(rs.getString(6));
			} else if (type == 40)
			{
				if (rs.getString(1) != null || !"".equals(rs.getString(1)))
				{
					figurecell.buildingid = Integer.parseInt(rs.getString(1));
				}
				figurecell.ieci = Integer.parseInt(rs.getString(2));
				figurecell.ilongitude = (Integer.parseInt(rs.getString(3)) / 4000) * 4000 + 2000;
				figurecell.ilatitude = (Integer.parseInt(rs.getString(4)) / 3600) * 3600 + 1800;
				figurecell.level = Integer.parseInt(rs.getString(5));
				figurecell.rsrp = Double.parseDouble(rs.getString(6));
			}

		}
		return figurecell;
	}

	@Override
	public String call() throws Exception
	{
		// TODO Auto-generated method stub
		long i = 0;
		String sql = "";
		StringBuffer tenGridBuffer = new StringBuffer();
		Set<Integer> tenEnbidSet = new HashSet<Integer>();
		ConcurrentHashMap<String, BufferedWriter> IoFileMap = new ConcurrentHashMap<String, BufferedWriter>();
		HashMap<Integer, FigureCell> FortyGridMap = new HashMap<Integer, FigureCell>();
		if (figureFileName.contains("building"))
		{
			sql = "select * from  " + figureFileName
					+ "  order by  (ilongitude/4000)*4000,(ilatitude/3600)*3600,level ,ilongitude,ilatitude";
		} else if (figureFileName.contains("coverface"))
		{
			sql = "select * from  " + figureFileName
					+ " order by  (ilongitude/4000)*4000,(ilatitude/3600)*3600,ilongitude,ilatitude";
		}
		DBHelper helper = new DBHelper();
		helper.setDBValue(user, passwd, ip, database);
		ResultSet rs = helper.GetResultSet(sql, null);
		try
		{
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			GridKey tenkey = new GridKey();
			GridKey fortykey = new GridKey();
			if (figureFileName.contains("coverface")
					&& (figureFileName.contains("40m") || figureFileName.contains("40M")))
			{
				while (rs.next())
				{
					i++;
					FigureCell fortyFigureCell = returnStringBuffer(columnCount, rs, 40);
					Thread dealFortyGrid = new Thread(new DealFortyGrid(FortyGridMap, fortyFigureCell, fortykey,
							"fortySimuFile_low", enbidSimuFile, ThreadNum, IoFileMap));
					dealFortyGrid.start();
					if (dealFortyGrid != null)
					{
						dealFortyGrid.join();
					}
					if (i % 10000 == 0)
					{
						System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date()) + "  表"
								+ figureFileName + "已经处理了" + i + "行");
					}
				}
				// --将最后一次的数据写进文件中
				if (FortyGridMap.size() > 0)
				{
					DealFortyGrid.writeEnbidFile(FortyGridMap, enbidSimuFile.getPath(), ThreadNum, "fortySimuFile_low",
							IoFileMap);
					// 写出指纹库
					FortyGridMap.clear();
					// 清空列表
				}
			} else
			{
				while (rs.next())
				{
					i++;
					FigureCell fortyFigureCell = returnStringBuffer(columnCount, rs, 40);
					FigureCell tenFigureCell = returnStringBuffer(columnCount, rs, 10);
					Thread dealTenGridThread = new Thread(new DealTenGrid(tenGridBuffer, tenEnbidSet, tenFigureCell,
							tenkey, "tenSimuFile", enbidSimuFile, ThreadNum, IoFileMap));
					dealTenGridThread.start();
					Thread dealFortyGridThread = new Thread(new DealFortyGrid(FortyGridMap, fortyFigureCell, fortykey,
							"fortySimuFile", enbidSimuFile, ThreadNum, IoFileMap));
					dealFortyGridThread.start();
					if (dealTenGridThread != null)
					{
						dealTenGridThread.join();
					}
					if (dealFortyGridThread != null)
					{
						dealFortyGridThread.join();
					}
					if (i % 10000 == 0)
					{
						System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date()) + "  表"
								+ figureFileName + "已经处理了" + i + "行");
					}
				}
				// --将最后一次的数据写进文件中
				if (tenGridBuffer.length() > 0)
				{
					DealTenGrid.writeEnbidFile(tenGridBuffer, tenEnbidSet, enbidSimuFile.getPath(), ThreadNum,
							"tenSimuFile", IoFileMap);
					// 写出指纹库
					tenGridBuffer.delete(0, tenGridBuffer.length());
					tenEnbidSet.clear();
					// 清空列表
				}
				if (FortyGridMap.size() > 0)
				{
					DealFortyGrid.writeEnbidFile(FortyGridMap, enbidSimuFile.getPath(), ThreadNum, "fortySimuFile",
							IoFileMap);
					// 写出指纹库
					FortyGridMap.clear();
					// 清空列表
				}
			}
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally
		{
			for (String path : IoFileMap.keySet())
			{
				IoFileMap.get(path).close();
			}
			IoFileMap.clear();
		}
		return "finished";
	}

}
