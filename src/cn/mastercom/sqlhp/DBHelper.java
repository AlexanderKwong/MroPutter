package cn.mastercom.sqlhp;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;

import cn.mastercom.sssvr.util.CalendarEx;
import cn.mastercom.sssvr.util.GreepPlumHelper;

public class DBHelper
{
	Connection _CONN = null;
	static String sUser;
	static String sPwd;
	static String ServerIp;
	static String DbName;

	// 静态块 启动就加载
	static
	{
		try
		{
			readConfigInfo();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public void setDBValue(String user, String pwd, String ip, String Dbname)
	{
		sUser = user;
		sPwd = pwd;
		ServerIp = ip;
		DbName = Dbname;
	}

	public static void main(String[] args)
	{
		CalendarEx curTime = new CalendarEx(new Date());
		DBHelper help = new DBHelper();
		JobStatus jb = new JobStatus();
		jb.FinishTime = new CalendarEx().toString(2);
		jb.Result = "成功";
		jb.Info = "dt";
		JobHelper.SetJobStatus(help, curTime, "evt2gbcp", jb);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void readConfigInfo()
	{
		try
		{
			// XMLWriter writer = null;// 声明写XML的对象
			SAXReader reader = new SAXReader();

			OutputFormat format = OutputFormat.createPrettyPrint();
			format.setEncoding("GBK");// 设置XML文件的编码格式

			String filePath = "conf/config_msdb.xml";
			File file = new File(filePath);
			if (file.exists())
			{
				Document doc = reader.read(file);// 读取XML文件

				{
					List<String> list = doc.selectNodes("//comm/serverip");
					if (list != null)
					{
						Iterator iter = list.iterator();
						while (iter.hasNext())
						{
							Element element = (Element) iter.next();
							ServerIp = element.getText();
							break;
						}
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/dbuser");
					Iterator iter = list.iterator();
					while (iter.hasNext())
					{
						Element element = (Element) iter.next();
						sUser = element.getText();
						break;
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/dbpassword");
					if (list != null)
					{
						Iterator iter = list.iterator();
						while (iter.hasNext())
						{
							Element element = (Element) iter.next();
							sPwd = element.getText();
							break;
						}
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/database");
					if (list != null)
					{
						Iterator iter = list.iterator();
						while (iter.hasNext())
						{
							Element element = (Element) iter.next();
							DbName = element.getText();
							break;
						}
					}
				}
			}
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/*
	 * public DBHelper(String sUser, String sPwd, String ServerIp, String
	 * DbName) { this.sUser = sUser; this.sPwd = sPwd; this.ServerIp = ServerIp;
	 * this.DbName = DbName; }
	 */

	// 取得连接
	private boolean GetConn()
	{
		if (_CONN != null)
			return true;
		try
		{
			String sDriverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
			String sDBUrl = "jdbc:sqlserver://" + ServerIp + ";databaseName=" + DbName;
			Class.forName(sDriverName);
			_CONN = DriverManager.getConnection(sDBUrl, sUser, sPwd);
		} catch (Exception ex)
		{
			System.out.println(ex.getMessage());
			return false;
		}
		return true;
	}

	// 关闭连接
	private void CloseConn()
	{
		try
		{
			_CONN.close();
			_CONN = null;
		} catch (Exception ex)
		{
			System.out.println(ex.getMessage());
			_CONN = null;
		}
	}

	// 测试连接
	public boolean TestConn()
	{
		if (!GetConn())
			return false;

		CloseConn();
		return true;
	}

	public ResultSet GetResultSet(String sSQL, Object[] objParams)
	{
		GetConn();
		ResultSet rs = null;
		try
		{
			PreparedStatement ps = _CONN.prepareStatement(sSQL);
			if (objParams != null)
			{
				for (int i = 0; i < objParams.length; i++)
				{
					ps.setObject(i + 1, objParams[i]);
				}
			}
			rs = ps.executeQuery();
		} catch (Exception ex)
		{
			System.out.println(ex.getMessage());
			CloseConn();
		} finally
		{
			// CloseConn();
		}
		return rs;
	}

	public Object GetSingle(String sSQL, Object... objParams)
	{
		GetConn();
		try
		{
			PreparedStatement ps = _CONN.prepareStatement(sSQL);
			if (objParams != null)
			{
				for (int i = 0; i < objParams.length; i++)
				{
					ps.setObject(i + 1, objParams[i]);
				}
			}
			ResultSet rs = ps.executeQuery();
			if (rs.next())
				return rs.getString(1);// 索引从1开始
		} catch (Exception ex)
		{
			System.out.println(ex.getMessage());
		} finally
		{
			CloseConn();
		}
		return null;
	}

	public DataTable GetDataTable(String sSQL, Object... objParams)
	{
		GetConn();
		DataTable dt = null;
		try
		{
			PreparedStatement ps = _CONN.prepareStatement(sSQL);
			if (objParams != null)
			{
				for (int i = 0; i < objParams.length; i++)
				{
					ps.setObject(i + 1, objParams[i]);
				}
			}
			ResultSet rs = ps.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();

			List<DataRow> row = new ArrayList<DataRow>(); // 表所有行集合
			List<DataColumn> col = null; // 行所有列集合
			DataRow r = null;// 单独一行
			DataColumn c = null;// 单独一列

			String columnName;
			Object value;
			int iRowCount = 0;
			while (rs.next())// 开始循环读取，每次往表中插入一行记录
			{
				iRowCount++;
				col = new ArrayList<DataColumn>();// 初始化列集合
				for (int i = 1; i <= rsmd.getColumnCount(); i++)
				{
					columnName = rsmd.getColumnName(i);
					value = rs.getObject(columnName);
					c = new DataColumn(columnName, value);// 初始化单元列
					col.add(c); // 将列信息加入到列集合
				}
				r = new DataRow(col);// 初始化单元行
				row.add(r);// 将行信息加入到行集合
			}
			dt = new DataTable(row);
			dt.RowCount = iRowCount;
			dt.ColumnCount = rsmd.getColumnCount();
		} catch (Exception ex)
		{
			System.out.println(ex.getMessage());
		} finally
		{
			CloseConn();
		}
		return dt;
	}

	public int UpdateData(String sSQL, Object[] objParams)
	{
		GetConn();
		int iResult = 0;

		try
		{
			Statement ps = _CONN.createStatement();
			iResult = ps.executeUpdate(sSQL);
		} catch (Exception ex)
		{
			System.out.println(ex.getMessage());
			return -1;
		} finally
		{
			CloseConn();
		}
		return iResult;
	}

}
