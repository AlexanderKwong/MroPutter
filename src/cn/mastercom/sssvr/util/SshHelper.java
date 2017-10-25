package cn.mastercom.sssvr.util;

import java.io.IOException;
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class SshHelper
{
	String hostname = "10.139.6.169";
	String username = "root";
	String password = "highgo";
	int    port = 22;
	public SshHelper(String _hostname, String _username, String _password,int _port)
	{
		hostname = _hostname;
		username = _username;
		password = _password;
		port = _port;
	}

	Connection conn = null;
	Session ssh = null;
	boolean bConnected = false;

	public boolean isConnected()
	{
		return bConnected;
	}

	public boolean Connect()
	{
		if (bConnected)
			return true;

		conn = new Connection(hostname,port);

		try
		{
			// 连接到主机
			conn.connect();
			// 使用用户名和密码校验
			boolean isconn = conn.authenticateWithPassword(username, password);
			if (!isconn)
			{
				System.out.println("用户名称或者是密码不正确");
				conn.close();
				return false;
			} 
			else
			{
				System.out.println("已经连接OK");
				ssh = conn.openSession();
				bConnected = true;
				return true;
			}
		} 
		catch (IOException e)
		{
			e.printStackTrace();
			conn.close();
		}

		return false;
	}

	public void Disconnect()
	{
		conn.close();
		ssh.close();
		bConnected = false;
	}

	public String execCommand(String sCmd)
	{
		String ret = "";
		try
		{
			System.out.println("execCommand:" + sCmd);
			ssh.execCommand(sCmd);
			// 只允许使用一行命令，即ssh对象只能使用一次execCommand这个方法，
			// 多次使用则会出现异常
			// 使用多个命令用分号隔开
			// ssh.execCommand("cd /root; sh hello.sh");

			// 将Terminal屏幕上的文字全部打印出来
			InputStream is = new StreamGobbler(ssh.getStdout());
			BufferedReader brs = new BufferedReader(new InputStreamReader(is));
			while (true)
			{
				String line = brs.readLine();
				if (line == null)
				{
					break;
				}
				if (ret != "")
					ret += "\r\n";
				ret += line;
			}
			brs.close();
			System.out.println("execCommand complete:" + sCmd);
		} catch (Exception e)
		{
			e.printStackTrace();
			bConnected = false;
			return null;
		}
		return ret;
	}

	public static void main(String[] args)
	{
		SshHelper ssh = new SshHelper("10.204.210.107", "szmt", "Szmt@12",50072);
		if (!ssh.Connect())
		{
			return;
		}

		try
		{
			String ret = ssh.execCommand("pwd;ls");

			if (ret != null)
			{
				System.out.println(ret);
			}
			ret = ssh.execCommand("pwd;ls");
			if (ret != null)
			{
				System.out.println(ret);
			}
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			ssh.Disconnect();
		}

	}

}
