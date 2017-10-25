package cn.mastercom.sssvr.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class SftpClientHelper {

	private String host = "192.168.1.172";
	private String username = "ftp";
	private String password = "ftp";
	private int port = 22;
	private int timeout = 1000;
	private ChannelSftp sftp = null;

	private ThreadLocal<ChannelSftp> sftpThreadLocal = new ThreadLocal<ChannelSftp>();
	private String encode;

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public ChannelSftp getSftp() {
		return sftp;
	}

	public void setSftp(ChannelSftp sftp) {
		this.sftp = sftp;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setEncoding(String encode) {
		this.encode = encode;
	}

	/**
	 * connect server via sftp
	 * 
	 * @throws Exception
	 */
	public boolean connect() throws Exception {
		try {
			if (sftp != null) {
				// System.out.println("sftp is not null");
			}
			JSch jsch = new JSch();
			jsch.getSession(username, host, port);
			Session sshSession = jsch.getSession(username, host, port);
			if (password != null) {
				sshSession.setPassword(password);
			}
			Properties sshConfig = new Properties();
			sshConfig.put("StrictHostKeyChecking", "no");
			sshSession.setConfig(sshConfig);
			sshSession.connect();
			Channel channel = sshSession.openChannel("sftp");
			channel.connect();
			sftp = (ChannelSftp) channel;
			sftp.setFilenameEncoding(encode);
			if (sftp.isConnected()) {
				// System.out.println("Connected to " + host + ".");
				sftpThreadLocal.set(sftp);
				return true;
			} else {
				sftp.disconnect();
				throw new Exception("SFTP server refused connection.");
			}

		} catch (Exception e) {
			if (sftp.isConnected()) {
				try {
					sftp.disconnect(); // 断开连接
				} catch (Exception e1) {
					throw new Exception("Could not disconnect from server.", e1);
				}
			}

		}
		return false;
	}

	/**
	 * Disconnect with server
	 */
	public void disconnect() {
		if (this.sftp != null) {
			if (this.sftp.isConnected()) {
				this.sftp.disconnect();
			} else if (this.sftp.isClosed()) {
				// System.out.println("sftp is closed already");
			}
		}

	}

	public SftpClientHelper(String host, String username, String password, int port, int time) {
		this.host = host;
		this.username = username;
		this.password = password;
		this.port = port;
		this.timeout = time;
	}

	/**
	 * 根据路径创建文件夹.
	 * 
	 * @param dir
	 *            路径 必须是 /xxx/xxx/xxx/ 不能就单独一个/
	 * @param sftp
	 *            sftp连接
	 * @throws Exception
	 *             异常
	 */
	public static boolean mkdir(final String dir, final ChannelSftp sftp) throws Exception {
		try {
			if (StringUtils.isBlank(dir))
				return false;
			String md = dir.replaceAll("\\\\", "/");
			if (md.indexOf("/") != 0 || md.length() == 1)
				return false;
			return mkdirs(md, sftp);
		} catch (Exception e) {
			exit(sftp);
			throw e;
		}
	}

	/**
	 * 递归创建文件夹.
	 * 
	 * @param dir
	 *            路径
	 * @param sftp
	 *            sftp连接
	 * @return 是否创建成功
	 * @throws SftpException
	 *             异常
	 */
	private static boolean mkdirs(final String dir, final ChannelSftp sftp) throws SftpException {
		String dirs = dir.substring(1, dir.length() - 1);
		String[] dirArr = dirs.split("/");
		String base = "";
		for (String d : dirArr) {
			base += "/" + d;
			if (dirExist(base + "/", sftp)) {
				continue;
			} else {
				sftp.mkdir(base + "/");
			}
		}
		return true;
	}

	/**
	 * 格式化路径.
	 * 
	 * @param srcPath
	 *            原路径. /xxx/xxx/xxx.yyy 或 X:/xxx/xxx/xxx.yy
	 * @return list, 第一个是路径（/xxx/xxx/）,第二个是文件名（xxx.yy）
	 */
	public static List<String> formatPath(final String srcPath) {
		List<String> list = new ArrayList<String>(2);
		String dir = "";
		String fileName = "";
		String repSrc = srcPath.replaceAll("\\\\", "/");
		int firstP = repSrc.indexOf("/");
		int lastP = repSrc.lastIndexOf("/");
		fileName = repSrc.substring(lastP + 1);
		dir = repSrc.substring(firstP, lastP);
		dir = "/" + (dir.length() == 1 ? dir : (dir + "/"));
		list.add(dir);
		list.add(fileName);
		return list;
	}

	/***
	 * upload file to remotePath
	 */

	public void put(String remoteAbsoluteFile, String localAbsoluteFile) throws Exception {
		put(remoteAbsoluteFile, localAbsoluteFile, false, true);
	}

	public void put(String remoteAbsoluteFile, InputStream is) throws Exception {
		put(remoteAbsoluteFile, is, false);
	}

	public void putFromHdfs(String remoteAbsoluteFile, String hdfsFile, HadoopFSOperations hdfs) throws Exception {
		put(remoteAbsoluteFile, hdfs.GetInputStream(hdfsFile), false);
	}

	public void putFromToHdfs(String remoteAbsoluteFile, String hdfsFile, HadoopFSOperations hdfs) throws Exception {
		get(remoteAbsoluteFile, hdfs.GetOutputStream(hdfsFile));
	}

	// public void putDirToHdfs(String remoteAbsoluteFile, String hdfsFile,
	// HadoopFSOperations hdfs ) throws Exception {
	// ArrayList<File> listFiles = listFiles(remoteAbsoluteFile, false);
	// try {
	// hdfs.mkdir(hdfsFile);
	// for(File file: listFiles)
	// {
	// if(file.getName().equals(".")|| file.getName().equals("..")){
	// continue;
	// }
	// else if(file.isFile())
	// {
	// get(remoteAbsoluteFile+ "/" + file.getName(),
	// hdfs.GetOutputStream(hdfsFile+"/" + file.getName()), false,true);
	// System.out.println("Upload file " + "Success"+ hdfsFile+"/" +
	// file.getName());
	// }
	// else
	// {
	// putDirToHdfs(remoteAbsoluteFile+"/" + file.getName(),hdfsFile+"/" +
	// file.getName(),hdfs);
	// }
	// }
	// } catch (Exception e) {
	// e.printStackTrace();
	//
	// }
	//
	// }

	/**
	 * 列出远程目录下所有的文件
	 * 
	 * @param remotePath
	 *            远程目录名
	 * @param autoClose
	 *            是否自动关闭当前连接
	 * 
	 * @return 远程目录下所有文件名的列表，目录不存在或者目录下没有文件时返回0长度的数组
	 * @throws Exception
	 */
	public ArrayList<File> listFiles(String remotePath, boolean autoClose) throws Exception {
		try {
			ArrayList<File> list = new ArrayList<File>();
			@SuppressWarnings("rawtypes")
			Vector ls = getSFTPClient().ls(remotePath);
			for (Object object : ls) {
				if (object instanceof com.jcraft.jsch.ChannelSftp.LsEntry) {
					String fileName = ((com.jcraft.jsch.ChannelSftp.LsEntry) object).getFilename();
					File file = new File(remotePath + "/" + fileName);
					list.add(file);
				}
			}
			return list;
		} catch (IOException e) {
			System.out.println("获取列表失败");
			throw new Exception("列出远程目录下所有的文件时出现异常", e);
		} finally {
			if (autoClose) {
				disconnect(); // 关闭链接
			}
		}
	}

	@SuppressWarnings("rawtypes")
	public File[] listFile(String remotePath, boolean autoClose) throws Exception {
		try {
			Vector ls = null;
			try {
				ls = getSFTPClient().ls(remotePath);
			} catch (Exception e) {
				System.out.println("sftp 获取列表失败.........");
			}
			File[] file = new File[ls.size() - 2];
			int count = 0;
			for (Object obj : ls) {
				if (obj instanceof com.jcraft.jsch.ChannelSftp.LsEntry) {
					String fileName = ((com.jcraft.jsch.ChannelSftp.LsEntry) obj).getFilename();
					if (fileName.equals(".") || fileName.equals("..")) {
						continue;
					}
					file[count] = new File(remotePath + "/" + fileName);
					count++;
				}
			}
			return file;
		} catch (Exception e) {
			return new File[0];
		} finally {
			if (autoClose) {
				disconnect(); // 关闭链接
			}
		}
	}

	/**
	 * 上传一个本地文件到远程指定文件
	 * 
	 * @param remoteAbsoluteFile
	 *            远程文件名(包括完整路径)
	 * @param localAbsoluteFile
	 *            本地文件名(包括完整路径)
	 * @param autoClose
	 *            是否自动关闭当前连接
	 * @return 成功时，返回true，失败返回false
	 * @throws Exception
	 */
	public void put(String remoteAbsoluteFile, String localAbsoluteFile, boolean autoClose, boolean autoCompress)
			throws Exception {
		File srcFile = new File(localAbsoluteFile);
		InputStream input = null;
		try {
			// 处理传输
			input = new FileInputStream(localAbsoluteFile);

			remoteAbsoluteFile = remoteAbsoluteFile.replaceAll("\\\\", "/");
			getSFTPClient().cd(remoteAbsoluteFile);
			getSFTPClient().put(input, srcFile.getName());

		} catch (Exception e) {
			throw new Exception("local file not found. or change dir exception", e);

		} finally {
			try {
				if (input != null) {
					input.close();
				}
			} catch (Exception e) {
				throw new Exception("Couldn't close FileInputStream.", e);
			}

			if (autoClose) {
				disconnect(); // 断开连接
			}
		}
	}

	public void put(String remoteAbsoluteFile, InputStream input, boolean autoClose) throws Exception {

		try {
			// 处理传输
			getSFTPClient().cd(remoteAbsoluteFile.replaceAll("\\\\", "/"));
			getSFTPClient().put(input, remoteAbsoluteFile);

		} catch (Exception e) {
			throw new Exception("local file not found.", e);
		} finally {
			try {
				if (input != null) {
					input.close();
				}
			} catch (Exception e) {
				throw new Exception("Couldn't close FileInputStream.", e);
			}
			if (autoClose) {
				disconnect(); // 断开连接
			}
		}
	}

	/**
	 * 下载一个远程文件到指定的流 处理完后记得关闭流
	 * 
	 * @param remoteAbsoluteFile
	 * @param output
	 * @param delFile
	 * @return
	 * @throws Exception
	 */
	public void get(String remoteAbsoluteFile, OutputStream output)
			throws Exception {

		File file = new File(remoteAbsoluteFile);
		// 处理传输
		String parent = file.getParent();
		parent = parent.replace("\\", "/");
		getSFTPClient().cd(parent);
		getSFTPClient().get(file.getName(), output);
	}

	public void get(String remoteAbsoluteFile, String localAbsoluteFile, boolean autoClose) throws Exception {
		OutputStream output = null;
		try {
			output = new FileOutputStream(localAbsoluteFile);
			get(remoteAbsoluteFile, output);
		} catch (FileNotFoundException e) {
			throw new Exception("local file not found.", e);
		} finally {
			try {
				if (output != null) {
					output.close();
				}
			} catch (IOException e) {
				throw new Exception("Couldn't close FileOutputStream.", e);
			}
		}
	}

	public long getFilesize(String remotePath) {
		long size = 0;
		try {
			size = getSFTPClient().lstat(remotePath.replaceAll("\\\\", "/")).getSize();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return size;
	}

	/**
	 * 重命名
	 * 
	 * @param newfile
	 *            原文件名称
	 * @param newfile
	 *            新文件名称
	 * @return
	 * @throws Exception
	 */
	public void rename(String oldfile, String newfile, final ChannelSftp sftp) throws Exception {
		sftp.rename(oldfile, newfile);
	}

	/**
	 * 删除文件-sftp协议.
	 * 
	 * @param deleteFile
	 *            要删除的文件
	 * @param sftp
	 *            sftp连接
	 * @throws Exception
	 *             异常
	 */
	public static void rmFile(final String deleteFile, final ChannelSftp sftp) throws Exception {
		try {
			sftp.rm(deleteFile);
		} catch (Exception e) {
			exit(sftp);
			throw e;
		}
	}

	/**
	 * 判断文件夹是否存在.
	 * 
	 * @param dir
	 *            文件夹路径， /xxx/xxx/
	 * @param sftp
	 *            sftp协议
	 * @return 是否存在
	 */
	public static boolean dirExist(final String dir, final ChannelSftp sftp) {
		try {
			Vector<?> vector = sftp.ls(dir);
			if (null == vector)
				return false;
			else
				return true;
		} catch (SftpException e) {
			return false;
		}
	}

	/**
	 * 关闭协议-sftp协议.
	 * 
	 * @param sftp
	 *            sftp连接
	 */
	public static void exit(final ChannelSftp sftp) {
		sftp.exit();
	}

	/**
	 * 返回一个FTPClient实例
	 * 
	 * @throws Exception
	 */
	public ChannelSftp getSFTPClient() throws Exception {
		if (sftpThreadLocal.get() != null && sftpThreadLocal.get().isConnected()) {
			return sftpThreadLocal.get();
		} else {
			connect(); // 连接到sftp服务器
			return sftpThreadLocal.get();
		}
	}

	@SuppressWarnings("rawtypes")
	public File[] listFile(String subPath) {
		File[] files = null;
		try {
			ChannelSftp sftp = getSFTPClient();
			sftp.cd(subPath);
			Vector ls = sftp.ls(subPath);
			files = new File[ls.size() - 2];
			int count = 0;
			for (Object obj : ls) {
				if (obj instanceof com.jcraft.jsch.ChannelSftp.LsEntry) {
					String fileName = ((com.jcraft.jsch.ChannelSftp.LsEntry) obj).getFilename();
					if (fileName.equals(".") || fileName.equals("..")) {
						continue;
					}
					files[count] = new File(subPath + "/" + fileName);
					count++;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return files;
	}

	public static void main(String[] args) throws Exception {
		SftpClientHelper sftphelp = new SftpClientHelper("192.168.1.31", "hmaster", "mastercom168", 22, 1000);

		sftphelp.setEncoding("gbk");
		// System.out.println("sftp connect successful");
		// sftphelp.mkdir("\\home\\hmaster\\mastercom2\\" ,sftp);
		// /hmaster/master/mastercom1/
		// File[] listFile =
		// sftphelp.listFile(sftphelp,"/home/hmaster/mastercom1/",true);
		// System.out.println(listFile.length);

		// sftphelp.put("\\home\\hmaster\\mastercom2\\","C:\\Users\\ch007\\Desktop\\PROC_JOB_SETSTATUS
		// .sql");

		// sftphelp.get("\\home\\hmaster\\mastercom1\\logss","C:\\Users\\ch007\\Desktop\\mm.txt",false
		// );

		// FileInputStream fis = new FileInputStream("");

		// sftphelp.put("\\home\\hmaster\\mastercom1\\",fis);

		// List<String> list = new ArrayList<String>();
		// Vector vector = sftphelp.getSftp().ls("/home/hmaster/mastercom");
		// try{
		// for(Object obj :vector){
		// if(obj instanceof com.jcraft.jsch.ChannelSftp.LsEntry){
		// String fileName =
		// ((com.jcraft.jsch.ChannelSftp.LsEntry)obj).getFilename();
		// if(fileName.contains(".")){
		// continue;
		// }
		// System.out.println(fileName);
		// }
		// }
		// }catch (Exception e){
		//
		// }
		// File file = new File("C:\\Users\\ch007\\Desktop\\mm.txt");
		// boolean flag = file.isFile();
		//
		// HadoopFSOperations hdfs = new
		// HadoopFSOperations("hdfs://192.168.1.31:9000/");
		// sftphelp.putDirToHdfs("/home/hmaster/sftp","/winter/test1",hdfs);
		sftphelp.connect();
		// Vector ls =
		// sftphelp.getSftp().ls("/home/hmaster/mastercom1/Hanxin_GnHTTP_1_20170920151200_88_66_fail1
		// (2).dat");

		// System.out.println(size);

		String mm = "\\home\\hmaster\\mastercom1\\Hanxin_GnHTTP_1_20170920151200_88_66_fail1 (2).dat";
		System.out.println(mm.replaceAll("\\\\", "/"));
		long size = sftphelp.getSftp().lstat(mm.replaceAll("\\\\", "/")).getSize();
		System.out.println(size);
	}
}
