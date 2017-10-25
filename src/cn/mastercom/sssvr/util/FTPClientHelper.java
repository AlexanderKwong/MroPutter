package cn.mastercom.sssvr.util;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.log4j.Logger;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

/**
 * FTP客户端
 * 
 * @author summersun_ym
 * @version $Id: FTPClientTemplate.java 2010-11-22 上午12:54:47 $
 */
public class FTPClientHelper {
    //---------------------------------------------------------------------
    // Instance data
    //---------------------------------------------------------------------
    /** logger */
    protected final Logger         log                  = Logger.getLogger(getClass());
    private ThreadLocal<FTPClient> ftpClientThreadLocal = new ThreadLocal<FTPClient>();

    private String                 host;
    private int                    port;
    private String                 username;
    private String                 password;

    private boolean                binaryTransfer       = true;
    private boolean                passiveMode          = true;
    private String                 encoding             = "UTF-8";
    private int                    clientTimeout        = 1000 * 60;

    
    public FTPClientHelper(String host, int port,String username,String password)
    {
    	setHost(host);
        setPort(port);
        setUsername(username);
        setPassword(password);
    }
    
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
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

    public boolean isBinaryTransfer() {
        return binaryTransfer;
    }

    public void setBinaryTransfer(boolean binaryTransfer) {
        this.binaryTransfer = binaryTransfer;
    }

    public boolean isPassiveMode() {
        return passiveMode;
    }

    public void setPassiveMode(boolean passiveMode) {
        this.passiveMode = passiveMode;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public int getClientTimeout() {
        return clientTimeout;
    }

    public void setClientTimeout(int clientTimeout) {
        this.clientTimeout = clientTimeout;
    }

    
    public boolean checkFileExist(String destFileName)
    {
    	try {
			if(getFTPClient().listNames(destFileName) !=null && getFTPClient().listNames(destFileName).length>0)
				return true;
		} catch (Exception e) {
		}
    	return false;
    }
    
    //---------------------------------------------------------------------
    // private method
    //---------------------------------------------------------------------
    /**
     * 返回一个FTPClient实例
     * 
     * @throws Exception
     */
    public FTPClient getFTPClient() throws Exception {
        if (ftpClientThreadLocal.get() != null && ftpClientThreadLocal.get().isConnected()) {
            return ftpClientThreadLocal.get();
        } else {
            FTPClient ftpClient = new FTPClient(); //构造一个FtpClient实例
            ftpClient.setControlEncoding(encoding); //设置字符集

            connect(ftpClient); //连接到ftp服务器
    
            //设置为passive模式
            if (passiveMode) {
                ftpClient.enterLocalPassiveMode();
            }
            setFileType(ftpClient); //设置文件传输类型
    
            try {
                ftpClient.setSoTimeout(clientTimeout);
            } catch (SocketException e) {
            	e.printStackTrace();
                throw new Exception("Set timeout error.", e);
            }
            ftpClientThreadLocal.set(ftpClient);
            return ftpClient;
        }
    }

    /**
     * 设置文件传输类型
     * 
     * @throws Exception
     * @throws IOException
     */
    private void setFileType(FTPClient ftpClient) throws Exception {
        try {
            if (binaryTransfer) {
                ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            } else {
                ftpClient.setFileType(FTP.ASCII_FILE_TYPE);
            }
        } catch (IOException e) {
            throw new Exception("Could not to set file type.", e);
        }
    }

    /**
     * 连接到ftp服务器
     * 
     * @param ftpClient
     * @return 连接成功返回true，否则返回false
     * @throws Exception
     */
    private boolean connect(FTPClient ftpClient) throws Exception {
        try {
            ftpClient.connect(host, port);

            // 连接后检测返回码来校验连接是否成功
            int reply = ftpClient.getReplyCode();

            if (FTPReply.isPositiveCompletion(reply)) {
                //登陆到ftp服务器
                if (ftpClient.login(username, password)) {
                    setFileType(ftpClient);
                    return true;
                }
            } else {
                ftpClient.disconnect();
                throw new Exception("FTP server refused connection.");
            }
        } catch (IOException e) {
            if (ftpClient.isConnected()) {
                try {
                    ftpClient.disconnect(); //断开连接
                } catch (IOException e1) {
                    throw new Exception("Could not disconnect from server.", e1);
                }

            }
            throw new Exception("Could not connect to server.", e);
        }
        return false;
    }


    //---------------------------------------------------------------------
    // public method
    //---------------------------------------------------------------------
    /**
     * 断开ftp连接
     * 
     * @throws Exception
     */
    public void disconnect() throws Exception {
        try {
            FTPClient ftpClient = getFTPClient();
            ftpClient.logout();
            if (ftpClient.isConnected()) {
                ftpClient.disconnect();
                ftpClient = null;                
            }
        } catch (IOException e) {
            throw new Exception("Could not disconnect from server.", e);
        }
    }
    
    public boolean mkdir(String pathname) throws Exception {
        return mkdir(pathname, null);
    }
    
    /**
     * 在ftp服务器端创建目录（不支持一次创建多级目录）
     * 
     * 该方法执行完后将自动关闭当前连接
     * 
     * @param pathname
     * @return
     * @throws Exception
     */
    public boolean mkdir(String pathname, String workingDirectory) throws Exception {
        return mkdir(pathname, workingDirectory, true);
    }
    
    /**
     * 在ftp服务器端创建目录（不支持一次创建多级目录）
     * 
     * @param pathname
     * @param autoClose 是否自动关闭当前连接
     * @return
     * @throws Exception
     */
    public boolean mkdir(String pathname, String workingDirectory, boolean autoClose) throws Exception {
        try {
            getFTPClient().changeWorkingDirectory(workingDirectory);
            return getFTPClient().makeDirectory(pathname);
        } catch (IOException e) {
            throw new Exception("Could not mkdir.", e);
        } finally {
            if (autoClose) {
                disconnect(); //断开连接
            }
        }
    }

    /**
     * 上传一个本地文件到远程指定文件
     * 
     * @param remoteAbsoluteFile 远程文件名(包括完整路径)
     * @param localAbsoluteFile 本地文件名(包括完整路径)
     * @return 成功时，返回true，失败返回false
     * @throws Exception
     */
    public boolean put(String remoteAbsoluteFile, String localAbsoluteFile) throws Exception {
        return put(remoteAbsoluteFile, localAbsoluteFile, false, false);
    }
    
    public boolean put(String remoteAbsoluteFile, InputStream is) throws Exception {
    	return put(remoteAbsoluteFile, is, false, false);
    }
    
    public boolean putFromHdfs(String remoteAbsoluteFile, String hdfsFile, HadoopFSOperations hdfs ) throws Exception {	
    	return put(remoteAbsoluteFile, hdfs.GetInputStream(hdfsFile), false, false);
    }
    
    public boolean putFileToHdfs(String remoteAbsoluteFile, String hdfsFile, HadoopFSOperations hdfs ) throws Exception {	
    	return get(remoteAbsoluteFile, hdfs.GetOutputStream(hdfsFile), false, false);
    }
    
    public boolean putDirToHdfs(String remoteAbsoluteFile, String hdfsFile, HadoopFSOperations hdfs ) throws Exception {	
    	FTPFile[] files = listFiles(remoteAbsoluteFile, false);
    	try {
			hdfs.mkdir(hdfsFile);
			for(FTPFile file:files)
			{
				if(file.isFile())
				{
					boolean ret =get(remoteAbsoluteFile+"/" + file.getName(), hdfs.GetOutputStream(hdfsFile+"/" + file.getName()), false,true);
					System.out.println("Upload file " + (ret? "Success":"Fail") + hdfsFile+"/" + file.getName());
				}
				else
				{
					putDirToHdfs(remoteAbsoluteFile+"/" + file.getName(),hdfsFile+"/" + file.getName(),hdfs);
				}
			}
		} catch (Exception e) {			
			e.printStackTrace();
			return false;
		}    	
    	return true;
    }
    
    public boolean putMergeToHdfs(String remoteAbsoluteFile, String hdfsFile, HadoopFSOperations hdfs, String destFileName ) throws Exception
	{
	   	FTPFile[] files = listFiles(remoteAbsoluteFile, false);
	   	while (hdfs.checkFileExist(hdfsFile + "/" + destFileName))
		{
	   		if(destFileName.length()>100)
	   		{
	   			if(hdfs.delete(hdfsFile + "/" + destFileName))
	   				break;
	   		}
			if (destFileName.contains(".x"))
				destFileName += "x";
			else
				destFileName += ".x";		
		}
	   	
	   	OutputStream os = hdfs.GetOutputStream(hdfsFile+"/" + destFileName);
	   	boolean ret = false;
    	try {
    		
			hdfs.mkdir(hdfsFile);
			for(FTPFile file:files)
			{
				if (file.getName().toLowerCase().contains(".processing"))
					continue;
				if(file.isFile())
				{
					ret =get(remoteAbsoluteFile+"/" + file.getName(), os, false,false);
					System.out.println("Upload file " + (ret? "Success":"Fail") + hdfsFile+"/" + file.getName());
				}				
			}
		} catch (Exception e) {			
			e.printStackTrace();
			return false;
		}    	
    	finally
    	{
    		if(os != null)
    		{
    			os.close();
    		}
    		if(!ret)
    		{
    			hdfs.delete(hdfsFile+"/" + destFileName);
    		}
    	}
    	return true;
	}

    
	final static int BLOCK_64K = 64 * 1024;
	final static int BLOCK_128K = 128 * 1024;
	final static int MAX_BLOCK_SIZE = 32 * 1024 * 1024;

	public static void int2Byte(byte[] b, int offset, int intValue) {
       for (int i = 0; i < 4; i++) {
            b[offset+i] = (byte) (intValue >> 8 * (3 - i) & 0xFF);
        }
    }
	
	/**
     * 上传一个本地文件到远程指定文件
     * 
     * @param remoteAbsoluteFile 远程文件名(包括完整路径)
     * @param localAbsoluteFile 本地文件名(包括完整路径)
     * @param autoClose 是否自动关闭当前连接
     * @return 成功时，返回true，失败返回false
     * @throws Exception
     */
    public boolean put(String remoteAbsoluteFile, InputStream input, boolean autoClose,boolean autoCompress) throws Exception {
        try {
            // 处理传输        	
            return getFTPClient().storeFile(remoteAbsoluteFile, input);
        } catch (FileNotFoundException e) {
            throw new Exception("local file not found.", e);
        } catch (IOException e) {
            throw new Exception("Could not put file to server.", e);
        } finally {
            try {
                if (input != null) {
                    input.close();
                }
            } catch (Exception e) {
                throw new Exception("Couldn't close FileInputStream.", e);
            }
            if (autoClose) {
                disconnect(); //断开连接
            }
        }
    }
    
    
    /**
     * 上传一个本地文件到远程指定文件
     * 
     * @param remoteAbsoluteFile 远程文件名(包括完整路径)
     * @param localAbsoluteFile 本地文件名(包括完整路径)
     * @param autoClose 是否自动关闭当前连接
     * @return 成功时，返回true，失败返回false
     * @throws Exception
     */
    public boolean put(String remoteAbsoluteFile, String localAbsoluteFile, boolean autoClose,boolean autoCompress) throws Exception {
        InputStream input = null;
        try {
            // 处理传输
        	if(autoCompress)
        	{
        		if(!remoteAbsoluteFile.endsWith("z"))
        		{
        			remoteAbsoluteFile += "z";
        		}
        		File srcFile = new File(localAbsoluteFile);
            	System.out.println(srcFile.length());
                
            	byte[] data = null;
        		try
        		{
        			data = Files.readAllBytes(srcFile.toPath());
        		}
        		catch (IOException e)
        		{
        			e.printStackTrace();
        		}

            	final int decompressedLength = data.length;
                LZ4Compressor compressor = LZ4Factory.nativeInstance().fastCompressor();
            	int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);
            	byte[] compressed = new byte[maxCompressedLength+4];
            	int2Byte(compressed,0,decompressedLength);
            	int compressedLength = compressor.compress(data, 0, decompressedLength, compressed, 4, maxCompressedLength);
            	compressed = Arrays.copyOfRange(compressed, 0, compressedLength+4);            	
                input = new ByteArrayInputStream(compressed);
        	}
        	else
        	{
        		input = new FileInputStream(localAbsoluteFile);
        	}
        	return getFTPClient().storeFile(remoteAbsoluteFile, input);
        } catch (FileNotFoundException e) {
            throw new Exception("local file not found.", e);
        } catch (IOException e) {
            throw new Exception("Could not put file to server.", e);
        } finally {
            try 
            {
                if (input != null) {
                    input.close();
                }
            } 
            catch (Exception e) {
                throw new Exception("Couldn't close FileInputStream.", e);
            }
            
            if (autoClose) {
                disconnect(); //断开连接
            }
        }
    }
    
    public boolean put(byte[] bts, String remoteAbsoluteFile, boolean autoClose) throws Exception 
    {
    	ByteArrayInputStream input = null;
    	try {
            // 处理传输
        	input = new ByteArrayInputStream(bts);
            return getFTPClient().storeFile(remoteAbsoluteFile, input);
        } catch (FileNotFoundException e) {
            throw new Exception("local file not found.", e);
        } catch (IOException e) {
            throw new Exception("Could not put file to server.", e);
        } finally {
            try {
                if (input != null) {
                    input.close();
                }
            } catch (Exception e) {
                throw new Exception("Couldn't close FileInputStream.", e);
            }
            if (autoClose) {
                disconnect(); //断开连接
            }
        }
    }

    /**
     * 下载一个远程文件到本地的指定文件
     * 
     * @param remoteAbsoluteFile 远程文件名(包括完整路径)
     * @param localAbsoluteFile 本地文件名(包括完整路径)
     * @return 成功时，返回true，失败返回false
     * @throws Exception
     */
    public boolean get(String remoteAbsoluteFile, String localAbsoluteFile) throws Exception {
        return get(remoteAbsoluteFile, localAbsoluteFile, true);
    }
    
    /**
     * 重命名
     * 
     * @param newfile 原文件名称
     * @param newfile 新文件名称
     * @return 成功时，返回true，失败返回false
     * @throws Exception
     */
    public boolean rename(String oldfile, String newfile) throws Exception {
        return getFTPClient().rename(oldfile, newfile);
    }

    /**
     * 下载一个远程文件到本地的指定文件
     * 
     * @param remoteAbsoluteFile 远程文件名(包括完整路径)
     * @param localAbsoluteFile 本地文件名(包括完整路径)
     * @param autoClose 是否自动关闭当前连接
     * 
     * @return 成功时，返回true，失败返回false
     * @throws Exception
     */
    public boolean get(String remoteAbsoluteFile, String localAbsoluteFile, boolean autoClose) throws Exception {
        OutputStream output = null;
        try {
            output = new FileOutputStream(localAbsoluteFile);
            return get(remoteAbsoluteFile, output, autoClose, false);
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

    /**
     * 下载一个远程文件到指定的流 处理完后记得关闭流
     * 
     * @param remoteAbsoluteFile
     * @param output
     * @return
     * @throws Exception
     */
    public boolean get(String remoteAbsoluteFile, OutputStream output) throws Exception {
        return get(remoteAbsoluteFile, output, true, false);
    }
    
    /*public boolean get(String remoteAbsoluteFile, FSDataOutputStream output) throws Exception {
        return get(remoteAbsoluteFile, output, true, false);
    }*/

    /**
     * 下载一个远程文件到指定的流 处理完后记得关闭流
     * 
     * @param remoteAbsoluteFile
     * @param output
     * @param delFile
     * @return
     * @throws Exception
     */
    public boolean get(String remoteAbsoluteFile, OutputStream output, boolean autoClose, boolean autoCloseOutPut) throws Exception {
        try {
            FTPClient ftpClient = getFTPClient();
            // 处理传输
            return ftpClient.retrieveFile(remoteAbsoluteFile, output);
        } catch (IOException e) {
            throw new Exception("Couldn't get file from server.", e);
        } finally {
        	if(autoCloseOutPut)
        	{
        		output.close();
        	}
        	
            if (autoClose) {
                disconnect(); //关闭链接
            }
        }
    }

    /**
     * 从ftp服务器上删除一个文件
     * 该方法将自动关闭当前连接
     * 
     * @param delFile
     * @return
     * @throws Exception
     */
    public boolean delete(String delFile) throws Exception {
        return delete(delFile, true);
    }
    
    public boolean deleteDir(String delFile, boolean autoClose) throws Exception {
        try {
        	
        	FTPFile[] listNames = getFTPClient().listFiles(delFile);
        	for(FTPFile ffile:listNames)
        	{
        		if(ffile.isDirectory())
        		{
        			deleteDir(delFile+"/"+ffile.getName(),false);
        		}
        		else 
        		{
        			delete(delFile+"/"+ffile.getName(),false);
        		}       			
        	}
            getFTPClient().removeDirectory(delFile);
            return true;
        } catch (IOException e) {
            throw new Exception("Couldn't delete file from server.", e);
        } finally {
            if (autoClose) {
                disconnect(); //关闭链接
            }
        }
    }
    
    /**
     * 从ftp服务器上删除一个文件
     * 
     * @param delFile
     * @param autoClose 是否自动关闭当前连接
     * 
     * @return
     * @throws Exception
     */
    public boolean delete(String delFile, boolean autoClose) throws Exception {
        try {
            getFTPClient().deleteFile(delFile);
            return true;
        } catch (IOException e) {
            throw new Exception("Couldn't delete file from server.", e);
        } finally {
            if (autoClose) {
                disconnect(); //关闭链接
            }
        }
    }
    
    /**
     * 批量删除
     * 该方法将自动关闭当前连接
     * 
     * @param delFiles
     * @return
     * @throws Exception
     */
    public boolean delete(String[] delFiles) throws Exception {
        return delete(delFiles, true);
    }

    /**
     * 批量删除
     * 
     * @param delFiles
     * @param autoClose 是否自动关闭当前连接
     * 
     * @return
     * @throws Exception
     */
    public boolean delete(String[] delFiles, boolean autoClose) throws Exception {
        try {
            FTPClient ftpClient = getFTPClient();
            for (String s : delFiles) {
                ftpClient.deleteFile(s);
            }
            return true;
        } catch (IOException e) {
            throw new Exception("Couldn't delete file from server.", e);
        } finally {
            if (autoClose) {
                disconnect(); //关闭链接
            }
        }
    }

    /**
     * 列出远程默认目录下所有的文件
     * 
     * @return 远程默认目录下所有文件名的列表，目录不存在或者目录下没有文件时返回0长度的数组
     * @throws Exception
     */
    public FTPFile[] listFiles() throws Exception {
        return listFiles(null, true);
    }
    
    public FTPFile[] listFiles(boolean autoClose) throws Exception {
        return listFiles(null, autoClose);
    }

    /**
     * 列出远程目录下所有的文件
     * 
     * @param remotePath 远程目录名
     * @param autoClose 是否自动关闭当前连接
     * 
     * @return 远程目录下所有文件名的列表，目录不存在或者目录下没有文件时返回0长度的数组
     * @throws Exception
     */
    public FTPFile[] listFiles(String remotePath, boolean autoClose) throws Exception {
        try {
        	FTPClient ftpClient = getFTPClient();
        	FTPFile[] listNames = getFTPClient().listFiles(remotePath);
            return listNames;
        } catch (IOException e) {
        	e.printStackTrace();
            throw new Exception("列出远程目录下所有的文件时出现异常", e);
        } finally {
            if (autoClose) {
                disconnect(); //关闭链接
            }
        }
    }
    
    /**
     * 列出远程目录下所有的目录
     * 
     * @param remotePath 远程目录名
     * @param autoClose 是否自动关闭当前连接
     * 
     * @return 远程目录下所有文件名的列表，目录不存在或者目录下没有文件时返回0长度的数组
     * @throws Exception
     */
    public FTPFile[] listDirs(String remotePath, boolean autoClose) throws Exception {
        try {
            FTPFile[] listNames = getFTPClient().listDirectories(remotePath);
            return listNames;
        } catch (IOException e) {
        	e.printStackTrace();
            throw new Exception("列出远程目录下所有的文件时出现异常", e);
        } finally {
            if (autoClose) {
                disconnect(); //关闭链接
            }
        }
    }
   
    public List<FTPFile> listFilesAll(String remotePath,String keyword) throws Exception {
        try {
        	List<FTPFile> listFiles = new ArrayList<FTPFile>();
        	FTPFile[] listNames = getFTPClient().listFiles(remotePath);
        	for(FTPFile ffile:listNames)
        	{
        		if(ffile.isDirectory())
        		{
        			List<FTPFile> listFilessub = listFilesAll(remotePath + "/" + ffile.getName(), keyword);
        			listFiles.addAll(listFilessub);
        		}
        		else if(keyword.length()==0 || ffile.getName().contains(keyword))
        		{
        			listFiles.add(ffile);
        		}       			
        	}
            return listFiles;
        } catch (IOException e) {
            throw new Exception("列出远程目录下所有的文件时出现异常", e);
        } 
    }
    
    public static String getParentPath(String path)
    {
    	int pos = path.replace("\\", "/").lastIndexOf("/");
    	if(pos>0)
    	{
    		return path.substring(0, pos);
    	}
    	return path;
    }
    
    public static String getName(String path)
    {
    	int pos = path.replace("\\", "/").lastIndexOf("/");
    	if(pos>0)
    	{
    		return path.substring(pos+1);
    	}
    	return path;
    } 
    
    public long getModificationTime(String path)
    {
    	FTPFile ffile = getFileStatus(path);
    	if(ffile != null)
    	{
    		return getModificationTime(ffile);
    	}
    	return 0;
    }

	private long getModificationTime(FTPFile ffile) {
		CalendarEx cal = new CalendarEx(ffile.getTimestamp().getTime());
		return cal.GetTime();
	}
    
    public FTPFile getFileStatus(String path)
	{
		System.out.println("getFileStatus：" + path);
		try
		{
			//Path curPath = new Path(path);
			if (checkFileExist(path))
			{
				FTPFile[] fileStatus = listFiles(getParentPath(path), false);
				int listlength = fileStatus.length;
				for (int i = 0; i < listlength; i++)
				{
					if (fileStatus[i].getName().equals(getName(path)))
					{
						System.out.println("getFileStatus：Success");
						return fileStatus[i];
					}
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}
    
    public List<String> listFileNamesAll(String remotePath, String keyword, int waitMinute) throws Exception {
        try {
        	List<String> listFiles = new ArrayList<String>();
        	FTPFile[] listNames = getFTPClient().listFiles(remotePath);
        	for(FTPFile ffile:listNames)
        	{
        		if(ffile.isDirectory())
        		{
        			List<String> listFilessub = listFileNamesAll(remotePath + "/" + ffile.getName(), keyword, waitMinute);
        			listFiles.addAll(listFilessub);
        		}
        		else if(keyword.length()==0 || ffile.getName().contains(keyword))
        		{
        			listFiles.add((remotePath + "/" + ffile.getName()).replace("//", "/"));
        		}       			
        	}
            return listFiles;
        } catch (IOException e) {
            throw new Exception("列出远程目录下所有的文件时出现异常", e);
        } 
    }
    
    public long getFilesize(String remotePath){
    	long size = 0;
    	try {
			FTPFile[] FTPFile = getFTPClient().listFiles(remotePath);
			size = FTPFile[0].getSize();			
		} catch (Exception e) {
			e.printStackTrace();
		}
    	return size;
    }

    public static void main(String[] args) throws Exception, InterruptedException {
    	
    	
/*		File file = new File("test.txt");
		FileOutputStream os = new FileOutputStream(file);
		os.close();
		System.out.println(file.getAbsolutePath());*/

        FTPClientHelper ftp = new FTPClientHelper("192.168.1.36",21,"ftpuser","ftpuser");
     
        ftp.setBinaryTransfer(true);
        ftp.setPassiveMode(true);
        ftp.setEncoding("utf-8");
        FTPFile[] listDirectories = ftp.getFTPClient().listDirectories("/test/mro/20170920");
        for (FTPFile ftpFile : listDirectories) {
        	System.out.println(ftpFile.getName());
		}
/*       List<String> lst = ftp.listFileNamesAll("/yunnandata/mme/20170731","",5);
        for(String filename:lst)
        {
        	System.out.println(filename);
        }
        ftp.disconnect();
        //HadoopFSOperations hdfs = new HadoopFSOperations("hdfs://192.168.1.31:9000");
        //ftp.putMergeToHdfs("/loc/20170727", "/mt_wlyh/loc/20170727", hdfs, "20170727.dat");
        
        @SuppressWarnings({ "unused", "static-access" })
		String ret = FTPClientHelper.getParentPath("/mrDecode/mt_wlyh");
        long modtime = ftp.getModificationTime("/mrDecode/mt_wlyh");
        ftp.deleteDir("/test",false);
        
        if(ftp.checkFileExist("/102920591_cqtsize32_dtsize30"))
        {
        	System.out.println("ok");
        }
        if(ftp.checkFileExist("/121.bin"))
        {
        	System.out.println("ok");
        }    
        
        String msg = "hello ftp2";
        ftp.put("/0_201605241602.cap","D:/pcap/0_201605241602.cap",true,true);
        //String[] files = ftp.listNames();
        //System.out.println(files);

        
        FTPFile[] files = ftp.listFiles("/",true);
        int ss = files.length;
        Calendar dt = files[0].getTimestamp();
         
        System.out.println(files[0].getLink() + " " + dt.getTime().toString());

        //boolean ret = ftp.put("/group/tbdev/query/user-upload/12345678910.txt", "D:/099_temp/query/12345.txt");
        //System.out.println(ret);
        //ftp.mkdir("asd", "user-upload");
        //boolean ret = ftp.get("/dsm.pdf", "D:/12345.txt");
        //System.out.println(ret);
        
        //HadoopFSOperations hdfs = new HadoopFSOperations("hdfs://111.40.229.110:8093/mt_wlyh");
        //Path f = new Path(hdfs.HADOOP_URL+"/input/gaotie1014.txt");
		//FSDataOutputStream os = hdfs.fs.create(f, true);
		
		// boolean ret = ftp.get("/gaotie1014.txt", os);
	     //System.out.println(ret);
		
        //ftp.disconnect();
        //ftp.mkdir("user-upload1");
        //ftp.disconnect();
        
        //String[] aa = {"/group/tbdev/query/user-upload/123.txt", "/group/tbdev/query/user-upload/SMTrace.txt"};
        //ftp.delete(aa);
         * 
         */
    }
} 
