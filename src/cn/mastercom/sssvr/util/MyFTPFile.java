package cn.mastercom.sssvr.util;

import org.apache.commons.net.ftp.FTPFile;

public class MyFTPFile
{
    FTPFile m_FTPFile = null;
    String m_FullPath = null;
    String fileName = null;
    
    public MyFTPFile(FTPFile ftpFile, String fullPath)
    {
        m_FTPFile = ftpFile;
        m_FullPath = fullPath;
    }
    
    public MyFTPFile(String fullPath)
    {
        m_FullPath = fullPath;
    }
    
    public String GetFullPath()
    {
        return m_FullPath;
    }
    
    public FTPFile GetFTPFile()
    {
        return m_FTPFile;
    }
    
}
