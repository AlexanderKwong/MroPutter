package cn.mastercom.sssvr.util;

import java.io.File;

public class SftpFile {

	File m_FTPFile = null;
    String m_FullPath = null;
    String fileName = null;
    
    public SftpFile(File ftpFile, String fullPath)
    {
        m_FTPFile = ftpFile;
        m_FullPath = fullPath;
    }
    
    public SftpFile(String fullPath)
    {
        m_FullPath = fullPath;
    }
    
    public String GetFullPath()
    {
        return m_FullPath;
    }
    
    public File GetFTPFile()
    {
        return m_FTPFile;
    }
}
