package cn.mastercom.sssvr.util;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.net.ftp.FTPFile;


public class FTPRuleHelper
{
    public List<MyFTPFile> ListFiles(FTPClientHelper fch, List<String> strs, Date date) throws Exception
    {
        List<MyFTPFile> result = new ArrayList<MyFTPFile>();
        for (String str : strs)
        {
            List<MyFTPFile> ls = ListFiles(fch, str, date);
            result.addAll(ls);
        }
        return result;
    }
    
    
    public List<MyFTPFile> ListFiles(FTPClientHelper fch, String str, Date date) throws Exception
    {
        str = str.replace("\\", "/").trim();
        if (!str.startsWith("/"))
        {
            str = "/" + str;
        }
        
        String s = (new FtpRuleTime()).ReplaceTime(str, date);
        
        return (new FtpRuleList()).ListFiles(fch, s, date);
    }
    
    public static void main(String[] args) throws Exception
    {
        FTPClientHelper fch = new FTPClientHelper("192.168.1.10",21,"ftpuser","ftpuser");
       
        FTPRuleHelper frh = new FTPRuleHelper();
        
        List<MyFTPFile> files = frh.ListFiles(fch, "/$LIST{*}/$LIST{*mr*}$LIMIT{1000-1-1,9999-1-1}", new Date()); //FTPFile[] files = fch.listDirs("/", false);
        
        int length = files.size();
    }
}
