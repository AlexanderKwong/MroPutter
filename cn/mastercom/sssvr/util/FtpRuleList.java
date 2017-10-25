package cn.mastercom.sssvr.util;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.net.ftp.FTPFile;

public class FtpRuleList
{
	public static int i = 0;
    public final String LISTFlag = "$LIST{";

    public List<MyFTPFile> ListFiles(FTPClientHelper fch, String str, Date date) throws Exception
    {
        FtpRuleLimit frl = new FtpRuleLimit();
        frl.GetTimeLimit(str, date);
        return listFiles(fch, frl.Path, frl.Min, frl.Max);
    }

    private List<MyFTPFile> listFiles(FTPClientHelper fch, String str, Date min, Date max) throws Exception
    {
        List<MyFTPFile> paths = new ArrayList<MyFTPFile>();
        int count = str.length() - str.replace(LISTFlag, "12345").length();// LIST次数

        paths.add(new MyFTPFile(str));
        for (int i = 0; i < count; i++)
        {
            paths = listFiles(fch, paths, min, max);
        }
        return paths;
    }

    /**
     * 对多个路径进行一次list
     * 
     * @param fch
     * @param ftpFiles
     * @param min
     * @param max
     * @return
     * @throws Exception
     */
    private List<MyFTPFile> listFiles(FTPClientHelper fch, List<MyFTPFile> ftpFiles, Date min, Date max)
            throws Exception
    {
        List<MyFTPFile> results = new ArrayList<MyFTPFile>();
       
        for (MyFTPFile myFTPFile : ftpFiles)
        {    	
            List<MyFTPFile> ls = listFiles(fch, myFTPFile, min, max);
            if (ls.size() > 0)
            {
                results.addAll(ls);
                ls.clear();
                
            }
        }
        return results;
    }

    /**
     * 对一个路径进行一次list
     * 
     * @param fch
     * @param str
     * @param min
     * @param max
     * @return
     * @throws Exception
     */
    private List<MyFTPFile> listFiles(FTPClientHelper fch, MyFTPFile myFtpFile, Date min, Date max) 
    {
    	List<MyFTPFile> results = new ArrayList<MyFTPFile>();
    	try{
            String[] arrs = splitLine(myFtpFile.GetFullPath());
            String subPath = arrs[0], strPattern = arrs[1], strValue = arrs[2];
            if (strPattern == "") strPattern = "*";
            String[] patterns = strPattern.split("\\|");

            FTPFile[] files = null;
            if (strValue.length() == 0)
            {
                files = listFiles(fch, subPath);

            }
            else
            {
                files = listDirs(fch, subPath);
            }
            int count = 0;
            for (FTPFile ftpFile : files)
            {
                if (isMatch(ftpFile, patterns))
                {
                    if (strValue.indexOf("/") != strValue.lastIndexOf("/")
                          //  || timeMatch(ftpFile.getTimestamp().getTime(), min, max)
                    		|| strValue.lastIndexOf("/") == 0 || strValue.lastIndexOf("/") == -1
                            )
                    {
                    	String fullPath = subPath + ftpFile.getName() + strValue;
                    	if(fullPath.length() != 0){	                    
    	                    MyFTPFile file = new MyFTPFile(ftpFile, fullPath);
    	                    results.add(file);
    	                    count ++;
                    	}else{
                    		break;
                    	}
                    }
                }
            }
    	}catch(Exception e){
    		e.printStackTrace();
    	}
    	  return results;
    }

    private String[] splitLine(String str) throws Exception
    {
        String[] result = new String[3];
        int index = str.indexOf(LISTFlag);
        if (index >= 0)
        {
            result[0] = str.substring(0, index);
            str = str.substring(index + LISTFlag.length());
            index = str.indexOf("}");
            if (index >= 0)
            {
                result[1] = str.substring(0, index);
                result[2] = str.substring(index + 1);
            }
        }

        return result;
    }

    /**
     * list file
     * 
     * @param fch
     * @param subPath
     * @return
     * @throws Exception
     */
    private FTPFile[] listFiles(FTPClientHelper fch, String subPath) throws Exception
    {
        return fch.listFiles(subPath, false);
    }

    /**
     * list dir
     * 
     * @param fch
     * @param subPath
     * @return
     * @throws Exception
     */
    private FTPFile[] listDirs(FTPClientHelper fch, String subPath) throws Exception
    {
        return fch.listDirs(subPath, false);
    }
    
    private boolean timeMatch(Date date, Date min, Date max)
    {
        if (date == null) return true;
        
        if (min == null || !date.before(min))
        {
            if (max == null || date.before(max))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * 过滤
     * 
     * @param file
     * @param patterns
     * @return
     */
    private boolean isMatch(FTPFile file, String[] patterns)
    {
        for (String pattern : patterns)
        {
            if (isMatch(file, pattern)) return true;
        }
        return false;
    }

    /**
     * 过滤
     * 
     * @param file
     * @param pattern
     * @return
     */
    private boolean isMatch(FTPFile file, String pattern)
    {
        while(pattern.contains("**"))
        {
            pattern.replace("**", "*");
        }
        
        String input = file.getName();
        input = input.trim().toLowerCase();
        pattern = pattern.trim().toLowerCase();

        if (pattern.contains("*"))
        {
            String[] arrs = pattern.split("\\*", -1);

            int arrsEnd = arrs.length - 1;
            if (arrsEnd == 1 && arrs[0].length() == 0 && arrs[arrsEnd].length() == 0) return true;// pattern == "*" / "**" / ......

            if (arrs[0].length() > 0 && (!input.startsWith(arrs[0]))) return false;
            if (arrs[arrsEnd].length() > 0 && (!input.endsWith(arrs[arrsEnd]))) return false;

            int inputStart = 0;
            for (String arr : arrs)
            {
                if (arr.length() == 0) continue;
                
                int inputIndex = input.indexOf(arr, inputStart);
                if (inputIndex < 0) return false;
                inputStart += arr.length();
            }
            return true;
        }
        else
        {
            return input == pattern;
        }
    }
}
