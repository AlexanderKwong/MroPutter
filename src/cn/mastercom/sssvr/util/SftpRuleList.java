package cn.mastercom.sssvr.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SftpRuleList {

	public static int i = 0;
    public final String LISTFlag = "$LIST{";
    
    public List<SftpFile> ListFiles(SftpClientHelper sfh, String str, Date date) throws Exception
    {
        FtpRuleLimit frl = new FtpRuleLimit();
        frl.GetTimeLimit(str, date);
        return listFiles(sfh, frl.Path, frl.Min, frl.Max);
    }
    
    private List<SftpFile> listFiles(SftpClientHelper sfh, String str, Date min, Date max) throws Exception
    {
        List<SftpFile> paths = new ArrayList<SftpFile>();
        int count = str.length() - str.replace(LISTFlag, "12345").length();// LIST次数

        paths.add(new SftpFile(str));
        for (int i = 0; i < count; i++)
        {
            paths = listFiles(sfh, paths, min, max);
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
    private List<SftpFile> listFiles(SftpClientHelper sfh, List<SftpFile> ftpFiles, Date min, Date max)
    {
        List<SftpFile> results = new ArrayList<SftpFile>();
       
        for (SftpFile sftpFile : ftpFiles)
        {    	
            List<SftpFile> ls = listFiles(sfh, sftpFile, min, max);
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
    private List<SftpFile> listFiles(SftpClientHelper sfh, SftpFile sftpFile, Date min, Date max) 
    {
    	List<SftpFile> results = new ArrayList<SftpFile>();
    	try{
            String[] arrs = splitLine(sftpFile.GetFullPath());
            String subPath = arrs[0], strPattern = arrs[1], strValue = arrs[2];
            if (strPattern == "") strPattern = "*";
            String[] patterns = strPattern.split("\\|");
                   
            File[] files = null;
            if (strValue.length() == 0)
            {
                files = listFiles(sfh, subPath);
            }
            else
            {
                files = listDir(sfh, subPath);
            }
            for (File ftpFile : files)
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
    	                    SftpFile file = new SftpFile(ftpFile, fullPath);
    	                    results.add(file);
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
    
    /**
     * list file
     * 
     * @param fch
     * @param subPath
     * @return
     * @throws Exception
     */
    private File[] listFiles(SftpClientHelper sfh, String subPath) throws Exception
    {
        return sfh.listFile(subPath, false);
    }
    
    private File[] listDir(SftpClientHelper sfh, String subPath)throws Exception
    {
    	return sfh.listFile(subPath, false);
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
     * 过滤
     * 
     * @param file
     * @param patterns
     * @return
     */
    private boolean isMatch(File file, String[] patterns)
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
    private boolean isMatch(File file, String pattern)
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
