package cn.mastercom.sssvr.util;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SftpRuleHelper {
	
	public List<SftpFile> ListFiles(SftpClientHelper shp, List<String> strs, Date date) throws Exception
    {
        List<SftpFile> result = new ArrayList<SftpFile>();
        for (String str : strs)
        {
            List<SftpFile> ls = ListFiles(shp, str, date);
            result.addAll(ls);
        }
        return result;
    }
	
	public List<SftpFile> ListFiles(SftpClientHelper shp, String str, Date date) throws Exception
    {
        str = str.replace("\\", "/").trim();
        if (!str.startsWith("/"))
        {
            str = "/" + str;
        }        
        String s = (new FtpRuleTime()).ReplaceTime(str, date);        
        return (new SftpRuleList()).ListFiles(shp, s, date);
    }
}
