package cn.mastercom.sssvr.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class FtpRuleLimit
{
    public final String TimeLimitFlag = "$LIMIT{";

    public String Path = null;
    public Date Min = null;
    public Date Max = null;

    public void GetTimeLimit(String line, Date date) throws Exception
    {
        int index = line.lastIndexOf(TimeLimitFlag);
        if (index < 0)
        {
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            
/*            if (cal.get(Calendar.YEAR) < 1970)
            {
                cal.setTime(new Date());
                
                cal.add(Calendar.HOUR, -48);
                Min = cal.getTime();
                
                cal.add(Calendar.HOUR, 35);
                Max = cal.getTime();
            }
            else
            {
                cal.setTime(date);
                
                cal.add(Calendar.HOUR, -10);
                Min = cal.getTime();
                
                cal.add(Calendar.HOUR, 34);
                Max = cal.getTime();
            }
*/            
            Path = line;
            return;
        }

        Path = line.substring(0, index);
        String str = line.substring(index + TimeLimitFlag.length(), line.length() - 1);

        getTimeLimit(str);
    }

    private void getTimeLimit(String str) throws Exception
    {
        SimpleDateFormat sdf = new SimpleDateFormat();

        int index = str.indexOf(",");

//        if (index >= 0)
//        {
//            Min = string2date(sdf, str.substring(0, index));
//            Max = string2date(sdf, str.substring(index + 1));
//        }
//        else
//        {
//            Min = string2date(sdf, "1000-01-01 00:00:00");
//            Max = string2date(sdf, str);
//        }

    }

    private Date string2date(SimpleDateFormat sdf, String str) throws Exception
    {
        str = str.trim();
        if (str.length() == 0) return null;

        String pattern = null;
        if (str.contains(" "))
        {
            if (str.contains("-"))
            {
                pattern = "yyyy-MM-dd HH:mm:ss";
            }
            else if (str.contains("/"))
            {
                pattern = "yyyy/MM/dd HH:mm:ss";
            }
            else
            {
                pattern = "yyyy.MM.dd HH:mm:ss";
            }
        }
        else
        {
            if (str.contains("-"))
            {
                pattern = "yyyy-MM-dd";
            }
            else if (str.contains("/"))
            {
                pattern = "yyyy/MM/dd";
            }
            else if (str.contains("."))
            {
                pattern = "yyyy.MM.dd";
            }
            else
            {
                if (str.length() == 6)
                {
                    pattern = "yyyyMM";
                }
                else if (str.length() == 8)
                {
                    pattern = "yyyyMMdd";
                }
                else if (str.length() == 10)
                {
                    pattern = "yyyyMMddHH";
                }
                else if (str.length() == 12)
                {
                    pattern = "yyyyMMddHHmm";
                }
                else
                {
                    pattern = "yyyyMMddHHmmss";
                }
            }
        }

        sdf.applyPattern(pattern);
        return sdf.parse(str);

    }

    public static void main(String[] args) throws Exception
    {
        String line = "abc$LIMIT{1000-01-01 00:00:00,1000-01-01 00:00:00}";

        FtpRuleLimit lrl = new FtpRuleLimit();
        lrl.GetTimeLimit(line, new Date());
    }

}
