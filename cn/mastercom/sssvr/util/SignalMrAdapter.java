package cn.mastercom.sssvr.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;


import java.util.TimeZone;

public class SignalMrAdapter implements Callable<Object>
{
    SimpleDateFormat format_from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    SimpleDateFormat format_to = new SimpleDateFormat("yyMMdd");

    private String fileName;
    private String bcpFilePath;
    private String backupPath;

    public SignalMrAdapter(String bcpFilePath, String backupPath, String file)
    {
        this.bcpFilePath = bcpFilePath;
        this.backupPath = backupPath;
        this.fileName = file;

    }

    public Object call()
    {

        Map<String, ArrayList<String>> results = new HashMap<String, ArrayList<String>>();
        File file = new File(fileName);
        String bcpName = file.getName();
        try
        {
            DecodeFile(results, fileName, bcpName);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.out.println("出错:" + e.getMessage());
            return fileName + " 解码失败!";
        }
        finally
        {
            backup();
        }

        if (results.size() > 0)
        {
            for (Entry<String, ArrayList<String>> entry : results.entrySet())
            {
                String date = entry.getKey();
                ArrayList<String> val = entry.getValue();
                Export(val, date, bcpName);
                val.clear();
            }
            results.clear();
        }

        return fileName + " 解码成功!";
    }

    private void backup()
    {
        try
        {
            File oldFile = new File(fileName);

            if (backupPath.length() > 0)
            {
                File dir = new File(backupPath);
                if (!dir.exists())
                {
                    dir.mkdirs();
                }

                File newFile = new File(dir.getPath() + "\\" + oldFile.getName());

                if (newFile.exists())
                {
                    FileOpt.deleteFile(newFile);
                }

                FileOpt.moveFile(oldFile, newFile);
            }
            else
            {
                FileOpt.deleteFile(oldFile);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public void DecodeFile(Map<String, ArrayList<String>> results, String file, String bcpName) throws IOException
    {
        FileInputStream fs = new FileInputStream(file);
        try
        {
            DecodeStream(results, fs, bcpName);
        }
        finally
        {
            fs.close();
        }
    }

    public void DecodeStream(Map<String, ArrayList<String>> results, InputStream fs, String bcpName) throws IOException
    {
        String encoding = "UTF-8"; // "GBK" // "UTF-8"
        InputStreamReader isr = new InputStreamReader(fs, encoding);
        try
        {
            DecodeReader(results, isr, bcpName);
        }
        finally
        {
            isr.close();
        }
    }

    public void DecodeReader(Map<String, ArrayList<String>> results, Reader fr, String bcpName) throws IOException
    {
        Sample_4G mr = new Sample_4G();
        String date = null;
        BufferedReader br = new BufferedReader(fr);
        String line = null;
        while ((line = br.readLine()) != null)
        {
            try
            {
                date = adapt(mr, line);
                if (date != null)
                {
                    ArrayList<String> ls = null;
                    if (results.containsKey(date))
                    {
                        ls = results.get(date);
                        ls.add(mr.GetData());
                        
                        if (ls.size() >= 50000)
                        {
                            Export(ls, date, bcpName);
                            ls.clear();
                        }
                    }
                    else
                    {
                        ls = new ArrayList<String>();
                        ls.add(mr.GetData());
                        results.put(date, ls);
                    }
                }
            }
            catch (Exception ex)
            {
            }
        }

    }

    // 返回日期
    private String adapt(Sample_4G mrResult, String line)
    {

        // 2017-03-28 00:01:29,535 INFO
        // [com.xwsx.action.NetworkInformationCollection] -
        // 网络信息采集解密后的字符串为：{"rxlev":"2147483647","lteCid":"204446989","lteRxlev":"-99","model":"vivo
        // Y13L","networkType":"WIFI","osVersion":"4.4.4","ltePci":"-1","wifiMac":"f4:29:81:b7:94:b5","imei":"867483024916181","lac":"2147483647","lteRsrq":"-8","ci":"2147483647","telNum":"15053401491","longitude":"116.288414","latitude":"37.432582","lteTac":"21327","imsi":"460023665695070","wifiRssi":"-48","lteSnr":"210"}

        mrResult.Clear();
        String sdate = null;

        int index = line.indexOf(',');
        if (index < 0) return null;
        try
        {
            TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
            Date t = format_from.parse(line.substring(0, index));
            mrResult.itime = (int) (t.getTime() / 1000L);
            sdate = format_to.format(t);
        }
        catch (ParseException e)
        {
            return null;
        }

        index = line.indexOf('{');
        if (index < 0) return null;

        line = line.substring(index + 1, line.length() - 1);

        String[] arrs = line.split(",");

        String[] wifi = new String[2];

        for (String s : arrs)
        {
            decodeOne(mrResult, wifi, s);
        }

        // 为了兼容之前的格式
        mrResult.UserLabel = wifi[0] + ";" + wifi[1] + ";";

        return sdate;
    }

    // 去掉首尾一个引号
    private String trimBE(String s)
    {
        if (s.startsWith("\""))
        {
            if (s.endsWith("\""))
            {
                return s.substring(1, s.length() - 1);
            }
        }

        return s;
    }

    // 解析一个键值对
    private void decodeOne(Sample_4G mr, String[] wifi, String s)
    {
        int index = s.indexOf(':');
        if (index < 0) return;

        String name = trimBE(s.substring(0, index).trim()).trim().toLowerCase();
        String value = trimBE(s.substring(index + 1, s.length()).trim()).trim();
        if (value.equals("2147483647")) return;// 2147483647,无效值,直接用默认值,所以可直接返回

        switch (name)
        {
        case "longitude":
            try
            {
                mr.ilongitude = (int) (Double.parseDouble(value) * 10000000);
            }
            catch (Exception e)
            {
            }
            break;

        case "latitude":
            try
            {
                mr.ilatitude = (int) (Double.parseDouble(value) * 10000000);
            }
            catch (Exception e)
            {
            }
            break;

        case "ltetac":
            try
            {
                mr.iLAC = Integer.parseInt(value);
            }
            catch (Exception e)
            {
            }
            break;

        case "ltecid":
            try
            {
                mr.iCI = Integer.parseInt(value);
                mr.Eci = mr.iCI;
            }
            catch (Exception e)
            {
            }
            break;

        case "imsi":
            try
            {
                mr.IMSI = Long.parseLong(value);
            }
            catch (Exception e)
            {
            }
            break;

        case "telnum":
            mr.MSISDN = value;
            break;

        case "lterxlev":
            try
            {
                mr.LteScRSRP = Integer.parseInt(value);
            }
            catch (Exception e)
            {
            }
            break;

        case "ltersrq":
            try
            {
                mr.LteScRSRQ = Integer.parseInt(value);
            }
            catch (Exception e)
            {
            }
            break;

        case "ltepci":
            try
            {
                mr.LteScPci = Integer.parseInt(value);
            }
            catch (Exception e)
            {
            }
            break;

        case "ltesnr":
            try
            {
                mr.LteScSinrUL = Integer.parseInt(value);
            }
            catch (Exception e)
            {
            }
            break;

        case "networktype":
            mr.networktype = value;
            break;

        case "wifimac":
            wifi[0] = value.replace(":", "");
            break;

        case "wifirssi":
            try
            {
                wifi[1] = Integer.toString(-Integer.parseInt(value));
            }
            catch (Exception e)
            {
                wifi[1] = "0";
            }
            break;
        }
    }

    public void Export(ArrayList<String> datas, String date, String fileName)
    {
        fileName = fileName.substring(0, fileName.length() - 4);
        String cqtFilename = "TB_CQTSIGNAL_SAMPLE_01_" + date + "\\" + fileName + "_" + date + ".sample";
        BufferedWriter cqtDw = null;
        try
        {
            File tempfile = new File(bcpFilePath + "\\TB_CQTSIGNAL_SAMPLE_01_" + date);
            if (!tempfile.exists())
            {
                tempfile.mkdirs();
            }
            File file = new File(bcpFilePath + "\\" + cqtFilename);
            cqtDw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
            for (String data : datas)
            {
                cqtDw.write(data + "\r\n");
            }
            cqtDw.flush();
        }
        catch (Exception e)
        {
            System.out.println(">>>" + e.toString());
        }
        finally
        {
            try
            {
                if (cqtDw != null)
                {
                    cqtDw.close();
                }
            }
            catch (IOException e)
            {
            }
        }
    }

    public static void main(String[] args) throws IOException
    {
        String file = "E:\\temp\\temp\\info4.log_2017-04-19";
        SignalMrAdapter sma = new SignalMrAdapter("E:\\temp\\temp", "", file);
        sma.call();
    }
}
