package cn.mastercom.sssvr.main;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.*;
import java.util.zip.*;

import com.ice.tar.*;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;



//import org.apache.cassandra.service.IReadCommand;
//import org.apache.commons.net.ftp.FTPFile;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Processor.list_privileges;
//import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.booleanValue_return;
//import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.stringLiteralSequence_return;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
//import org.stringtemplate.v4.compiler.CodeGenerator.primary_return;



import cn.mastercom.sssvr.util.CalendarEx;
import cn.mastercom.sssvr.util.HadoopFSOperations;
import cn.mastercom.sssvr.util.HdfsExplorer;
import cn.mastercom.sssvr.util.LocalFile;
import cn.mastercom.sssvr.util.ReturnConfig;

@SuppressWarnings("unused")
/**
 * 解码MRO
 * 
 *
 */
public class MroDecoder extends Thread
{
    /**
     * 定义一个内部“任务”类，fileList:待处理的文件名组成的list hdfsPathName：解码后文件放的位置
     *
     */
    enum FilePackType
    {
        none, zip, tar, targz
    }

    // zip包装类,阻止流被关闭
    class ZipInputStream2 extends ZipInputStream
    {
        public ZipInputStream2(InputStream in)
        {
            super(in);
            CanClose = false;
        }

        public Boolean CanClose;

        @Override
        public void close() throws IOException
        {
            if (CanClose) super.close();
        }
    }

    // tar包装类,阻止流被关闭
    class TarInputStream2 extends TarInputStream
    {
        public TarInputStream2(InputStream in)
        {
            super(in);
            CanClose = false;

        }

        public Boolean CanClose;

        @Override
        public void close() throws IOException
        {
            if (CanClose) super.close();
        }
    }

    class TaskInfo
    {
        public FilePackType packType = FilePackType.none;// zip//tar//
        public List<Integer> entryIndexes = new ArrayList<Integer>();// 小文件所在序号
        public List<String> fileList = new ArrayList<String>();// 待处理文件完整路径组成的list
        public String hdfsPathName; // 待处理文件即将要存放的文件夹完整路径
    }

    /**
     * 
     * @param flag
     *            0表示解码，1表示上传
     */
    public MroDecoder(int flag)
    {
        this.flag = flag;
        if (flag == 1) FileMover.Init();
    }

    /**
     * 内部线程类，多线程进行解码并将解码后的信息写进文件
     */
    class FileParserCallable implements Callable<Object>
    {
        private TaskInfo task;

        FileParserCallable(TaskInfo taskInfo)
        {
            this.task = taskInfo;
        }

        // @SuppressWarnings("static-access")
        @Override
        public Object call() throws Exception
        {
            RuningTask++;
            if (task != null)
            {
                try
                {
                    System.out.println(new Date().toString() + " bing process hdfsName: " + task.hdfsPathName + ","
                            + task.fileList.size()); // 输出这个任务中有多少个文件

                    if (task.fileList.size() == 0)
                    {
                        RuningTask--;
                        return false;
                    }

                    String dir = task.hdfsPathName;
                    // LocalFile.makeDir(dir);
                    File f = new File(dir);
                    String gsmDir = f.getParentFile().getParentFile().getParent() + "/gsm/" + f.getName();
                    // LocalFile.makeDir(gsmDir);

                    doFiles(dir, gsmDir);

                }
                catch (Exception e)
                {
                    e.printStackTrace();
                    System.out.println((new Date()).toString() + " Task Exec Error:" + task.hdfsPathName + "\r\n"
                            + e.getMessage());
                    System.out.println(e.getStackTrace());
                    RuningTask--;
                    return "Task Exec Error:" + task.hdfsPathName + "\r\n" + e.getMessage();
                }
                finally
                {

                }
            }
            RuningTask--;
            System.out.println("Task Exec Success:" + task.hdfsPathName + ",RuningTasks:" + RuningTask);
            return "Task Exec Success:" + task.hdfsPathName;
        }

        private void doFiles(String dir, String gsmDir)
        {
            if (task.fileList.size() == 0) return;

            if (task.packType == FilePackType.none)
            {
                doTask(dir, gsmDir);
            }
            else
            {
                doPackTask(task.packType, task.fileList.get(0), task.entryIndexes, dir, gsmDir);
            }
        }

        private void doTask(String dir, String gsmDir)
        {
            //System.out.println(new Date().toString() + " bing process hdfsName: " + task.hdfsPathName + ","
            //        + task.fileList.size()); // 输出这个任务中有多少个文件

            int total = task.fileList.size();
            int fileNum = 0;
            for (String file : task.fileList)
            {
                if (MainSvr.bExitFlag) break;

                decodeFile(file, dir, gsmDir);

                backupFile(file);

                fileNum++;
                logProcessing(fileNum, total);
            }
        }

        private void doPackTask(FilePackType packType, String file, List<Integer> indexes, String dir, String gsmDir)
        {
            //System.out.println(new Date().toString() + " bing process hdfsName: " + task.hdfsPathName + ","
            //       + task.entryIndexes.size()); // 输出这个任务中有多少个文件

            int total = indexes.size();
            if (total == 0) return;

            FileInputStream stream = null;

            try
            {
                stream = new FileInputStream(file);

                if (packType == FilePackType.zip)
                {
                    doPackTask_zip(stream, indexes, dir, gsmDir);
                }
                else if (packType == FilePackType.tar)
                {
                    doPackTask_tar(stream, indexes, dir, gsmDir);
                }
                else if (packType == FilePackType.targz)
                {
                    doPackTask_targz(stream, indexes, dir, gsmDir);
                }
            }
            catch (FileNotFoundException e)
            {
                e.printStackTrace();
                System.out.println("File Not Found:" + file);
            }
            catch (IOException e)
            {
                e.printStackTrace();
                System.out.println("IOException:" + file);
                System.out.println(e.getStackTrace());
            }
            finally
            {
                if (stream != null)
                {
                    try
                    {
                        stream.close();
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }

                }
            }
        }

        private void doPackTask_zip(InputStream stream, List<Integer> indexes, String dir, String gsmDir)
                throws IOException
        {
            int total = indexes.size();
            ZipInputStream2 zip = new ZipInputStream2(stream);
            zip.CanClose = false;

            try
            {
                int index = 0;
                while (true)
                {
                    if (MainSvr.bExitFlag) break;

                    ZipEntry entry = zip.getNextEntry();
                    if (entry == null) break;

                    if (index == indexes.get(0))
                    {
                        indexes.remove(0);
                        decodeStream(zip, entry.getName(), dir, gsmDir);

                        int size = indexes.size();

                        logProcessing(total - size, total);
                        if (size == 0) break;
                    }

                    index++;
                }

            }
            finally
            {
                zip.CanClose = true;
                zip.close();
            }
        }

        private void doPackTask_tar(InputStream stream, List<Integer> indexes, String dir, String gsmDir)
                throws IOException
        {
            int total = indexes.size();
            TarInputStream2 tar = new TarInputStream2(stream);
            tar.CanClose = false;

            try
            {
                int index = 0;
                while (true)
                {
                    if (MainSvr.bExitFlag) break;

                    TarEntry entry = tar.getNextEntry();
                    if (entry == null) break;

                    if (index == indexes.get(0))
                    {
                        indexes.remove(0);
                        decodeStream(tar, entry.getName(), dir, gsmDir);

                        int size = indexes.size();

                        logProcessing(total - size, total);
                        if (size == 0) break;
                    }

                    index++;
                }

            }
            finally
            {
                tar.CanClose = true;
                tar.close();
            }
        }

        private void doPackTask_targz(InputStream stream, List<Integer> indexes, String dir, String gsmDir)
                throws IOException
        {
            GZIPInputStream gz = null;
            try
            {
                gz = new GZIPInputStream(stream);
                doPackTask_tar(gz, indexes, dir, gsmDir);
            }
            finally
            {
                if (gz != null) gz.close();
            }
        }

        private void logProcessing(int value, int total)
        {
            if (value % 100 == 0) System.out.println((new Date()).toString() + " " + task.hdfsPathName
                    + " has deal files:" + value + "/" + total + ", Running Tasks:" + RuningTask);
        }

        private void decodeFile(String file, String dir, String gsmDir)
        {
            FileInputStream stream = null;
            try
            {
                stream = new FileInputStream(file);
                decodeStream(stream, file, dir, gsmDir);
            }
            catch (FileNotFoundException e)
            {
                e.printStackTrace();
                System.out.println("File Not Found:" + file);
            }
            finally
            {
                if (stream != null)
                {
                    try
                    {
                        stream.close();
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
        }

        private void decodeStream(InputStream stream, String file, String dir, String gsmDir)
        {
            try
            {
                List<String> lines = Decode(stream, file);
                String name = LocalFile.getFileName(file);
                saveResult(lines, name, dir, gsmDir);
            }
            catch (XMLStreamException e)
            {
                // e.printStackTrace();
                System.out.println("XML格式错误：" + file + "," + e.getMessage());
            }
            catch (Exception e)
            {
                // e.printStackTrace();
                System.out.println("解码失败：" + file + "," + e.getMessage());
                System.out.println(e.getStackTrace());
            }
        }

        private void saveResult(List<String> lines, String name, String dir, String gsmDir)
        {
            if (lines.size() == 0) return;

            LocalFile.makeDir(dir);

            String file = dir + "/MR_" + name + ".bcp";
            saveFile(file, lines);

            //if (DealQci == 1)
            //{
                //List<String> gsmLines = new ArrayList<String>();

                // for (String line : lines)
                // {
                // if (line.charAt(0) == '2')
                // {
                // gsmLines.add(line);
                // }
                // }
                //
                // if (gsmLines.size() > 0)
                // {
                // LocalFile.makeDir(gsmDir);
                // String gsmFile = gsmDir + "/MR_" + name + ".bcp";
                // saveFile(gsmFile, gsmLines);
                // }

            //}

        }

        private void saveFile(String file, List<String> lines)
        {
            if (lines.size() == 0) return;
            OutputStream fs = null;

            try
            {
                if (Compress == 1)
                {
                    file += ".gz";
                    // 正在写文件
                    file += ".processing";
                    fs = new GZIPOutputStream(new FileOutputStream(file));
                }
                else
                {
                    file += ".processing";
                    fs = new FileOutputStream(file);
                }

                for (String line : lines)
                {
                    byte[] byt = (line + "\r\n").substring(2).getBytes();
                    fs.write(byt, 0, byt.length);
                }

                fs.flush();

            }
            catch (FileNotFoundException e)
            {
                e.printStackTrace();
                System.out.println("File Not Found:" + file);
            }
            catch (IOException e)
            {
                e.printStackTrace();
                System.out.println("IOException:" + file);
            }
            finally
            {
                if (fs != null)
                {
                    try
                    {
                        fs.close();
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                }
            }

            // 写完文件后改名
            int length = file.length() - 11;
            LocalFile.renameFile(file, file.substring(0, length));
        }

    }

    private static String hdfsRoot = "";
    private static String RootUser = "";
    private static String RootPass = "jian(12)";

    private static String MrFilePath = "";
    private static String MrBkPath = "";
    private static String BcpBkPath = "";
    private static String MrDecodePath = "";
    private static int MinFiles = 500;
    private static int Compress = 1;
    private static int DelayMinute = 90;

    private static int MoverId = 0;
    public static int nThreadNum = 10;
    public static int MrOutputNum = 10;
    public static String BcpBkHours = "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23";
    private static int DealQci = 0;
    private static FilePackType MrFilePackType = FilePackType.none;

    static HadoopFSOperations hdfs = null;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    /**
     * 读取配置文件
     */
    public static void readConfigInfo()
    {
        try
        {
            // XMLWriter writer = null;// 声明写XML的对象
            SAXReader reader = new SAXReader();

            OutputFormat format = OutputFormat.createPrettyPrint();
            format.setEncoding("GBK");// 设置XML文件的编码格式

            String filePath = "conf/config.xml";
            File file = new File(filePath);
            if (file.exists())
            {
                Document doc = reader.read(file);// 读取XML文件

                {
                    List<String> list = doc.selectNodes("//comm/HdfsRoot");
                    Iterator iter = list.iterator();
                    while (iter.hasNext())
                    {
                        Element element = (Element) iter.next();
                        hdfsRoot = element.getText();
                        break;
                    }
                }

                {
                    List<String> list = doc.selectNodes("//comm/RootUser");
                    Iterator iter = list.iterator();
                    while (iter.hasNext())
                    {
                        Element element = (Element) iter.next();
                        RootUser = element.getText();
                        break;
                    }
                }

                {
                    List<String> list = doc.selectNodes("//comm/MrFilePath");
                    Iterator iter = list.iterator();
                    while (iter.hasNext())
                    {
                        Element element = (Element) iter.next();
                        MrFilePath = element.getText();
                        break;
                    }
                }

                {
                    List<String> list = doc.selectNodes("//comm/MinFiles");
                    Iterator iter = list.iterator();
                    while (iter.hasNext())
                    {
                        Element element = (Element) iter.next();
                        MinFiles = Integer.parseInt(element.getText());
                        break;
                    }
                }

                {
                    List<String> list = doc.selectNodes("//comm/MrBkPath");
                    Iterator iter = list.iterator();
                    while (iter.hasNext())
                    {
                        Element element = (Element) iter.next();
                        MrBkPath = element.getText();
                        break;
                    }
                }

                {
                    List<String> list = doc.selectNodes("//comm/BcpBkPath");
                    Iterator iter = list.iterator();
                    while (iter.hasNext())
                    {
                        Element element = (Element) iter.next();
                        BcpBkPath = element.getText();
                        break;
                    }
                }

                {
                    List<String> list = doc.selectNodes("//comm/BcpBkHours");
                    Iterator iter = list.iterator();
                    while (iter.hasNext())
                    {
                        Element element = (Element) iter.next();
                        BcpBkHours = element.getText();
                        break;
                    }
                }

                {
                    List<String> list = doc.selectNodes("//comm/MrDecodePath");
                    Iterator iter = list.iterator();
                    while (iter.hasNext())
                    {
                        Element element = (Element) iter.next();
                        MrDecodePath = element.getText();
                        break;
                    }
                }

                /*
                 * { List<String> list = doc.selectNodes("//comm/NameNodeIp");
                 * Iterator iter = list.iterator(); while (iter.hasNext()) {
                 * Element element = (Element) iter.next(); NameNodeIp =
                 * element.getText(); break; } }
                 */

                {
                    List<String> list = doc.selectNodes("//comm/MoverId");
                    Iterator iter = list.iterator();
                    while (iter.hasNext())
                    {
                        Element element = (Element) iter.next();
                        MoverId = Integer.parseInt(element.getText());
                        break;
                    }
                }

                {
                    List<String> list = doc.selectNodes("//comm/MrOutputNum");
                    Iterator iter = list.iterator();
                    while (iter.hasNext())
                    {
                        Element element = (Element) iter.next();
                        MrOutputNum = Integer.parseInt(element.getText());
                        break;
                    }
                }

                {
                    List<String> list = doc.selectNodes("//comm/Compress");
                    Iterator iter = list.iterator();
                    while (iter.hasNext())
                    {
                        Element element = (Element) iter.next();
                        Compress = Integer.parseInt(element.getText());
                        break;
                    }
                }

                {
                    List<String> list = doc.selectNodes("//comm/DelayMinute");
                    Iterator iter = list.iterator();
                    while (iter.hasNext())
                    {
                        Element element = (Element) iter.next();
                        DelayMinute = Integer.parseInt(element.getText());
                        break;
                    }
                }

                {
                    List<String> list = doc.selectNodes("//comm/ThreadNum");
                    Iterator iter = list.iterator();
                    while (iter.hasNext())
                    {
                        Element element = (Element) iter.next();
                        nThreadNum = Integer.parseInt(element.getText());
                        break;
                    }
                }

                {
                    List<String> list = doc.selectNodes("//comm/RootPass");
                    Iterator iter = list.iterator();
                    try
                    {
                        while (iter.hasNext())
                        {
                            Element element = (Element) iter.next();
                            if (element != null) RootPass = element.getText();
                            break;
                        }
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                    }
                }

                {
                    List<String> list = doc.selectNodes("//comm/DealQci");
                    Iterator iter = list.iterator();
                    try
                    {
                        while (iter.hasNext())
                        {
                            Element element = (Element) iter.next();
                            if (element != null) 
                            	DealQci = Integer.parseInt(element.getText());
                            break;
                        }
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                    }
                }

                {
                    List<String> list = doc.selectNodes("//comm/MrFilePackType");
                    Iterator iter = list.iterator();
                    try
                    {
                        while (iter.hasNext())
                        {
                            Element element = (Element) iter.next();
                            if (element != null)
                            {
                                String value = element.getText().toLowerCase().trim();
                                if (value.equals("zip"))
                                {
                                    MrFilePackType = FilePackType.zip;
                                }
                                else if (value.equals("tar"))
                                {
                                    MrFilePackType = FilePackType.tar;
                                }
                                else if (value.equals("targz"))
                                {
                                    MrFilePackType = FilePackType.targz;
                                }
                                else
                                {
                                    MrFilePackType = FilePackType.none;
                                }
                            }
                            break;
                        }
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                    }
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static String GetDateString(String str) 
	{     
	    String regEx = "201\\d{5}";
	    Pattern pattern = Pattern.compile(regEx);
	    Matcher matcher = pattern.matcher(str);
	    boolean rs = matcher.find();
	    if(rs)
	    {
	    	return matcher.group();
	    }
	    return ""; 
	}
    
    static String GetBackPath(String MroFile)
    {
        File f = new File(MroFile);
        if (MrFilePackType == FilePackType.none)
        {
            // TD-LTE_MRE_HUAWEI_010148010242_290822_20160509024500.xml.gz
            // TD-LTE_MRO_ZTE_ OMC1_ 307285_20160511114500.zip

            // "hdfs://10.139.6.169:9000/mt_wlyh/Data/" + suffix+"/"+
            // dirName,fileName,suffix.toLowerCase().replace("mt", "")))

            String[] vct = f.getName().split("_");

            if (vct.length >= 5)
            {
                String suffix = vct[1].toLowerCase();
                if (suffix.equals("mro")) suffix += "mt";
                return MrBkPath + "/" + vct[2] + "/" + vct[1] + "/" + vct[5].substring(0, 8) + "/" + vct[4] + "/";
            }
            return "";
        }
        else
        {
            return MrBkPath + "/" +GetDateString(MroFile);
        }
    }

    /**
     * 解码后文件存放位置
     * 
     * @param MroFile
     * @param MoveId
     * @return
     */
    static String GetOutputPath(String MroFile, Integer MoveId, boolean CheckTime)
    {
        // TD-LTE_MRE_HUAWEI_010148010242_290822_20160509024500.xml.gz
        // TD-LTE_MRO_ZTE_ OMC1_ 307285_20160511114500.zip
        try
        {
            File f = new File(MroFile);

            // "hdfs://10.139.6.169:9000/mt_wlyh/Data/" + suffix+"/"+
            // dirName,fileName,suffix.toLowerCase().replace("mt", "")))
            String[] vct = f.getName().split("\\_");

            if (vct.length >= 5)
            {
            	if(CheckTime)
            	{
	                // 2016 0511 1145
	                String TimeStr = vct[5].substring(0, 12);
	                CalendarEx cal = new CalendarEx(new Date());
	                cal = cal.AddMinutes(-DelayMinute); 
	                if (TimeStr.compareTo(cal.getDateStr12()) >= 0) 
	                	return "";
            	}

                String suffix = vct[1].toLowerCase();

                int enbid = 0;
                try
                {
                    enbid = Integer.parseInt(vct[4]);
                }
                catch (NumberFormatException e)
                {

                }

                enbid = enbid % MrOutputNum; // 为了分成更小文件吗？
                // char lastChar = vct[4].charAt(vct[4].length()-1);

                /*
                 * if(lastChar =='1' || lastChar =='3' || lastChar =='5' ||
                 * lastChar =='7' || lastChar =='9') lastChar = '1'; else
                 * lastChar = '0';
                 */

                if (suffix.equals("mro")) suffix += "mt";

                // return (MrDecodePath.length() > 0 ? MrDecodePath : hdfsRoot)
                // + "/mt_wlyh/Data/" + suffix + "/"
                // + vct[5].substring(2, 8) + "/" + vct[2] + MoveId.toString() +
                // "_" + vct[5].substring(2, 12) + "."
                // + vct[1] + enbid;

                return (MrDecodePath.length() > 0 ? MrDecodePath : hdfsRoot) + "/mt_wlyh/Data/" + suffix + "/"
                        + vct[5].substring(2, 8) + "/" + vct[2] + MoveId.toString() + "_" + vct[5].substring(2, 12)
                        + "_" + enbid;
            }
        }
        catch (Exception e)
        {

            e.printStackTrace();
        }
        System.out.println("GetOutputPath Errror:" + MroFile);
        return "";
    }

    Map<String, Integer> taskMap = new HashMap<String, Integer>();// 全局变量 //?
    static int RuningTask = 0;// 全局变量

    private Stack<TaskInfo> getTaskInfos(List<String> fileList)
    {
        Map<String, List<String>> file_map = new HashMap<String, List<String>>();// 这个map中key表示分组文件夹valuelist中存放的是要装在这个文件夹中的文件完整路径

        for (String file : fileList)
        {
            String hdfsName = GetOutputPath(file, MoverId, true);
            if (hdfsName.length() == 0) continue;

            if (file_map.containsKey(hdfsName))
            {
                List<String> files = file_map.get(hdfsName);
                files.add(file);
            }
            else
            {
                List<String> files = new ArrayList<String>();
                files.add(file);
                file_map.put(hdfsName, files);
            }
        }

        Stack<TaskInfo> taskInfoStack = new Stack<TaskInfo>();
        for (Entry<String, List<String>> item : file_map.entrySet())
        {
            String hdfsName = item.getKey();
            List<String> files = item.getValue();
            if (files.size() > MinFiles)
            {
                addTaskInfo(taskInfoStack, hdfsName, files);
            }
            else
            {
                if (taskMap.containsKey(hdfsName))
                {
                    int dealTimes = taskMap.get(hdfsName);
                    if (dealTimes > 3)
                    {
                        addTaskInfo(taskInfoStack, hdfsName, files);
                    }
                    else
                    {
                        dealTimes++;
                        taskMap.put(hdfsName, dealTimes);
                    }
                }
                else
                {
                    taskMap.put(hdfsName, 1);
                }
            }
        }

        return taskInfoStack;

    }

    private void addTaskInfo(Stack<TaskInfo> taskInfoStack, String hdfsName, List<String> files)
    {
        TaskInfo newtask = new TaskInfo();
        newtask.hdfsPathName = hdfsName;
        newtask.fileList = files;
        taskInfoStack.push(newtask);
        taskMap.remove(hdfsName);
    }

    // pack files
    private Stack<TaskInfo> getPackTaskInfos(List<String> fileList, FilePackType packType)
    {
        Stack<TaskInfo> taskInfoStack = new Stack<TaskInfo>();
        for (String file : fileList)
        {
            getPackTaskInfos_oneFile(taskInfoStack, packType, file);
        }
        return taskInfoStack;
    }

    // pack file
    private void getPackTaskInfos_oneFile(Stack<TaskInfo> taskInfoStack, FilePackType packType, String file)
    {
        Map<String, List<Integer>> file_map = new HashMap<String, List<Integer>>();// 这个map中key表示分组文件夹valuelist中存放的是要装在这个文件夹中的文件完整路径
        FileInputStream stream = null;
        try
        {
            stream = new FileInputStream(file);

            if (packType == FilePackType.zip)
            {
                getPackTaskInfos_zip(file_map, stream);
            }
            else if (packType == FilePackType.tar)
            {
                getPackTaskInfos_tar(file_map, stream);
            }
            else if (packType == FilePackType.targz)
            {
                getPackTaskInfos_targz(file_map, stream);
            }

            for (Entry<String, List<Integer>> item : file_map.entrySet())
            {
                String hdfsName = item.getKey();
                List<Integer> indexes = item.getValue();

                addPackTaskInfo(taskInfoStack, packType, file, hdfsName, indexes);
            }

        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
            System.out.println("File Not Found:" + file);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            System.out.println("IOException:" + file);
            System.out.println(e.getStackTrace());
        }
        finally
        {
            if (stream != null)
            {
                try
                {
                    stream.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }
    }

    // zip
    private void getPackTaskInfos_zip(Map<String, List<Integer>> file_map, InputStream stream) throws IOException
    {
        ZipInputStream zip = new ZipInputStream(stream);
        try
        {
            ZipEntry entry = null;
            int index = 0;
            while (true)
            {
                entry = zip.getNextEntry();
                if (entry == null) break;

                if (!entry.isDirectory())
                {
                    String entryName = entry.getName();
                    String hdfsName = GetOutputPath(entryName, MoverId, false);
                    addPackIndex(file_map, hdfsName, index);
                }
                index++;
            }
        }
        finally
        {
            zip.close();
        }
    }

    // Tar.gz
    private void getPackTaskInfos_targz(Map<String, List<Integer>> file_map, InputStream stream) throws IOException
    {
        GZIPInputStream gz = null;
        try
        {
            gz = new GZIPInputStream(stream);
            getPackTaskInfos_tar(file_map, gz);
        }
        finally
        {
            if (gz != null) gz.close();
        }
    }

    // Tar
    private void getPackTaskInfos_tar(Map<String, List<Integer>> file_map, InputStream stream) throws IOException
    {
        TarInputStream tar = new TarInputStream(stream);
        try
        {
            TarEntry entry = null;
            int index = 0;
            while (true)
            {
                entry = tar.getNextEntry();
                if (entry == null) break;

                if (!entry.isDirectory())
                {
                    String entryName = entry.getName();
                    String hdfsName = GetOutputPath(entryName, MoverId, false);
                    addPackIndex(file_map, hdfsName, index);
                }
                index++;
            }
        }
        finally
        {
            tar.close();
        }
    }

    private void addPackIndex(Map<String, List<Integer>> file_map, String hdfsName, int index)
    {
        if (file_map.containsKey(hdfsName))
        {
            List<Integer> indexes = file_map.get(hdfsName);
            indexes.add(index);
        }
        else
        {
            List<Integer> indexes = new ArrayList<Integer>();
            indexes.add(index);
            file_map.put(hdfsName, indexes);
        }
    }

    private void addPackTaskInfo(Stack<TaskInfo> taskInfoStack, FilePackType packType, String file, String hdfsName,
            List<Integer> indexes)
    {
        TaskInfo newtask = new TaskInfo();
        newtask.packType = packType;
        newtask.hdfsPathName = hdfsName;
        newtask.fileList.add(file);
        newtask.entryIndexes = indexes;
        taskInfoStack.push(newtask);
    }

    private Stack<TaskInfo> getTaskInfosByPackType(List<String> fileList, FilePackType packType)
    {
        if (packType == FilePackType.none)
        {
            return getTaskInfos(fileList);
        }
        return getPackTaskInfos(fileList, packType);
    }

    private void backupPackFiles(List<String> fileList)
    {
        for (String file : fileList)
        {
            backupFile(file);
        }
    }

    static void backupFile(String file)
    {
        if (MrBkPath.length() > 0)
        {
            String bakPath = GetBackPath(file);
            LocalFile.renameFile(file, bakPath + "/" + new File(file).getName());
        }
        else
        {
            LocalFile.deleteFile(file);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })

    /**
     * 将数据分成组装进dictionaryMap中，然后按照分好的分组进行解码，并将文件写进相应的文件夹中
     */
    public void DecodeMrFiles()
    {
        try
        {
            System.out.println((new Date()).toString() + " Scan Files to Process: ");
            List<String> fileList = LocalFile.getAllFiles(new File(MrFilePath), "MR", 2);// list中的元素格式如：D:\mastercom\MroMreData\TD-LTE_MRE_ALCATEL_OMCR2_0_20160521000000.xml.gz完整路径
            System.out.println((new Date()).toString() + " Find Unprocessed files: " + fileList.size());

            Stack<TaskInfo> stack = getTaskInfosByPackType(fileList, MrFilePackType);

            if (stack.size() == 0)
            {
                return;
            }
            RuningTask = 0;
            ExecutorService pool = Executors.newFixedThreadPool(nThreadNum);// 创建一个固定大小的线程池
            // 创建多个有返回值的任务
            List<Future> listTask = new ArrayList<Future>();// 存放各个线程返回的结果
            int taskIdx = stack.size();

            for (int i = 0; i < taskIdx; i++)
            {// 多线程解码
                Callable fm = new FileParserCallable((TaskInfo) stack.pop());
                // 执行任务并获取Future对象
                Future f1 = pool.submit(fm);
                listTask.add(f1);
            }

            // 关闭线程池
            // pool.shutdown();

            // 获取所有并发任务的运行结果

            for (Future f : listTask)
            {
                try
                {
                    f.get();
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }

            pool.shutdown();
            System.out.println("pool complete!");
            if (MrFilePackType != FilePackType.none) backupPackFiles(fileList);
            System.gc();
            System.runFinalization();
        }
        catch (Exception e1)
        {
            // e1.printStackTrace();
            System.out.println("DecodeMrFiles error:" + e1.getMessage());
        }
    }

    public void Init()
    {
        System.out.println("MroDecoder Init.");
        readConfigInfo();
        if (hdfsRoot.length() > 4) 
        	hdfs = new HadoopFSOperations(hdfsRoot);
    }

    public static void main(String[] args)
    {
     	if (ReturnConfig.returnconfig("conf/config.xml", "//comm//DEAL_TianJinMr").equals("1"))
		{
			FileMover.Init();// 启动ftp搬移程序
			MroDecoder md = new MroDecoder(3);
			md.Init();
			md.start();
			return;
		}
        //MroDecoder md = new MroDecoder(0);
        //md.Init();
        //md.start();

        //MroDecoder md2 = new MroDecoder(1);
        //md2.Init();
        //md2.start();
        
        /*try
        {
            List<String> list = md.Decode("E:\\temp\\TD-LTE_MRO_NSN_OMC_585728_20170324133000.xml");
            BufferedWriter infoWriter = new BufferedWriter(new FileWriter("E:\\temp\\decode\\MRO_501889_0000_x.bcp"));

            for (String line : list)
            {
                infoWriter.write(line.substring(2) + "\r\n");
            }
            infoWriter.close();
        }
        catch (Exception e)
        { //
            e.printStackTrace();
        }

        
         * try { List<String> list =
         * md.Decode("d:/TD-LTE_MRO_NSN_OMC_585728_20170324133000.xml.gz");
         * BufferedWriter infoWriter = new BufferedWriter(new
         * FileWriter("d:/TD-LTE_MRO_NSN_OMC_585728_20170324133000.xml.gz.bcp"))
         * ; for(String line:list) { infoWriter.write(line+"\r\n"); }
         * infoWriter.close(); } catch (Exception e) { //e.printStackTrace(); }
         */



        // md.Init();
        // md.run();
    }

    static String[] m_columns = null;
    static HashMap<String, Integer> m_columnsIndexMap = null;

    static
    {
        m_columns = new String[] { "gsmflag", "begintime", "enbid", "userlabel", "cellid", "earfcn", "subframenbr",
                "mmecode", // 全局变量数组
                "mmegroupid", "mmeues1apid", "weight", "eventtype", "ltescrsrp", "ltencrsrp", "ltescrsrq", "ltencrsrq",
                "ltescearfcn", "ltescpci", "ltencearfcn", "ltencpci", "gsmncellcarrierrssi", "gsmncellbcch",
                "gsmncellncc", "gsmncellbcc", "tdspccpchrscp", "tdsncelluarfcn", "tdscellparameterid", "ltescbsr",
                "ltescrttd", "ltesctadv", "ltescaoa", "ltescphr", "ltescrip", "ltescsinrul", "ltescplrulqci1",
                "ltescplrulqci2", "ltescplrulqci3", "ltescplrulqci4", "ltescplrulqci5", "ltescplrulqci6",
                "ltescplrulqci7", "ltescplrulqci8", "ltescplrulqci9", "ltescplrdlqci1", "ltescplrdlqci2",
                "ltescplrdlqci3", "ltescplrdlqci4", "ltescplrdlqci5", "ltescplrdlqci6", "ltescplrdlqci7",
                "ltescplrdlqci8", "ltescplrdlqci9", "ltescri1", "ltescri2", "ltescri4", "ltescri8", "ltescpuschprbnum",
                "ltescpdschprbnum", "ltescenbrxtxtimediff" };

        m_columnsIndexMap = new HashMap<>();
        for (int i = 0; i < m_columns.length; i++)
        {
            m_columnsIndexMap.put(m_columns[i], i);
        }
    }

    /**
     * 解码
     * 
     * @param file
     * @return
     * @throws XMLStreamException
     * @throws IOException
     */
    public List<String> Decode(String file) throws XMLStreamException, IOException
    {
        FileInputStream fs = new FileInputStream(file);
        try
        {
            return Decode(fs, file);
        }
        finally
        {
            fs.close();
        }
    };

    public List<String> Decode(InputStream stream, String file) throws XMLStreamException, IOException
    {
        file = file.toLowerCase();
        if (file.endsWith(".gz"))
        {
            return decodeGz(stream);
        }
        else if (file.endsWith(".zip"))
        {
            return decodeZip(stream);
        }
        else
        {
            return decodeStream(stream);
        }
    };

    public List<String> DecodeContent(String xmlText) throws XMLStreamException, IOException
    {
        Reader sr = new StringReader(xmlText);
        try
        {
            return decodeReader(sr);
        }
        finally
        {
            sr.close();
        }
    }

    /**
     * 压缩文件的解码，一个中间函数
     * 
     * @param fs
     * @return
     * @throws XMLStreamException
     * @throws IOException
     */
    private List<String> decodeGz(InputStream fs) throws XMLStreamException, IOException
    {
        GZIPInputStream gis = new GZIPInputStream(fs);
        try
        {
            return decodeStream(gis);
        }
        finally
        {
            gis.close();
        }
    };

    private List<String> decodeZip(InputStream fs) throws XMLStreamException, IOException
    {
        ZipInputStream zip = new ZipInputStream(fs);
        zip.getNextEntry();

        try
        {
            return decodeStream(zip);
        }
        finally
        {
            zip.close();
        }
    };

    /**
     * 解码过程中的一个函数
     * 
     * @param stream
     * @return
     * @throws XMLStreamException
     */
    private List<String> decodeStream(InputStream stream) throws XMLStreamException
    {
        XMLInputFactory xif = XMLInputFactory.newInstance();// 创建StAX分析工厂
        XMLStreamReader reader = xif.createXMLStreamReader(stream);// 创建分析器
        try
        {
            return decodeXmlReader(reader);
        }
        finally
        {
            reader.close();
        }
    };

    private List<String> decodeReader(Reader sr) throws XMLStreamException
    {
        XMLInputFactory xif = XMLInputFactory.newInstance();// 创建StAX分析工厂
        XMLStreamReader reader = xif.createXMLStreamReader(sr);// 创建分析器
        try
        {
            return decodeXmlReader(reader);
        }
        finally
        {
            reader.close();
        }
    };
    
    private List<String> decodeXmlReader(XMLStreamReader reader) throws XMLStreamException
    {
        if(DealQci == 1)
        {
            return decodeXmlReaderQci(reader);
        }
        else
        {
            return decodeXmlReaderRsrp(reader);
        }
    }

    class MroSample
    {
        /*
         * "gsmflag", "begintime", "enbid", "userlabel", "cellid", "earfcn",
         * "subframenbr", "mmecode", // 全局变量数组 "mmegroupid", "mmeues1apid",
         * "weight", "eventtype",
         */
        public String[] CellInfo = null;

        public String[] QcisValue = null;

        public List<String[]> Rsrps = new ArrayList<String[]>();

        public int[] RsrpIndexMap = null;
        public int[] QciIndexMap = null;

        public MroSample(int headerLen)
        {
            CellInfo = new String[headerLen];
        }

    }

    /**
     * 解码数据(rsrp only)
     * 
     * @param reader
     * @return
     * @throws XMLStreamException
     */
    private List<String> decodeXmlReaderRsrp(XMLStreamReader reader) throws XMLStreamException
    {

        List<MroSample> sample_map = new ArrayList<MroSample>();
        // Map<String, MroSample> sample_map = new HashMap<String, MroSample>();

        MroSample sample = null;

        String namespaceURI = null;
        String eNBId = null;
        String userLabel = null;
        String[] names = null;
        String[] values = null;
        String BeginTime = null;

        String CellId = "";
        String Earfcn = "";
        String SubFrameNbr = "";
        String PRBNbr = "";

        int rsrpIndex = 0;
        boolean isQci = false;

        int[] indexMap = null;// 求出文件中字段与最后输出结果的对应关系

        while (reader.hasNext())
        {
            if (MainSvr.bExitFlag) break;
            int event = reader.next();// 读取下个事件
            if (event != XMLStreamReader.START_ELEMENT) continue;

            String name = reader.getLocalName();// xml文件的

            switch (name)
            {
            case "fileHeader":
                BeginTime = reader.getAttributeValue(namespaceURI, "startTime").replace('T', ' ');
                break;

            case "eNB"://
                eNBId = reader.getAttributeValue(namespaceURI, "id");
                userLabel = reader.getAttributeValue(namespaceURI, "userLabel");
                if (userLabel == null) userLabel = "";
                break;

            case "smr":
                String _name = reader.getElementText().replace("MR.", "").replace(".", "");
                names = _name.split("\\s+");// "\\s+"正则表达式表示
                                            // 空格,回车,换行等空白符,+号表示一个或多个的意思

                indexMap = new int[names.length];
                rsrpIndex = -1;
                for (int i = 0; i < names.length; i++)
                {
                    names[i] = names[i].toLowerCase();
                    names[i] = names[i].replace("fdd", "");
                    names[i] = names[i].replace("tdd", "");

                    if (names[i].equals("ltescrsrp"))
                    {
                        rsrpIndex = i;
                    }

                    if (!isQci && names[i].indexOf("ltescplrulqci") >= 0)
                    {
                        isQci = true;
                    }

                    if (m_columnsIndexMap.containsKey(names[i]))
                    {
                        indexMap[i] = m_columnsIndexMap.get(names[i]);
                    }
                }

                break;

            case "object":
                if (rsrpIndex >= 0)
                {
                    String EventType = reader.getAttributeValue(namespaceURI, "EventType");
                    if (EventType == null)
                    {
                        EventType = "MRO";
                    }

                    String MmeCode = reader.getAttributeValue(namespaceURI, "MmeCode");
                    String MmeGroupId = reader.getAttributeValue(namespaceURI, "MmeGroupId");
                    String MmeUeS1apId = reader.getAttributeValue(namespaceURI, "MmeUeS1apId");
                    String TimeStamp = reader.getAttributeValue(namespaceURI, "TimeStamp");

                    String id = reader.getAttributeValue(namespaceURI, "id");

                    String[] items = decodeCell(id);
                    CellId = items[0];
                    Earfcn = items[1];
                    SubFrameNbr = items[2];

                    // String key = eNBId + "_" + CellId + "_" + MmeUeS1apId +
                    // "_" + TimeStamp;
                    // if (sample_map.containsKey(key))
                    // {
                    // sample = sample_map.get(key);
                    // }
                    // else
                    // {
                    sample = new MroSample(12);
                    sample_map.add(sample);
                    // sample_map.put(key, sample);

                    if (BeginTime == null || TimeStamp.contains("T"))
                    {
                        TimeStamp = TimeStamp.replace('T', ' ');
                    }
                    else
                    {
                        TimeStamp = BeginTime.substring(0, 11) + TimeStamp;
                    }

                    sample.CellInfo[0] = "0";
                    sample.CellInfo[1] = TimeStamp;
                    sample.CellInfo[2] = eNBId;
                    sample.CellInfo[3] = userLabel;
                    sample.CellInfo[4] = CellId;
                    sample.CellInfo[5] = Earfcn;
                    sample.CellInfo[6] = SubFrameNbr;
                    sample.CellInfo[7] = MmeCode;
                    sample.CellInfo[8] = MmeGroupId;
                    sample.CellInfo[9] = MmeUeS1apId;
                    sample.CellInfo[10] = null;
                    sample.CellInfo[11] = EventType;
                    // }

                    // if (rsrpIndex >= 0)
                    // {
                    sample.RsrpIndexMap = indexMap;
                    // }
                    // else
                    // {
                    // sample.QciIndexMap = indexMap;
                    // }

                }

                break;

            case "v":
                if (rsrpIndex >= 0)
                {
                    String _value = reader.getElementText().trim();
                    values = _value.split("\\s+");
                    int length = values.length;

                    if (!values[rsrpIndex].equals("NIL"))
                    {
                        sample.Rsrps.add(values);
                    }
                }
                break;
            }
        }
        return export(sample_map);
    }

    /**
     * 解码数据(Qci)
     * 
     * @param reader
     * @return
     * @throws XMLStreamException
     */
    private List<String> decodeXmlReaderQci(XMLStreamReader reader) throws XMLStreamException
    {

        // List<MroSample> sample_map = new ArrayList<MroSample>();
        Map<String, MroSample> sample_map = new HashMap<String, MroSample>();

        MroSample sample = null;

        String namespaceURI = null;
        String eNBId = null;
        String userLabel = null;
        String[] names = null;
        String[] values = null;
        String BeginTime = null;

        String CellId = "";
        String Earfcn = "";
        String SubFrameNbr = "";
        String PRBNbr = "";

        int rsrpIndex = 0;
        boolean isQci = false;

        int[] indexMap = null;// 求出文件中字段与最后输出结果的对应关系

        while (reader.hasNext())
        {
            if (MainSvr.bExitFlag) break;
            int event = reader.next();// 读取下个事件
            if (event != XMLStreamReader.START_ELEMENT) continue;

            String name = reader.getLocalName();// xml文件的

            switch (name)
            {
            case "fileHeader":
                BeginTime = reader.getAttributeValue(namespaceURI, "startTime").replace('T', ' ');
                break;

            case "eNB"://
                eNBId = reader.getAttributeValue(namespaceURI, "id");
                userLabel = reader.getAttributeValue(namespaceURI, "userLabel");
                if (userLabel == null) userLabel = "";
                break;

            case "smr":
                String _name = reader.getElementText().replace("MR.", "").replace(".", "");
                names = _name.split("\\s+");// "\\s+"正则表达式表示
                                            // 空格,回车,换行等空白符,+号表示一个或多个的意思

                indexMap = new int[names.length];
                rsrpIndex = -1;
                for (int i = 0; i < names.length; i++)
                {
                    names[i] = names[i].toLowerCase();
                    names[i] = names[i].replace("fdd", "");
                    names[i] = names[i].replace("tdd", "");

                    if (names[i].equals("ltescrsrp"))
                    {
                        rsrpIndex = i;
                    }

                    if (!isQci && names[i].indexOf("ltescplrulqci") >= 0)
                    {
                        isQci = true;
                    }

                    if (m_columnsIndexMap.containsKey(names[i]))
                    {
                        indexMap[i] = m_columnsIndexMap.get(names[i]);
                    }
                }

                break;

            case "object":
                if (rsrpIndex >= 0 || isQci)
                {
                    String EventType = reader.getAttributeValue(namespaceURI, "EventType");
                    if (EventType == null)
                    {
                        EventType = "MRO";
                    }

                    String MmeCode = reader.getAttributeValue(namespaceURI, "MmeCode");
                    String MmeGroupId = reader.getAttributeValue(namespaceURI, "MmeGroupId");
                    String MmeUeS1apId = reader.getAttributeValue(namespaceURI, "MmeUeS1apId");
                    String TimeStamp = reader.getAttributeValue(namespaceURI, "TimeStamp");

                    String id = reader.getAttributeValue(namespaceURI, "id");

                    String[] items = decodeCell(id);
                    CellId = items[0];
                    Earfcn = items[1];
                    SubFrameNbr = items[2];

                    String key = eNBId + "_" + CellId + "_" + MmeUeS1apId + "_" + TimeStamp;
                    if (sample_map.containsKey(key))
                    {
                        sample = sample_map.get(key);
                    }
                    else
                    {
                        sample = new MroSample(12);
                        // sample_map.add(sample);
                        sample_map.put(key, sample);

                        if (BeginTime == null || TimeStamp.contains("T"))
                        {
                            TimeStamp = TimeStamp.replace('T', ' ');
                        }
                        else
                        {
                            TimeStamp = BeginTime.substring(0, 11) + TimeStamp;
                        }

                        sample.CellInfo[0] = "0";
                        sample.CellInfo[1] = TimeStamp;
                        sample.CellInfo[2] = eNBId;
                        sample.CellInfo[3] = userLabel;
                        sample.CellInfo[4] = CellId;
                        sample.CellInfo[5] = Earfcn;
                        sample.CellInfo[6] = SubFrameNbr;
                        sample.CellInfo[7] = MmeCode;
                        sample.CellInfo[8] = MmeGroupId;
                        sample.CellInfo[9] = MmeUeS1apId;
                        sample.CellInfo[10] = null;
                        sample.CellInfo[11] = EventType;
                    }

                    if (rsrpIndex >= 0)
                    {
                        sample.RsrpIndexMap = indexMap;
                    }
                    else
                    {
                        sample.QciIndexMap = indexMap;
                    }
                }

                break;

            case "v":
                if (rsrpIndex >= 0 || isQci)
                {
                    String _value = reader.getElementText().trim();
                    values = _value.split("\\s+");
                    int length = values.length;

                    if (rsrpIndex >= 0)
                    {
                        if (!values[rsrpIndex].equals("NIL"))
                        {
                            sample.Rsrps.add(values);
                        }
                    }
                    else
                    {
                        sample.QcisValue = values;
                    }

                }
                break;
            }
        }
        return export(sample_map);
    }

    private void resetResult(String[] result, boolean all)
    {
        for (int i = 0; i < result.length; i++)
        {
            if (all || i > 11)
            {
                result[i] = null;
            }
        }
    }

    private List<String> export(Map<String, MroSample> map)
    {
        String[] result = new String[m_columns.length];
        StringBuilder sb = new StringBuilder();
        List<String> lst = new ArrayList<String>();

        for (MroSample sample : map.values())
        {
            exportSample(sb, result, lst, sample);
        }

        return lst;
    }

    private List<String> export(List<MroSample> samples)
    {
        String[] result = new String[m_columns.length];
        StringBuilder sb = new StringBuilder();
        List<String> lst = new ArrayList<String>();

        for (MroSample sample : samples)
        {
            exportSample(sb, result, lst, sample);
        }

        return lst;
    }

    private void exportSample(StringBuilder sb, String[] result, List<String> lst, MroSample sample)
    {
        // 重置
        resetResult(result, true);

        int length = sample.CellInfo.length;
        for (int i = 0; i < length; i++)
        {
            if (sample.CellInfo[i] != null && !sample.CellInfo[i].equals("NIL"))
            {
                result[i] = sample.CellInfo[i];
            }
        }

        length = sample.Rsrps.size();
        for (int i = 0; i < length; i++)
        {
            // 重置
            sb.setLength(0);
            resetResult(result, false);

            sample2string(sb, result, sample, i);
            lst.add(sb.toString());

        }
    }

    private void sample2string(StringBuilder sb, String[] result, MroSample sample, int index)
    {
        String[] values = sample.Rsrps.get(index);
        for (int i = 0; i < values.length; i++)
        {
            if (!values[i].equals("NIL"))
            {
                int n = sample.RsrpIndexMap[i];
                if (n >= 0)
                {
                    result[n] = values[i];
                }
            }
        }

        result[10] = String.valueOf(index + 1);

        if (sample.QcisValue != null && sample.QciIndexMap != null)
        {
            values = sample.QcisValue;
            for (int i = 0; i < values.length; i++)
            {
                if (!values[i].equals("NIL"))
                {
                    int n = sample.QciIndexMap[i];
                    if (n >= 0)
                    {
                        result[n] = values[i];
                    }
                }
            }
        }

        arrs2string(sb, result);
    }

    private void arrs2string(StringBuilder sb, String[] result)
    {
        for (String str : result)
        {
            if (str != null)
            {
                sb.append(str);
            }
            sb.append('\t');
        }
        int length = sb.length() - 1;
        sb.setLength(length);
    }

    private String[] decodeCell(String cell)
    {
        String[] results = new String[4];
        String[] arrs = cell.split(":");
        int length = arrs.length;
        if (length > 0)
        {
            results[0] = getCellId(arrs[0]);
        }

        if (length > 1)
        {
            results[1] = arrs[1];
        }

        if (length > 2)
        {
            results[2] = arrs[2];
        }
        return results;
    }

    private String getCellId(String value)
    {
        int index = value.indexOf("-");
        if (index >= 0)
        {
            return value.substring(index + 1);
        }
        else
        {
            int id = Integer.parseInt(value);
            id = id & 255;
            return Integer.toString(id);
        }
    }

    int flag = 0; // 全局变量

    public void run()
    {

        if (flag == 0)
        {
            if (MrOutputNum < 2) MrOutputNum = 2;
            if (MrOutputNum > 100) MrOutputNum = 100;

            System.out.println(new Date() + " Mrdecoder thread begin");
            System.out.println(new Date() + " MinFiles=" + MinFiles);
            System.out.println(new Date() + " DelayMinute=" + DelayMinute);
            System.out.println(new Date() + " ThreadNum=" + nThreadNum);
            System.out.println(new Date() + " MrOutputNum=" + MrOutputNum);
            System.out.println(new Date() + " MrFilePath=" + MrFilePath);
        }
        else
        {
            System.out.println(new Date() + " BcpBkPath=" + BcpBkPath);
            System.out.println(new Date() + " MrToHdfs thread begin");
            if (BcpBkPath.length() > 0)
            {
                FileMover.MrBcpBkPath = BcpBkPath;
                String[] vct = BcpBkHours.split(",");
                FileMover.MrBcpBkHours.clear();
                for (int i = 0; i < vct.length; i++)
                {
                    int nHour = Integer.parseInt(vct[i]);
                    FileMover.MrBcpBkHours.add(nHour);
                }
                LocalFile.makeDir(BcpBkPath);
            }
        }

        try
        {
            Thread.sleep(10000);
        }
        catch (InterruptedException e1)
        {
        }

        while (!MainSvr.bExitFlag)
        { // 循环解码/上传文件：循环调用多线程
            try
            {
                if (flag == 0)
                {
                    if (MrFilePath.length() == 0) 
                    	break;
                    DecodeMrFiles();// 解码并生成文件
                    Thread.sleep(60000);
                }
                else if (flag == 1)
                {
                    if (MrDecodePath.length() == 0) 
                    	break;
                    FileMover.MoveMroFilesTohdfs(MrDecodePath);
                     Thread.sleep(10000);
                }
                else if (flag == 2)
                {
                    if (MrDecodePath.length() == 0) 
                    	break;
                    FileMover.MoveMroFilesToFtp(MrDecodePath);
                    Thread.sleep(30000);
                }
                else if (flag == 3)
                {
                    if (MrDecodePath.length() == 0) 
                    	break;
          //          FileMover.MoveMroFilesTohdfsFromFtp(MrDecodePath);
                    Thread.sleep(30000);
                }
            }
            catch (Exception e)
            {
                System.out.println("Thread " + flag + " error:" + e.getMessage());
            }
        }

        if (flag == 0)
            System.out.println("Mrdecoder thread end");
        else
            System.out.println("MrToHdfs thread end");

    }
}
