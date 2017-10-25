package cn.mastercom.sssvr.main;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.dom4j.Document;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import cn.mastercom.sssvr.util.CalendarEx;
import cn.mastercom.sssvr.util.LocalFile;
import cn.mastercom.sssvr.util.SignalMrAdapter;

public class FileMoverShanDong extends Thread
{

    public static void main(String[] args)
    {
        FileMoverShanDong fm = new FileMoverShanDong();

        // 1
        fm.Init();

        // 2
        fm.DecodeFiles();

        // 3
        // FileMoverShanDong fm = new FileMoverShanDong();
        // fm.start();
    }

    private String sourceFilePath = "";
    private String bcpFilePath = "";
    private String backupPath = "";
    private int waitMinute = 1;
    private int threadNum = 1;

    public boolean Init()
    {
        if (readConfigInfo())
        {
            System.out.println("FileMoverShanDong thread start!");

            try
            {
                Thread.sleep(1000);
            }
            catch (InterruptedException e1)
            {
            }

            return true;
        }

        System.out.println("FileMoverShanDong 参数获取失败!");
        return false;
    }

    private boolean readConfigInfo()
    {
        try
        {
            String filePath = "conf/config_sdzt.xml";
            File file = new File(filePath);
            if (file.exists())
            {
                SAXReader reader = new SAXReader();
                Document doc = reader.read(file);// 读取XML文件

                Node node = doc.selectSingleNode("//comm/SourceFilePath");
                sourceFilePath = node.getText();

                node = doc.selectSingleNode("//comm/BcpFilePath");
                bcpFilePath = node.getText();

                node = doc.selectSingleNode("//comm/BackUpPath");
                backupPath = node.getText();

                node = doc.selectSingleNode("//comm/WaitMinute");
                waitMinute = Integer.parseInt(node.getText());

                node = doc.selectSingleNode("//comm/ThreadNum");
                threadNum = Integer.parseInt(node.getText());

                return threadNum > 0;
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        return false;
    }

    @Override
	public void run()
    {
        if (!Init()) return;

        while (!MainSvr.bExitFlag)
        { // 循环解码/上传文件：循环调用多线程
            try
            {
                DecodeFiles();
                Thread.sleep(10000);
            }
            catch (Exception e)
            {
                System.out.println("Thread " + " error:" + e.getMessage());
            }
        }

        System.out.println("FileMoverShanDong thread end");
    }


    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void DecodeFiles()
    {
        try
        {
            List<String> fileList = LocalFile.getAllFiles(new File(sourceFilePath), ".log", waitMinute);
            System.out.println( (new Date()).toString() + " Find Unprocessed files: " + fileList.size());

            if (fileList.size() == 0) return;

            int tnum = Math.min(threadNum, fileList.size());
            // 创建一个线程池
            ExecutorService pool = Executors.newFixedThreadPool(tnum);
            // 创建多个有返回值的任务
            List<Future> listTask = new ArrayList<Future>();

            // 多线程解码
            for (String file : fileList)
            {
                Callable fm = new SignalMrAdapter(bcpFilePath, backupPath, file);
                // 执行任务并获取Future对象
                Future f1 = pool.submit(fm);
                listTask.add(f1);
            }

            // 获取所有并发任务的运行结果
            for (Future f : listTask)
            {
                try
                {
                    System.out.println(">>>" + f.get().toString());
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                    System.out.println("出错:" + e.getMessage());
                }
            }

        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.out.println("出错:" + e.getMessage());
        }
    }

}
