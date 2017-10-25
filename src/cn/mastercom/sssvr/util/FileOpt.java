package cn.mastercom.sssvr.util;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

public class FileOpt
{
	public static boolean moveFile(File oldFile, File newFile)
	{
		boolean flag = false;
		if (!newFile.getParentFile().exists())
		{
			newFile.getParentFile().mkdirs();
		}
		flag = oldFile.renameTo(newFile);
		return flag;
	}

	public static boolean deleteFile(File file)
	{
		boolean flag = false;
		file.delete();
		return flag;
	}

	/**
	 * 只比较下一层文件的修改时间
	 * 
	 * @param path
	 * @return
	 */
	public static long lastModifyTime(String path, long initTime)
	{
		File file = new File(path);
		File filelist[] = null;
		long lastModifyTime = initTime;
		if (!file.exists())
		{
			System.out.println("文件目录" + path + "不存在!");
		} else
		{
			filelist = file.listFiles();
			for (int i = 0; i < filelist.length; i++)
			{
				if (filelist[i].lastModified() > lastModifyTime)
				{
					lastModifyTime = filelist[i].lastModified();
				}
			}
		}
		return lastModifyTime;
	}

	public static long lastModifyTime(String[] paths)
	{
		long lastModifyTime = 0L;
		for (String s : paths)
		{
			lastModifyTime = lastModifyTime(s, lastModifyTime);
		}
		return lastModifyTime;
	}

	/**
	 * 递归返回文件夹下所有文件的路径
	 * 
	 * @param dic
	 * @param fileList
	 * @return
	 */
	public static ArrayList<String> getFile(File dic, ArrayList<String> fileList)
	{
		if (dic.isDirectory())
		{
			File childrenFile[] = dic.listFiles();
			for (int i = 0; i < childrenFile.length; i++)
			{
				if (childrenFile[i].isDirectory())
				{
					getFile(childrenFile[i], fileList);
				} else
				{
					fileList.add(childrenFile[i].getPath());
				}
			}
		} else
		{
			fileList.add(dic.getPath());
		}
		return fileList;

	}

	public static ArrayList<File> getFiles(File dic, ArrayList<File> fileList)
	{
		if (dic.isDirectory())
		{
			File childrenFile[] = dic.listFiles();
			for (int i = 0; i < childrenFile.length; i++)
			{
				if (childrenFile[i].isDirectory())
				{
					getFiles(childrenFile[i], fileList);
				} else
				{
					fileList.add(childrenFile[i]);
				}
			}
		} else
		{
			fileList.add(dic);
		}
		return fileList;
	}

	public static void delFolder(String folderPath)
	{
		try
		{
			delAllFile(folderPath); // 删除完里面所有内容
			String filePath = folderPath;
			filePath = filePath.toString();
			java.io.File myFilePath = new java.io.File(filePath);
			myFilePath.delete(); // 删除空文件夹
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	// 删除指定文件夹下所有文件
	// param path 文件夹完整绝对路径
	public static boolean delAllFile(String path)
	{
		boolean flag = false;
		File file = new File(path);
		if (!file.exists())
		{
			return flag;
		}
		if (!file.isDirectory())
		{
			return flag;
		}
		String[] tempList = file.list();
		File temp = null;
		for (int i = 0; i < tempList.length; i++)
		{
			if (path.endsWith(File.separator))
			{
				temp = new File(path + tempList[i]);
			} else
			{
				temp = new File(path + File.separator + tempList[i]);
			}
			if (temp.isFile())
			{
				temp.delete();
			}
			if (temp.isDirectory())
			{
				delAllFile(path + "/" + tempList[i]);// 先删除文件夹里面的文件
				delFolder(path + "/" + tempList[i]);// 再删除空文件夹
				flag = true;
			}
		}
		return flag;
	}

	public static String renamePath(String oldPath, String srcPath, String replacePath)
	{
		return oldPath.replace(srcPath, replacePath);
	}

	// public static void main(String args[])
	// {
	// delAllFile("D:\\mastercom");
	// }
}
