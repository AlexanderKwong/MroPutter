package cn.mastercom.sssvr.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import cn.mastercom.sssvr.main.MainSvr;

public class LocalFile
{
	public static long CheckSpace(String sPath)
	{
		File win = new File(sPath);
		if (!win.exists())
			return 0;
		if (win.isFile())
			win = win.getParentFile();
		//System.out.println("Free space = " + win.getUsableSpace());
		return win.getUsableSpace();
	}

	public static boolean deleteFile(String sPath)
	{
		boolean flag = false;
		File file = new File(sPath);
		// 路径为文件且不为空则进行删除
		if (file.isFile() && file.exists())
		{
			file.delete();
			//System.out.println("deleteFile " + sPath);
			flag = true;
		} else if (!file.exists())
		{
			flag = true;
		}
		return flag;
	}

	/**
	 * 读取本地文件到HDFS系统<br>
	 * 请保证文件格式一直是UTF-8，从本地->HDFS
	 * 
	 */
	/**
	 * 
	 * @param localDirname 源文件所在位置
	 * @param hdfsPath要放在服务器的位置
	 * @param destFileName要合并成的文件名称
	 * @param filter
	 * @return
	 */
	public static boolean  MergeFile(String localDirname, String destPath, String destFileName, String filter,boolean Compress )
	{
		try
		{
			File dir = new File(localDirname);
			if (!dir.isDirectory())
			{
				System.out.println(localDirname + "is not a dir.");
				return false;
			}

			File[] files = dir.listFiles();
			if (files.length == 0)
				return false;

			System.out.println("Begin merge " + localDirname + " to " + destPath);

			if (checkFileExist(destPath + "/" + destFileName))
			{
				deleteFile(destPath + "/" + destFileName);
			}

			makeDir(destPath);

			File outfile = new File(destPath + "/" + destFileName);
			OutputStream os = null;
			
			if (Compress) { 
				os = new GZIPOutputStream(new FileOutputStream(outfile));
			}
			else{
				os = new FileOutputStream(outfile);
			}
	
			byte[] buffer = new byte[10240000];

			for (int i = 0; i < files.length; i++)
			{
				if (MainSvr.bExitFlag == true)
					break;

				File file = files[i];
				//if (!file.getName().toLowerCase().contains(filter.toLowerCase()))
				//	continue;
				FileInputStream is = new FileInputStream(file);
				GZIPInputStream gis = null;
				if (file.getName().toLowerCase().endsWith("gz"))
					gis = new GZIPInputStream(is);

				while (MainSvr.bExitFlag != true)
				{
					int bytesRead = 0;
					if (gis == null)
						bytesRead = is.read(buffer);
					else
						bytesRead = gis.read(buffer, 0, buffer.length);
					if (bytesRead >= 0)
					{
						os.write(buffer, 0, bytesRead);
					}
					else
					{
						break;
					}
				}
				if (gis != null)
					gis.close();
				is.close();
			}
			os.close();
			if (MainSvr.bExitFlag)
				return false;
			System.out.println("Success move " + localDirname + " to " + destPath);
			return true;
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return false;
	}
	
	
	public static boolean deleteFile_xdr(String sPath)
	{
		boolean flag = false;
		File file = new File(sPath);
		if (file.isFile() && file.exists())
		{
			flag = file.delete();
		}
		return flag;
	}

	public static boolean checkLocakFileExist(String filename)
	{
		try
		{
			File f = new File(filename);
			return f.exists();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		return false;
	}

	public static boolean makeDir(String dirName)
	{
		File file = new File(dirName);
		// 如果文件夹不存在则创建
		if (!file.exists() && !file.isDirectory())
		{
			// System.out.println("//不存在");
			file.mkdirs();
		}
		return true;
	}

	// 获取文件名,不含后缀
	public static String getFileName(String file)
	{
		File f = new File(file);
		String name = f.getName();
		int pos = name.lastIndexOf('.');
		if (pos >= 0)
		{
			return name.substring(0, pos);
		} else
		{
			return name;
		}
	}

	public static void renameFile(String oldname, String newname)
	{
		if (!oldname.equals(newname))
		{// 新的文件名和以前文件名不同时,才有必要进行重命名
			makeDir(new File(newname).getParent());
			File oldfile = new File(oldname);
			File newfile = new File(newname);
			if (newfile.exists())// 若在该目录下已经有一个文件和新文件名相同，则不允许重命名
			{
				newfile.delete();
			}

			oldfile.renameTo(newfile);
		}
	}

	public static boolean bkFile(String oldname, String newname)
	{
		if (!oldname.equals(newname))
		{// 新的文件名和以前文件名不同时,才有必要进行重命名
			makeDir(new File(newname).getParent());
			File oldfile = new File(oldname);
			File newfile = new File(newname);
			if (newfile.exists())// 若在该目录下已经有一个文件和新文件名相同，则不允许重命名
			{
				newfile.delete();
			}
			return oldfile.renameTo(newfile);
		} else
		{
			return false;
		}
	}

	public static void renameDirectory(String fromDir, String toDir)
	{

		File from = new File(fromDir);

		if (!from.exists() || !from.isDirectory())
		{
			System.out.println("Directory does not exist: " + fromDir);
			return;
		}

		File to = new File(toDir);

		// Rename
		if (from.renameTo(to))
			System.out.println("Success!");
		else
			System.out.println("Error");

	}

	public static void main(String[] args)
	{
		// LocalFile.renameDirectory("D:/test/output", "D:/test/mro/output");
		LocalFile.MergeFile("A:\\test","a:/","out.txt.gz","",true);

	}

	/**
	 * 找到该路径下所有满足条件的文件，将文件完整路径添加到list返回
	 * 
	 * @param dir
	 * @param filter 多个关键字用空格分开
	 * @param waitMinute
	 * @return
	 * @throws Exception
	 */
	public final static List<String> getAllFiles(File dir, String filter, int waitMinute) throws Exception
	{
		System.out.println("Scan 目录"+dir.getAbsolutePath());

		List<String> fileList = new ArrayList<String>();
		if (!dir.exists())
			return fileList;
		File[] fs = dir.listFiles();

		for (int i = 0; i < fs.length; i++)
		{
			if (fs[i].isDirectory())
			{
				try
				{
					fileList.addAll(getAllFiles(fs[i], filter, waitMinute));
				} catch (Exception e)
				{
					System.out.println("getAllFiles error:" + dir.getName() + ", " + e.getMessage());
				}
			} 
			else
			{
				if (waitMinute > 0)
				{
					long lastDirModifyTime = fs[i].lastModified() / 1000L;
					final CalendarEx cal = new CalendarEx(new Date());
					if (lastDirModifyTime + waitMinute * 60 > cal._second)// 最后修改时间距当前时间不足等待时间认为该文件不能进行上传
					{
						continue;
					}
				}
				boolean bFltResult = true;
				if(filter.length()>0)
				{
					String[] vct =filter.split(" ");
					for(String flt: vct)
					{
						if (!(!flt.equals("") && fs[i].getName().toLowerCase().contains(flt.toLowerCase())))
						{
							bFltResult = false;
							break;
						}
					}
				}

				if(bFltResult == true)
				{
					fileList.add(fs[i].getAbsolutePath());
				}
			}
		}
		if(fileList.size()>0)
			System.out.println("目录"+dir.getAbsolutePath() + " 文件个数：" + fileList.size());
		return fileList;
	}
	
	public final static List<String> getAllDirs(File dir, String filter, int waitMinute,int nDepth) throws Exception
	{
		List<String> fileList = new ArrayList<String>();
		if (!dir.exists())
			return fileList;
		
		File[] fs = dir.listFiles();

		for (int i = 0; i < fs.length; i++)
		{
			if (fs[i].isDirectory())
			{
				try
				{
					if(nDepth>0)
					{
						fileList.addAll(getAllDirs(fs[i], filter, waitMinute,nDepth-1));
					}
					else if(nDepth==0 && fs[i].getName().equals("upload"))
					{
						fileList.addAll(getAllDirs(fs[i], filter, waitMinute,nDepth));
					}
					else
					{
						fileList.add(fs[i].getAbsolutePath());
					}
				} catch (Exception e)
				{
					System.out.println("getAllFiles error:" + dir.getName() + ", " + e.getMessage());
				}
			} 			
		}
		return fileList;
	}
	
	public final static List<String> getAllFiles(String files[], String filter, int waitMinute) throws Exception
	{
		List<String> fileList = new ArrayList<String>();
		for (String s : files)
		{
			File file = new File(s);
			fileList.addAll(getAllFiles(file, filter, waitMinute));
		}
		return fileList;
	}

	public final static List<String> getAllFiles(ArrayList<String> files, String filter, int waitMinute)
			throws Exception
	{
		List<String> fileList = new ArrayList<String>();
		for (String s : files)
		{
			File file = new File(s);
			fileList.addAll(getAllFiles(file, filter, waitMinute));
		}
		return fileList;
	}

	/**
	 * 删除目录（文件夹）以及目录下的文件
	 * 
	 * @param sPath
	 *            被删除目录的文件路径
	 * @return 目录删除成功返回true，否则返回false
	 */
	public static boolean deleteDirectory(String sPath)
	{
		// 如果sPath不以文件分隔符结尾，自动添加文件分隔符
		if (!sPath.endsWith(File.separator))
		{
			sPath = sPath + File.separator;
		}
		File dirFile = new File(sPath);
		// 如果dir对应的文件不存在，或者不是一个目录，则退出
		if (!dirFile.exists() || !dirFile.isDirectory())
		{
			return false;
		}
		boolean flag = true;
		// 删除文件夹下的所有文件(包括子目录)
		File[] files = dirFile.listFiles();
		for (int i = 0; i < files.length; i++)
		{
			// 删除子文件
			if (files[i].isFile())
			{
				flag = deleteFile(files[i].getAbsolutePath());
				if (!flag)
					break;
			} // 删除子目录
			else
			{
				flag = deleteDirectory(files[i].getAbsolutePath());
				if (!flag)
					break;
			}
		}
		if (!flag)
			return false;
		// 删除当前目录
		if (dirFile.delete())
		{
			return true;
		} else
		{
			return false;
		}
	}

	public static boolean checkFileExist(String filename)
	{
		try
		{
			File file = new File(filename);
			if (file.exists())
			{
				return true;
			}
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		return false;
	}
	
	public static boolean checkFileExists(String filename)
	{
		try
		{
			File file = new File(filename);
			if (file.exists())
			{
				File[] listFiles = file.listFiles();
				for (File path : listFiles) {
					if(path.getName().equals("_SUCCESS")){
						return true;
					}
				}
				return false;
			}			
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		return false;
	}
}
