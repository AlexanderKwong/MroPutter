package cn.mastercom.sssvr.util;

/**
 * 
 */

import java.io.File;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class Test
{
	private static Logger logger = Logger.getLogger(Test.class);

	/**
	 * method destination : privent starting more than one program process in
	 * system processes
	 * 
	 * @param programName
	 */
	private static void lockSingletonProgramFile(String programName)
	{

		final String startFailureMessage = "Error:start " + programName
				+ " application";
		String lockFile = System.getProperty("lockFile");
		logger.info("start " + programName + " application with [lockFile] : "
				+ lockFile);
		if (null == lockFile)
		{
			String userDir = System.getProperty("user.dir");
			File userDirFile = new File(userDir);
			lockFile = userDirFile.getParent() + File.separator + programName
					+ ".lock";
			logger.warn(
					"does not provide lockFile, it will use default lockFile which is ["
							+ lockFile + "]");
		}
		RandomAccessFile raf = null;
		FileChannel fileChannel = null;
		FileLock flock = null;
		FileWriter writer = null;
		try
		{
			File file = new File(lockFile);
			if (!file.exists())
			{
				String parent = file.getParent();
				File folder = new File(parent);
				if (!folder.exists() || !folder.isDirectory())
				{
					if (!folder.mkdirs())
					{
						logger.error(startFailureMessage
								+ " failure: create lock file folder failure:"
								+ parent);
						System.exit(-1);
					}
				}
				if (!file.createNewFile())
				{
					logger.error(startFailureMessage
							+ " failure: create lock file failure:" + lockFile);
					System.exit(-1);
				}
			}
			writer = new FileWriter(file);
			writer.write(programName);
			/**
			 * Here,we force flush data into lock file. If there already has a
			 * process in system processes, it will catch Exception.
			 */
			writer.flush();
			writer.close();
			raf = new RandomAccessFile(file, "rw");
			fileChannel = raf.getChannel();
			flock = fileChannel.tryLock();// start to try locking lock file
			/**
			 * <pre>
			 * Note: 
			 * Here, at first time, you cann't release or close these resources.
			 * If you do it, you will find that it cann't prevent more than one program process
			 * running in system processes.
			 * </pre>
			 */
		}
		catch (Exception e)
		{
			logger.error(startFailureMessage + " failure: lock file is ["
					+ lockFile + "]:" + e.getMessage(), e);
			try
			{
				/**
				 * <pre>
				 * Note:
				 * If you start program process failure, 
				 * you need to try releasing and closing these resources.
				 * </pre>
				 */
				if (null != writer)
				{
					writer.close();
				}
				if (null != flock)
				{
					flock.release();
				}
				if (null != fileChannel)
				{
					fileChannel.close();
				}
				if (null != raf)
				{
					raf.close();
				}
			}
			catch (Exception ex)
			{
				logger.error(
						"Error: close resource failure:" + ex.getMessage(), ex);
			}
			logger.error("There is a "
					+ programName
					+ " application process in system processes. Now exit starting!");
			System.exit(-1);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		//lockSingletonProgramFile(Test.class.getName());
		ExecutorService exec = Executors.newSingleThreadExecutor();
		exec.execute(new Runnable()
		{
			@Override
			public void run()
			{
				while (true)
				{
					try
					{
						System.out.println("hello.");
						TimeUnit.SECONDS.sleep(3);
					}
					catch (InterruptedException e)
					{
						e.printStackTrace();
					}
				}
			}
		});
		exec.shutdown();
	}

}
