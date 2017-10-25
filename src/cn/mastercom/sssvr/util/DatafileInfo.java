package cn.mastercom.sssvr.util;

public class DatafileInfo {
	public String filename = "";
	public long filesize = 0;
	public long modificationTime =0;
	/**
	 * 文件类
	 * @param _filename
	 * @param _filesize
	 * @param _modificationTime
	 */
	public DatafileInfo(String _filename, long _filesize,long _modificationTime)
	{
		filename = _filename;
		filesize = _filesize;
		modificationTime = _modificationTime;
	}
}
