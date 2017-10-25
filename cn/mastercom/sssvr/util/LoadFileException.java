/**
 * LoadFileException.java. Created by Chris on 2006-10-18
 */
package cn.mastercom.sssvr.util;

/**
 * LoadFileException, 加载文件异常。
 * 
 * @author Chris
 * Created on 2006-10-18
 */
@SuppressWarnings("serial")
public class LoadFileException extends Exception
{
    /**
     * 缺省构造函数
     */
    public LoadFileException()
    {
        super();
    }

    /**
     * 带消息和原因参数的构造函数
     * 
     * @param message 消息
     * @param cause 原因
     */
    public LoadFileException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * 带消息参数的构造函数
     * 
     * @param message 消息
     */
    public LoadFileException(String message)
    {
        super(message);
    }

    /**
     * 带原因参数的构造函数
     * 
     * @param cause 原因
     */
    public LoadFileException(Throwable cause)
    {
        super(cause);
    }
}
