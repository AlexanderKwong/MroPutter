/**
 * InvalidFormatException.java. Created by Chris on 2006-10-18
 */
package cn.mastercom.sssvr.util;

/**
 * InvalidFormatException, 无效格式异常。
 * 
 * @author Chris
 * Created on 2006-10-18
 */
@SuppressWarnings("serial")
public class InvalidFormatException extends Exception
{
    /**
     * 缺省构造函数
     */
    public InvalidFormatException()
    {
        super();
    }

    /**
     * 带消息和原因参数的构造函数
     * 
     * @param message 消息
     * @param cause 原因
     */
    public InvalidFormatException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * 带消息参数的构造函数
     * 
     * @param message 消息
     */
    public InvalidFormatException(String message)
    {
        super(message);
    }

    /**
     * 带原因参数的构造函数
     * 
     * @param cause 原因
     */
    public InvalidFormatException(Throwable cause)
    {
        super(cause);
    }
}
