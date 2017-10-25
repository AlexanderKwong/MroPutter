/**
 * ItemNotFoundException.java. Created by Chris on 2006-10-18
 */
package cn.mastercom.sssvr.util;

/**
 * ItemNotFoundException, 配置项未找到异常。
 * 
 * @author Chris
 * Created on 2006-10-18
 */
@SuppressWarnings("serial")
public class ItemNotFoundException extends Exception
{
    /**
     * 缺省构造函数
     */
    public ItemNotFoundException()
    {
        super();
    }

    /**
     * 带消息和原因参数的构造函数
     * 
     * @param message 消息
     * @param cause 原因
     */
    public ItemNotFoundException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * 带消息参数的构造函数
     * 
     * @param message 消息
     */
    public ItemNotFoundException(String message)
    {
        super(message);
    }

    /**
     * 带原因参数的构造函数
     * 
     * @param cause 原因
     */
    public ItemNotFoundException(Throwable cause)
    {
        super(cause);
    }
}
