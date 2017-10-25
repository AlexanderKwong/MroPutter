/**
 * 2007-10-10下午02:57:09
 */
package cn.mastercom.sssvr.util;

/**
 * NumberConverter, 完成数字不同格式间的转换，如多个字节合并为整型等。
 * 
 * @author Chris
 * Created on 2007-2-26
 */
public final class NumberConverter
{
    /**
     * 将2个字节转换为short类型
     * 
     * @param high short类型高字节
     * @param low short类型低字节
     * @return 转换后的short类型
     */
    public static final short byte2Short(byte high, byte low)
    {
        return (short)((high << 8) | (low & 0xFF));
    }
    
    public static final short byte2ShortWin(byte high, byte low)
    {
        return (short)((low << 8) | (high & 0xFF));
    }

    /**
     * 将2个字节转换为short类型
     * 
     * @param bytes 存储字节的数组
     * @param offset 要转换的字节开始的偏移量
     * @return 转换后的short类型
     */
    public static final short bytes2Short(byte[] bytes, int offset)
    {
        return byte2Short(bytes[offset++], bytes[offset]) ;
    }
    
    /**
     * 将4个字节转换为int类型
     * 
     * @param high1 int类型最高字节
     * @param high2 int类型次高字节
     * @param low1 int类型次低字节
     * @param low2 int类型最低字节
     * @return 转换后的int类型
     */
    public static final int byte2Int(byte high1, byte high2,
            byte low1, byte low2)
    {
        return (high1 << 24) | (high2 << 16 & 0xFF0000)
                | (low1 << 8 & 0xFF00) | (low2 & 0xFF);
    }
    
    public static final int byte2IntWin(byte high1,byte high2,byte low1,byte low2)
    {
    	return (low2 << 24) | (low1 << 16 & 0xFF0000)
        | (high2 << 8 & 0xFF00) | (high1 & 0xFF);
    }
    
    /**
     * 将4个字节转换为int类型
     * 
     * @param bytes 存储字节的数组
     * @param offset 要转换的字节开始的偏移量
     * @return 转换后的int类型
     */
    public static final int bytes2Int(byte[] bytes, int offset)
    {
        return byte2Int(bytes[offset++], bytes[offset++], bytes[offset++], bytes[offset]);
    }
    
    /**
     * 将8个字节转换为long类型
     * 
     * @param high1 long类型最高字节
     * @param high2 long类型次高字节
     * @param high3 long类型第3个字节
     * @param high4 long类型第4个字节
     * @param low1 long类型第5个字节
     * @param low2 long类型第6个字节
     * @param low3 long类型第7个字节
     * @param low4 long类型最低字节
     * @return 转换后的long类型
     */
    public static final long byte2Long(byte high1, byte high2, byte high3, byte high4,
            byte low1, byte low2, byte low3, byte low4)
    {
        return ((long)high1 << 56) 
                | (((long)high2 << 48) & 0xFF000000000000L)
                | (((long)high3 << 40) & 0xFF0000000000L)
                | (((long)high4 << 32) & 0xFF00000000L)
                | (((long)low1 << 24) & 0xFF000000L)
                | (((long)low2 << 16) & 0x00FF0000L)
                | (((long)low3 << 8) & 0xFF00)
                | (low4 & 0xFF);
    }
    
    public static final long byte2LongWin(byte high1, byte high2, byte high3, byte high4,
            byte low1, byte low2, byte low3, byte low4)
    {
        return ((long)low4 << 56) 
                | (((long)low3 << 48) & 0xFF000000000000L)
                | (((long)low2 << 40) & 0xFF0000000000L)
                | (((long)low1 << 32) & 0xFF00000000L)
                | (((long)high4 << 24) & 0xFF000000L)
                | (((long)high3 << 16) & 0x00FF0000L)
                | (((long)high2 << 8) & 0xFF00)
                | (high1 & 0xFF);
    }

    /**
     * 将8个字节转换为long类型
     * 
     * @param bytes 存储字节的数组
     * @param offset 要转换的字节开始的偏移量
     * @return 转换后的long类型
     */
    public static final long bytes2Long(byte[] bytes, int offset)
    {
        return byte2Long(bytes[offset++], bytes[offset++], bytes[offset++], 
                bytes[offset++], bytes[offset++], bytes[offset++], 
                bytes[offset++], bytes[offset]);
    }
    
    /**
     * 将一个short类型转化为2个字节
     * 
     * @param s 要转换的short类型
     * @param b 存储转换后的字节的字节数组
     * @param offset 存储转换后的字节在字节数组中偏移量
     */
    public static final void short2Bytes(short s, byte[] b, int offset)
    {
        b[offset++] = (byte)(s >> 8);
        b[offset] = (byte)s;
    }

    /**
     * 将一个short类型转化为2个字节
     * 
     * @param s 要转换的short类型
     * @param b 存储转换后的字节的字节数组
     * @param offset 存储转换后的字节在字节数组中偏移量
     */
    public static final void short2BytesWin(short s, byte[] b, int offset)
    {
        b[offset++] = (byte)s;
        b[offset] = (byte)(s >> 8);
    }

    /**
     * 将一个int类型转化为4个字节
     * 
     * @param i 要转换的int类型
     * @param b 存储转换后的字节的字节数组
     * @param offset 存储转换后的字节在字节数组中偏移量
     */
    public static final void int2Bytes(int i, byte[] b, int offset)
    {
        b[offset++] = (byte)(i >> 24);
        b[offset++] = (byte)(i >> 16);
        b[offset++] = (byte)(i >> 8);
        b[offset] = (byte)i;
    }
    
    /**
     * 将一个int类型转化为WINDOWS 4个字节
     * @param i
     * @param b
     * @param offset
     */
    public static final void int2BytesWin(int i, byte[] b, int offset)
    {
    	b[offset++] = (byte) i;
    	b[offset++] = (byte)(i >> 8);
    	b[offset++] = (byte)(i >> 16);
    	b[offset++] = (byte)(i >> 24);    	
    }
    
    
    public static final int ipString2Int(String ip)
    {
        String[] ips = ip.split("\\.");
        if (ips.length >= 4)
        {
            return (Integer.parseInt(ips[0]) << 24)
                    & (Integer.parseInt(ips[1]) << 16)
                    & (Integer.parseInt(ips[2]) << 8)
                    & Integer.parseInt(ips[3]);
        }
        return 0;
    }
    
    public static final String ipInt2String(int ip)
    {
        return "" + ((ip >> 24) & 0xFF) + "." + ((ip >> 16) & 0xFF) + "." + ((ip >> 8) & 0xFF) + "." + (ip & 0xFF);
    }
}
