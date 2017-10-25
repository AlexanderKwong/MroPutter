/**
 * CreateFileException.java. Created by Chris on 2006-10-18
 */
package cn.mastercom.sssvr.util;

import java.io.File;
import java.util.HashMap;

/**
 * Xml配置管理类工厂
 * 
 * @author konglw
 * @time 2006-10-18
 */
public class XMLConfigFileFactory
{
    /**
     * 私有构造函数，不可创建实例
     */
    private XMLConfigFileFactory()
    {
    }

    /**
     * 得到XML类型配置文件
     * 
     * @return XML配置文件
     * @throws LoadFileException 
     */
    public static XMLConfigFile getFile(String configFileName)
    {
        XMLConfigFile configFile = configFiles.get(configFileName);
        if (configFile == null)
        {
            configFile = new XMLConfigFile(configFileName);
            try
            {
                configFile.load();
            }
            catch (LoadFileException e)
            {
                e.printStackTrace();
                File file = new File(configFileName);
                if (file.exists())
                {
                    file.renameTo(new File(configFileName + ".bak"));
                }
            }
            configFiles.put(configFileName, configFile);
        }
        return configFile;
    }
    
    private static HashMap<String, XMLConfigFile> configFiles;
    
    static
    {
        configFiles = new HashMap<String, XMLConfigFile>();
    }
}
