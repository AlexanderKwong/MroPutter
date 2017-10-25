/**
 * CreateFileException.java. Created by Chris on 2006-10-18
 */
package cn.mastercom.sssvr.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.crimson.tree.XmlDocument;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


/**
 * XML配置文件类，提供获取、设置、增加、清除配置等接口，可以设置多个配置段，每个配置段
 * 又可以配置多个配置项，每个配置项可以是一个名字值对，也可以仅仅是一个值。支持的值
 * 类型包括字符串、整数、浮点数以及JavaBean对象。每个一Bean对象支持字符串、整数、
 * 浮点数、列表、映射表以及其它满足此条件的JavaBean对象的属性。 
 * 简单配置文件如下： 
 * <Configs> 
 *   <Config name="One">
 *     <Item name="1" class="String">a</Item> 
 *     <Item name="2" class="Integer">1</Item>
 *     <Item name="3" class="Float">1.0</Item> 
 *     <Item name="4" class="JavaBean1">
 *       <Item name="Field1" class="String">b</Item> 
 *       <Item name="Field2" class="List"> 
 *         <Item class="String">c</Item> 
 *         <Item class="Integer">2</Item>
 *       </Item>
 *       <Item name="Field3" class="Map"> 
 *         <Item class="String" key="true">c</Item>
 *         <Item class="Integer">2</Item> 
 *       </Item> 
 *       <Item name="Field4 "class="JavaBean2"> 
 *         <Item name="Field1" class="String">b</Item> 
 *       </Item>
 *     </Item> 
 *     <Item name="5">a</Item> 
 *     <Item>a</Item> 
 *     <Item name="6"></Item>
 *   </Config> 
 *   <Config name="Another"> 
 *   </Config> 
 * </Configs> 
 * 详细配置文件及程序样例参见Sample。
 * 
 * @company mastercom.cn
 * @author konglw
 * @time 2006-10-18 
 * Modified by Chris 2006-10-18
 */
public class XMLConfigFile
{
    /**
     * 创建指定文件名的配置文件实例，此实例不能从包外创建。
     * 
     * @param configFileName 配置文件名
     */
    XMLConfigFile(String configFileName)
    {
        this.configFileName = configFileName;
    }

    /**
     * 加载配置文件。
     * 
     * @throws LoadFileException 加载失败
     */
    public void load() throws LoadFileException
    {
        try 
        {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            // 先判断本地是否存在指定名称的文件
            try
            {
                File file = new File(configFileName);
                if (file.exists())
                {
                    try
                    {
                        document = db.parse(file);
                        trimEmptyNodes(document.getDocumentElement());
                        return;
                    }
                    catch (Exception e1)
                    {
                        e1.printStackTrace();
                    }
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
            // 没有或加载不成功就当成资源下载
            URL url = getClass().getResource(configFileName);
            if (url == null)
            {
                url = getClass().getResource("/" + configFileName);
            }
            if (url != null)
            {
                document = db.parse(url.openStream());
                trimEmptyNodes(document.getDocumentElement());
            }
        }
        catch (Exception e)
        {
            throw new LoadFileException(e);
        }
    }

    /**
     * 保存配置到文件中。
     * 
     * @throws FileNotLoadedException 文件未加载或未创建
     * @throws IOException 保存失败
     */
    public void save() throws IOException, FileNotLoadedException
    {
        if (document == null)
        {
            throw new FileNotLoadedException();
        }
        OutputStreamWriter outWriter = null;
        try
        {
            File file = new File(configFileName);
            File path = file.getParentFile();
            if (path != null && !path.exists())
            {
                path.mkdirs();
            }
            outWriter = new OutputStreamWriter(new FileOutputStream(file));
            ((XmlDocument)document).write(outWriter, "GB2312");
        }
        finally
        {
            if (outWriter != null)
            {
                outWriter.close();
            }
        }
    }

    /**
     * 获取文件中所有配置的ID列表
     * 
     * @return 文件中所有配置的ID列表
     * @throws FileNotLoadedException 文件未加载或未创建
     */
    public List<String> getConfigIDs() throws FileNotLoadedException
    {
        if (document == null)
        {
            throw new FileNotLoadedException();
        }
        Element root = document.getDocumentElement();
        List<String> configIDs = new LinkedList<String>();
        NodeList configs = root.getChildNodes();
        for (int i = 0; i < configs.getLength(); i++)
        {
            Node config = configs.item(i);
            if (config.getNodeType() == Node.ELEMENT_NODE)
            {
                configIDs.add(((Element)config).getAttribute(ATTRIBUTE_NAME));
            }
        }
        return configIDs;
    }

    /**
     * 获取指定名称的String类型的配置项
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @return 配置项
     * @throws FileNotLoadedException 文件未加载或未创建
     * @throws ItemNotFoundException 配置项不存在
     * @throws InvalidFormatException 配置项格式不正确
     */
    public String getString(String configName, String itemName)
            throws FileNotLoadedException, ItemNotFoundException
    {
        Element item = getItem(configName, ATTRIBUTE_NAME, itemName);
        if (item == null || item.getChildNodes().getLength() == 0)
        {
            throw new ItemNotFoundException();
        }
        return item.getChildNodes().item(0).getNodeValue();
    }

    /**
     * 获取指定名称的int类型的配置项
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @return 配置项
     * @throws FileNotLoadedException 文件未加载或未创建
     * @throws ItemNotFoundException 配置项不存在
     * @throws InvalidFormatException 配置项格式不正确
     */
    public int getInt(String configName, String itemName)
            throws FileNotLoadedException, ItemNotFoundException,
            InvalidFormatException
    {
        String value = getString(configName, itemName);
        try
        {
            return Integer.parseInt(value);
        }
        catch (NumberFormatException e)
        {
            throw new InvalidFormatException();
        }
    }

    /**
     * 获取指定名称的float类型的配置项
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @return 配置项
     * @throws FileNotLoadedException 文件未加载或未创建
     * @throws ItemNotFoundException 配置项不存在
     * @throws InvalidFormatException 配置项格式不正确
     */
    public float getFloat(String configName, String itemName)
            throws FileNotLoadedException, ItemNotFoundException,
            InvalidFormatException
    {
        String value = getString(configName, itemName);
        try
        {
            return Float.parseFloat(value);
        }
        catch (NumberFormatException e)
        {
            throw new InvalidFormatException();
        }
    }

    /**
     * 获取指定名称的对象类型的配置项，对于String，int，float的类型也可
     * 获取，对于int和float类型的配置项，返回包装类。
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @return 配置项，对于int和float类型的配置项，返回包装类
     * @throws FileNotLoadedException 文件未加载或未创建
     * @throws ItemNotFoundException 配置项不存在
     * @throws InvalidFormatException 配置项格式不正确
     */
    public Object getObject(String configName, String itemName)
            throws FileNotLoadedException,
            ItemNotFoundException, InvalidFormatException
    {
        return getObject(configName, ATTRIBUTE_NAME, itemName);
    }

    /**
     * 获取指定属性名为指定值的对象类型的配置项，对于String，int，float的类型也可
     * 获取，此时属性名应为name，对于int和float类型的配置项，返回包装类。
     * 
     * @param configName 指定配置段名称
     * @param fieldName 属性名，获取String，int，float的类型时为name
     * @param fieldValue 属性值
     * @return 配置项，对于int和float类型的配置项，返回包装类
     * @throws FileNotLoadedException 文件未加载或未创建
     * @throws ItemNotFoundException 配置项不存在
     * @throws InvalidFormatException 配置项格式不正确
     */
    public Object getObject(String configName, String fieldName,
            Object fieldValue) throws FileNotLoadedException,
            ItemNotFoundException, InvalidFormatException
    {
        Element item = getItem(configName, fieldName, fieldValue);
        if (item == null)
        {
            throw new ItemNotFoundException();
        }
        return makeObject(item);
    }

    /**
     * 获取指定配置段下所有配置项，以列表形式返回，不包含格式不正确的配置项。如果有 
     * 配置项格式不正确，会在日志中注明
     * 
     * @param configName 指定配置段名称
     * @return 配置段下所有配置项，对于int和float类型的配置项，返回包装类
     * @throws FileNotLoadedException 文件未加载或未创建
     * @throws ItemNotFoundException 配置项不存在
     */
    @SuppressWarnings("rawtypes")
	public List getObjectList(String configName)
            throws FileNotLoadedException, ItemNotFoundException
    {
        Element config = getConfig(configName);
        if (config == null)
        {
            throw new ItemNotFoundException();
        }

        List<Object> objects = new LinkedList<Object>();
        NodeList items = config.getChildNodes();
        for (int i = 0; i < items.getLength(); i++)
        {
            Node item = items.item(i);
            if (item.getNodeType() == Node.ELEMENT_NODE)
            {
                try
                {
                    objects.add(makeObject((Element)item));
                }
                catch (InvalidFormatException e)
                {
//                    logger.warn("配置项<"
//                            + ((Element)item).getAttribute(ATTRIBUTE_NAME)
//                            + ">格式不正确，无法转成指定对象。", e);
                }
            }
        }
        return objects;
    }

    /**
     * 获取指定配置段下所有配置项，以映射表形式返回，key为属性名为指定值的对象，
     * value为配置项。不包含格式不正确的配置项。如果有配置项格式不正确，会在日志中
     * 注明。对于String，int，float的类型的配置项也可获取，此时属性名应为name， 
     * 对于int和float类型的配置项，返回包装类。
     * 
     * @param configName 指定配置段名称
     * @param keyName 作为key值的属性名，获取String，int，float的类型时为name
     * @return 配置段下所有配置项，对于int和float类型的配置项，返回包装类
     * @throws FileNotLoadedException 文件未加载或未创建
     * @throws ItemNotFoundException 配置项不存在
     */
    public Map<Object, Object> getObjectMap(String configName, String keyName)
            throws FileNotLoadedException, ItemNotFoundException
    {
        Element config = getConfig(configName);
        if (config == null)
        {
            throw new ItemNotFoundException();
        }

        HashMap<Object, Object> objects = new HashMap<Object, Object>();
        NodeList items = config.getChildNodes();
        for (int i = 0; i < items.getLength(); i++)
        {
            Node item = items.item(i);
            if (item.getNodeType() == Node.ELEMENT_NODE)
            {
                try
                {
                    if (keyName.equals(ATTRIBUTE_NAME))
                    {
                        String key = ((Element)item).getAttribute(keyName);
                        if (key.length() > 0)
                        {
                            objects.put(key, makeObject((Element)item));
                            continue;
                        }
                    }

                    Element keyField = getField((Element)item, keyName);
                    if (keyField != null)
                    {
                        objects.put(makeObject(keyField),
                                makeObject((Element)item));
                    }
                }
                catch (InvalidFormatException e)
                {
//                    logger.warn("配置项<" 
//                            + ((Element)item).getAttribute(ATTRIBUTE_NAME) 
//                            + ">无法转成指定对象。", e);
                }
            }
        }
        return objects;
    }

    /**
     * 设置配置段
     * 
     * @param configName 指定配置段名称
     * @throws CreateFileException 创建配置文件失败
     */
    public void setConfig(String configName) throws CreateFileException
    {
        Element config = makeConfig(configName);
        clearElement(config);
    }

    /**
     * 设置指定配置段下String类型的指定配置项名称，值为空
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @throws CreateFileException 创建配置文件失败
     */
    public void setString(String configName, String itemName)
            throws CreateFileException
    {
        Element item = makeItem(configName, itemName);
        clearElement(item);
    }

    /**
     * 设置指定配置段下指定配置项名称的String类型的配置项
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @param itemValue 指定配置项
     * @throws CreateFileException 创建配置文件失败
     */
    public void setString(String configName, String itemName, String itemValue)
            throws CreateFileException
    {
        Element item = makeItem(configName, itemName);
        clearElement(item);
        item.appendChild(document.createTextNode(itemValue));
    }

    /**
     * 设置指定配置段下int类型的指定配置项名称，值为空
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @throws CreateFileException 创建配置文件失败
     */
    public void setInt(String configName, String itemName)
            throws CreateFileException
    {
        Element item = makeItem(configName, itemName);
        item.setAttribute(ATTRIBUTE_CLASS, CLASS_INTEGER);
        clearElement(item);
    }

    /**
     * 设置指定配置段下指定配置项名称的int类型的配置项
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @param itemValue 指定配置项
     * @throws CreateFileException 创建配置文件失败
     */
    public void setInt(String configName, String itemName, int itemValue)
            throws CreateFileException
    {
        Element item = makeItem(configName, itemName);
        item.setAttribute(ATTRIBUTE_CLASS, CLASS_INTEGER);
        clearElement(item);
        item.appendChild(document.createTextNode(Integer.toString(itemValue)));
    }

    /**
     * 设置指定配置段下float类型的指定配置项名称，值为空
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @throws CreateFileException 创建配置文件失败
     */
    public void setFloat(String configName, String itemName)
            throws CreateFileException
    {
        Element item = makeItem(configName, itemName);
        item.setAttribute(ATTRIBUTE_CLASS, CLASS_FLOAT);
        clearElement(item);
    }

    /**
     * 设置指定配置段下指定配置项名称的float类型的配置项
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @param itemValue 指定配置项
     * @throws CreateFileException 创建配置文件失败
     */
    public void setFloat(String configName, String itemName, float itemValue)
            throws CreateFileException
    {
        Element item = makeItem(configName, itemName);
        item.setAttribute(ATTRIBUTE_CLASS, CLASS_FLOAT);
        clearElement(item);
        item.appendChild(document.createTextNode(Float.toString(itemValue)));
    }

    /**
     * 设置指定配置段下指定配置项名称，值为空
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @throws CreateFileException 创建配置文件失败
     */
    public void setObject(String configName, String itemName)
            throws CreateFileException
    {
        setString(configName, itemName);
    }

    /**
     * 设置指定配置段下指定配置项名称，配置项类型可以为String、Integer、Float或
     * 其它JavaBean对象
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @param object 指定配置项
     * @throws CreateFileException 创建配置文件失败
     */
    public void setObject(String configName, String itemName, Object object)
            throws CreateFileException
    {
        if (object instanceof String)
        {
            setString(configName, itemName, (String)object);
        }
        else if (object instanceof Integer)
        {
            setInt(configName, itemName, ((Integer)object).intValue());
        }
        else if (object instanceof Float)
        {
            setFloat(configName, itemName, ((Integer)object).floatValue());
        }
        else
        {
            Element config = makeConfig(configName);
            Element item = null;
            try
            {
                item = getItem(configName, ATTRIBUTE_NAME, itemName);
            }
            catch (FileNotLoadedException e)
            {
                assert(false);
            }
            removeElement(item);

            item = makeElement(object, itemName);
            config.appendChild(item);
        }
    }

    /**
     * 设置指定配置段下的配置项，配置项从指定列表中获取，配置项均没有名称。配置项类型
     * 可以为String、Integer、Float或其它JavaBean对象
     * 
     * @param configName 指定配置段名称
     * @param objects 配置项列表
     * @throws CreateFileException 创建配置文件失败
     */
    public void setObjectList(String configName, List<?> objects)
            throws CreateFileException
    {
        Element config = makeConfig(configName);
        clearElement(config);

        for (Object object : objects)
        {
            Element item = makeElement(object, null);
            config.appendChild(item);
        }
    }

    /**
     * 设置指定配置段下的配置项，配置项从指定映射表中获取，如果映射表的key类型为
     * String，则key值作为配置项，否则配置项均没有名称。配置项类型可以为String、 
     * Integer、Float或其它JavaBean对象
     * 
     * @param configName 指定配置段名称
     * @param objects 配置项列表
     * @throws CreateFileException 创建配置文件失败
     */
    public void setObjectMap(String configName, Map<Object, Object> objects)
            throws CreateFileException
    {
        Element config = makeConfig(configName);
        clearElement(config);

        for (Map.Entry<Object, Object> entry : objects.entrySet())
        {
            String itemName = null;
            if (entry.getKey() instanceof String)
            {
                itemName = (String)entry.getKey();
            }
            Element item = makeElement(entry.getValue(), itemName);
            config.appendChild(item);
        }
    }

    /**
     * 增加指定配置段下指定配置项名称的String类型的配置项
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @param itemValue 指定配置项
     * @throws CreateFileException 创建配置文件失败
     */
    public void addString(String configName, String itemName, String itemValue)
            throws CreateFileException
    {
        Element item = addItem(configName, itemName);
        item.appendChild(document.createTextNode(itemValue));
    }

    /**
     * 增加指定配置段下指定配置项名称的int类型的配置项
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @param itemValue 指定配置项
     * @throws CreateFileException 创建配置文件失败
     */
    public void addInt(String configName, String itemName, int itemValue)
            throws CreateFileException
    {
        Element item = addItem(configName, itemName);
        item.setAttribute(ATTRIBUTE_CLASS, CLASS_INTEGER);
        item.appendChild(document.createTextNode(Integer.toString(itemValue)));
    }

    /**
     * 增加指定配置段下指定配置项名称的float类型的配置项
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @param itemValue 指定配置项
     * @throws CreateFileException 创建配置文件失败
     */
    public void addFloat(String configName, String itemName, float itemValue)
            throws CreateFileException
    {
        Element item = addItem(configName, itemName);
        item.setAttribute(ATTRIBUTE_CLASS, CLASS_FLOAT);
        item.appendChild(document.createTextNode(Float.toString(itemValue)));
    }

    /**
     * 增加指定配置段下指定配置项名称的JavaBean类型的配置项
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @param itemValue 指定配置项
     * @throws CreateFileException 创建配置文件失败
     */
    public void addObject(String configName, String itemName, Object object)
            throws CreateFileException
    {
        Element config = makeConfig(configName);

        Element item = makeElement(object, itemName);
        config.appendChild(item);
    }

    /**
     * 增加指定配置段下的配置项，配置项从指定列表中获取，配置项均没有名称。配置项类型
     * 可以为String、Integer、Float或其它JavaBean对象
     * 
     * @param configName 指定配置段名称
     * @param objects 配置项列表
     * @throws CreateFileException 创建配置文件失败
     */
    public void addObjectList(String configName, List<?> objects)
            throws CreateFileException
    {
        Element config = makeConfig(configName);

        for (Object object : objects)
        {
            Element item = makeElement(object, null);
            config.appendChild(item);
        }
    }

    /**
     * 增加指定配置段下的配置项，配置项从指定映射表中获取，如果映射表的key类型为
     * String，则key值作为配置项，否则配置项均没有名称。配置项类型可以为String、 
     * Integer、Float或其它JavaBean对象
     * 
     * @param configName 指定配置段名称
     * @param objects 配置项列表
     * @throws CreateFileException 创建配置文件失败
     */
    public void addObjectMap(String configName, Map<Object, Object> objects)
            throws CreateFileException
    {
        Element config = makeConfig(configName);

        Set<Map.Entry<Object, Object>> entrySet = objects.entrySet();
        for (Map.Entry<Object, Object> entry : entrySet)
        {
            String itemName = null;
            if (entry.getKey() instanceof String)
            {
                itemName = (String)entry.getKey();
            }
            Element item = makeElement(entry.getValue(), itemName);
            config.appendChild(item);
        }
    }

    /**
     * 清除指定配置段下的所有配置项
     * 
     * @param configName 指定配置段名称
     * @throws FileNotLoadedException 文件未加载或未创建
     */
    public void clearConfig(String configName) throws FileNotLoadedException
    {
        Element config = getConfig(configName);

        clearElement(config);
    }

    /**
     * 清除指定配置下指定名称的配置项的值
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @throws FileNotLoadedException 文件未加载或未创建
     */
    public void clearString(String configName, String itemName)
            throws FileNotLoadedException
    {
        Element item = getItem(configName, ATTRIBUTE_NAME, itemName);

        clearElement(item);
    }

    /**
     * 清除指定配置下指定名称的配置项的值
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @throws FileNotLoadedException 文件未加载或未创建
     */
    public void clearInteger(String configName, String itemName)
            throws FileNotLoadedException
    {
        clearString(configName, itemName);
    }

    /**
     * 清除指定配置下指定名称的配置项的值
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @throws FileNotLoadedException 文件未加载或未创建
     */
    public void clearFloat(String configName, String itemName)
            throws FileNotLoadedException
    {
        clearString(configName, itemName);
    }

    /**
     * 清除指定配置下指定名称的配置项的值
     * 
     * @param configName 指定配置段名称
     * @param fieldName 属性名，清除String，int，float的类型时为name
     * @param fieldValue 属性值
     * @throws FileNotLoadedException 文件未加载或未创建
     */
    public void clearObject(String configName, String fieldName,
            String fieldValue) throws FileNotLoadedException
    {
        Element item = getItem(configName, fieldName, fieldValue);

        clearElement(item);
    }

    /**
     * 删除指定配置段
     * 
     * @param configName 指定配置段名称
     * @throws FileNotLoadedException 文件未加载或未创建
     */
    public void removeConfig(String configName) throws FileNotLoadedException
    {
        Element config = getConfig(configName);

        removeElement(config);
    }

    /**
     * 删除指定配置下指定名称的配置项
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @throws FileNotLoadedException 文件未加载或未创建
     */
    public void removeString(String configName, String itemName)
            throws FileNotLoadedException
    {
        Element item = getItem(configName, ATTRIBUTE_NAME, itemName);

        removeElement(item);
    }

    /**
     * 删除指定配置下指定名称的配置项
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @throws FileNotLoadedException 文件未加载或未创建
     */
    public void removeInteger(String configName, String itemName)
            throws FileNotLoadedException
    {
        removeString(configName, itemName);
    }

    /**
     * 删除指定配置下指定名称的配置项
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @throws FileNotLoadedException 文件未加载或未创建
     */
    public void removeFloat(String configName, String itemName)
            throws FileNotLoadedException
    {
        removeString(configName, itemName);
    }

    /**
     * 删除指定配置下指定名称的配置项
     * 
     * @param configName 指定配置段名称
     * @param fieldName 属性名，删除String，int，float的类型时为name
     * @param fieldValue 属性值
     * @throws FileNotLoadedException 文件未加载或未创建
     */
    public void removeObject(String configName, String fieldName,
            String fieldValue) throws FileNotLoadedException
    {
        Element item = getItem(configName, fieldName, fieldValue);

        removeElement(item);
    }

    /**
     * 获取指定配置段的Element
     * 
     * @param configName 指定配置段名称
     * @return 指定配置段的Element，找不到则返回null
     * @throws FileNotLoadedException 文件未加载或未创建
     */
    private Element getConfig(String configName) throws FileNotLoadedException
    {
        if (document == null)
        {
            throw new FileNotLoadedException();
        }
        Element root = document.getDocumentElement();
        NodeList configs = root.getChildNodes();
        for (int i = 0; i < configs.getLength(); i++)
        {
            Node config = configs.item(i);
            if (config.getNodeType() == Node.ELEMENT_NODE)
            {
                if (((Element)config).getAttribute(ATTRIBUTE_NAME).equals(
                        configName))
                {
                    return (Element)config;
                }
            }
        }
        return null;
    }

    /**
     * 获取指定配置段下指定属性名的属性为指定值的配置项Element
     * 
     * @param configName 指定配置段名称
     * @param fieldName 属性名，获取String，int，float的类型时为name
     * @param fieldValue 属性值
     * @return 指定配置项的Element，找不到则返回null
     * @throws FileNotLoadedException 文件未加载或未创建
     */
    private Element getItem(String configName, String fieldName,
            Object fieldValue) throws FileNotLoadedException
    {
        Element config = getConfig(configName);
        if (config != null)
        {
            return getItem(config, fieldName, fieldValue);
        }
        return null;
    }

    /**
     * 获取指定配置段下指定属性名的属性为指定值的配置项的Element
     * 
     * @param config 指定配置段的Element
     * @param fieldName 属性名，获取String，int，float的类型时为name
     * @param fieldValue 属性值
     * @return 指定配置项的Element，找不到则返回null
     */
    private Element getItem(Element config, String fieldName, Object fieldValue)
    {
        NodeList items = config.getChildNodes();
        for (int i = 0; i < items.getLength(); i++)
        {
            Node item = items.item(i);
            if (item.getNodeType() == Node.ELEMENT_NODE)
            {
                if (fieldName.equals(ATTRIBUTE_NAME)
                        && fieldValue instanceof String)
                {
                    if (fieldValue.equals(((Element)item)
                            .getAttribute(fieldName)))
                    {
                        return (Element)item;
                    }
                }

                Element field = getField((Element)item, fieldName);
                if (field != null)
                {
                    Object fieldObjct = null;
                    try
                    {
                        fieldObjct = makeObject(field);
                        if (fieldValue.equals(fieldObjct))
                        {
                            return (Element)item;
                        }
                    }
                    catch (InvalidFormatException e)
                    {
                    }
                }
            }
        }
        return null;
    }

    /**
     * 获取指定配置项下指定属性名的属性的Element
     * 
     * @param item 指定配置项的Element
     * @param fieldName 属性名，获取String，int，float的类型时为name
     * @return 指定属性名的属性的Element，找不到则返回null
     */
    private Element getField(Element item, String fieldName)
    {
        NodeList fields = item.getChildNodes();
        for (int i = 0; i < fields.getLength(); i++)
        {
            Node field = fields.item(i);
            if (field.getNodeType() == Node.ELEMENT_NODE)
            {
                if (fieldName.equals(((Element)field)
                        .getAttribute(ATTRIBUTE_NAME)))
                {
                    return (Element)field;
                }
            }
        }
        return null;
    }

    /**
     * 将指定的Element创建为对象
     * 
     * @param element 指定的Element
     * @return 根据Element的类型返回String、Integer、Float或JavaBean对象
     * @throws InvalidFormatException 配置项格式不正确
     */
    private Object makeObject(Element element) throws InvalidFormatException
    {
        String className = element.getAttribute(ATTRIBUTE_CLASS);
        if (className.length() == 0 || className.equals(CLASS_STRING))
        {
            NodeList fields = element.getChildNodes();
            if (fields.getLength() == 0)
            {
                return "";
            }
            String value = fields.item(0).getNodeValue();
            if (fields.getLength() > 1)
            {
                int index = value.lastIndexOf('\n');
                if (index >= 0)
                {
                    value = value.substring(0, value.lastIndexOf('\n'));
                    if (value.charAt(value.length() - 1) == '\r')
                    {
                        value = value.substring(0, value.length() - 1);
                    }
                }
            }
            return value;
        }
        else if (className.equals(CLASS_INTEGER))
        {
            NodeList fields = element.getChildNodes();
            if (fields.getLength() == 0)
            {
                throw new InvalidFormatException();
            }
            String value = fields.item(0).getNodeValue();
            if (fields.getLength() > 1)
            {
                int index = value.lastIndexOf('\n');
                if (index >= 0)
                {
                    value = value.substring(0, value.lastIndexOf('\n'));
                    if (value.charAt(value.length() - 1) == '\r')
                    {
                        value = value.substring(0, value.length() - 1);
                    }
                }
            }
            try
            {
                return Integer.valueOf(value);
            }
            catch (NumberFormatException e)
            {
                throw new InvalidFormatException(e);
            }
        }
        else if (className.equals(CLASS_FLOAT))
        {
            NodeList fields = element.getChildNodes();
            if (fields.getLength() == 0)
            {
                throw new InvalidFormatException();
            }
            String value = fields.item(0).getNodeValue();
            if (fields.getLength() > 1)
            {
                int index = value.lastIndexOf('\n');
                if (index >= 0)
                {
                    value = value.substring(0, value.lastIndexOf('\n'));
                    if (value.charAt(value.length() - 1) == '\r')
                    {
                        value = value.substring(0, value.length() - 1);
                    }
                }
            }
            try
            {
                return Float.valueOf(value);
            }
            catch (NumberFormatException e)
            {
                throw new InvalidFormatException(e);
            }
        }
        else if (className.equals(CLASS_LIST))
        {
            LinkedList<Object> list = new LinkedList<Object>();
            NodeList fields = element.getChildNodes();
            for (int i = 0; i < fields.getLength(); i++)
            {
                Node field = fields.item(i);
                if (field.getNodeType() == Node.ELEMENT_NODE)
                {
                    list.add(makeObject((Element)field));
                }
            }
            return list;
        }
        else if (className.equals(CLASS_MAP))
        {
            HashMap<Object, Object> map = new HashMap<Object, Object>();
            NodeList fields = element.getChildNodes();
            for (int i = 0; i < fields.getLength(); i++)
            {
                Node field = fields.item(i);
                if (field.getNodeType() == Node.ELEMENT_NODE)
                {
                    map.put(makeKeyObject((Element)field),
                            makeObject((Element)field));
                }
            }
            return map;
        }
        else
        {
            Class<?> itemClass = null;
            Object object = null;
            try
            {
                itemClass = Class.forName(className);
                object = itemClass.newInstance();
            }
            catch (Exception e)
            {
                throw new InvalidFormatException(e);
            }
            NodeList fields = element.getChildNodes();
            for (int i = 0; i < fields.getLength(); i++)
            {
                Node field = fields.item(i);
                if (field.getNodeType() == Node.ELEMENT_NODE)
                {
                    try
                    {
                        Object filedObject = makeObject((Element)field);
                        String MethodName = SET_METHOD_PRE_NAME
                                + ((Element)field).getAttribute(ATTRIBUTE_NAME);
                        Method[] methods = itemClass.getMethods();
                        for (int j = 0; j < methods.length; j++)
                        {
                            if (MethodName.equals(methods[j].getName()))
                            {
                                Method setMethod = methods[j];
                                setMethod.invoke(object, filedObject);
                                break;
                            }
                        }
                    }
                    catch (Exception e1)
                    {
//                        logger.warn("类<" + className + ">对象属性<"
//                                + ((Element)field).getAttribute(ATTRIBUTE_NAME) 
//                                + ">设置不成功。", e1);
                    }
                }
            }
            return object;
        }
    }

    /**
     * 创建指定Element中标明为key的属性的对象，如果不存在此属性则取Element的Name属性
     * 的值，如果不存在Name属性或没有赋值，则取最后一个属性创建对象。
     * 
     * @param element 指定的Element
     * @return 指定Element中标明为key的属性的对象
     */
    private Object makeKeyObject(Element element)
    {
        Object fieldObject = null;
        NodeList fields = element.getChildNodes();
        for (int i = 0; i < fields.getLength(); i++)
        {
            try
            {
                Node field = fields.item(i);
                if (field.getNodeType() == Node.ELEMENT_NODE)
                {
                    fieldObject = makeObject((Element)field);
                    if (((Element)field).getAttribute(ATTRIBUTE_KEY)
                            .equals(VALUE_TRUE))
                    {
                        return fieldObject;
                    }
                }
            }
            catch (InvalidFormatException e)
            {
            }
        }

        if (element.getAttribute(ATTRIBUTE_NAME).length() > 0)
        {
            return element.getAttribute(ATTRIBUTE_NAME);
        }

        return fieldObject;
    }

    /**
     * 获取指定配置段名称的配置段Element，不存在则创建一个
     * 
     * @param configName 指定配置段名称
     * @return 指定配置段名称的配置段Element
     * @throws CreateFileException 创建配置文件失败
     */
    private Element makeConfig(String configName) throws CreateFileException
    {
        if (document == null)
        {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db;
            try
            {
                db = dbf.newDocumentBuilder();
            }
            catch (ParserConfigurationException e)
            {
                throw new CreateFileException(e);
            }
            document = db.newDocument();
            Element root = document.createElement(TAG_ROOT);
            document.appendChild(root);
        }

        Element config = null;
        try
        {
            config = getConfig(configName);
        }
        catch (FileNotLoadedException e)
        {
            assert (false);
        }
        if (config == null)
        {
            Element root = document.getDocumentElement();
            config = document.createElement(TAG_CONFIG);
            config.setAttribute(ATTRIBUTE_NAME, configName);
            root.appendChild(config);
        }
        return config;
    }

    /**
     * 获取指定配置段名称的配置项Element，不存在则创建一个
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @return 指定配置段名称的配置段Element
     * @throws CreateFileException 创建配置文件失败
     */
    private Element makeItem(String configName, String itemName)
            throws CreateFileException
    {
        Element config = makeConfig(configName);
        Element item = getItem(config, ATTRIBUTE_NAME, itemName);
        if (item == null)
        {
            item = document.createElement(TAG_ITEM);
            if (itemName != null)
            {
                item.setAttribute(ATTRIBUTE_NAME, itemName);
            }
            config.appendChild(item);
        }
        return item;
    }

    /**
     * 根据指定对象创建Element
     * 
     * @param object 指定对象
     * @param itemName 对象名称
     * @return 根据指定对象创建Element
     */
    @SuppressWarnings("unchecked")
	private Element makeElement(Object object, String itemName)
    {
        Element element = document.createElement(TAG_ITEM);
        if (itemName != null)
        {
            element.setAttribute(ATTRIBUTE_NAME, itemName);
        }
        if (object instanceof String)
        {
            element.appendChild(document.createTextNode((String)object));
        }
        else if (object instanceof Integer)
        {
            element.setAttribute(ATTRIBUTE_CLASS, CLASS_INTEGER);
            element.appendChild(document.createTextNode(((Integer)object).toString()));
        }
        else if (object instanceof Float)
        {
            element.setAttribute(ATTRIBUTE_CLASS, CLASS_FLOAT);
            element.appendChild(document.createTextNode(((Float)object).toString()));
        }
        else if (object instanceof List)
        {
            element.setAttribute(ATTRIBUTE_CLASS, CLASS_LIST);
            for (Object field : (List<?>)object)
            {
                element.appendChild(makeElement(field, null));
            }
        }
        else if (object instanceof Map)
        {
            element.setAttribute(ATTRIBUTE_CLASS, CLASS_MAP);
            for (Object entry : ((Map<?, ?>)object).entrySet())
            {
                Object fieldObject = ((Map.Entry<Object, Object>)entry)
                        .getValue();
                Object fieldKeyObject = ((Map.Entry<Object, Object>)entry)
                        .getKey();
                Element field = makeElement(fieldObject, null);
                Element keyField = makeElement(fieldKeyObject, null);
                keyField.setAttribute(ATTRIBUTE_KEY, VALUE_TRUE);
                field.appendChild(keyField);
                element.appendChild(field);
            }
        }
        else
        {
            Class<? extends Object> objectClass = object.getClass();
            element.setAttribute(ATTRIBUTE_CLASS, objectClass.getName());
            Method[] methods = objectClass.getMethods();
            for (Method method : methods)
            {
                if (method.getName().startsWith(GET_METHOD_PRE_NAME))
                {
                    if (method.getName().equals(METHOD_GETCLASS))
                    {
                        continue;
                    }
                    try
                    {
                        Object fieldObject = method.invoke(object, (Object[])null);
                        if (fieldObject != null)
                        {
                            Element field = makeElement(fieldObject, method.getName().substring(GET_METHOD_PRE_NAME.length()));
                            element.appendChild(field);
                        }
                    }
                    catch (Exception e)
                    {
                        //logger.warn(e);
                    }
                }
            }
        }
        return element;
    }

    /**
     * 增加指定配置段名称的配置项的Element
     * 
     * @param configName 指定配置段名称
     * @param itemName 指定配置项名称
     * @return 指定配置段名称的配置段Element
     * @throws CreateFileException 创建配置文件失败
     */
    private Element addItem(String configName, String itemName)
            throws CreateFileException
    {
        Element config = makeConfig(configName);
        Element item = document.createElement(TAG_ITEM);
        if (itemName != null)
        {
            item.setAttribute(ATTRIBUTE_NAME, itemName);
        }
        config.appendChild(item);

        return item;
    }
    
    /**
     * 清除指定的Element项
     * 
     * @param element 指定的Element
     */
    private void clearElement(Element element)
    {
        if (element == null)
        {
            return;
        }
        while (element.getFirstChild() != null)
        {
            element.removeChild(element.getFirstChild());
        }
    }

    /**
     * 删除指定的Element项
     * 
     * @param element 指定的Element
     */
    private void removeElement(Element element)
    {
        if (element == null)
        {
            return;
        }
        Node parent = element.getParentNode();
        parent.removeChild(element);
    }

    /**
     * 删除配置文件加载时DOM自动添加的空行
     * 
     * @param parentNode 结点
     */
    private void trimEmptyNodes(Node parentNode)
    {
        Node nextchild = parentNode.getFirstChild();
        Node anchorNode = null;
        while (nextchild != null)
        {
            if (nextchild.getNodeType() == Node.TEXT_NODE
                    && isEmptyText(nextchild.getNodeValue()))
            {
                parentNode.removeChild(nextchild);
                if (anchorNode == null)
                {
                    nextchild = parentNode.getFirstChild();
                }
                else
                {
                    nextchild = anchorNode.getNextSibling();
                }
            }
            else
            {
                anchorNode = nextchild;
                trimEmptyNodes(nextchild);// iterate call
                nextchild = nextchild.getNextSibling();
            }
        }
    }

    /**
     * 判断是否空行
     * 
     * @param theText 行
     * @return 是否空行
     */
    private boolean isEmptyText(String theText)
    {
        if (theText.trim().length() == 0)
        {
            if (theText.indexOf('\n') == -1 && (theText.indexOf('\r') == -1))
            {
                return false;
            }
            else
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Logger logger 日志管理器
     */
    //private static Logger logger;
    static
    {
        //Log4j.defaultInitOverride = "true";
        //logger = Logger.getLogger(XMLConfigFile.class);
    }

    /**
     * String configFileName 配置文件名
     */
    private String configFileName;

    /**
     * Document document XML文档
     */
    private Document document;

    /**
     * String TAG_ROOT 根标签 
     */
    private final static String TAG_ROOT = "Configs";

    /**
     * String TAG_CONFIG 配置组标签
     */
    private final static String TAG_CONFIG = "Config";

    /**
     * String TAG_ITEM 配置项标签
     */
    private final static String TAG_ITEM = "Item";

    /**
     * String ATTRIBUTE_NAME 名称属性
     */
    public final static String ATTRIBUTE_NAME = "name";

    /**
     * String ATTRIBUTE_CLASS 类属性
     */
    private final static String ATTRIBUTE_CLASS = "class";

    /**
     * String ATTRIBUTE_KEY 关键项属性
     */
    private final static String ATTRIBUTE_KEY = "key";
    
    /**
     * String VALUE_TRUE 真值
     */
    private final static String VALUE_TRUE = "true";

    /**
     * String CLASS_STRING 字符串类
     */
    private final static String CLASS_STRING = "String";

    /**
     * String CLASS_INTEGER 整型类
     */
    private final static String CLASS_INTEGER = "Integer";

    /**
     * String CLASS_FLOAT 浮点数类
     */
    private final static String CLASS_FLOAT = "Float";

    /**
     * String CLASS_LIST 列表类
     */
    private final static String CLASS_LIST = "List";

    /**
     * String CLASS_MAP 映射类
     */
    private final static String CLASS_MAP = "Map";

    /**
     * String GET_METHOD_PRE_NAME getter方法前缀
     */
    private final static String GET_METHOD_PRE_NAME = "get";

    /**
     * String SET_METHOD_PRE_NAME setter方法前缀
     */
    private final static String SET_METHOD_PRE_NAME = "set";
    
    /**
     * String METHOD_GETCLASS  获取类对象方法名
     */
    private final static String METHOD_GETCLASS = "getClass";
}
