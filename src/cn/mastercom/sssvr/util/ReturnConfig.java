package cn.mastercom.sssvr.util;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;

public class ReturnConfig
{
	public static String returnconfig(String filePath, String paramater)
	{
		String value = "";
		SAXReader reader = new SAXReader();
		OutputFormat format = OutputFormat.createPrettyPrint();
		format.setEncoding("GBK");// 设置XML文件的编码格式

		File file = new File(filePath);
		if (file.exists())
		{
			try
			{
				Document doc = reader.read(file);
				List<String> list = doc.selectNodes(paramater);
				Iterator iter = list.iterator();
				while (iter.hasNext())
				{
					Element element = (Element) iter.next();
					value = element.getText();
					break;
				}
			} catch (DocumentException e)
			{
				value = "";
			}
		}
		return value;
	}

}
