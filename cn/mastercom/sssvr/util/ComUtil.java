/**
 * 
 */
package cn.mastercom.sssvr.util;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.Window;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.StringTokenizer;
import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTabbedPane;
import javax.swing.JTextField;
import javax.swing.SwingConstants;
import javax.swing.UIManager;
import javax.swing.border.EmptyBorder;

import org.apache.log4j.Logger;

/**
 * 公用工具函数包
 * 
 * @author qiaoxianqiang
 */
public class ComUtil {
	
	/**
	 * 20072007-1-5上午09:19:54 
	 * 根据输入的组件，获取一个包含该组件的带边的面板
	 * @param c
	 * @param top
	 * @param left
	 * @param bottom
	 * @param right
	 * @return
	 */
	public static JPanel getOuterPane(Component c, int top, int left,
			int bottom, int right) {
		JPanel outerPane = new JPanel(new GridBagLayout());
		outerPane.add(c, new GBC(0, 0).setFill(GridBagConstraints.BOTH).setWeight(100, 100)
				.setInsets(top, left, bottom, right));
		return outerPane;
	}
	
	/**
	 * 根据输入的组件，获取一个包含该组件的面板边间隔的面板，所有的间隔采用一个唯一的值
	 * @param c
	 * @param value
	 * @return
	 */
	public static JPanel getOuterPane(Component c,int value){
		JPanel outerPane = new JPanel(new GridBagLayout());
		outerPane.add(c, new GBC(0, 0).setFill(GridBagConstraints.BOTH).setWeight(100, 100)
				.setInsets(value, value, value, value));
		return outerPane;
	}

	/**
	 * 创建一个包含此窗体的Ｆｒａｍｅ
	 * 
	 * @param c
	 */
	public static void createFrame(Component c) {
		JFrame f = new JFrame();
		f.addWindowListener(new WindowAdapter() {
			@Override
			public void windowClosing(WindowEvent e) {
				System.exit(0);
			}
		});

		f.getContentPane().add(c);

		f.setSize(new Dimension(830, 550));
		f.setVisible(true);
	}

	/**
	 * 创建一个包含此窗体的对话框
	 * 
	 * @param c
	 */
	public static void createDialog(Component c) {
		JDialog f = new JDialog();
		f.addWindowListener(new WindowAdapter() {
			@Override
			public void windowClosing(WindowEvent e) {
				System.exit(0);
			}
		});

		f.getContentPane().add(c,BorderLayout.NORTH);

		f.setSize(new Dimension(830, 550));
		f.setVisible(true);
	}

	/**
	 * 转换成ｗｉｎｄｏｗｓ平台字节序
	 * @param value
	 * @return
	 */
	public static int toWindowsPlatFormValue(int value){
		byte b0 = (byte) ((value >> 24)&0x00FF);
		byte b1 = (byte) ((value >> 16)&0x00FF);
		byte b2 = (byte) ((value >> 8)&0x00FF);
		byte b3 = (byte) ((value)&0x00FF);
		return b3<<24|b2<<16|b1<<8|b0;
	}
	
	/**
	 * 获取14位编码的字符串 3 8 3
	 * @return
	 */
	public static String getCode14String(int code14) {
		if(code14 == -1){
			return "";
		}
		int i1 = code14>>11;
		int i2 = code14>>3;
		i2 = i2&0x00ff;
		int i3 = code14 & 0x0007;
		return String.format("X%1$02X-X%2$02X-X%3$02X",i1,i2,i3);
	}
	
	/**
	 * 获取24位编码的字符串 8 8 8
	 * @return
	 */
	public static String getCode24String(int code24) {
		if(code24 == -1){
			return "";
		}
		int i1 = code24 >> 16;
		int i2 = code24>> 8;
		i2 = i2&0x00ff;
		int i3 = code24 & 0x0000ff;
		return String.format("X%1$02X-X%2$02X-X%3$02X",i1,i2,i3);
	}

	public static int getSpCode14(String obj){
		if((obj == null)||(obj.equals(""))){
			return 0;
		}
		int[] nTmp = new int[3];
		StringTokenizer tokenizer = new StringTokenizer(obj, "-");
		int i = 0;
		while(tokenizer.hasMoreTokens()){
			String str = tokenizer.nextToken();
			str = str.substring(1);// 去掉“X”
			
			nTmp[i] = Integer.parseInt(str, 16);
			i = i + 1;
		}
		
		return (nTmp[0]<<11) + (nTmp[1] << 3) + (nTmp[2]);
	}
	
	public static int getSpCode24(String obj){
		if((obj == null)||(obj.equals(""))){
			return 0;
		}
		int[] nTmp = new int[3];
		StringTokenizer tokenizer = new StringTokenizer(obj, "-");
		int i = 0;
		while(tokenizer.hasMoreTokens()){
			String str = tokenizer.nextToken();
			str = str.substring(1);// 去掉“X”
			
			nTmp[i] = Integer.parseInt(str, 16);
			i = i + 1;
		}
		
		return (nTmp[0]<<16) + (nTmp[1]<<8) + (nTmp[2]);	
	}	
	/**
	 * 创建一个新文件，如果文件存在，删除该文件。
	 * @param path
	 */
	public static void createOneNewFile(String path){
		File f = new File(path);
		try {
			if(!f.createNewFile()){
				f.delete();
				f.createNewFile();
			}
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
	
	/**
	 * 去掉控件的边框
	 * 
	 * @param c
	 */
	public static void setNullBorder(JComponent c) {

		if (c instanceof JComponent) {
			Insets sts = new Insets(0, 0, 0, 0);
			EmptyBorder er = new EmptyBorder(sts);
			c.setBorder(er);
		}
	}

	public static byte[] ipStringToByteValue(String text) throws ParseException {
		StringTokenizer tokenizer = new StringTokenizer(text, ".");
		byte[] a = new byte[4];
		for (int i = 0; i < 4; i++) {
			int b = 0;
			if (!tokenizer.hasMoreTokens()) {
				throw new ParseException("Too few bytes", 0);
			}
			try {
				b = Integer.parseInt(tokenizer.nextToken().trim());
			} catch (NumberFormatException e) {
				throw new ParseException("Not an integer", 0);
			}
			if (b < 0 || b >= 256) {
				throw new ParseException("Byte out of range", 0);
			}
			a[i] = (byte) b;
		}
		if (tokenizer.hasMoreTokens())
			throw new ParseException("Too many bytes", 0);
		return a;
	}

	public static int ipStringToIntValue(String text) throws ParseException {
		byte[] b4 = ipStringToByteValue(text);
		int a0 = ((b4[0] & 0x000000FF));
		int a1 = ((b4[1] & 0x000000FF));
		int a2 = ((b4[2] & 0x000000FF));
		int a3 = ((b4[3] & 0x000000FF));
		return ((a0 << 24) + (a1 << 16) + (a2 << 8) + a3);
	}

	/**
	 * 根据ｉｐ地址的整形值获取对应的ｉｐ字符串，不足３位的以空格补齐。 20062006-12-29上午10:58:47
	 * 
	 * @param ip
	 * @return
	 */
	public static String converIPintToString(int ip) {
		int a0 = (ip >> 24) & 0x000000ff;
		int a1 = (ip >> 16) & 0x000000ff;
		int a2 = (ip >> 8) & 0x000000FF;
		int a3 = ip & 0x000000ff;
		return String.format("%1$3d.%2$3d.%3$3d.%4$3d", a0, a1, a2, a3);
	}

	/**
	 * 根据输入的ｉｐ地址获取ｂｙｔｅ数组 20062006-12-29上午11:06:22
	 * 
	 * @param ip
	 * @return
	 */
	public static byte[] getIpBytes(int ip) {
		byte[] r = new byte[4];
		r[0] = (byte) ((ip >> 24) & 0xff);
		r[1] = (byte) ((ip >> 16) & 0xff);
		r[2] = (byte) ((ip >> 8) & 0xff);
		r[3] = (byte) (ip & 0xff);
		return r;
	}

	public static void main(String s[]) {
		
		System.out.println(toWindowsPlatFormValue(234));
		
		getCalendarFromString("20071225081612000");
		String ip = "192.168.1.1";

		int asss = 0xFF << 24;
		System.out.println(asss);

		try {
			byte[] a = ipStringToByteValue(ip);
			System.out.println(a.toString());
			int a1 = ipStringToIntValue(ip);
			System.out.println(converIPintToString(a1));

		} catch (ParseException e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * 将面板中所有JCheckBox控件选择清除
	 * 
	 * @param c
	 */
	public static void clearAllCheckBox(Container c) {
		Component[] allc = c.getComponents();
		for (int i = 0; i < allc.length; i++) {
			if (allc[i] instanceof JCheckBox) {
				JCheckBox cb = (JCheckBox) allc[i];
				cb.setSelected(false);
			}
			if (allc[i] instanceof Container) {
				clearAllCheckBox((Container) allc[i]);
			}
		}
	}

	/**
	 * 将面板中所有TextArea控件内容清除
	 * 
	 * @param c
	 */
	public static void clearAllTextFiled(Container c) {
		Component[] allc = c.getComponents();
		for (int i = 0; i < allc.length; i++) {
			if (allc[i] instanceof JTextField) {
				JTextField tf = (JTextField) allc[i];
				tf.setText("");
			}
			if (allc[i] instanceof Container) {
				clearAllTextFiled((Container) allc[i]);
			}
		}
	}

	/**
	 * 将给定容器所有控件设置其Enable状态
	 * 
	 * @param c
	 * @param flag
	 */
	public static void setAllComponentEnable(Container c, boolean flag) {
		c.setEnabled(flag);
		Component[] allc = c.getComponents();
		for (int i = 0; i < allc.length; i++) {
			allc[i].setEnabled(flag);
			if (allc[i] instanceof Container) {
				setAllComponentEnable((Container) allc[i], flag);
			}
		}
	}


	/**
	 * 创建类似于windows adobe的tabbedpane
	 * @param tabPlacement
	 * @return
	 */
	public static JTabbedPane createTabbedPane(int tabPlacement) {
		switch (tabPlacement) {
		case SwingConstants.LEFT:
		case SwingConstants.RIGHT:
			Object textIconGap = UIManager.get("TabbedPane.textIconGap");
			Insets tabInsets = UIManager.getInsets("TabbedPane.tabInsets");
			UIManager.put("TabbedPane.textIconGap", new Integer(1));
			UIManager.put("TabbedPane.tabInsets", new Insets(tabInsets.left,
					tabInsets.top, tabInsets.right, tabInsets.bottom));
			JTabbedPane tabPane = new JTabbedPane(tabPlacement);
			UIManager.put("TabbedPane.textIconGap", textIconGap);
			UIManager.put("TabbedPane.tabInsets", tabInsets);
			return tabPane;
		default:
			return new JTabbedPane(tabPlacement);
		}
	}

	/**
	 * 20072007-1-5下午01:19:52 将窗口置于屏幕中间
	 * 
	 * @param f
	 */
	public static void setLocationCenter(Window f) {
		Toolkit kit = Toolkit.getDefaultToolkit();
		Dimension screenSize = kit.getScreenSize();
		int screenWidth = screenSize.width;
		int screenHeight = screenSize.height;
		f.setLocation((screenWidth - f.getBounds().width) / 2,
				(screenHeight - f.getBounds().height) / 2);
	}

	/**
	 * 切换控件状态
	 * 
	 * @param c
	 */
	public static void changeComponentVisible(Component c,boolean flag) {
		if (c == null) {
			return;
		}

		c.setVisible(flag);
	}
	/**
	 * 根据真实用户号码获取数据平台表示的数据值
	 * @param num
	 * @return
	 */
	public static long getUsernumValueByString(String num){
		long tmp = Long.parseLong(num);
		long result = (long) (tmp + Math.pow(10, num.length()));
		return result;
	}

	/**
	 * 根据输入的数据平台用户号码数值获取真实用户号码
	 * @param value
	 * @return
	 */
	public static String getUserNumByDataValue(long value){
		return String.valueOf(value).substring(1);
	}
	
	/**
	 * 获取时间描述字串，输入为时间标识的毫秒数
	 * @param time
	 * @return
	 */
	public static String getDateString(long time){
		Date d = new Date(time);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss.S");
		return sdf.format(d);
	}
	
	/**
	 * 移动一个文件
	 * @param existFileName
	 * @param newName
	 * @return
	 */
	public static boolean moveFile(String existFileName,String newName){
		File existFile = new File(existFileName);
		File newFile = new File(newName);
		boolean res = existFile.renameTo(newFile);
		if(res == false){
			logger.error("×××××××移动文件失败：" + existFileName);
		}else{
			//logger.error("×××××移动文件成功：" + existFileName);
		}
		
		return res;
	}
	
	/**
	 * 根据文件名称获取日历时间（精确到分种）
	 * @param name　符合如下格式：yyyymmddhhmmssSSS 20071009164120562
	 * @return
	 */
	public static Calendar getCalendarFromString(String name){
		String in = name.substring(0,12);
		int year = Integer.valueOf(in.substring(0,4));
		int month = Integer.valueOf(in.substring(4,6));
		int day = Integer.valueOf(in.substring(6,8));
		int hour = Integer.valueOf(in.substring(8,10));
		int min = Integer.valueOf(in.substring(10, 12));
		
		return new GregorianCalendar(year,month,day,hour,min);
	}
	
	public static Logger logger = Logger.getLogger(ComUtil.class
			.getName());

}
