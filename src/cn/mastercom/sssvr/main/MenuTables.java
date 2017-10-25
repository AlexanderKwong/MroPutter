package cn.mastercom.sssvr.main;

import java.awt.Dimension;
import java.awt.ScrollPane;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.swing.JCheckBox;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.border.EmptyBorder;

import cn.mastercom.sssvr.util.ReturnConfig;

import javax.swing.JTable;
import javax.swing.JButton;
import javax.swing.JTextField;
import javax.swing.ScrollPaneConstants;
import javax.swing.WindowConstants;

public class MenuTables extends JFrame
{

	private JScrollPane jsp;
	private JPanel contentPane;
	private JTable table;
	private static ResultSet Rs;
	private Map<String, Integer> map;
	private ScrollPane sp;
	private JButton confirmBtn;
	private MenuTables frame;
	private String text;

	public void initialize(ResultSet Rs, final String ip, final String database, final String user, final String passwd)
	{
		int Theight = 39;
		int checkbox = 39;
		int flag = 20;
		int conf = 20;
		int ds = 200;

		frame = new MenuTables();
		frame.setTitle("可供选择的数据");
		frame.setVisible(true);
		frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);

		contentPane = new JPanel();
		jsp = new JScrollPane(contentPane);
		jsp.setPreferredSize(new Dimension(20, 20));
		jsp.getViewport().add(contentPane);
		jsp.setViewportView(contentPane);
		jsp.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_ALWAYS);
		// jsp.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		jsp.setBounds(0, 0, 860, 640);

		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		// frame.setContentPane(contentPane);
		contentPane.setLayout(null);
		frame.getContentPane().add(jsp);
		map = new HashMap<String, Integer>();
		try
		{
			while (Rs.next())
			{
				String tableName = Rs.getString(1);
				final JTextField jtf = new JTextField();
				jtf.setText(tableName);
				jtf.setBounds(220, Theight, 520, 27);
				Theight = Theight + flag;
				contentPane.add(jtf);
				jtf.setColumns(10);
				final JCheckBox checkBox = new JCheckBox("选中");
				checkBox.setBounds(72, checkbox, 149, 29);
				contentPane.add(checkBox);
				checkbox = checkbox + flag;
				checkBox.addActionListener(new ActionListener()
				{
					@Override
					public void actionPerformed(ActionEvent e)
					{
						// TODO Auto-generated method stub
						if (checkBox.isSelected())
						{
							checkBox.setSelected(true);
							String text = jtf.getText();
							// System.out.println("选中的表有=" + text);
							map.put(text, 1);
						} else if (!checkBox.isSelected())
						{
							checkBox.setSelected(false);
							String text = jtf.getText();
							// System.out.println("取消选中的表有=" + text);
							map.remove(text);
						}

					}
				});

			}
		} catch (SQLException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		;
		confirmBtn = new JButton("确定");
		confirmBtn.setBounds(310, checkbox + conf, 110, 42);
		contentPane.add(confirmBtn);
		contentPane.setPreferredSize(new Dimension(850, checkbox + ds));
		frame.setBounds(500, 200, 880, 640);
		confirmBtn.addActionListener(new ActionListener()
		{
			@Override
			public void actionPerformed(ActionEvent e)
			{
				// TODO Auto-generated method stub
				System.out.println("选择表：" + map.size() + "个,具体如下：");
				String tableNames[] = new String[map.size()];
				int i = 0;
				for (String key : map.keySet())
				{
					System.out.println(key);
					tableNames[i] = key;
					i++;
				}
				new ReturnConfig();
				String path = ReturnConfig.returnconfig("conf/config_figureFix.xml", "//comm//newfigureku");
				File savePath = new File(path);
				if (savePath.exists())
				{
					File[] files = savePath.listFiles();
					if (files.length > 0)
					{
						System.out.println("指纹库路径：" + path + "下存在文件，请移走该目录下文件后，重启程序！");
						savePath = null;
					}
				} else
				{
					savePath.mkdir();
				}
				if (savePath != null)
				{
					System.out.println("汇聚后文件将存放在:" + savePath.getPath() + "文件夹下！");
				}
				if (tableNames.length > 0 && savePath != null)
				{
					CreateEnbidFigure cef = new CreateEnbidFigure(ip, database, user, passwd);
					cef.CreatFigureBySqlDB(tableNames, savePath);
				} else
				{
					System.out.println("选择路径不规范，请检查相关路径！");
				}
				frame.setVisible(false);
			}
		});
	}
}
