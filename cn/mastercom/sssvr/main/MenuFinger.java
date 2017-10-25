package cn.mastercom.sssvr.main;

import java.awt.EventQueue;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.ResultSet;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;
import cn.mastercom.sqlhp.DBHelper;
import javax.swing.JLabel;
import javax.swing.JTextField;
import javax.swing.WindowConstants;
import javax.swing.JButton;

public class MenuFinger extends JFrame
{

	private JPanel contentPane;
	private static JTextField IpName;
	private JLabel user;
	private static JTextField username;
	private JLabel password;
	private static JTextField pwd;
	private JLabel dbName;
	private static JTextField dbNames;
	private static JButton confirmbtn;
	private static JButton cancle;
	private static ResultSet rs;
	private static MenuFinger frame;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args)
	{
		EventQueue.invokeLater(new Runnable()
		{
			@Override
			public void run()
			{
				try
				{
					frame = new MenuFinger();
					frame.setTitle("数据连接");
					frame.setVisible(true);
				} catch (Exception e)
				{
					e.printStackTrace();
				}
				event();
			}
		});
	}

	/**
	 * Create the frame.
	 */
	public MenuFinger()
	{
		setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
		setBounds(600, 200, 654, 482);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPane);
		contentPane.setLayout(null);

		JLabel dbIP = new JLabel();
		dbIP.setText("数据库IP：");
		dbIP.setBounds(92, 36, 90, 39);
		contentPane.add(dbIP);

		IpName = new JTextField();
		IpName.setBounds(197, 36, 286, 45);
		contentPane.add(IpName);
		IpName.setColumns(10);
		IpName.setText("192.168.1.92");

		user = new JLabel();
		user.setText("用户名：");
		user.setBounds(92, 120, 81, 21);
		contentPane.add(user);

		username = new JTextField();
		username.setBounds(197, 105, 286, 45);
		contentPane.add(username);
		username.setColumns(10);
		username.setText("dtauser");

		password = new JLabel();
		password.setText("密码：");
		password.setBounds(92, 188, 81, 21);
		contentPane.add(password);

		pwd = new JTextField();
		pwd.setBounds(197, 185, 286, 45);
		contentPane.add(pwd);
		pwd.setColumns(10);
		pwd.setText("dtauser");

		dbName = new JLabel();
		dbName.setText("数据库名：");
		dbName.setBounds(92, 277, 108, 21);
		contentPane.add(dbName);

		dbNames = new JTextField();
		dbNames.setBounds(197, 269, 286, 45);
		contentPane.add(dbNames);
		dbNames.setColumns(10);
		dbNames.setText("RAMS_shenzhen");

		confirmbtn = new JButton("确认连接");
		confirmbtn.setBounds(141, 347, 123, 48);
		contentPane.add(confirmbtn);

		cancle = new JButton("取消");
		cancle.setBounds(360, 346, 123, 50);
		contentPane.add(cancle);
	}

	public static void event()
	{
		confirmbtn.addActionListener(new ActionListener()
		{
			@Override
			public void actionPerformed(ActionEvent e)
			{
				// TODO Auto-generated method stub
				String DBIP = IpName.getText();
				String PWD = pwd.getText();
				String USERNAME = username.getText();
				String DBNAME = dbNames.getText();
				// System.out.println(DBIP+"---"+PWD+"---"+USERNAME+"---"+DBNAME);
				DBHelper dh = new DBHelper();
				dh.setDBValue(USERNAME, PWD, DBIP, DBNAME);
				String sSQL = "select name from sysobjects where xtype='U' and name like '%tb_simu_result_%'";
				rs = dh.GetResultSet(sSQL, null);
				if (rs != null)
				{
					MenuTables mt = new MenuTables();
					mt.initialize(rs, DBIP, DBNAME, USERNAME, PWD);
					frame.setVisible(false);
				}
			}
		});
		cancle.addActionListener(new ActionListener()
		{
			@Override
			public void actionPerformed(ActionEvent e)
			{
				// TODO Auto-generated method stub
				System.out.println("关闭");
				frame.setVisible(false);
			}
		});

	}

}
