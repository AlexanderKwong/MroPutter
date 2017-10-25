package cn.mastercom.sssvr.main;

import javax.swing.*;
import javax.swing.plaf.FontUIResource;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import cn.mastercom.simu.adjust.CreatOttCellGrid;
import cn.mastercom.sssvr.util.CalendarEx;
import cn.mastercom.sssvr.util.ComUtil;
import cn.mastercom.sssvr.util.FileOpt;
import cn.mastercom.sssvr.util.HdfsExplorer;
import cn.mastercom.sssvr.util.ResourcesDepository;
import cn.mastercom.sssvr.util.ReturnConfig;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;

public class MainSvr extends JFrame {

	private static final long serialVersionUID = 1L;
	public static boolean bExitFlag = false;

	public JTextField jLabel = null;
	public JTextField txCapstatus = null;
	public JTextField txCapMode = null;

	public JToolBar filterToolbar = null;
	public String fileName = null;
	private final String metal = "javax.swing.plaf.metal.MetalLookAndFeel";
	private final String Office2003 = "org.fife.plaf.Office2003.Office2003LookAndFeel";
	private final String OfficeXP = "org.fife.plaf.OfficeXP.OfficeXPLookAndFeel";
	private final String VisualStudio2005 = "org.fife.plaf.VisualStudio2005.VisualStudio2005LookAndFeel";
	private final String windows = "com.sun.java.swing.plaf.windows.WindowsLookAndFeel";
	private String currentLookAndFeel = windows;
	private final ButtonGroup lafMenuGroup = new ButtonGroup();
	public JFileChooser fc = null;
	public ComboBoxEditor cbEditor = null;

	private JMenuBar menuBar;
	public JButton buttonFoward;
	public JButton buttonBack;
	public JButton buttonBegin;
	public JButton buttonEnd;
	public JButton buttonOpen;
	public JButton buttonSave;
	public JButton buttonClose;
	public JButton buttonRecv;
	public JButton buttonReload;
	public JCheckBox autoScr;
	public JCheckBox dispPause;
	private JLabel jLabel2;
	private DateChooser jDateChooser;
	JCheckBox samButton;
	JCheckBox eventButton;
	JCheckBox gridButton;
	JCheckBox location23GButton;
	// private WholeNumberField jTextFieldMaxEvent;
	private JPanel jContentPane;

	private DateChooser getJTextField() {
		if (jDateChooser == null)
			jDateChooser = new DateChooser();
		return jDateChooser;
	}

	private JPanel getJContentPane() {
		if (jContentPane == null) {
			GridBagConstraints gridBagConstraints5 = new GridBagConstraints();
			gridBagConstraints5.gridx = 1;
			gridBagConstraints5.gridy = 5;
			GridBagConstraints gridBagConstraints4 = new GridBagConstraints();
			gridBagConstraints4.gridx = 1;
			gridBagConstraints4.gridy = 3;
			GridBagConstraints gridBagConstraints3 = new GridBagConstraints();
			// gridBagConstraints3.fill = 3;
			gridBagConstraints3.gridy = 1;
			// gridBagConstraints3.weightx = 1.0D;
			gridBagConstraints3.gridx = 1;
			GridBagConstraints gridBagConstraints2 = new GridBagConstraints();
			gridBagConstraints2.gridx = 0;
			gridBagConstraints2.gridy = 1;
			// jLabel1 = new JLabel();
			// jLabel1.setText("\u6700\u5927\u4E8B\u4EF6\u6570\u76EE");
			GridBagConstraints gridBagConstraints1 = new GridBagConstraints();
			// gridBagConstraints1.fill = 3;
			gridBagConstraints1.gridy = 0;
			// gridBagConstraints1.weightx = 1.0D;
			gridBagConstraints1.gridx = 1;
			GridBagConstraints gridBagConstraints = new GridBagConstraints();
			gridBagConstraints.gridx = 0;
			gridBagConstraints.gridy = 0;
			jLabel2 = new JLabel();
			jLabel2.setText("请选择要处理的日期：");
			jContentPane = new JPanel();
			jContentPane.setLayout(new GridBagLayout());
			jContentPane.setPreferredSize(new Dimension(80, 88));
			jContentPane.add(jLabel2, gridBagConstraints);
			jContentPane.add(getJTextField(), gridBagConstraints1);

			samButton = new JCheckBox("Sample");
			samButton.setMnemonic(KeyEvent.VK_C);
			samButton.setSelected(true);

			eventButton = new JCheckBox("Event");
			eventButton.setMnemonic(KeyEvent.VK_G);
			eventButton.setSelected(true);

			gridButton = new JCheckBox("Grid");
			gridButton.setMnemonic(KeyEvent.VK_H);
			gridButton.setSelected(true);

			location23GButton = new JCheckBox("23G");
			location23GButton.setMnemonic(KeyEvent.VK_T);
			location23GButton.setSelected(false);

			jContentPane.add(samButton, gridBagConstraints2);
			jContentPane.add(eventButton, gridBagConstraints3);
			jContentPane.add(gridButton, gridBagConstraints4);
			jContentPane.add(location23GButton, gridBagConstraints5);
		}

		return jContentPane;
	}

	/** ********************************************************************* */
	/**
	 * Creates a JRadioButtonMenuItem for the Look and Feel menu
	 */
	// 改变外观
	private JMenuItem createLafMenuItem(final String label, final int mnemonic, final String laf,
			final ButtonGroup lafMenuGroup) {
		final JMenuItem mi = (new JRadioButtonMenuItem(label));
		lafMenuGroup.add(mi);
		mi.setMnemonic(mnemonic);
		// mi.getAccessibleContext().setAccessibleDescription(
		// accessibleDescription);
		mi.addActionListener(new ChangeLookAndFeelAction(this, laf));
		mi.setEnabled(isAvailableLookAndFeel(laf));
		return mi;
	}

	protected boolean isAvailableLookAndFeel(final String laf) {
		try {
			final Class<?> lnfClass = Class.forName(laf);
			final LookAndFeel newLAF = (LookAndFeel) (lnfClass.newInstance());
			return newLAF.isSupportedLookAndFeel();
		} catch (final Exception e) { // If ANYTHING weird happens, return false
			return false;
		}
	}

	public void setLookAndFeel(final String laf) {
		if (currentLookAndFeel != laf) {
			currentLookAndFeel = laf;
			updateLookAndFeel();
		}
	}

	public void updateLookAndFeel() {
		try {
			UIManager.setLookAndFeel(currentLookAndFeel);
			// 是否使用粗体
			// UIManager.put("swing.boldMetal", Boolean.FALSE);
			//
			//
			// UIDefaults UId = UIManager.getDefaults();
			// FontUIResource f = new FontUIResource("宋体", Font.PLAIN, 15);
			// UId.put("Button.font", f);
			// UId.put("TextField.font", f);
			// UId.put("Label.font", f);
			// UId.put("TextArea.font", f);
			// UId.put("TableHeader.font", f);
			// UId.put("Table.font", f);
			// UId.put("Tree.font", f);
			// UId.put("TabbedPane.font", f);
			// UId.put("Menu.font", f);
			// UId.put("ComboBox.font", f);

			SwingUtilities.updateComponentTreeUI(this);

			toolBar.removeAll();
			addButtons(toolBar);
			final File path = fc.getCurrentDirectory();
			fc = new JFileChooser();
			fc.setCurrentDirectory(path);
			// mainView.getProgressBar(this);
		} catch (final Exception ex) {
			System.out.println("Failed loading L&F: " + currentLookAndFeel);
			ex.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(final String[] args) {
		init();
		final MainSvr thisClass = new MainSvr();
		thisClass.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		thisClass.setTitle("文件搬移程序：" + System.getProperty("user.dir"));
		thisClass.setVisible(true);
	}

	/**
	 * This is the default constructor
	 */
	public MainSvr() {
		super();
		// System.out.println(Long.MAX_VALUE);

		initialize();
		addWindowListener(new WindowAdapter() {
			@Override
			public void windowClosing(WindowEvent e) {
				exit();
			}
		});
		log.info("init...");
		new CreatOttCellGrid().start();

		if (ReturnConfig.returnconfig("conf/config.xml", "//comm//DEAL_SDZT").equals("1")) {// 山东展厅
			FileMoverShanDong fm = new FileMoverShanDong();
			fm.start();
			GridStatistics gridStatistics = new GridStatistics();
			gridStatistics.start();// 启动栅格统计功能
		} else if (ReturnConfig.returnconfig("conf/config.xml", "//comm//DEAL_SHXDR").equals("1")) {// 上海XDR上传
			FileMoverShangHai fm = new FileMoverShangHai();
			fm.start();
		} else if (ReturnConfig.returnconfig("conf/config.xml", "//comm//DEAL_NingXiaXDR").equals("1")) {// 宁夏xdr上传
			FileMoverNingXia fm = new FileMoverNingXia();
			fm.start();
		} else if (ReturnConfig.returnconfig("conf/config.xml", "//comm//DEAL_BeiJingXDR").equals("1")) {
			FileMoverBeiJing fileMover = new FileMoverBeiJing();
			fileMover.start();
		} else if (ReturnConfig.returnconfig("conf/config.xml", "//comm//DEAL_ShanXiMr").equals("1")) {
			FileMover.Init();// 启动ftp搬移程序
			MroDecoder md = new MroDecoder(2);
			md.Init();
			md.start();
		} else if (ReturnConfig.returnconfig("conf/config.xml", "//comm//DEAL_TianJinMr").equals("1")) {
			FileMover.Init();// 启动ftp搬移程序
			MroDecoder md = new MroDecoder(3);
			md.Init();
			md.start();
		} else {
			if (ReturnConfig.returnconfig("conf/config.xml", "//comm//MrFilePath").length() > 0) {// MR解码和上传
				FileMover.Init();// 启动搬移程序
				MroDecoder md = new MroDecoder(0);
				md.Init();
				md.start();

				MroDecoder mrhdfs = new MroDecoder(1);
				mrhdfs.Init();
				mrhdfs.start();
			} else if (ReturnConfig.returnconfig("conf/config.xml", "//comm//DEAL_FIGUREFIX").equals("1")) {// 指纹定位
				FingerprintFixMr figureFix = new FingerprintFixMr();
				figureFix.start();// 启动指纹定位功能
				GridStatistics gridStatistics = new GridStatistics();
				gridStatistics.start();// 启动栅格统计功能
			} else if (ReturnConfig.returnconfig("conf/config.xml", "//comm//DEAL_BCP").equals("1")) {// BCP文件下载
				FileMover.Init();// 启动搬移程序
			}
		}

	}

	public void exit() {
		Object[] options = { "确定", "取消" };
		JOptionPane pane2 = new JOptionPane("真想退出吗?", JOptionPane.QUESTION_MESSAGE, JOptionPane.YES_NO_OPTION, null,
				options, options[1]);
		JDialog dialog = pane2.createDialog(this, "警告");
		dialog.setVisible(true);
		Object selectedValue = pane2.getValue();
		if (selectedValue == null || selectedValue == options[1]) {
			setDefaultCloseOperation(WindowConstants.DO_NOTHING_ON_CLOSE);

		} else if (selectedValue == options[0]) {
			MainSvr.bExitFlag = true;
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			setDefaultCloseOperation(EXIT_ON_CLOSE);
		}
	}

	// 添加菜单栏
	protected JMenuBar addMenuBar() {
		final JMenuBar menuBar = new JMenuBar();
		menuBar.doLayout();
		JMenu menu;
		JMenuItem menuItem;
		// ***********************
		// Build the first menu.
		menu = new JMenu("文件(F)");
		menu.setMnemonic(KeyEvent.VK_F);
		menuBar.add(menu);
		// 打开
		menuItem = new JMenuItem("HDFS 管理器", ResourcesDepository.getIcon("images/open.gif"));
		menuItem.setMnemonic(KeyEvent.VK_O);
		menuItem.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(final ActionEvent e) {
				HdfsExplorer disk = new HdfsExplorer(false);
				try {
					disk.treeMake();
				} catch (UnknownHostException e1) {
					e1.printStackTrace();
				}
			}

		});
		menu.add(menuItem);

		// 关闭
		menuItem = new JMenuItem("关闭", ResourcesDepository.getIcon("images/close.gif"));
		menuItem.setMnemonic(KeyEvent.VK_C);
		menuItem.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(final ActionEvent e) {
				// mainView.resetAll();
			}
		});
		menu.add(menuItem);
		// 退出系统
		menuItem = new JMenuItem("退出", KeyEvent.VK_X);
		menuItem.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(final ActionEvent e) {
				System.exit(0);
			}
		});
		menu.add(menuItem);

		// Build the first menu.
		menu = new JMenu("视图(V)");
		menu.setMnemonic(KeyEvent.VK_V);
		menuBar.add(menu);

		menuItem = new JCheckBoxMenuItem("工具栏");
		menuItem.setMnemonic(KeyEvent.VK_T);
		menuItem.setSelected(true);
		menuItem.addActionListener(new ActionListener() {
			boolean flag = false;

			@Override
			public void actionPerformed(final ActionEvent e) {
				toolBar.setVisible(flag);
				flag = !flag;
			}
		});
		menu.add(menuItem);

		menuItem = new JCheckBoxMenuItem("过滤工具栏");
		menuItem.setMnemonic(KeyEvent.VK_F);
		menuItem.setSelected(true);
		menuItem.addActionListener(new ActionListener() {

			boolean flag = false;

			@Override
			public void actionPerformed(final ActionEvent e) {
				filterToolbar.setVisible(flag);
				flag = !flag;
			}
		});
		menu.add(menuItem);

		// ***********************
		menu = new JMenu("观感(L)");
		menu.setMnemonic(KeyEvent.VK_L);
		menuBar.add(menu);
		menuItem = createLafMenuItem("Metal", KeyEvent.VK_M, metal, lafMenuGroup);

		if (currentLookAndFeel.equals(metal)) {
			menuItem.setSelected(true); // this is the default l&f
		}
		menu.add(menuItem);
		menuItem = createLafMenuItem("Windows", KeyEvent.VK_W, windows, lafMenuGroup);
		if (currentLookAndFeel.equals(windows)) {
			menuItem.setSelected(true); // this is the default l&f
		}
		menu.add(menuItem);

		menuItem = createLafMenuItem("Office XP", KeyEvent.VK_X, OfficeXP, lafMenuGroup);
		if (currentLookAndFeel.equals(OfficeXP)) {
			menuItem.setSelected(true); // this is the default l&f
		}
		menu.add(menuItem);

		menuItem = createLafMenuItem("Office 2003", KeyEvent.VK_O, Office2003, lafMenuGroup);
		if (currentLookAndFeel.equals(Office2003)) {
			menuItem.setSelected(true); // this is the default l&f
		}
		menu.add(menuItem);

		menuItem = createLafMenuItem("Visual Studio 2005", KeyEvent.VK_V, VisualStudio2005, lafMenuGroup);
		if (currentLookAndFeel.equals(VisualStudio2005)) {
			menuItem.setSelected(true); // this is the default l&f
		}
		menu.add(menuItem);

		// Build the first menu.
		menu = new JMenu("入库文件下载(D)");
		menu.setMnemonic(KeyEvent.VK_D);
		menuBar.add(menu);

		menuItem = new JMenuItem("入库文件下载");
		menuItem.setMnemonic(66);
		menuItem.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {

				if (bMoveSampleEventRunning) {
					return;
				}

				Object options[] = { "\u786E\u5B9A", "\u53D6\u6D88" };

				JPanel pane = getJContentPane();
				int answer = JOptionPane.showOptionDialog(null, pane, "日期选择", 0, -1, null, options, options[1]);
				if (answer == 0) {
					MoveSampleEvent mse = new MoveSampleEvent();
					mse.start();
				}
			}
		});
		menu.add(menuItem);

		// 工参校准
		menu = new JMenu("指纹库(Z)");
		menu.setMnemonic(KeyEvent.VK_Z);
		menuBar.add(menu);

		// 根据解码后的数据，利用主小区的eci进行关联，修正公参中的频点和pci（目前只能用我们自己解码的数据进行校正）
		menuItem = new JMenuItem("工参校准");
		menuItem.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {
				if (bMoveSampleEventRunning) {
					return;
				}
				JFileChooser fileChooser = new JFileChooser();
				fileChooser.setDialogTitle("请选择数据文件夹");
				fileChooser.setApproveButtonText("确定");
				fileChooser.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
				fileChooser.setMultiSelectionEnabled(true);
				int result = fileChooser.showOpenDialog(null);
				File localPath[] = fileChooser.getSelectedFiles();

				if (localPath.length > 0) {
					System.out.println("用以下文件的数据进行工参校正：");
					for (File s : localPath) {
						System.out.println(s.getPath());
					}
				}
				fileChooser = new JFileChooser();
				fileChooser.setDialogTitle("请选择要矫正工参文件");
				fileChooser.setApproveButtonText("确定");
				fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
				result = fileChooser.showOpenDialog(null);
				File CommonFile = fileChooser.getSelectedFile();
				if (CommonFile != null) {
					System.out.println("要校正的工参文件位于:" + CommonFile.getPath());
				}
				if (localPath.length > 0 && CommonFile != null) {
					AdjustGongCan adjustgongcan = new AdjustGongCan(CommonFile, localPath);
					try {
						adjustgongcan.adjustGongcan();
						System.out.println("工参修正完毕");
					} catch (Exception e1) {
						System.out.println("工参修正失败");
						e1.printStackTrace();
					}
				} else {
					System.out.println("请重新选择校准工参文件目录和工参文件");
				}
			}
		});
		menu.add(menuItem);

		menuItem = new JMenuItem("生成Cell_Grid");
		// 根据sample数据生成cell_grid，因为sample中没有level的信息，
		// 所以生成的cell_grid中level为-1，rsrp为相同key值的cellgrid的和的平均数
		menuItem.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {

				if (bMoveSampleEventRunning) {
					return;
				}
				JFileChooser fileChooser = new JFileChooser();
				fileChooser.setDialogTitle("请选择数据文件");
				fileChooser.setApproveButtonText("确定");
				fileChooser.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
				fileChooser.setMultiSelectionEnabled(true);
				int result = fileChooser.showOpenDialog(null);
				File localPath[] = fileChooser.getSelectedFiles();
				ArrayList<File> localFiles = new ArrayList<File>();
				if (localPath.length > 0) {
					System.out.println("用以下文件的数据生成Cell_Grid文件：");
					for (File s : localPath) {
						if (!s.isDirectory()) {
							localFiles.add(s);
						} else {
							FileOpt.getFiles(s, localFiles);
						}
					}
					for (File s : localFiles) {
						System.out.println(s.getPath());
					}
				}
				fileChooser = new JFileChooser();
				fileChooser.setDialogTitle("请选择新生成Cell_Grid文件存放位置");
				fileChooser.setApproveButtonText("确定");
				fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
				result = fileChooser.showOpenDialog(null);
				File savePath = fileChooser.getSelectedFile();
				if (savePath != null) {
					System.out.println("新生成的Cell_Grid文件将存放在：");
					System.out.println(savePath.getPath());
				}
				if (localPath.length > 0 && savePath != null) {
					System.out.println("正在生成Cell_Grid文件，请稍等！");
					CreatCellGridFile.creatCellGridFile(localFiles, savePath);
					System.out.println("修正指纹库成功！新生成Cell_Grid文件位于" + savePath.getPath());
				} else {
					System.out.println("选择路径不合法，请重新选择！");
				}

			}
		});
		menu.add(menuItem);

		// 指纹库校准，根据提供的cellgrid（标准指纹库），校准 目标指纹库的rsrp,（eci/经纬度/level）

		menuItem = new JMenuItem("指纹库校准");
		menuItem.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {

				if (bMoveSampleEventRunning) {
					return;
				}
				JFileChooser fileChooser = new JFileChooser();
				fileChooser.setDialogTitle("请选择数据文件");
				fileChooser.setApproveButtonText("确定");
				fileChooser.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
				fileChooser.setMultiSelectionEnabled(true);
				int result = fileChooser.showOpenDialog(null);
				File localPath[] = fileChooser.getSelectedFiles();
				ArrayList<File> localFiles = new ArrayList<File>();
				if (localPath.length > 0) {
					System.out.println("用以下文件的数据进行指纹库校正：");
					for (File s : localPath) {
						if (!s.isDirectory()) {
							localFiles.add(s);
						} else {
							FileOpt.getFiles(s, localFiles);
						}
					}
					for (File s : localFiles) {
						System.out.println(s.getPath());
					}
				}
				fileChooser = new JFileChooser();
				fileChooser.setDialogTitle("请选择要矫正指纹库文件");
				fileChooser.setApproveButtonText("确定");
				fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
				fileChooser.setMultiSelectionEnabled(true);
				result = fileChooser.showOpenDialog(null);
				File SimuFiles[] = fileChooser.getSelectedFiles();
				System.out.println("待校准的指纹库文件位于：");
				for (int i = 0; i < SimuFiles.length; i++) {
					System.out.println(SimuFiles[i].getPath());
				}

				fileChooser = new JFileChooser();
				fileChooser.setDialogTitle("请选择新生成指纹文件存放位置");
				fileChooser.setApproveButtonText("确定");
				fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
				result = fileChooser.showOpenDialog(null);
				File savePath = fileChooser.getSelectedFile();
				if (savePath != null) {
					System.out.println("新生成的指纹库文件将存放在：");
					System.out.println(savePath.getPath());
				}
				if (localPath.length > 0 && SimuFiles.length > 0 && savePath != null) {
					System.out.println("正在校准指纹库，请稍等！");
					AdjustSimu.adjustSimu(localFiles, SimuFiles, savePath);
					System.out.println("修正指纹库成功！新生成指纹库文件位于" + savePath.getPath());
				} else {
					System.out.println("选择路径不合法，请重新选择！");
				}
			}
		});
		menu.add(menuItem);

		menuItem = new JMenuItem("指纹库去重");
		menuItem.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {

				if (bMoveSampleEventRunning) {
					return;
				}
				JFileChooser fileChooser = new JFileChooser();
				fileChooser.setDialogTitle("请选择10*10指纹库文件");
				fileChooser.setApproveButtonText("确定");
				fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
				fileChooser.setMultiSelectionEnabled(true);
				int result = fileChooser.showOpenDialog(null);
				File localPath[] = fileChooser.getSelectedFiles();
				for (int i = 0; i < localPath.length; i++) {
					System.out.println("选择的10*10的指纹库位于：" + localPath[i].getPath());
				}
				fileChooser = new JFileChooser();
				fileChooser.setDialogTitle("请选择40*40指纹库文件");
				fileChooser.setApproveButtonText("确定");
				fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
				result = fileChooser.showOpenDialog(null);
				File SimuFile = fileChooser.getSelectedFile();
				System.out.println("选择的40*40的指纹库位于：" + SimuFile.getPath());
				if (localPath.length > 0 && SimuFile != null) {
					// FingerFilter ff = new FingerFilter(localPath, SimuFile);
					// ff.compareFinger();
				} else {
					System.out.println("选择路径不合法，请重新选择！");
				}
			}
		});
		menu.add(menuItem);

		menuItem = new JMenuItem("Hadoop指纹生成");
		menuItem.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (bMoveSampleEventRunning) {
					return;
				}
				JFileChooser fileChooser = new JFileChooser();
				fileChooser.setDialogTitle("请选择指纹库文件");
				fileChooser.setApproveButtonText("确定");
				fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
				fileChooser.setMultiSelectionEnabled(true);
				int result = fileChooser.showOpenDialog(null);
				File figureFile[] = fileChooser.getSelectedFiles();
				if (figureFile.length > 0) {
					System.out.println("选择指纹库文件如下：");
					for (File s : figureFile) {
						System.out.println(s.getPath());
					}
				}
				fileChooser = new JFileChooser();
				fileChooser.setDialogTitle("请选择工参文件");
				fileChooser.setApproveButtonText("确定");
				fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
				result = fileChooser.showOpenDialog(null);
				File CommonFile = fileChooser.getSelectedFile();
				if (CommonFile != null) {
					System.out.println("工参文件位于:" + CommonFile.getPath());
				}
				fileChooser = new JFileChooser();
				fileChooser.setDialogTitle("请选择生成hadoop指纹库文件存放路径");
				fileChooser.setApproveButtonText("确定");
				fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
				result = fileChooser.showOpenDialog(null);
				File savePath = fileChooser.getSelectedFile();
				if (savePath != null) {
					System.out.println("生成的指纹文件将存放在:" + savePath.getPath() + "文件夹下！");
				}
				if (figureFile.length > 0 && CommonFile != null && savePath != null) {
					MergersFigureKu mgf = new MergersFigureKu(figureFile, CommonFile, savePath);
					mgf.createFigureForHadoop();
				} else {
					System.out.println("请重新选择指纹库文件、工参文件、保存路径");
				}
			}
		});

		menu.add(menuItem);

		menuItem = new JMenuItem("指纹汇聚");
		menuItem.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (bMoveSampleEventRunning) {
					return;
				}
				JFileChooser fileChooser = new JFileChooser();
				fileChooser.setDialogTitle("请选择指纹库文件");
				fileChooser.setApproveButtonText("确定");
				fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
				fileChooser.setMultiSelectionEnabled(true);
				int result = fileChooser.showOpenDialog(null);
				File figureFile[] = fileChooser.getSelectedFiles();
				if (figureFile.length > 0) {
					System.out.println("选择指纹库文件如下：");
					for (File s : figureFile) {
						System.out.println(s.getPath());
					}
				}
				fileChooser = new JFileChooser();
				fileChooser.setDialogTitle("请选择汇聚后文件存放路径");
				fileChooser.setApproveButtonText("确定");
				fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
				result = fileChooser.showOpenDialog(null);
				File savePath = fileChooser.getSelectedFile();
				if (savePath != null) {
					System.out.println("汇聚后文件将存放在:" + savePath.getPath() + "文件夹下！");
				}
				if (figureFile.length > 0 && savePath != null) {
					MergeFigure.mergeFigure(figureFile, savePath);
				} else {
					System.out.println("选择路径不规范，请重新选择相关路径！");
				}
			}
		});
		menu.add(menuItem);

		menuItem = new JMenuItem("enbid构建指纹库");
		menuItem.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (bMoveSampleEventRunning) {
					return;
				}
				// MenuFinger.main(null);
			}
		});
		menu.add(menuItem);

		menuItem = new JMenuItem("enbid指纹(读文件)");
		menuItem.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (bMoveSampleEventRunning) {
					return;
				}
				JFileChooser fileChooser = new JFileChooser();
				fileChooser.setDialogTitle("请选择指纹库文件");
				fileChooser.setApproveButtonText("确定");
				fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
				fileChooser.setMultiSelectionEnabled(true);
				int result = fileChooser.showOpenDialog(null);
				File figureFile[] = fileChooser.getSelectedFiles();
				String figurefiles[] = new String[figureFile.length];
				if (figureFile.length > 0) {
					System.out.println("选择指纹库文件如下：");
					for (int i = 0; i < figureFile.length; i++) {
						System.out.println(figureFile[i].getPath());
						figurefiles[i] = figureFile[i].getPath();
					}
				}
				String path = new ReturnConfig().returnconfig("conf/config_figureFix.xml", "//comm//newfigureku");
				File savePath = new File(path);
				if (savePath.exists()) {
					File[] files = savePath.listFiles();
					if (files.length > 0) {
						System.out.println("指纹库路径：" + path + "下存在文件，请移走该目录下文件后，重启程序！");
						savePath = null;
					}
				} else {
					savePath.mkdir();
				}
				if (savePath != null) {
					System.out.println("汇聚后文件将存放在:" + savePath.getPath() + "文件夹下！");
				}
				if (figureFile.length > 0 && savePath != null) {
					CreateEnbidFigure.CreatFigureByFile(figurefiles, savePath.getPath());
				} else {
					System.out.println("选择路径不规范，请检查相关路径！");
				}
			}
		});
		menu.add(menuItem);

		menu = new JMenu("栅格统计(S)");
		menu.setMnemonic(KeyEvent.VK_S);
		menuBar.add(menu);

		menuItem = new JMenuItem("栅格统计");
		menuItem.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				GridStatistics gridStatistics = new GridStatistics();
				gridStatistics.start();// 启动栅格统计功能
			}
		});
		menu.add(menuItem);

		return menuBar;

	}

	protected void addButtons(final JToolBar toolBar) {
		JButton button = null;

		buttonBegin = new JButton("采集", ResourcesDepository.getIcon("images/niccard.gif"));
		buttonBegin.setToolTipText("开始采集");
		buttonBegin.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(final ActionEvent e) {

			}
		});
		toolBar.add(buttonBegin);

		buttonRecv = new JButton("接收", ResourcesDepository.getIcon("images/startcap.gif"));
		buttonRecv.setToolTipText("接收数据");
		buttonRecv.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(final ActionEvent e) {

			}
		});
		toolBar.add(buttonRecv);

		buttonEnd = new JButton("停止", ResourcesDepository.getIcon("images/endcap.gif"));
		buttonEnd.setToolTipText("打开数据文件");
		buttonEnd.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(final ActionEvent e) {

			}
		});
		toolBar.add(buttonEnd);

		toolBar.addSeparator();

		buttonOpen = new JButton("打开", ResourcesDepository.getIcon("images/open.gif"));
		buttonOpen.setToolTipText("打开数据文件");
		buttonOpen.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(final ActionEvent e) {
				// buttonEnd.setEnabled(Boolean.FALSE);
				// buttonBegin.setEnabled(Boolean.FALSE);
				// openDataFile();
			}
		});
		toolBar.add(buttonOpen);

		buttonClose = new JButton("关闭", ResourcesDepository.getIcon("images/close.gif"));
		buttonClose.setToolTipText("关闭当前文件");
		buttonClose.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(final ActionEvent e) {
				txCapMode.setText("");
				txCapstatus.setText("");
				// mainView.resetAll();
			}
		});
		toolBar.add(buttonClose);

		buttonReload = new JButton("重载", ResourcesDepository.getIcon("images/refresh.gif"));
		buttonReload.setToolTipText("重新打开当前文件");
		buttonReload.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(final ActionEvent e) {

			}
		});
		toolBar.add(buttonReload);

		buttonSave = new JButton("保存", ResourcesDepository.getIcon("images/save-big.gif"));
		buttonSave.setToolTipText("保存信令到文件");
		buttonSave.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(final ActionEvent e) {

			}
		});
		toolBar.add(buttonSave);
		toolBar.addSeparator();

		buttonBack = new JButton("后退", ResourcesDepository.getIcon("images/left.gif"));
		buttonBack.setToolTipText("显示上一条信令");
		buttonBack.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(final ActionEvent e) {

			}
		});
		toolBar.add(buttonBack);
		buttonBack.setEnabled(Boolean.FALSE);

		buttonFoward = new JButton("前进", ResourcesDepository.getIcon("images/right.gif"));
		// buttonFoward.setVerticalTextPosition(SwingConstants.BOTTOM);
		// buttonFoward.setHorizontalTextPosition(SwingConstants.CENTER);
		buttonFoward.setToolTipText("显示下一条信令");
		buttonFoward.setEnabled(Boolean.FALSE);
		buttonFoward.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(final ActionEvent e) {

			}
		});
		toolBar.add(buttonFoward);

		button = new JButton("指定", ResourcesDepository.getIcon("images/jump_to.gif"));
		// button.setVerticalTextPosition(SwingConstants.BOTTOM);
		// button.setHorizontalTextPosition(SwingConstants.CENTER);
		button.setToolTipText("显示指定序号信令");
		button.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(final ActionEvent e) {

			}
		});
		toolBar.add(button);

		button = new JButton("第一", ResourcesDepository.getIcon("images/top.gif"));
		button.setToolTipText("显示第一条信令");
		// button.setVerticalTextPosition(SwingConstants.BOTTOM);
		// button.setHorizontalTextPosition(SwingConstants.CENTER);
		button.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(final ActionEvent e) {

			}
		});
		toolBar.add(button);

		button = new JButton("最后", ResourcesDepository.getIcon("images/bottom.gif"));
		// button.setVerticalTextPosition(SwingConstants.BOTTOM);
		// button.setHorizontalTextPosition(SwingConstants.CENTER);
		button.setToolTipText("显示最后一条信令");
		button.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(final ActionEvent e) {

			}

		});
		toolBar.add(button);

		autoScr = new JCheckBox("滚动");
		autoScr.setToolTipText("自动滚动显示");
		autoScr.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(final ActionEvent e) {

			}
		});
		toolBar.add(autoScr);

		dispPause = new JCheckBox("暂停");
		dispPause.setToolTipText("暂停显示");
		dispPause.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(final ActionEvent e) {

			}
		});
		toolBar.add(dispPause);
	}

	JLabel filterLb = null;
	JButton applyBt = null;
	JButton clearBt = null;
	JToolBar toolBar = null;

	/**
	 * 
	 */
	public void updateButtonState() {

	}

	private void initialize() {
		try {
			UIManager.setLookAndFeel(currentLookAndFeel);
			// 是否使用粗体
			UIManager.put("swing.boldMetal", Boolean.FALSE);

			UIDefaults UId = UIManager.getDefaults();
			FontUIResource font = new FontUIResource("宋体", Font.PLAIN, 12);
			UId.put("Button.font", font);
			UId.put("TextField.font", font);
			UId.put("Label.font", font);
			UId.put("TextArea.font", font);
			UId.put("TableHeader.font", font);
			UId.put("Table.font", font);
			UId.put("Tree.font", font);
			UId.put("TabbedPane.font", font);
			UId.put("Menu.font", font);
			UId.put("ComboBox.font", font);

			SwingUtilities.updateComponentTreeUI(this);
		} catch (final Exception e) {
			System.err.println("Oops!  Something went wrong!");
		}
		final Image topicon = ResourcesDepository.getImage("images/logo.gif");
		setIconImage(topicon);
		fc = new JFileChooser();

		// 添加菜单栏
		menuBar = addMenuBar();
		setJMenuBar(menuBar);

		// Create the toolbar.
		toolBar = new JToolBar();
		addButtons(toolBar);

		filterToolbar = new JToolBar();
		applyBt = new JButton("应用", ResourcesDepository.getIcon("images/apply-16.gif"));

		applyBt.addActionListener(new java.awt.event.ActionListener() {

			@Override
			public void actionPerformed(final java.awt.event.ActionEvent e) {
				// applyFilter();
			}

			/**
			* 
			*/

		});

		clearBt = new JButton("清除", ResourcesDepository.getIcon("images/delete.gif"));
		clearBt.addActionListener(new java.awt.event.ActionListener() {

			@Override
			public void actionPerformed(final java.awt.event.ActionEvent e) {

			}
		});

		filterLb = new JLabel("信令过滤 ");

		final JPanel toolPanel = new JPanel();
		toolPanel.setLayout(new BorderLayout());
		// toolBar.setPreferredSize(new Dimension(800,25));
		toolPanel.add(toolBar, "West");
		toolPanel.add(filterToolbar/* ,"East" */);

		final JPanel panel = new JPanel();
		panel.setLayout(new BorderLayout());
		// panel.add(filterToolbar, "North");

		// panel.add(getSplPane());

		// JPanel statusPanel = createStatusPane();

		// panel.add(statusPanel, "South");
		panel.add(getMyContentPane());
		setLayout(new BorderLayout());
		// this.add(toolPanel, "North");
		this.add(panel);
		setExtendedState(Frame.MAXIMIZED_BOTH);
		/*
		 * jLabel.addMouseListener(new java.awt.event.MouseAdapter() {
		 * 
		 * @Override public void mouseClicked(final java.awt.event.MouseEvent e)
		 * {
		 * 
		 * } });
		 */
		// getSplPane().helpLabel = jLabel;

		setVisible(true);
	}

	/**
	 * 
	 */
	@SuppressWarnings("unused")
	private JPanel createStatusPane() {
		final JPanel panel = new JPanel();

		panel.setLayout(new GridBagLayout());

		txCapstatus = new JTextField("");
		txCapstatus.setPreferredSize(new Dimension(300, 25));
		txCapstatus.setEditable(false);
		txCapstatus.setBorder(BorderFactory.createLoweredBevelBorder());

		jLabel = new JTextField("");
		jLabel.setPreferredSize(new Dimension(400, 25));
		jLabel.setEditable(false);
		jLabel.setBorder(BorderFactory.createLoweredBevelBorder());

		txCapMode = new JTextField("");
		txCapMode.setPreferredSize(new Dimension(400, 25));
		txCapMode.setEditable(false);
		txCapMode.setBorder(BorderFactory.createLoweredBevelBorder());

		return panel;

	}

	public class ChangeLookAndFeelAction extends AbstractAction {

		/**
		* 
		*/
		private static final long serialVersionUID = 1L;
		MainSvr frame;
		String laf;

		protected ChangeLookAndFeelAction(final MainSvr frame, final String laf) {
			super("ChangeTheme");
			this.frame = frame;
			this.laf = laf;
		}

		@Override
		public void actionPerformed(final ActionEvent e) {
			frame.setLookAndFeel(laf);
		}
	}

	private JPanel myContentPane = null;
	public static ConsoleTextArea consoleTextArea;
	static Logger log = Logger.getLogger(MainSvr.class.getName());

	public static void init() {
		try {
			consoleTextArea = new ConsoleTextArea();
			consoleTextArea.setEditable(false);
			consoleTextArea.setLineWrap(true);
			consoleTextArea.setFont(java.awt.Font.decode("宋体"));

		} catch (IOException e) {
			System.err.println("不能创建LoopedStreams：" + e);
			System.exit(1);
		}

		// 加载日志处理模块
		PropertyConfigurator.configure("conf/log4jForSSSVR.properties");
		log.info("Loaded log4j module finished. ");
	}

	/**
	 * This method initializes jContentPane
	 * 
	 * @return javax.swing.JPanel
	 */
	public JPanel getMyContentPane() {
		if (myContentPane == null) {
			myContentPane = new JPanel();
			myContentPane.setLayout(new BorderLayout());

			JPanel paneTmp = new JPanel(new GridBagLayout());
			paneTmp.setBorder(BorderFactory.createTitledBorder("运行日志记录"));
			paneTmp.add(new JScrollPane(consoleTextArea),
					new cn.mastercom.sssvr.util.GBC(0, 0).setFill(GridBagConstraints.BOTH).setWeight(100, 100));
			myContentPane.add(ComUtil.getOuterPane(paneTmp, 1, 8, 1, 8), java.awt.BorderLayout.CENTER);
		}
		return myContentPane;
	}

	public static boolean bMoveSampleEventRunning = false;

	class MoveSampleEvent extends Thread {
		public void run() {
			bMoveSampleEventRunning = true;
			FileMover mm = new FileMover(-1);

			if (samButton.isSelected()) {
				mm.GetSampleinfoFromHdfs(new CalendarEx(jDateChooser.getDate()), false, "FG");
				mm.GetVilSampleinfoFromHdfs(new CalendarEx(jDateChooser.getDate()), true);
			}
			if (eventButton.isSelected()) {
				mm.GetEventinfoFromHdfs(new CalendarEx(jDateChooser.getDate()), false);
			}
			if (gridButton.isSelected()) {
				mm.GetGridinfoFromHdfs(new CalendarEx(jDateChooser.getDate()), false, "", "");
				mm.GetGridinfoFromHdfs(new CalendarEx(jDateChooser.getDate()), false, "", "10");
				mm.GetGridinfoFromHdfs(new CalendarEx(jDateChooser.getDate()), false, "FG", "");
				mm.GetGridinfoFromHdfs(new CalendarEx(jDateChooser.getDate()), false, "FG", "10");

				mm.GetCellgridInfoFromHdfs(new CalendarEx(jDateChooser.getDate()), false, "", "");
				mm.GetCellgridInfoFromHdfs(new CalendarEx(jDateChooser.getDate()), false, "", "10");
				mm.GetCellgridInfoFromHdfs(new CalendarEx(jDateChooser.getDate()), false, "FG", "");
				mm.GetCellgridInfoFromHdfs(new CalendarEx(jDateChooser.getDate()), false, "FG", "10");
				mm.GetCellinfoFromHdfs(new CalendarEx(jDateChooser.getDate()), false, "FG");
				mm.GetCellinfoFromHdfs(new CalendarEx(jDateChooser.getDate()), false, "");
			}
			if (location23GButton.isSelected()) {
				mm.Get23GLocationInfoFromHdfs(new CalendarEx(jDateChooser.getDate()), false);
				mm.Get23GEventinfoFromHdfs(new CalendarEx(jDateChooser.getDate()), false);

				mm.Get2GCellGridinfoFromHdfs(new CalendarEx(jDateChooser.getDate()), false);
				mm.Get2GridinfoFromHdfs(new CalendarEx(jDateChooser.getDate()), false);
				mm.Get2GEventinfoFromHdfs(new CalendarEx(jDateChooser.getDate()), false);
				mm.Get2GCellinfoFromHdfs(new CalendarEx(jDateChooser.getDate()), false);

				mm.Get3GCellGridinfoFromHdfs(new CalendarEx(jDateChooser.getDate()), false);
				mm.Get3GGridinfoFromHdfs(new CalendarEx(jDateChooser.getDate()), false);
				mm.Get3GEventinfoFromHdfs(new CalendarEx(jDateChooser.getDate()), false);
				mm.Get3GCellinfoFromHdfs(new CalendarEx(jDateChooser.getDate()), false);
			}
			bMoveSampleEventRunning = false;
			JOptionPane.showMessageDialog(null, "入库文件下载完成", "操作结果", JOptionPane.INFORMATION_MESSAGE);
		}
	}
}
