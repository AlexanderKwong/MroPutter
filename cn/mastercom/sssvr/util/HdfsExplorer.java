package cn.mastercom.sssvr.util;

import java.awt.*;
import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.StringSelection;
import java.awt.datatransfer.Transferable;
import java.awt.datatransfer.UnsupportedFlavorException;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.event.TreeExpansionEvent;
import javax.swing.event.TreeExpansionListener;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.plaf.FontUIResource;
//import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableColumn;
import javax.swing.table.TableModel;
import javax.swing.table.TableRowSorter;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.TreePath;

//import org.apache.cassandra.cli.CliParser.newColumnFamily_return;
//import org.apache.cassandra.thrift.Cassandra.system_add_column_family_args;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
//import org.apache.hadoop.util.TestRunJar;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;

import cn.mastercom.sssvr.main.ConsoleTextArea;
import cn.mastercom.sssvr.sparktask.MainModel;
//import cn.mastercom.filterSpark.DataFilter;
import cn.mastercom.filterSpark.TableFiledAndAttr;

public class HdfsExplorer extends DefaultMutableTreeNode implements ActionListener{
//	private String hdfsRoot = "hdfs://10.139.6.169:9000";
//	private String RootUser = "";
//	private String RootPass = "jian(12)";
//	private String NameNodeIp = "10.139.6.169";
	private String hdfsRoot = "";
	private String RootUser = "";
	private String RootPass = "";
	private String NameNodeIp = "";
	private static HdfsExplorer hdfsExplorer;
	private static int i11 =0;
	int rowCount;
	static Logger log = Logger.getLogger(HdfsExplorer.class.getName());

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static HdfsExplorer instance() {
		// TODO Auto-generated method stub
		return hdfsExplorer;
	}

	public void readConfigInfo() {
		try {
			// XMLWriter writer = null;// 声明写XML的对象
			SAXReader reader = new SAXReader();

			OutputFormat format = OutputFormat.createPrettyPrint();
			format.setEncoding("GBK");// 设置XML文件的编码格式

			String filePath = "conf/config.xml";
			File file = new File(filePath);
			if (file.exists()) {
				Document doc = reader.read(file);// 读取XML文件

				{
					List<String> list = doc.selectNodes("//comm/HdfsRoot");
					Iterator iter = list.iterator();
					while (iter.hasNext()) {
						Element element = (Element) iter.next();
						hdfsRoot = element.getText();
						break;
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/RootUser");
					Iterator iter = list.iterator();
					while (iter.hasNext()) {
						Element element = (Element) iter.next();
						RootUser = element.getText();
						break;
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/NameNodeIp");
					Iterator iter = list.iterator();
					while (iter.hasNext()) {
						Element element = (Element) iter.next();
						NameNodeIp = element.getText();
						break;
					}
				}

				{
					List<String> list = doc.selectNodes("//comm/RootPass");
					Iterator iter = list.iterator();
					try {
						while (iter.hasNext()) {
							Element element = (Element) iter.next();
							if (element != null)
								RootPass = element.getText();
							break;
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static ConsoleTextArea consoleTextArea;

	public HdfsExplorer(boolean bDuli) {
		this.bDuli = bDuli;
		if (bDuli) {
			try {
				consoleTextArea = new ConsoleTextArea();
				consoleTextArea.setEditable(false);
				consoleTextArea.setLineWrap(true);
				consoleTextArea.setFont(java.awt.Font.decode("宋体"));
			} catch (IOException e) {
				e.printStackTrace();
			}
			PropertyConfigurator.configure("conf/log4jHE.properties");
			log.info("Loaded log4j module finished. ");

		}
		readConfigInfo();

		ssh = new SshHelper(NameNodeIp, RootUser, RootPass, 22);
		hdfs = new HadoopFSOperations(hdfsRoot);
	}

	protected String getClassName(final Object o) {
		final String classString = o.getClass().getName();
		final int dotIndex = classString.lastIndexOf(".");
		return classString.substring(dotIndex + 1);
	}

	@Override
	public void actionPerformed(final ActionEvent e) {
		final JMenuItem source = (JMenuItem) (e.getSource());
		final String s = "Action event detected." + "    Event source: " + source.getText() + " (an instance of "
				+ getClassName(source) + ")";
		System.out.println(s);
	}

	boolean bDuli = true;
	private static final long serialVersionUID = 6999685634761062308L;
	private SshHelper ssh = null;
	// new SshHelper("10.139.6.169","root","jian(12)");
	HadoopFSOperations hdfs = null;
	// new HadoopFSOperations();
	TxtWindow showByTxt = new TxtWindow();

	private class MyFile {// 这个内部类的用处在于在JTree节点的select发生时,对所选文件进行格式转化
		private FileStatus file;

		public MyFile(FileStatus file) {
			this.file = file;
		}

		public int fileNum = 0;
		public long totalSize = 0;

		@Override
		public String toString() {
			if (file == null)
				return ".";
			String name = file.getPath().getName().trim();
			if (file.isDirectory()) {
				if(i11%2==1){
					name += " (" + (fileNum>0?fileNum + ",":"") +GetFileLengthDesc(hdfs.GetFileLen(file.getPath())) + ")";
				}
				else if (file.isDirectory() && fileNum > 0) {
					name += "(" + fileNum + "," + GetFileLengthDesc(totalSize) + ")";
				}
			}
			return name;
		}
	}

	DefaultMutableTreeNode treeRoot;
	DefaultTableModel tableModel;
	JTable table;
	JTable table1;
	DefaultMutableTreeNode parent;
	Object[][] list = { {} };
	JTree tree = null;
	JPopupMenu popupTable;

	// 全局变量 记录table中获得临时数据信息
	String tempFileName = "";// 文件名
	String tempFilesize = ""; //文件大小
	String tempDir =""; //文件目录
	boolean tempIsDir = false;// 是否目录
	String tempPath = "";// 路径
	String tempParentPath = "";// 父路径
	String tempFileDate = ""; //文件最后修改时间
	TreePath treeExpandedPath = null;// 记录展开当前树结点，当树没有被选中时用此结点刷新

	public JTextField tfSubFoldName;
	public JTextField tfSubFoldName2;
	public JTextField tfSubFoldName3;
	public String condition=null;
	public String dataType=null;
	public String dataPath=null;
	private JPanel contentPanel = null;
	private JPanel contentPane2 = null;
	private JLabel label = null;
	private JLabel label1 = null;
	private JLabel label2 = null;
	private JLabel label3 = null;
	private JComboBox<String> tfSubFoldName1;
	private JButton jButton=null;
//	public JTextArea jTextArea=null;
	public JPanel jPanel1=null;
	boolean download=false;
   public String filedattr="";

	public static String GetFileLengthDesc(long len) {
		java.text.DecimalFormat df = new java.text.DecimalFormat("#.#");
		if (len > 1024 * 1000 * 1000) {
			return df.format(len / 1024000000.0d) + " G";
		}
		if (len > 1024 * 1000) {
			return df.format(len / 1024000.0) + " M";
		}
		if (len > 1024) {
			return df.format(len / 1024.0) + " K";
		}

		return len + " B";
	
	}
	public static long  GetFileLengthDescByKB(long len) {
		java.text.DecimalFormat df = new java.text.DecimalFormat("#.#");
		if (len > 1024) {
			return (len/1024);
		}
		if( len>0){
			len=1;
			return 1;
		}

		return  0;
	}
//新建文件夹
	private JPanel GetContentPanel() {
		if (contentPanel == null) {
			contentPanel = new JPanel();
			contentPanel.setBorder(new EmptyBorder(5, 5, 5, 5));
			contentPanel.setLayout(null);

			label = new JLabel("\u8BF7\u8F93\u5165\u5B50\u76EE\u5F55\u7684\u540D\u79F0");
			label.setBounds(10, 23, 125, 15);
			contentPanel.add(label);

			tfSubFoldName = new JTextField();
			tfSubFoldName.setBounds(20, 48, 350, 21);
			contentPanel.add(tfSubFoldName);
			tfSubFoldName.setColumns(10);
			contentPanel.setPreferredSize(new Dimension(100, 88));
		}
		if (label != null)
			label.setText("\u8BF7\u8F93\u5165\u5B50\u76EE\u5F55\u7684\u540D\u79F0");
		return contentPanel;
	}
//筛选工具
	private JPanel GetFilterPanel() {
		if (contentPane2 == null) {
			contentPane2 = new JPanel();
			contentPane2.setBorder(new EmptyBorder(5, 5, 5, 5));
			contentPane2.setLayout(null);
			
			//TODO 查所有表
			FilterReadFromXml filterReadFromXml = new FilterReadFromXml();
			//初始化两个值
			int tableNameListLength =0;
			ArrayList<String> tableNameList = new ArrayList<String>();

			tableNameList = filterReadFromXml.getTableName();
			tableNameListLength =  tableNameList.size();


			//String[] tablenameString = new String[tableNameListLength+1];
			//tablenameString[0]="请选择类型";
//			for(int i=1;i<tableNameListLength+1;i++){
//				tablenameString[i]=tableNameList.get(i-1);
//
//			}
			String[] tablenameString = new String[tableNameListLength];
			for(int i=0;i<tableNameListLength;i++){
				tablenameString[i]=tableNameList.get(i);

			}
			
			

			label3 = new JLabel("数据类型：");
			label3.setBounds(20, 25, 70, 15);
			contentPane2.add(label3);
			
			tfSubFoldName1 = new JComboBox<String>(tablenameString);
			tfSubFoldName1.setBounds(95, 23, 280, 21);
			contentPane2.add(tfSubFoldName1);
			tfSubFoldName1.addActionListener(this);
			dataType =(String) tfSubFoldName1.getSelectedItem();
			
			final JTextArea jTextArea=new JTextArea();
			jTextArea.setLineWrap(true);
			JScrollPane scrollPane=new JScrollPane(jTextArea);
			scrollPane.setBounds(480, 23, 300, 200);
			contentPane2.add(scrollPane);

			label1 = new JLabel("筛选条件：");
			label1.setBounds(20, 100, 125, 15);
			contentPane2.add(label1);
			
			tfSubFoldName2 = new JTextField();
			tfSubFoldName2.setBounds(95, 98, 380, 21);
			contentPane2.add(tfSubFoldName2);
			tfSubFoldName2.setColumns(10);

			label2 = new JLabel("存储路径：");
			label2.setBounds(20, 175, 125, 15);
			contentPane2.add(label2);

			tfSubFoldName3 = new JTextField();
			tfSubFoldName3.setBounds(95, 173, 380, 21);
			contentPane2.add(tfSubFoldName3);
			tfSubFoldName3.setColumns(10);

			jButton =new JButton("查看类型");
			jButton.setBounds(380, 23, 95, 23);
			contentPane2.add(jButton);
			jButton.addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent e) {
					dataType =(String) tfSubFoldName1.getSelectedItem();
					
					FilterReadFromXml filterReadFromXml = new FilterReadFromXml();
					ArrayList<String> tableList = filterReadFromXml.getTableName();
					String[] fieldAndAttr =null;
					String text="";
						try {
							fieldAndAttr =	filterReadFromXml.getFieldAndAttr(dataType);
						} catch (DocumentException e1) {
							e1.printStackTrace();
						}
						text=text+ "Field: "+fieldAndAttr[0]+"<br/>"
								+ "Attribute: "+fieldAndAttr[1];
						String[] fields = fieldAndAttr[0].split("#");
						String[] attrs=fieldAndAttr[1].split("#");
						String row = "";
						for (int j = 0; j < fields.length; j++) {
							 row =row+ "fieldName: "+fields[j]+ " Attribute: "+attrs[j]+"\n";
						}
						
					jTextArea.setText(row);
				}
			});
			contentPane2.setPreferredSize(new Dimension(800, 240));
		}
		if (label3 != null)
			label3.setText("数据类型：");
		if (label1 != null)
			label1.setText("筛选条件：");
		if (label2 != null)
			label2.setText("存储路径：");
		return contentPane2;
	}
	
	private JPanel GetRenamePanel(String oldName) {
		if (contentPanel == null) {
			contentPanel = new JPanel();
			contentPanel.setBorder(new EmptyBorder(5, 5, 5, 5));
			contentPanel.setLayout(null);

			label = new JLabel("\u8BF7\u8F93\u5165\u5B50\u76EE\u5F55\u7684\u540D\u79F0");
			label.setBounds(10, 23, 125, 15);
			contentPanel.add(label);

			tfSubFoldName = new JTextField();
			tfSubFoldName.setBounds(20, 48, 350, 21);
			contentPanel.add(tfSubFoldName);
			tfSubFoldName.setColumns(10);
			contentPanel.setPreferredSize(new Dimension(100, 88));
		}
		label.setText("输入新名称");
		tfSubFoldName.setText(oldName);
		return contentPanel;
	}

	private JPanel GetMovePanel() {
		if (contentPanel == null) {
			contentPanel = new JPanel();
			contentPanel.setBorder(new EmptyBorder(5, 5, 5, 5));
			contentPanel.setLayout(null);

			label = new JLabel("\u8BF7\u8F93\u5165\u5B50\u76EE\u5F55\u7684\u540D\u79F0");
			label.setBounds(10, 23, 125, 15);
			contentPanel.add(label);

			tfSubFoldName = new JTextField();
			tfSubFoldName.setBounds(20, 48, 450, 21);
			contentPanel.add(tfSubFoldName);
			tfSubFoldName.setColumns(10);
			contentPanel.setPreferredSize(new Dimension(800, 88));
		}
		label.setText("输入新路径：");
		tfSubFoldName.setText("");
		return contentPanel;
	}
	
//剪切粘贴	
	private JPanel GetReMovePanel(String oldName) {
		if (contentPanel == null) {
			contentPanel = new JPanel();
			contentPanel.setBorder(new EmptyBorder(5, 5, 5, 5));
			contentPanel.setLayout(null);

			label = new JLabel("\u8BF7\u8F93\u5165\u5B50\u76EE\u5F55\u7684\u540D\u79F0");
			label.setBounds(10, 23, 220, 15);
			contentPanel.add(label);

			tfSubFoldName = new JTextField();
			tfSubFoldName.setBounds(20, 48, 650, 36);
			contentPanel.add(tfSubFoldName);
			tfSubFoldName.setColumns(10);
			contentPanel.setPreferredSize(new Dimension(500, 200));
		}
		label.setText("按如下格式输入新地址");
		tfSubFoldName.setText(oldName);
		return contentPanel;
	}
	//执行cmd方法
	private void runCmd(String acmd,String apath) {
		ssh.execCommand(acmd);
		HadoopFSOperations hadoopFSOperations = new HadoopFSOperations();
		hadoopFSOperations.deleteFiles(apath);
		ssh.Disconnect();
		TreePath path = tree.getSelectionPath().getParentPath();
		RefreshSelectedNode(path);
	}
	
	/**
	 *
	 * @param thePath
	 * @param theFileName
	 * @param thePathParent
	 * @throws HeadlessException
	 */
//粘贴
	public  void getSystemClipboard(String curpath){//获取系统剪切板的文本内容[如果系统剪切板复制的内容是文本]  
	    Transferable t = Toolkit.getDefaultToolkit().getSystemClipboard().getContents(null);  //跟上面三行代码一样  
	    try {   
	        if (null != t && t.isDataFlavorSupported(DataFlavor.stringFlavor)) {   
	        String tpath = (String)t.getTransferData(DataFlavor.stringFlavor);
	        if(tpath.substring(0,2).equals("cp")){
		        if(!hdfs.hdfsCopyUtils(tpath.substring(2),curpath)){
						if (ssh.Connect()) {
							String sCmd = "/home/hmaster/MyCloudera/APP/hadoop/hadoop/bin/hadoop fs -cp " + tpath.substring(2) + " " + curpath + ";";
							String ret = ssh.execCommand(sCmd);
							ssh.Disconnect();
						}else{
							System.out.println("ssh连接失败");
						}
				   }
	        }else if (tpath.substring(0,2).equals("mv")) {
	        	if(!hdfs.movefile(tpath.substring(2),curpath)){
					if (ssh.Connect()) {
						String sCmd = "hadoop fs -mv " + tpath.substring(2) + " " + curpath + ";";
						String ret = ssh.execCommand(sCmd);
						ssh.Disconnect();
					}else{
						System.out.println("ssh连接失败");
					}
			   }
			}
	        }  
	        
	    } catch (UnsupportedFlavorException e) {  
	        System.out.println("Error tip: "+e.getMessage());  
	    } catch (IOException e) {   
	       System.out.println("Error tip: "+e.getMessage()); } 
	}  

	

	/**
	 * 刷新父结点
	 */
	private void refreshParentNode() {
		TreePath path = tree.getSelectionPath();
		if (path == null) {
			RefreshSelectedNode(treeExpandedPath);// 若树结点未被选中则使用展开树结点
		} else {
			RefreshSelectedNode(path.getParentPath());
		}
	}

	private void RefreshSelectedNode(TreePath path) {
		DefaultMutableTreeNode selectnode = (DefaultMutableTreeNode) path.getLastPathComponent();
		if (selectnode == treeRoot) {
			treeRoot.removeAllChildren();
			readfiles("/", treeRoot);
			SwingUtilities.invokeLater(new   Runnable() 
			{ 
				public   void   run() 
				{ 
				tree.updateUI(); 
				} 
			});
		} else {

			tree.collapsePath(path);
			tree.expandPath(path);
		}
	}

	public void treeMake() throws UnknownHostException { //
															// InetAddress local
															// =
															// InetAddress.getLocalHost();
		treeRoot = new DefaultMutableTreeNode(hdfs.HADOOP_URL);

		try {
			FileStatus fileStatus = hdfs.getFileStatus(hdfs.HADOOP_URL);
			//刷新主目录(测试)
			refreshParentNode();
			
			if (fileStatus != null) {
				treeRoot.setUserObject(new MyFile(fileStatus));
			}
		} catch (Exception e3) {

		}
		tree = new JTree(treeRoot);
		final DefaultTreeCellRenderer renderer = new MyTreeCellRenderer();
		renderer.setLeafIcon(new ImageIcon("images/txt.gif"));
		renderer.setOpenIcon(new ImageIcon("images/expand.gif"));
		tree.setCellRenderer(renderer);

		JScrollPane scrolltree = new JScrollPane(tree);
		scrolltree.setPreferredSize(new Dimension(300, 500));
		String[] row = { "文件","大小", "类型","路径", "父路径", "最后修改日期","文件大小排序(KB)"};
		
		
		tableModel = new DefaultTableModel(list, row){
			@Override
		 public Class<?> getColumnClass(int column) {  
		        rowCount = getRowCount();
		        try {
					if(rowCount <=1){
						return int.class;
					}else{
						return  getValueAt(0, column).getClass();
					}
				} catch (Exception e) {
				}
		        return int.class;
		    }   
		};

		    
		table = new JTable(tableModel);
		// 通过点击表头来排序列中数据resort data by clicking table header
		TableRowSorter<TableModel> sorter = new TableRowSorter<TableModel>(tableModel);
		//设置排序的列
//		sorter.setSortable(1, false); 
		
		table.setRowSorter(sorter);
		table.setRowHeight(20);
		table.getTableHeader().setFont(new Font("宋体", Font.BOLD, 16));

		hideColumn(table, 3);// 隐藏---路径
		hideColumn(table, 4);// 隐藏---父路径

		JScrollPane scrollTable = new JScrollPane(table);
		scrollTable.setPreferredSize(new Dimension(500, 500));

		JPanel paneTmp = new JPanel(new GridBagLayout());
		paneTmp.setBorder(BorderFactory.createTitledBorder("运行日志记录"));
		paneTmp.add(new JScrollPane(consoleTextArea),
				new cn.mastercom.sssvr.util.GBC(0, 0).setFill(GridBagConstraints.BOTH).setWeight(300, 300));
		paneTmp.setPreferredSize(new Dimension(800, 200));

		final JFrame jf = new JFrame();
		jf.setTitle("HDFS 管理器");
		//菜单栏
				JMenuBar menuBar = new JMenuBar();
				jf.setJMenuBar(menuBar);
				JMenu menu = new JMenu("文件(F)");
				menu.setMnemonic(KeyEvent.VK_F);
				menuBar.add(menu);
				
				JMenuItem mntmNewMenuItem_1 = new JMenuItem("显示/隐藏目录大小");
				menu.add(mntmNewMenuItem_1);
				
				mntmNewMenuItem_1.addActionListener(new ActionListener() {
					
					@Override
					public void actionPerformed(ActionEvent e) {
						i11=i11+1;
						refreshParentNode();
					}
				});
		//设置可移动分割线
		JSplitPane jSplitPane =new JSplitPane();
		jSplitPane.setOneTouchExpandable(true);//让分割线显示出箭头
        jSplitPane.setContinuousLayout(true);//操作箭头，重绘图形
        jSplitPane.setOrientation(JSplitPane.HORIZONTAL_SPLIT);//设置分割线方向
        jSplitPane.setLeftComponent(scrolltree);
        jSplitPane.setRightComponent(scrollTable);
        jSplitPane.setDividerSize(3);//设置分割线的宽度
        jSplitPane.setDividerLocation(300);//设定分割线的距离左边的位置
        
        JSplitPane jSplitPaneTotal =new JSplitPane();
        jSplitPaneTotal.setOneTouchExpandable(true);
        jSplitPaneTotal.setContinuousLayout(true);
        jSplitPaneTotal.setOrientation(JSplitPane.VERTICAL_SPLIT);
        jSplitPaneTotal.setTopComponent(jSplitPane);
        jSplitPaneTotal.setBottomComponent(paneTmp);
        jSplitPaneTotal.setDividerSize(3);
        jSplitPaneTotal.setDividerLocation(500);
        
        jf.getContentPane().add(BorderLayout.CENTER,jSplitPaneTotal);
      
		jf.setExtendedState(Frame.MAXIMIZED_BOTH);
		jf.setVisible(true);
		final Image topicon = ResourcesDepository.getImage("images/logo2.gif");
		jf.setIconImage(topicon);
		if (bDuli)
			jf.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		try {
			UIManager.setLookAndFeel(currentLookAndFeel);
			// 是否使用粗体
			UIManager.put("swing.boldMetal", Boolean.FALSE);

			UIDefaults UId = UIManager.getDefaults();
			FontUIResource font = new FontUIResource("宋体", Font.PLAIN, 13);
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

//			SwingUtilities.updateComponentTreeUI(jf);
			SwingUtilities.invokeLater(new   Runnable() 
			{ 
				public   void   run() 
				{ 
					SwingUtilities.updateComponentTreeUI(jf); 
				} 
			});
		} catch (final Exception e) {
			System.err.println("Oops!  Something went wrong!");
		}
		fileChooser = new JFileChooser();

		readfiles("/", treeRoot);
		tree.setRowHeight((int) (tree.getRowHeight() * 1.15));
		tree.expandPath(new TreePath(treeRoot));
		tree.addTreeExpansionListener(new TreeExpansionListener() {
			@Override
			public void treeCollapsed(TreeExpansionEvent e) {
				TreePath path = e.getPath();
				if (path == null)
					return;
				DefaultMutableTreeNode selectnode = (DefaultMutableTreeNode) path.getLastPathComponent();
				if (selectnode == treeRoot)
					return;
				// JOptionPane.showMessageDialog(null,
				// "--------tree-----treeCollapsed----------");
				if (selectnode.getChildCount() > 1) {
					for (int i = selectnode.getChildCount() - 1; i >= 0; i--) {
						TreePath pathChild = new TreePath(selectnode.getChildAt(i));
						DefaultMutableTreeNode childnode = (DefaultMutableTreeNode) pathChild.getLastPathComponent();
						if (childnode.getUserObject() instanceof MyFile) {
							MyFile file = (MyFile) childnode.getUserObject();
							if (file.file != null)
								selectnode.remove(i);
						}
					}
				}
				addNullNode(selectnode);// 关闭树后增加一个空节点

			}

			@Override
			public void treeExpanded(TreeExpansionEvent e) {
				TreePath path = e.getPath();
				// JOptionPane.showMessageDialog(null,
				// "--------tree-----treeExpanded----path------"+path.toString());
				if (path == null)
					return;
				treeExpandedPath = path;// 记录展开当前树结点，当树没有被选中时用此结点刷新
				// JOptionPane.showMessageDialog(null,
				// "--------tree-----treeExpanded---treeExpandedPath-------"+treeExpandedPath.toString());
				DefaultMutableTreeNode selectnode = (DefaultMutableTreeNode) path.getLastPathComponent();
				if (selectnode == treeRoot)
					return;
				if (selectnode.getChildCount() > 1)
					return;
				if (!selectnode.isLeaf()) {
					// 这里加上类型判断
					if (!(selectnode.getUserObject() instanceof MyFile)) {
						return;
					}
					FileStatus file_select = ((MyFile) selectnode.getUserObject()).file;
					// 读取文件夹下文件添加下层节点
					readfiles(file_select.getPath().toString(), selectnode);
					removeFirstChildNode(selectnode);// 删除原有的空节点
//					tree.updateUI();
					SwingUtilities.invokeLater(new   Runnable() 
					{ 
					public   void   run() 
					{ 
					tree.updateUI(); 
					} 
					});

					tableModel.setRowCount(0);
					FileStatus[] fileList = hdfs.listStatus(file_select.getPath().toString());
					list = fu(fileList);
					MyFile userObj = (MyFile) selectnode.getUserObject();
					userObj.fileNum = 0;
					userObj.totalSize = 0;
					for (int i = 0; i < fileList.length; i++) {
						tableModel.addRow(list[i]);
						if (fileList[i].isFile()) {
							userObj.fileNum++;
//							userObj.totalSize += fileList[i].GetFileLengthDesc(hdfs.GetFileLen(file_select.getPath()));
							userObj.totalSize += fileList[i].getLen();
						}
					}
					selectnode.setUserObject(userObj);
//					tree.updateUI();
					SwingUtilities.invokeLater(new   Runnable() 
					{ 
					public   void   run() 
					{ 
					tree.updateUI(); 
					} 
					});
				} else {
					tableModel.setRowCount(0);
					try {
						FileStatus file_select = ((MyFile) selectnode.getUserObject()).file;
//						String sizeFile = HdfsExplorer.GetFileLengthDescByKB(file_select.getLen());
						String sizeFile = HdfsExplorer.GetFileLengthDesc(file_select.getLen());
						CalendarEx cal = new CalendarEx(file_select.getModificationTime() / 1000000, 0);
						Object re[][] = { { file_select.getPath().getName(), sizeFile, cal.toString() } };
						list = re;
						tableModel.addRow(list[0]);

					} catch (Exception e2) {
						e2.printStackTrace();
					}
				}
			}
		});

		tree.addTreeSelectionListener(new TreeSelectionListener() {// 添加listener
			@Override
			public void valueChanged(TreeSelectionEvent arg0) {
				TreePath path = tree.getSelectionPath();
				if (path == null)
					return;

				DefaultMutableTreeNode selectnode = (DefaultMutableTreeNode) path.getLastPathComponent();
				if (selectnode == treeRoot)
					return;
				if (!selectnode.isLeaf()) {
					// 这里加上类型判断
					if (!(selectnode.getUserObject() instanceof MyFile)) {
						return;
					}
					FileStatus file_select = ((MyFile) selectnode.getUserObject()).file;

					if (selectnode.getChildCount() > 1)
						return;

					// 读取文件夹下文件添加下层节点
					readfiles(file_select.getPath().toString(), selectnode);
					removeFirstChildNode(selectnode);// 删除原有的空节点
//					tree.updateUI();
					SwingUtilities.invokeLater(new   Runnable() 
					{ 
					public   void   run() 
					{ 
					tree.updateUI(); 
					} 
					});

					tableModel.setRowCount(0);
					FileStatus[] fileList = hdfs.listStatus(file_select.getPath().toString());
					list = fu(fileList);
					for (int i = 0; i < fileList.length; i++) {
						tableModel.addRow(list[i]);
					}
				} else {
					tableModel.setRowCount(0);
					try {
						FileStatus file_select = ((MyFile) selectnode.getUserObject()).file;
						String owner = "文件[" + file_select.getOwner() + " " + file_select.getPermission().toString() + "]";
						String fileSize = HdfsExplorer.GetFileLengthDesc(file_select.getLen());
						long sizeFile = HdfsExplorer.GetFileLengthDescByKB(file_select.getLen());
						CalendarEx cal = new CalendarEx(file_select.getModificationTime() / 1000, 0);
//						Object re[][] = { { file_select.getPath().getName(), owner, cal.toString(2),
//								file_select.getPath().toString(), file_select.getPath().getParent().toString(),sizeFile } };
						Object re[][] = { { file_select.getPath().getName(),fileSize ,owner, file_select.getPath().toString(),
					          file_select.getPath().getParent().toString(),cal.toString(2) ,sizeFile} };
						list = re;
						tableModel.addRow(list[0]);
//						System.out.println("=================================="+list[0].length);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		});

		tree.addMouseListener(new java.awt.event.MouseAdapter() {
			@Override
			public void mousePressed(MouseEvent e) {
				jTree1_mousePressed(e);
			}
		});
		tree.addKeyListener(new KeyListener()
		{
			@Override
			public void keyPressed(KeyEvent e)
			{
				// TODO Auto-generated method stub
				if(e.isControlDown() && e.isShiftDown() && e.getKeyCode()=='C')
				{
					FileStatus theFile = GetSelectedFile();
					Toolkit.getDefaultToolkit().getSystemClipboard()
							.setContents(new StringSelection("cp"+theFile.getPath().toString()),null);
				}
				if(e.isControlDown() && e.isShiftDown() && e.getKeyCode()== 'V')
				{
					FileStatus theFile = GetSelectedFile();
					Chmod(theFile);
					getSystemClipboard(theFile.getPath().toString());
					TreePath path = tree.getSelectionPath();
					RefreshSelectedNode(path);
				}
				if(e.isControlDown() && e.getKeyCode()== 'D')
				{
					deleteFile();
				}
			}
			@Override
			public void keyTyped(KeyEvent e)
			{
			}
			@Override
			public void keyReleased(KeyEvent e)
			{
			}
		});

		{
			popupText = new JPopupMenu();
			JMenuItem menuItem = new JMenuItem("清空");
			menuItem.addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(final ActionEvent e) {
					consoleTextArea.setText("");
				}
			});
			popupText.add(menuItem);
		}
			//从HDFS下载
		popup = new JPopupMenu();
		JMenuItem menuItem = new JMenuItem("从HDFS下载...");
		menuItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent e) {
				
				final Object lock = new Object();
				
				FileStatus theFile = GetSelectedFile();
				if (theFile == null)
					return;
				
				final String hdfsPath = theFile.getPath().toString();
				final String hdfsFileName = theFile.getPath().getName().toString();
				boolean isDir = theFile.isDirectory();

				//JFileChooser fileChooser = new JFileChooser();
				//FileSystemView fsv = FileSystemView.getFileSystemView();
				//fileChooser.setCurrentDirectory(fsv.getHomeDirectory());
				fileChooser.setDialogTitle("请选择目的文件夹");
				fileChooser.setApproveButtonText("确定");
				fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
				int result = fileChooser.showOpenDialog(null);
				if (JFileChooser.APPROVE_OPTION == result) {
					//Chmod(theFile);
					final String localPath = fileChooser.getSelectedFile().getPath();
					if (isDir)
					{
						try
						{
							new Thread(new Runnable()
							{
								@Override
								public void run()
								{
									synchronized (lock)
									{
										try
										{
											lock.wait();
										}
										catch (InterruptedException e)
										{
											// TODO Auto-generated catch block
											e.printStackTrace();
										}
										JOptionPane.showMessageDialog(null, "下载成功！");
									}
								}
							}).start();
							Thread.sleep(50);
			                new Thread(new Runnable()
							{
								@Override
								public void run()
								{
									synchronized(lock)
									{
										try
										{
//											hdfs.fs.copyToLocalFile(false,new Path(hdfsPath), new Path(localPath),true);
											hdfs.readHdfsDirToLocal(hdfsPath, localPath + "/" + hdfsFileName, "");
											Thread.sleep(500);
										}
										catch (Exception e)
										{
											// TODO Auto-generated catch block
											e.printStackTrace();
										}
										lock.notify();
									}
								}
							}).start();
			                
			               
						}
						catch (Exception e2)
						{
							// TODO: handle exception
						}
					}
					else{
						try
						{
							DistributedFileSystem dfs = (DistributedFileSystem) hdfs.fs;
							long length = dfs.getContentSummary(new Path(hdfsPath)).getLength();
							hdfs.readFileFromHdfs(hdfsPath, localPath, length,1);
						}
						catch (Exception e1)
						{
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						
					}
				}
			}
		});
		popup.add(menuItem);
		
		menuItem = new JMenuItem("归并压缩下载");
		menuItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent e) {
				FileStatus theFile = GetSelectedFile();
				if (theFile == null)
					return;
				
				String hdfsPath = theFile.getPath().toString();
				String hdfsFileName = theFile.getPath().getName().toString();
				boolean isDir = theFile.isDirectory();

				fileChooser.setDialogTitle("请选择目的文件夹");
				fileChooser.setApproveButtonText("确定");
				fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
				int result = fileChooser.showOpenDialog(null);
				if (JFileChooser.APPROVE_OPTION == result) {					
					String localPath = fileChooser.getSelectedFile().getPath();
					if (isDir)
					{
						hdfs.getMerge(hdfsPath, localPath + "/" + hdfsFileName, "",true);
						JOptionPane.showMessageDialog(null, "下载成功！");
					}
				}
			}
		});
		popup.add(menuItem);
		
		//下载5MB
		menuItem = new JMenuItem("下载5MB...");
		menuItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent e) {
				FileStatus theFile = GetSelectedFile();
				if (theFile == null)
					return;
				if (theFile.isDirectory())
					return;
				
				String hdfsPath = theFile.getPath().toString();
				//JFileChooser fileChooser = new JFileChooser();
				//FileSystemView fsv = FileSystemView.getFileSystemView();
				//fileChooser.setCurrentDirectory(fsv.getHomeDirectory());
				fileChooser.setDialogTitle("请选择目的文件夹");
				fileChooser.setApproveButtonText("确定");
				fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
				int result = fileChooser.showOpenDialog(null);
				if (JFileChooser.APPROVE_OPTION == result) {
					//Chmod(theFile);
					String localPath = fileChooser.getSelectedFile().getPath();
					hdfs.readFileFromHdfs(hdfsPath, localPath, 5012000,1);
					JOptionPane.showMessageDialog(null, "下载成功！");
				}
			}
		});
		popup.add(menuItem);
		//数据筛选工具
		menuItem = new JMenuItem("数据筛选工具");
		menuItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent e) {
				Object options[] = { "\u786E\u5B9A", "\u53D6\u6D88" };
				JPanel pane = GetFilterPanel();
				int answer = JOptionPane.showOptionDialog(null, pane, "数据筛选工具", 0, -1, null, options, options[1]);
				FileStatus theFile = GetSelectedFile();
				TableFiledAndAttr tableFiledAndAttr =new TableFiledAndAttr();
				 filedattr= tableFiledAndAttr.tableFilter(dataType);
					dataType =(String) tfSubFoldName1.getSelectedItem();
					dataPath=tfSubFoldName3.getText();
					condition=tfSubFoldName2.getText();
					if(answer == 0){
					//Chmod(theFile);
//					if (!hdfs.mkdir(dataPath)) {
					MainModel mainModel =	MainModel.GetInstance();
					mainModel.loadConfig();
                    String sparkCmd = mainModel.GetSparkConfig().getSparkCmd();
                    String cmd =String.format("%s "+'"'+"%s"+'"'+" "+'"'+"%s"+'"'+" "+'"'+"%s"+'"'+" "+'"'+"%s"+'"',
						     sparkCmd,
							filedattr,
							theFile.getPath().toString(),
							condition,
							dataPath);
					if (ssh.Connect()) {
//						//调用执行方法
						runCmd(cmd, dataPath);
					}
				}
			}
		});
		
		popup.add(menuItem);
		
//新建文件夹
		menuItem = new JMenuItem("新建文件夹...");
		menuItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent e) {
				FileStatus theFile = GetSelectedFile();
				if (!theFile.isDirectory())
					return;
				
				Object options[] = { "\u786E\u5B9A", "\u53D6\u6D88" };

				JPanel pane = GetContentPanel();
				int answer = JOptionPane.showOptionDialog(null, pane, "创建子目录", 0, -1, null, options, options[1]);
				if (answer == 0) {
					Chmod(theFile);
					System.out.println("xinjianwenjianjia");
					if (hdfs.checkFileExist(theFile.getPath().toString() + "/" + tfSubFoldName.getText().trim())) {
						return;
					}
					if (!hdfs.mkdir(theFile.getPath().toString() + "/" + tfSubFoldName.getText().trim())) {
						if (ssh.Connect()) {
							String sCmd = "hadoop fs -mkdir " + theFile.getPath().toString() + "/"
									+ tfSubFoldName.getText().trim() + ";";
							String ret = ssh.execCommand(sCmd);
							JOptionPane.showMessageDialog(null, ret, "创建结果", JOptionPane.INFORMATION_MESSAGE);
							ssh.Disconnect();
						}
					}
					TreePath path = tree.getSelectionPath();
					RefreshSelectedNode(path);
				}
			}
		});
		popup.add(menuItem);
//上传文件
		menuItem = new JMenuItem("上传文件...");
		menuItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent e) {
				FileStatus theFile = GetSelectedFile();
				if (theFile == null)
					return;
				
				//JFileChooser fileChooser = new JFileChooser();
				//FileSystemView fsv = FileSystemView.getFileSystemView();
				//fileChooser.setCurrentDirectory(fsv.getHomeDirectory());
				fileChooser.setDialogTitle("请选择要上传的文件");
				fileChooser.setApproveButtonText("确定");
				fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
				fileChooser.setMultiSelectionEnabled(true);

				int result = fileChooser.showOpenDialog(null);
				if (JFileChooser.APPROVE_OPTION == result) {
					Chmod(theFile);
					File[] files = fileChooser.getSelectedFiles();

					for (File file : files) {
						if (file.isFile()) {
							if (!hdfs.copyFileToHDFS(file.getPath(), theFile.getPath().toString(),false)) {
								System.out.println("上传失败:" + file.getPath());
							} else {
								System.out.println("上传成功:" + file.getPath());
							}
						}
					}
					TreePath path = tree.getSelectionPath();
					RefreshSelectedNode(path);
				}
			}
		});
		popup.add(menuItem);
//上传文件夹
		menuItem = new JMenuItem("上传文件夹...");
		menuItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent e) {
				FileStatus theFile = GetSelectedFile();
				if (theFile == null)
					return;

				fileChooser.setDialogTitle("请选择要上传的目录");
				fileChooser.setApproveButtonText("确定");
				fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
				fileChooser.setMultiSelectionEnabled(true);

				int result = fileChooser.showOpenDialog(null);
				if (JFileChooser.APPROVE_OPTION == result) {
					Chmod(theFile);
					File[] files = fileChooser.getSelectedFiles();

					for (File file : files) {
						if (file.isDirectory()) {
							if (!hdfs.CopyDirTohdfs(file.getPath().toString(),
									theFile.getPath().toString() + "/" + file.getName(),false)) {
								System.out.println("上传失败:" + file.getPath());
							} else {
								System.out.println("上传成功:" + file.getPath());
							}
						}
					}
					TreePath path = tree.getSelectionPath();
					RefreshSelectedNode(path);
				}
			}
		});
		popup.add(menuItem);
//归并上传文件夹		
		menuItem = new JMenuItem("归并上传文件夹...");
		menuItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent e) {
				FileStatus theFile = GetSelectedFile();
				if (theFile == null)
					return;
				
				fileChooser.setDialogTitle("请选择要上传的目录");
				fileChooser.setApproveButtonText("确定");
				fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
				fileChooser.setMultiSelectionEnabled(true);
				fileChooser.setMultiSelectionEnabled(false);

				int result = fileChooser.showOpenDialog(null);
				if (JFileChooser.APPROVE_OPTION == result) {
					Chmod(theFile);
					File file = fileChooser.getSelectedFile();
					if (!hdfs.putMerge(file.getPath().toString(), theFile.getPath().toString(), file.getName(), ""))
					{
						System.out.println("上传失败:" + file.getPath());
					} else {
						System.out.println("上传成功:" + file.getPath());
					}
					TreePath path = tree.getSelectionPath();
					RefreshSelectedNode(path);
				}
			}
		});
		popup.add(menuItem);
//添加SUCCESS标志文件		
		menuItem = new JMenuItem("添加SUCCESS标志文件");
		menuItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent e) {
				FileStatus theFile = GetSelectedFile();
				String fileNameSuccess = theFile.getPath() + "/_SUCCESS";
				boolean ret= hdfs.CreateEmptyFile(fileNameSuccess);
				if(ret)
				{
					System.out.println("添加SUCCESS标志文件成功");
					TreePath path = tree.getSelectionPath();
					RefreshSelectedNode(path.getParentPath());
				}
				else
				{
					System.out.println("添加SUCCESS标志文件失败");
				}
			}
		});
		popup.add(menuItem);
//分割符
		popup.addSeparator();
//删除
		menuItem = new JMenuItem("删除");
		menuItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent e) {
				deleteFile();
			}
		});
		popup.add(menuItem);
//重命名
		menuItem = new JMenuItem("重命名");
		menuItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent e) {
				FileStatus theFile = GetSelectedFile();
				Chmod(theFile);
				String thePath = theFile.getPath().toString();
				String theFileName = theFile.getPath().getName();
				String thePathParent = theFile.getPath().getParent().toString();
				renameHdfs(thePath, theFileName, thePathParent);
				TreePath path = tree.getSelectionPath();
				RefreshSelectedNode(path.getParentPath());
			}
		});
		popup.add(menuItem);
//赋权777	
		menuItem = new JMenuItem("赋权777");
		menuItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent e) {
				FileStatus theFile = GetSelectedFile();
				Chmod(theFile);
			}
		});
		popup.add(menuItem);
//复制完整名称
		menuItem = new JMenuItem("复制完整名称");
		menuItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent e) {
				FileStatus theFile = GetSelectedFile();
				Toolkit.getDefaultToolkit().getSystemClipboard()
						.setContents(new StringSelection(theFile.getPath().toString()), null);
			}
		});
		popup.add(menuItem);
//剪切
		menuItem = new JMenuItem("剪切");
		menuItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent e) {
				FileStatus theFile = GetSelectedFile();
				Toolkit.getDefaultToolkit().getSystemClipboard()
						.setContents(new StringSelection("mv"+theFile.getPath().toString()), null);
			}
		});
		popup.add(menuItem);

//复制
		menuItem = new JMenuItem("复制");
		menuItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent e) {
				FileStatus theFile = GetSelectedFile();
				Toolkit.getDefaultToolkit().getSystemClipboard()
						.setContents(new StringSelection("cp"+theFile.getPath().toString()), null);
			}
		});
		popup.add(menuItem);
//粘贴
		menuItem = new JMenuItem("粘贴");
		menuItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent e) {
				FileStatus theFile = GetSelectedFile();
				Chmod(theFile);
				getSystemClipboard(theFile.getPath().toString());
				TreePath path = tree.getSelectionPath();
				RefreshSelectedNode(path);
//				RefreshSelectedNode(path.getParentPath());
			}
		});
		popup.add(menuItem);
		

		popupTable = new JPopupMenu();
		menuItem = new JMenuItem("从HDFS下载...");
		menuItem.addActionListener(popupTableListener());
		popupTable.add(menuItem);
		menuItem = new JMenuItem("下载5MB...");
		menuItem.addActionListener(popupTableListener());
		popupTable.add(menuItem);
		popupTable.addSeparator();
		menuItem = new JMenuItem("删除");
		menuItem.addActionListener(popupTableListener());
		popupTable.add(menuItem);
		menuItem = new JMenuItem("重命名");
		menuItem.addActionListener(popupTableListener());
		popupTable.add(menuItem);
		menuItem = new JMenuItem("剪切");
		menuItem.addActionListener(popupTableListener());
		popupTable.add(menuItem);
		popupTable.addSeparator();
		menuItem = new JMenuItem("预览");
		menuItem.addActionListener(popupTableListener());
		popupTable.add(menuItem);
		menuItem = new JMenuItem("复制完整名称");
		menuItem.addActionListener(popupTableListener());
		popupTable.add(menuItem);
		menuItem = new JMenuItem("复制表信息");
		menuItem.addActionListener(popupTableListener());
		popupTable.add(menuItem);

		// Add listener to components that can bring up popup menus.
		final MouseListener popupListener = new PopupListener();
		tree.addMouseListener(popupListener);

		final MouseListener popupTableListener = new PopupTableListener();
		table.addMouseListener(popupTableListener);

		final MouseListener popupTextListener = new PopupTextListener();
		consoleTextArea.addMouseListener(popupTextListener);
		
	}

	private void deleteFile()
	{
		FileStatus theFile = GetSelectedFile();
		int n = JOptionPane.showConfirmDialog(null, "确认删除" + theFile.getPath() + "吗？", "确认删除框",
				JOptionPane.YES_NO_OPTION);
		if (n == JOptionPane.NO_OPTION) {
			return;
		}
		if (theFile != null) {
			Chmod(theFile);
			String thePath = theFile.getPath().toString();
			boolean isDir = theFile.isDirectory();
			deleteHdfsFile(thePath, isDir);
			TreePath path = tree.getSelectionPath();
			RefreshSelectedNode(path.getParentPath());
		}
	}
	/**
	 * @return
	 */
	private ActionListener popupTableListener() {
		return new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent e) {
				if (table.getSelectedRow() < 0)
					return;
				String strAction = ((JMenuItem) e.getSource()).getText().trim();

				if (strAction.equals("删除")) {
					if (!isSure("<html>您确定要进行<b><font size=5> " + strAction + " </font></b>操作吗？"))
						return;
					for (int row : table.getSelectedRows()) {
						getTableInfoByRow(row);// 提取 tempPath, tempIsDir
						deleteHdfsFile(tempPath, tempIsDir);// 删除操作
						// tableModel.removeRow(row);
					}
					refreshParentNode();
					JOptionPane.showMessageDialog(null, strAction + "成功！");
				} else if (strAction.equals("重命名")) {
					for (int row : table.getSelectedRows()) {
						getTableInfoByRow(row);// 提取 tempPath, tempIsDir
						renameHdfs(tempPath, tempFileName, tempParentPath);
						// tableModel.setValueAt(tfSubFoldName.getText().trim(),row,0);
					}
					refreshParentNode();
				} else if (strAction.equals("剪切")) {
					moveHdfs();
					refreshParentNode();
				} else if (strAction.equals("从HDFS下载...") || strAction.equals("下载5MB...")) {
					if (!isSure("<html>您确定要进行<b><font size=5> " + strAction + " </font></b>操作吗？"))
						return;

					//JFileChooser fileChooser = new JFileChooser();
					//FileSystemView fsv = FileSystemView.getFileSystemView();
					//fileChooser.setCurrentDirectory(fsv.getHomeDirectory());
					fileChooser.setDialogTitle("请选择目的文件夹");
					fileChooser.setApproveButtonText("确定");
					fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
					int result = fileChooser.showOpenDialog(null);
					String localPath = fileChooser.getSelectedFile().getPath();

					if (JFileChooser.APPROVE_OPTION == result) {
						for (int row : table.getSelectedRows()) {
							getTableInfoByRow(row);// 提取 tempPath, tempIsDir

							if (tempIsDir) {
								if (strAction.equals("从HDFS下载...")) {
									hdfs.readHdfsDirToLocal(tempPath, localPath + "/" + tempFileName, "");
								} else {
									// 下载5MB...
								}
							} else {
								//Chmod(tempPath,tempIsDir);
								if (strAction.equals("从HDFS下载...")) {
									hdfs.readFileFromHdfs(tempPath, localPath, -1,1);
								} else {
									hdfs.readFileFromHdfs(tempPath, localPath, 5012000,1);// 下载5MB...
								}
							}
						}
						JOptionPane.showMessageDialog(null, strAction + "成功！");
					}

				} else if (strAction.equals("预览")) {
					String str="";
					for (int row : table.getSelectedRows()) {
						getTableInfoByRow(row);// 提取 tempPath, tempIsDir
						//Chmod(tempPath,tempIsDir);
						str = hdfs.viewFileFromHdfs(tempPath, 501200);//预览500K						
						break;
					}					
					showByTxt.setShowFile(str);
					showByTxt.setVisible(true);
				} else if (strAction.equals("复制完整名称")) {
					for (int row : table.getSelectedRows()) {
						getTableInfoByRow(row);// 提取 tempPath, tempIsDir
						Toolkit.getDefaultToolkit().getSystemClipboard().setContents(new StringSelection(tempPath),
								null);
						break;
					}
				} else if (strAction.equals("复制表信息")) {
					   
				    String fileInfo ="";
				    for(int j:table.getSelectedRows()){
				    	getTableInfoByRow(j);
				    	 fileInfo += tempFileName+"\t"+tempFilesize+"\t"+tempDir+"\t"+tempFileDate+"\n";
				    }
				    Toolkit.getDefaultToolkit().getSystemClipboard().setContents(new StringSelection(fileInfo),
							null);
			}
			}

		};
	}

	// 给你的JTree添加鼠标事件.通过鼠标位置获得树路经,然后选中该路径.就可以.
	void jTree1_mousePressed(MouseEvent e) {
		TreePath path = tree.getPathForLocation(e.getX(), e.getY());
		tree.setSelectionPath(path);
		// JOptionPane.showMessageDialog(null,
		// "--------tree-----jTree1_mousePressed----------");
	}

	JPopupMenu popup;
	JPopupMenu popupText;

	private void Chmod(FileStatus theFile) {
		String permission = theFile.getPermission().toString();
		if (permission.length() == 9 && permission.charAt(7) == '-') {
			if (ssh.Connect()) {
				String sCmd = "hadoop fs -chmod -R 777  " + theFile.getPath().toString() + ";";
				ssh.execCommand(sCmd);
				ssh.Disconnect();
			}
			else
			{
				System.out.println("ssh 未连接");
			}
		}
	}
	
	private void Chmod(String path,boolean isDir) {
		if(!isDir){
			if (ssh.Connect()) {
				String sCmd = "hadoop fs -chmod -R 777  " + path + ";";
				ssh.execCommand(sCmd);
				ssh.Disconnect();
			}
		}
	}
	
	
	/**
	 * 提取 全局变量 记录table中获得临时数据信息
	 * 
	 * @param row
	 */
	private void getTableInfoByRow(int row) {
		tempFileName = getValue(row, 0);
		tempFilesize = getValue(row, 1);
		 tempDir = getValue(row, 2);
		if (tempDir.indexOf("目录") > -1) {
			tempIsDir = true;
		} else {
			tempIsDir = false;
		}
		// String tempPath = getValue(row,2);
		tempPath = getValue(row, 3);
		tempParentPath = getValue(row, 4);
		tempFileDate = getValue(row, 5);
	}

	private String getValue(int row, int column) {
		// DefaultTableModel tableModel = (DefaultTableModel) table.getModel();
		return tableModel.getValueAt(table.convertRowIndexToModel(row), column).toString();// 方法1：获取排序后实际行号值
		// return table.getValueAt(row, column).toString();//方法2：直接从table获取值
	}

	private void hideColumn(JTable tb, int index) {
		TableColumn tc = tb.getColumnModel().getColumn(index);
		tc.setMinWidth(0);
		tc.setMaxWidth(0);
		tc.setPreferredWidth(0);
		tc.setWidth(0);
		tb.getTableHeader().getColumnModel().getColumn(index).setMaxWidth(0);
		tb.getTableHeader().getColumnModel().getColumn(index).setMinWidth(0);
	}

	/**
	 * 剪切
	 * 
	 * @param
	 * @param
	 * @param
	 * @throws HeadlessException
	 */
	/*
	 * @param thePath
	 * @param theFileName
	 * @param thePathParent
	 */
	private void moveHdfs() throws HeadlessException {
		Object options[] = { "\u786E\u5B9A", "\u53D6\u6D88" };

		JPanel pane = GetMovePanel();
		int answer = JOptionPane.showOptionDialog(null, pane, "剪切", 0, -1, null, options, options[1]);
		if (answer == 0) {
			for (int row : table.getSelectedRows()) {
				getTableInfoByRow(row);// 提取 tempPath, tempIsDir
				if (!hdfs.movefile(tempPath, tfSubFoldName.getText().trim() + "/" + this.tempFileName)) {
					if (ssh.Connect()) {
						String sCmd = "hadoop fs -mv " + tempPath + " " + tfSubFoldName.getText().trim() + "/"
								+ tempFileName + ";";
						 ssh.execCommand(sCmd);
//						String ret = ssh.execCommand(sCmd);
						// JOptionPane.showMessageDialog(
						// null,ret,"创建结果",JOptionPane.INFORMATION_MESSAGE);
						ssh.Disconnect();
					}
				}
			}
		}
	}

	/**
	 * 重命名
	 * 
	 * @param thePath
	 * @param theFileName
	 * @param thePathParent
	 * @throws HeadlessException
	 */
	private void renameHdfs(String thePath, String theFileName, String thePathParent) throws HeadlessException {
		Object options[] = { "\u786E\u5B9A", "\u53D6\u6D88" };

		JPanel pane = GetRenamePanel(theFileName);
		int answer = JOptionPane.showOptionDialog(null, pane, "重命名", 0, -1, null, options, options[1]);
		if (answer == 0) {
			if (!hdfs.movefile(thePath, thePathParent + "/" + tfSubFoldName.getText().trim())) {
				if (ssh.Connect()) {
					String sCmd = "hadoop fs -mv " + thePath + " " + thePathParent + "/"
							+ tfSubFoldName.getText().trim() + ";";
					ssh.execCommand(sCmd);
//					String ret = ssh.execCommand(sCmd);
					// JOptionPane.showMessageDialog(
					// null,ret,"创建结果",JOptionPane.INFORMATION_MESSAGE);
					ssh.Disconnect();
				}
			}

		}
	}

	/**
	 * 删除文件
	 * 
	 * @param hdfsPath
	 * @param isDir
	 * @throws HeadlessException
	 */
	/*
	 * @param bDeleted
	 */
	private void deleteHdfsFile(String hdfsPath, boolean isDir) throws HeadlessException {
		boolean bDeleted = false;
		try {
			if (hdfs.delete(hdfsPath)) {
				if (!hdfs.checkFileExist(hdfsPath)) {
					// TreePath path = tree.getSelectionPath();
					// RefreshSelectedNode(path.getParentPath());
					bDeleted = true;
				}
			}
		} catch (Exception e1) {

		}
		if (!bDeleted) {
			if (ssh.Connect()) {
				String sCmd = "hadoop fs -rm \"" + hdfsPath + "\";";
				if (isDir)
					sCmd = "hadoop fs -rmr \"" + hdfsPath + "\";";
				String ret = ssh.execCommand(sCmd);
				System.out.println(ret);
				//JOptionPane.showMessageDialog(null, ret, "删除结果", JOptionPane.INFORMATION_MESSAGE);
				ssh.Disconnect();
			}
		}
	}

	/**
	 * @param selectnode
	 */
	public void addNullNode(DefaultMutableTreeNode selectnode) {
		int nNum = selectnode.getChildCount();
		if (nNum < 1) {
			FileStatus stubadd = null;
			DefaultMutableTreeNode stub = new DefaultMutableTreeNode(new MyFile(stubadd));
			selectnode.add(stub);
		}
	}

	/**
	 * @param selectnode
	 */
	private void removeFirstChildNode(DefaultMutableTreeNode selectnode) {
		DefaultMutableTreeNode firstchild = (DefaultMutableTreeNode) selectnode.getFirstChild();
		int nNum = selectnode.getChildCount();
		if (nNum > 1)
			selectnode.remove(firstchild);
	}

	private FileStatus GetSelectedFile() {
		TreePath path = tree.getSelectionPath();
		if (path == null)
			return null;
		DefaultMutableTreeNode selectnode = (DefaultMutableTreeNode) path.getLastPathComponent();

		// 这里加上类型判断
		if (!(selectnode.getUserObject() instanceof MyFile)) {
			return null;
		}
		FileStatus file_select = ((MyFile) selectnode.getUserObject()).file;
		// System.out.println(file_select.getPath());
		return file_select;
	}

	public void readfiles(String file, DefaultMutableTreeNode node) {// 读取所选节点,获取子节点
																		// JOptionPane.showMessageDialog(null,
																		// "--------tree-----readfiles----------"+file);
		FileStatus list[] = hdfs.listStatus(file);
		if (list == null)
			return;
		for (int i = list.length-1; i >= 0; i--) {
			
			FileStatus file_inlist = list[i];
			// String filename = file_inlist.getName();

			if (file_inlist.isDirectory()) {
				parent = new DefaultMutableTreeNode(new MyFile(file_inlist));
				// 添加空白文件夹节点 使子节点显示为文件夹
				FileStatus stubadd = null;
				DefaultMutableTreeNode stub = new DefaultMutableTreeNode(new MyFile(stubadd));
				parent.add(stub);
				//计算所有文件夹大小
				parent.setUserObject(new MyFile(list[i]));
				node.add(parent);
			} else {
				DefaultMutableTreeNode son = new DefaultMutableTreeNode(new MyFile(file_inlist));
				node.add(son);
			}
		}
	}

	public String size(File file) throws IOException {// 读取文件的大小
		FileInputStream fileLength = new FileInputStream(file);
		String sizefile = fileLength.available() + "字节";
		fileLength.close();
		return sizefile;
	}

	public Date lastTime(File file) {
		long lastModified = file.lastModified();// 取得最后一次修改的时间
		Date date = new Date(lastModified);
		date.setTime(lastModified);
		return date;
	}

	public Object[][] fu(FileStatus[] file) {

		Object[][] m = new Object[file.length][7];// 增加文件路径，父路径

		for (int i = 0; i < file.length; i++) {
			m[i][0] = file[i].getPath().getName();
			m[i][1] = HdfsExplorer.GetFileLengthDesc(hdfs.GetFileLen(file[i].getPath()));
			try {
				// 这里有问题,如果是目录,怎么取大小?所以要用if
				// m[i][1] = size(file[i]);
				if (file[i].isDirectory()) {
					m[i][2] = "目录[" + file[i].getOwner() + " " + file[i].getPermission().toString() + "]";
				} else {
//					m[i][1] = HdfsExplorer.GetFileLengthDesc(file[i].getLen());
					m[i][2] = "文件[" + file[i].getOwner() + " " + file[i].getPermission().toString() + "]";
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			m[i][3] = file[i].getPath().toString();
			m[i][4] = file[i].getPath().getParent().toString();
			CalendarEx cal = new CalendarEx(file[i].getModificationTime() / 1000, 0);

			m[i][5] = cal.toString(2);
			if(file[i].isDirectory()){
				m[i][6]=0 ;
			}else{
//				String size = String.valueOf(HdfsExplorer.GetFileLengthDescByKB(file[i].getLen()));
//				m[i][4] = size ;
			m[i][6] =(int)HdfsExplorer.GetFileLengthDescByKB(file[i].getLen());
			}
		}
		return m;
	}

	public int getColumnCount() {
		return 3;
	}

	public int getRowCount(File[] file) {
		return file.length;
	}

	public Object getValueAt(int row, int col) {
		return list[row][col];
	}

	private final static String windows = "com.sun.java.swing.plaf.windows.WindowsLookAndFeel";
	private static String currentLookAndFeel = windows;
	static JFileChooser fileChooser = null;//new JFileChooser();
	public static void main(String[] args) {
		String path1 = System.getProperty("user.dir");
		System.setProperty("hadoop.home.dir", path1+"/hadoop");
		// 加载日志处理模块
		HdfsExplorer disk = new HdfsExplorer(true);
		try {
			disk.treeMake();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	private boolean isSure(String msg) {
		return (JOptionPane.showConfirmDialog(null, msg, "消息", JOptionPane.YES_NO_OPTION) == JOptionPane.YES_OPTION);
	}

	class PopupListener extends MouseAdapter {
		@Override
		public void mousePressed(final MouseEvent e) {
			maybeShowPopup(e);
		}

		@Override
		public void mouseReleased(final MouseEvent e) {
			maybeShowPopup(e);
		}

		private void maybeShowPopup(final MouseEvent e) {
			if (e.isPopupTrigger()) {
				popup.show(e.getComponent(), e.getX(), e.getY());
			}
		}
	}

	class PopupTextListener extends MouseAdapter {
		@Override
		public void mousePressed(final MouseEvent e) {
			maybeShowPopup(e);
		}

		@Override
		public void mouseReleased(final MouseEvent e) {
			maybeShowPopup(e);
		}

		private void maybeShowPopup(final MouseEvent e) {
			if (e.isPopupTrigger()) {
				popupText.show(e.getComponent(), e.getX(), e.getY());
			}
		}
	}

	class PopupTableListener extends MouseAdapter {
		@Override
		public void mousePressed(final MouseEvent e) {
			maybeShowPopup(e);
		}

		@Override
		public void mouseReleased(final MouseEvent e) {
			maybeShowPopup(e);
		}

		private void maybeShowPopup(final MouseEvent e) {
			if (e.isPopupTrigger()) {
				popupTable.show(e.getComponent(), e.getX(), e.getY());
			}
		}
	}

	public void run() {
		// TODO Auto-generated method stub
		
	}
}
