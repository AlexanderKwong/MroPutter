/**
 * 
 */
package cn.mastercom.sssvr.main;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import javax.swing.BorderFactory;
import javax.swing.JDialog;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.WindowConstants;

import cn.mastercom.sssvr.util.ComUtil;
import cn.mastercom.sssvr.util.GBC;

/**
 * @author tangjzh
 * 
 */
@SuppressWarnings("serial")
public class MainFrame extends JFrame {

	private JPanel myContentPane = null;

	/**
	 * This is the default constructor
	 */
	public MainFrame() {
		super();
		addWindowListener(new WindowAdapter() {
			@Override
			public void windowClosing(WindowEvent e) {
				exit();
			}
		});
		initialize();
	}

	public void exit() {
		Object[] options = { "确定", "取消" };
		JOptionPane pane2 = new JOptionPane("真想退出吗?",
				JOptionPane.QUESTION_MESSAGE, JOptionPane.YES_NO_OPTION, null,
				options, options[1]);
		JDialog dialog = pane2.createDialog(this, "警告");
		dialog.setVisible(true);
		Object selectedValue = pane2.getValue();
		if (selectedValue == null || selectedValue == options[1]) {
			setDefaultCloseOperation(WindowConstants.DO_NOTHING_ON_CLOSE);

		} else if (selectedValue == options[0]) {
			MainSvr.bExitFlag = true;
			
			
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			setDefaultCloseOperation(EXIT_ON_CLOSE);
		}
	}

	/**
	 * This method initializes this
	 * 
	 * @return void
	 */
	private void initialize() {

		this.setTitle("MRO文件搬移程序");
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
			paneTmp.add(new JScrollPane(MainSvr.consoleTextArea), new GBC(0, 0)
					.setFill(GridBagConstraints.BOTH).setWeight(100, 100));
			myContentPane.add(ComUtil.getOuterPane(paneTmp, 1, 8, 1, 8),
					java.awt.BorderLayout.CENTER);
		}
		return myContentPane;
	}

}
