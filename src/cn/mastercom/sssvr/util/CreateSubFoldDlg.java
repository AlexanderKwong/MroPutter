package cn.mastercom.sssvr.util;

import java.awt.BorderLayout;
import java.awt.FlowLayout;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;
import javax.swing.JLabel;
import javax.swing.JTextField;

public class CreateSubFoldDlg extends JDialog
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6293368277852813204L;
	private final JPanel contentPanel = new JPanel();
	

	/**
	 * Launch the application.
	 */
	public static void main(String[] args)
	{
		try
		{
			CreateSubFoldDlg dialog = new CreateSubFoldDlg();
			dialog.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
			dialog.setVisible(true);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public JTextField tfSubFoldName;
	private JPanel GetContentPanel()
	{
		contentPanel.setBorder(new EmptyBorder(5, 5, 5, 5));
		contentPanel.setLayout(null);
		
		JLabel label = new JLabel("\u8BF7\u8F93\u5165\u5B50\u76EE\u5F55\u7684\u540D\u79F0");
		label.setBounds(10, 23, 125, 15);
		contentPanel.add(label);
		
		tfSubFoldName = new JTextField();
		tfSubFoldName.setBounds(20, 48, 350, 21);
		contentPanel.add(tfSubFoldName);
		tfSubFoldName.setColumns(10);
		return contentPanel;
	}
	/**
	 * Create the dialog.
	 */
	public CreateSubFoldDlg()
	{
		setTitle("\u521B\u5EFA\u5B50\u76EE\u5F55");
		setBounds(100, 100, 415, 242);
		getContentPane().setLayout(new BorderLayout());
		
		getContentPane().add(GetContentPanel(), BorderLayout.CENTER);
		{
			JPanel buttonPane = new JPanel();
			buttonPane.setLayout(new FlowLayout(FlowLayout.RIGHT));
			getContentPane().add(buttonPane, BorderLayout.SOUTH);
			{
				JButton okButton = new JButton("\u786E\u5B9A");
				okButton.setActionCommand("OK");
				buttonPane.add(okButton);
				getRootPane().setDefaultButton(okButton);
			}
			{
				JButton cancelButton = new JButton("\u53D6\u6D88");
				cancelButton.setActionCommand("Cancel");
				buttonPane.add(cancelButton);
			}
		}
	}
}
