package cn.mastercom.sssvr.util;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import java.awt.BorderLayout;
import java.awt.*; 

import javax.swing.JTextArea;

import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import javax.swing.JScrollPane;

public class TxtWindow extends JFrame {

	JTextArea textArea = new JTextArea();
	JScrollPane jp=new JScrollPane();

	
	public TxtWindow() {
		setTitle("\u9884\u89C8");
		//setUndecorated(true);
		setSize(900, 600);
		Dimension dim = Toolkit.getDefaultToolkit().getScreenSize();  
	    setLocation((dim.width - getWidth()) / 2, (dim.height - getHeight()) / 2);  
		getContentPane().add(jp, BorderLayout.CENTER);
		jp.setViewportView(textArea);
		
		JPanel panel = new JPanel();
		getContentPane().add(panel, BorderLayout.SOUTH);
		
		JButton btnNewButton = new JButton("\u5173\u95ED");
		btnNewButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				setVisible(false);
			}
		});
		panel.add(btnNewButton);
		
	}
	
	public void setShowFile(String str){
		try {
			textArea.setText(str);
			//textArea.append(str);
		} catch (Exception exception) {
			exception.printStackTrace();
		}
	}
}
