package cn.mastercom.sssvr.util;

import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.AbstractCellEditor;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JOptionPane;
import javax.swing.JTable;
import javax.swing.table.TableCellEditor;
import javax.swing.table.TableCellRenderer;

public class ImgTableRender  extends AbstractCellEditor
		implements TableCellRenderer, ActionListener, TableCellEditor {

	private static final long serialVersionUID = 1L;

	private JButton button = null;
	
	public ImgTableRender() {

		button = new JButton("");
		button.setIcon(new ImageIcon("images/txt.gif"));
		//button.addActionListener(this);

	}

	@Override

	public Object getCellEditorValue() {

		// TODO Auto-generated method stub

		return null;

	}

	@Override

	public Component getTableCellRendererComponent(JTable table, Object value,

			boolean isSelected, boolean hasFocus, int row, int column) {

		// TODO Auto-generated method stub

		return button;

	}

	@Override

	public void actionPerformed(ActionEvent e) {

		// TODO Auto-generated method stub

		
	}

	@Override

	public Component getTableCellEditorComponent(JTable table, Object value,

			boolean isSelected, int row, int column) {

		// TODO Auto-generated method stub

		return button;

	}

}
