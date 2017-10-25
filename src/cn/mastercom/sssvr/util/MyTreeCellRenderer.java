package cn.mastercom.sssvr.util;

import java.awt.Component;
import javax.swing.ImageIcon;
import javax.swing.JTree;
import javax.swing.tree.DefaultTreeCellRenderer;

public class MyTreeCellRenderer extends DefaultTreeCellRenderer{

    @Override
    public Component getTreeCellRendererComponent(JTree tree, Object value, boolean sel, boolean expanded, boolean leaf, int row, boolean hasFocus) {
        super.getTreeCellRendererComponent(tree, value, sel, expanded, leaf, row, hasFocus);
        if (leaf) {
            setLeafIconByValue(value);
        }
            return this;    
    }
    public void setLeafIconByValue(Object value) {
        ImageIcon ii;
        String str = value.toString();
//        if (str.indexOf(".") > 0) {
//            ii = new ImageIcon("images/txt.gif");
//        } else {
//        	ii = new ImageIcon("images/nullNode.gif");
//        }
//        //System.out.println(value.toString());
        if(str.indexOf(".")==0){
        	ii = new ImageIcon("images/nullNode.gif");
        }else{
        	ii = new ImageIcon("images/txt.gif");
        }
        this.setIcon(ii);
    }
}
