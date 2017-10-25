package cn.mastercom.filterSpark;

import java.util.ArrayList;
import java.util.List;

import org.dom4j.DocumentException;

import cn.mastercom.sssvr.util.FilterReadFromXml;

public class TableFiledAndAttr implements java.io.Serializable{
	String FieldSplitSign = " ";
	String tableField = "";
	String tableAttr = "";
	
	public String tableFilter(String tablenamestr){
	FilterReadFromXml filterReadFromXml = new FilterReadFromXml();
	String[] tableFieldAndAttr = null;
	try {
		tableFieldAndAttr=filterReadFromXml.getFieldAndAttr(tablenamestr);
	} catch (DocumentException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
//	tableField = tableFieldAndAttr[0];
//    tableAttr = tableFieldAndAttr[1];
//    FieldSplitSign = tableFieldAndAttr[2];
	String tableFiledAttr=tablenamestr;
    for (int i = 0; i < tableFieldAndAttr.length; i++) {
//		if(i==0){
//			tableFiledAttr=tableFieldAndAttr[i];
//		}
//		else{
			tableFiledAttr=tableFiledAttr+"&"+tableFieldAndAttr[i];
//		}
	}
    return tableFiledAttr;
    
	}
}
