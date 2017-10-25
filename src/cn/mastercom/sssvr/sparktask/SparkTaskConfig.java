package cn.mastercom.sssvr.sparktask;


public class SparkTaskConfig extends FileConfigure{

	public SparkTaskConfig(String confPath) {
		super(confPath);
		getAutoDo();
		getSparkCmd();
	}
	
    public String getSparkCmd()
	{
    	String value = getValue("sparkCmd", "").toString();
    	System.out.println(value);
    	return getValue("sparkCmd", "").toString();
	}
    
    //DeteleDays
//    public String getDeteleDays()
//	{
//		return getValue("DeteleDays", "").toString();
//	}

//    public boolean setDeteleDays(String DeteleDays)
//	{
//		return setValue("DeteleDays", DeteleDays);
//	}
    
    //auto do
    public boolean getAutoDo()
	{
		return Boolean.parseBoolean(getValue("AutoDo", "false").toString());
	}

    public boolean setAutoDo(boolean AutoDo)
	{
		return setValue("AutoDo", AutoDo);
	} 
	
}
