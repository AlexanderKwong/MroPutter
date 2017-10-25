package cn.mastercom.telwake.omc;


import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import cn.mastercom.sssvr.main.MainSvr;

/**
 * @author qxq20072007-4-11上午10:50:51
 * 向共享内存中写本程序的共享内存区域
 * 
 * 
 */
public class WriteSelfInfo extends Thread{
	
	private String memoryName ;

	/**
	 * 写共享内存信息
	 * @param offset
	 * @param bts
	 */
	public static native void writeShareMemory(String memoryName,int offset,byte[] bts);
	
	/**
	 * 得到当前的进程ＩＤ
	 * @return
	 */
	public static native int getCurrentProcessID();
	
	public static int toWindowsPlatFormValue(int value){
		byte b0 = (byte) ((value >> 24)&0x00FF);
		byte b1 = (byte) ((value >> 16)&0x00FF);
		byte b2 = (byte) ((value >> 8)&0x00FF);
		byte b3 = (byte) ((value)&0x00FF);
		return b3<<24|b2<<16|b1<<8|b0;
	}

	
	private static byte[] getForWriteBytes(){
		ByteArrayOutputStream baos = new ByteArrayOutputStream(4);
		DataOutputStream dos = new DataOutputStream(baos);
		try {
			//程序状态标识信息
			dos.writeInt(toWindowsPlatFormValue(1));
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return baos.toByteArray();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			WriteSelfInfo wsi = new WriteSelfInfo("EaiIntfSvr");
			wsi.writeMemory(0, getForWriteBytes());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		
		while(MainSvr.bExitFlag == false)
		{
			try {
				WriteSelfInfo wsi = new WriteSelfInfo("EaiIntfSvr");
				wsi.writeMemory(0, getForWriteBytes());
				//System.out.println("WriteShareMemory Succeed.");
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				Thread.sleep(30000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	

	public void writeMemory(int offset,byte[] bts){
		try {
			writeShareMemory(memoryName,offset,bts);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public WriteSelfInfo(String memName){
		this.memoryName = memName;
	}

	public WriteSelfInfo() {

	}

	public String getMemoryName() {
		return memoryName;
	}
	
	static {
		try {
			System.loadLibrary("lib/WriteShareMemory");
			System.out.println("Load WriteShareMemory Dll.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
