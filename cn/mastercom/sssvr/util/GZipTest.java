package cn.mastercom.sssvr.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;


public class GZipTest {

	public static void decode(InputStream in){
		try {
			GZIPInputStream gzipin = new GZIPInputStream(in);
			byte []buff = new byte[102400];
			while (gzipin.read(buff, 0, 102400) > 0) {
				String info = new String(buff,"UTF-8");
				System.out.println(info);
				
			}
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}
	/**
	 * @param args
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException {
		decode(new FileInputStream("z"));

	}

}
