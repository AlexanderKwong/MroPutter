package cn.mastercom.sssvr.util;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESedeKeySpec;
import javax.crypto.spec.IvParameterSpec;

public class testAes {
	public static byte[] des3EncodeCBC(byte[] key, byte[] keyiv, byte[] data)  
	            throws Exception {  
			SecretKey  deskey = null;  
	        DESedeKeySpec spec = new DESedeKeySpec(key);  
	        SecretKeyFactory keyfactory = SecretKeyFactory.getInstance("desede");  
	        deskey = keyfactory.generateSecret(spec);  
	        Cipher cipher = Cipher.getInstance("desede" + "/CBC/PKCS5Padding");  
	        IvParameterSpec ips = new IvParameterSpec(keyiv);  
	        cipher.init(Cipher.ENCRYPT_MODE, deskey, ips);  
	        byte[] bOut = cipher.doFinal(data);  
	        return bOut;  
	    }  
}
