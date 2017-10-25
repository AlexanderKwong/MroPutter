package cn.mastercom.sssvr.util;

import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

/**
 * @author Webb
 * 2017-4-18下午3:07:36
 */
public class SecretKeySpecTemp{
	private static final String AES = "AES";
	private static final String AES_CBC = "AES/CBC/PKCS5Padding";
	
	public static String aesDecrypt(byte[] input, byte[] key, byte[] iv) {
		byte[] decryptResult = aes(input, key, iv, Cipher.DECRYPT_MODE);
		return new String(decryptResult);
	}
	
	public static String aesEncrypt(byte[] input, byte[] key, byte[] iv) {
		byte[] decryptResult = aes(input, key, iv, Cipher.ENCRYPT_MODE);
		return Hex.encodeHexString(decryptResult);
	}
	
	private static byte[] aes(byte[] input, byte[] key, byte[] iv, int mode) {
		try {
			SecretKey secretKey = new SecretKeySpec(key, AES);
			IvParameterSpec ivSpec = new IvParameterSpec(iv);
			Cipher cipher = Cipher.getInstance(AES_CBC);
			cipher.init(mode, secretKey, ivSpec);
			return cipher.doFinal(input);
		} catch (GeneralSecurityException e) {
			// throw Exceptions.unchecked(e);
			return null;
		}
	}
	public static byte[] hexDecode(String input) {
		try {
			return Hex.decodeHex(input.toCharArray());
		} catch (DecoderException e) {
			throw new IllegalStateException("Hex Decoder exception", e);
		}
	}
	public static void main(String[] args) throws UnsupportedEncodingException {

		String ss = aesEncrypt(
				"18610676365".getBytes(),
				hexDecode("f6b333f905bf62939b4f6d29f257c2bb"),//key1
				hexDecode("1a424b4565be6628a807403d67dce7bd")//key2
		);
		
		//解密  18610676365
		System.out.println(
				aesDecrypt(
						hexDecode(ss),
						hexDecode("f6b333f905bf62939b4f6d29f257c2bb"),//key1
						hexDecode("1a424b4565be6628a807403d67dce7bd")//key2
				)
		);
	}
}

