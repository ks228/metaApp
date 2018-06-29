/**
 * 
 */
package com.capgemini.mrapid.metaApp.utils;



import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;

/**
 * @author audasi
 *
 */
public class Decrypt {
	public static String decrypt(String strToDecrypt, String secret)
    {
        try
        {
        	SecretKeySpec secretKey = Key.setKey(secret);
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
            cipher.init(Cipher.DECRYPT_MODE, secretKey);
            return new String(cipher.doFinal(Base64.decodeBase64(strToDecrypt)));
        }
        catch (Exception e)
        {
            System.out.println("Error while decrypting: " + e.toString());
        }
        return null;
    }
}
