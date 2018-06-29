/**
 * 
 */
package com.capgemini.mrapid.metaApp.utils;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import javax.crypto.spec.SecretKeySpec;

/**
 * @author audasi
 *
 */
public class Key {
	
	   private static SecretKeySpec secretKey;
	   private static byte[] key;

	  public static SecretKeySpec setKey(String myKey)
	    {
	        MessageDigest sha = null;
	        try {
	            key = myKey.getBytes("UTF-8");
	            sha = MessageDigest.getInstance("SHA-1");
	            key = sha.digest(key);
	            key = Arrays.copyOf(key, 16);
	            secretKey = new SecretKeySpec(key, "AES");
	            return secretKey;
	        }
	        catch (NoSuchAlgorithmException e) {
	            e.printStackTrace();
	            return null;
	        }
	        catch (UnsupportedEncodingException e) {
	            e.printStackTrace();
	            return null;
	        }
	    }
}
