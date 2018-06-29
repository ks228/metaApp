package com.capgemini.mrapid.metaApp.utils;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESedeKeySpec;
import javax.xml.bind.DatatypeConverter;

/**
 * 
 * Class that encrypts & decrypts data string(password)
 * @author Anuradha Dede
 */

public class PwdDecryptor {

	private static final String strPassPhrase = "abcdefghij123456789ABCDE"; // min 24 chars

	private SecretKeyFactory factory;
	private SecretKey key;
	private Cipher cipher;

	private void loadEncrtpDecrptProperty() {
		try {
			this.factory = SecretKeyFactory.getInstance("DESede");
			key = factory.generateSecret(new DESedeKeySpec(strPassPhrase.getBytes()));
			cipher = Cipher.getInstance("DESede");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Encrypts data
	 * @param Data
	 * @return
	 * @throws Exception
	 */
	public String encrypt(String Data) throws Exception {
		loadEncrtpDecrptProperty();
		cipher.init(Cipher.ENCRYPT_MODE, key);
		String encrtData = DatatypeConverter.printBase64Binary(cipher.doFinal(Data.getBytes()));
		return encrtData;

	}

	/**
	 * Decrypts data
	 * @param encryptedData
	 * @return
	 * @throws Exception
	 */
	public String decrypt(String encryptedData) throws Exception {
		loadEncrtpDecrptProperty();
		cipher.init(Cipher.DECRYPT_MODE, key);
		String decrpData = new String(cipher.doFinal(DatatypeConverter.parseBase64Binary(encryptedData)));
		return decrpData;
	}
	
}
