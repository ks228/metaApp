package com.capgemini.mrapid.metaApp.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.capgemini.mrapid.metaApp.exceptions.EmptyFileExcetion;
import com.capgemini.mrapid.metaApp.metaparser.ISchemaReader;

/**
 * @author Anurag Udasi
 * Class is to read provided xml schema file
 * and extract information required for parser
 */

public class XMLSchemaFileReader implements ISchemaReader {

	final static Logger log = Logger.getLogger(XMLSchemaFileReader.class);
	
	public ArrayList<String> scanSchemaFile(FileReader reader) {
		// TODO Auto-generated method stub
		StringBuilder buffer = new StringBuilder();
		ArrayList<String> ddlList = new ArrayList<String>();
		BufferedReader bufferedReader = null;
		try {
			bufferedReader = new BufferedReader(reader);
			String line;
			while ((line = bufferedReader.readLine()) != null) {

					buffer.append(line);
			}
			
			if (buffer.toString().isEmpty()) 
				throw new EmptyFileExcetion("Provided Schema file is empty ... !");
			
			ddlList.add(buffer.toString());

		} catch (Exception e) {
			log.error(e.getMessage());
		} finally{
			try{
				if(bufferedReader != null)
					bufferedReader.close();
			}catch(Exception e){
				log.error(e.getMessage());
			}
		}
		return ddlList;
		}

}
