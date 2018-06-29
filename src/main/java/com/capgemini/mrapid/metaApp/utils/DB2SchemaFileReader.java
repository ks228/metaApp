package com.capgemini.mrapid.metaApp.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.capgemini.mrapid.metaApp.exceptions.EmptyFileExcetion;
import com.capgemini.mrapid.metaApp.metaparser.ISchemaReader;



/**
 * Class reads DB2 format schema
 * @author Anurag Udasi
 * DB2SchemaFileReader
 * Class implements method for reading schema files
 * and extract information required for parser  
 */
public class DB2SchemaFileReader implements ISchemaReader {

	final static Logger log = Logger.getLogger(DB2SchemaFileReader.class);
	
	public ArrayList<String> scanSchemaFile(FileReader reader) {
		// TODO Auto-generated method stub
				StringBuilder buffer = new StringBuilder();
			ArrayList<String> ddlList = new ArrayList<String>();
			BufferedReader bufferedReader = null;
			try {
				bufferedReader = new BufferedReader(reader);
				String line;
				while ((line = bufferedReader.readLine()) != null) {

					if (line.contains(";")) {
						buffer.append(line);
						ddlList.add(buffer.toString());
						buffer.setLength(0);
					} else {
						buffer.append(line);
						buffer.append("\n");
					}
				}
				if (ddlList.size() == 0) 
					throw new EmptyFileExcetion("Provided Schema file is empty ... !");
				
				reader.close();
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
