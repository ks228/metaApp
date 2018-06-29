package com.capgemini.mrapid.metaApp.schemagenerator.impl;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.metaparser.ISchemaReader;
import com.capgemini.mrapid.metaApp.utils.DB2SchemaFileReader;
import com.capgemini.mrapid.metaApp.utils.XMLSchemaFileReader;


/**
 * Class Schema File Reader Factory class
 * @author Anurag Udasi
 *
 */

public class FileReaderFactory {
	
	/**
	 * Creates schema file reader based on type
	 * @param inputDataType
	 * @return
	 */
	public static ISchemaReader createSchemaReader(String inputDataType){
		
		if(inputDataType.equalsIgnoreCase(Constants.DDL_SOURCE))
			return new DB2SchemaFileReader();
		else if(inputDataType.equalsIgnoreCase(Constants.XML_SOURCE))
			return new XMLSchemaFileReader();
		else if(inputDataType.equalsIgnoreCase(Constants.FLAT_FILE_SOURCE))
			return new XMLSchemaFileReader();
		return null;
				
	}

}
