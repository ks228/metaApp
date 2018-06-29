package com.capgemini.mrapid.metaApp.schemagenerator.impl;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.metaparser.IParser;
import com.capgemini.mrapid.metaApp.metaparser.impl.DB2Parser;
import com.capgemini.mrapid.metaApp.metaparser.impl.ExcelParser;
import com.capgemini.mrapid.metaApp.metaparser.impl.FlatFileParser;
import com.capgemini.mrapid.metaApp.metaparser.impl.RdbmsParser;
import com.capgemini.mrapid.metaApp.metaparser.impl.XMLParser;

/**
 * Class - Parser Factory that calls parser based in input type
 * @author Anurag Udasi
 *
 */
public class ParserFactory {
	
	/**
	 * @param inputDataType
	 * @return
	 */
	public static IParser createParser(String inputDataType){
		
		if(inputDataType.equalsIgnoreCase(Constants.DDL_SOURCE))
			return new DB2Parser();
		else if(inputDataType.equalsIgnoreCase(Constants.XML_SOURCE))
			return new XMLParser();
		else if(inputDataType.equalsIgnoreCase(Constants.EXCEL_SOURCE))
			return new ExcelParser();
		else if(inputDataType.equalsIgnoreCase(Constants.FLAT_FILE_SOURCE))
			return new FlatFileParser();
		else if(inputDataType.equalsIgnoreCase(Constants.RDBMS_SOURCE))
			return new RdbmsParser();
		return null;			
	}

}
