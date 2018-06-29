package com.capgemini.mrapid.metaApp.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.json.JSONArray;
import org.json.JSONObject;

import com.capgemini.mrapid.metaApp.constants.JSONConstants;
import com.capgemini.mrapid.metaApp.utils.ParserUtils;
import com.capgemini.mrapid.metaApp.constants.ExcelConstants;

 
/**
 * Class : Primary Key Supplementary Class
 * Method "readSupplementaryPrimaryKeys" :-  
 * 		1.Read excel sheet having primary keys for tables from property file.
 * 		2.Construct Map having "Table" as key and "Primary Keys" as value. 
 * Method "getSupplementaryPrimaryKeys" :-
 * 		1.Fetch Primary keys for given table.
 * @author Anurag Udasi
 */
public class PKSupplementaryXML {
	    
	    final static Logger log = Logger.getLogger(PKSupplementaryXML.class);
		
	/**
	 * Get Supplementary primary keys from Map for given table 
	 * @param tableName
	 * @param tablePKValues
	 * @return
	 */
	public static JSONArray getSupplementaryPrimaryKeys(String tableName,Map<String, String> tablePKValues){
		JSONArray jArrayPrimaryKey = new JSONArray();
		try{
			jArrayPrimaryKey.put(tablePKValues.get(tableName));
		}
		catch (Exception e) {
			log.error(e.getMessage());
		} 
		return jArrayPrimaryKey;
	}

	/**
	 * Get Supplementary primary key position from Map for given table
	 * @param tableName
	 * @param tablePKValues
	 * @param sourceSchema
	 * @return
	 */
	public static ArrayList<Integer> getSupplementaryPrimaryKeyPosition(String tableName,Map<String, String> tablePKValues,JSONArray sourceSchema){
		ArrayList<Integer> jArrayPrimaryKeyposition = new ArrayList<Integer>();
		ArrayList<String> primaryKey = new ArrayList<String>();
		try{
			if(tablePKValues.get(tableName).contains(",")){
				String[] primary = tablePKValues.get(tableName).split("\\,");	
				for(int i=0;i< primary.length;i++){
					primaryKey.add(primary[i]);
				}
			}else{
				primaryKey.add(tablePKValues.get(tableName));
			}
			for(int i=0; i<primaryKey.size();i++){
				String primaryKeyValue = ParserUtils.cleanString(primaryKey.get(i));
				for (int j = 0; j < sourceSchema.length(); j++) {
					JSONObject column = (JSONObject) sourceSchema.get(j);
					String name = column.getString(JSONConstants.COLUMN_NAME);
					String seq = column.getString(JSONConstants.COLUMN_SEQUENCE);
					if(primaryKeyValue.equalsIgnoreCase(name)){
						jArrayPrimaryKeyposition.add(Integer.parseInt(seq)-1);
					}
				}
			}
		}
		catch (Exception e) {
			log.error(e.getMessage());
		} 
		return jArrayPrimaryKeyposition;
	}
	
	/**
	 * Read Supplementary primary keys for given country from provided excel sheet
	 * @param workbook
	 * @return
	 */
	public static Map<String,String> readSupplementaryPrimaryKeysDynamic(Workbook workbook){
        Map<String, String> tablePrimaryKeyValues = new HashMap<String, String>();
		ArrayList<String> columnHeader = new ArrayList<String>();
		Sheet primaryKeySheet = null;
		String table ="";
        String key = "";
        
		try {
			primaryKeySheet = workbook.getSheet(ExcelConstants.PRIMARY_KEY_SHEET_NAME);
			if(primaryKeySheet != null){
				Iterator<Row> rowIterator = primaryKeySheet.iterator();
				while (rowIterator.hasNext()) 
				{
					Row row = rowIterator.next();
					Iterator<Cell> cellIterator = row.cellIterator();
					
					if(row.getRowNum() == 0){
						while (cellIterator.hasNext()) 
						{
							Cell cell = cellIterator.next();
							columnHeader.add(cell.getStringCellValue());		
						}
					}else{
						table ="";
				        key = "";
						for (int columnCount=0; columnCount<columnHeader.size(); columnCount++) {
							   Cell cell = row.getCell(columnCount, Row.RETURN_BLANK_AS_NULL);
							   if (cell == null) {
							      continue;
							   } else {
									if(columnHeader.get(columnCount).equalsIgnoreCase(ExcelConstants.PRIMARY_TABLE_NAME))
									{
										table = ParserUtils.cleanString(cell.getStringCellValue());
										
									}
									else if(columnHeader.get(columnCount).equalsIgnoreCase(ExcelConstants.PRIMARY_KEY_COLUMN))
									{
										key = ParserUtils.cleanString(cell.getStringCellValue());
									}
								}
							}
						tablePrimaryKeyValues.put(table, key);
					}
				}
			}
			
		} catch (Exception e) {
			log.error(e.getMessage());
		} 
		return tablePrimaryKeyValues;
	
	}

}
 