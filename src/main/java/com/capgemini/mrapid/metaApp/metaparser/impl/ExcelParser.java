/**
 * ExcelParser
 * It implements parser method for parsing XML schema and generating JSON schema
 * @author Anurag Udasi
 *
 */
package com.capgemini.mrapid.metaApp.metaparser.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.constants.ExcelConstants;
import com.capgemini.mrapid.metaApp.constants.JSONConstants;
import com.capgemini.mrapid.metaApp.exceptions.EmptyFileExcetion;
import com.capgemini.mrapid.metaApp.metaparser.IParser;
import com.capgemini.mrapid.metaApp.pojo.ColumnStructure;
import com.capgemini.mrapid.metaApp.utils.CommonUtils;
import com.capgemini.mrapid.metaApp.utils.ParserUtils;

public class ExcelParser implements IParser {
	final static Logger log = Logger.getLogger(ExcelParser.class);

	public static ArrayList<Integer> date_column_position;
	public static ArrayList<Integer> time_column_position;
	public static ArrayList<String> length_column_position_and_value;
	public static ArrayList<String> primaryKey;
	public static ArrayList<String> partitionCol;
	public static Integer srcSeqCount, srcRowCount, descSeqCount;


	public JSONObject createJsonfromSchema(ArrayList<String> schema,
			JSONObject metaInformation, Map<String, String> propertiesMap,
			String srcSystem, String country, String table,
			Map<String, String> datatypeMap, Workbook workbook,Map<String,String> excelPropertyMap, String classification) {

		date_column_position = new ArrayList<Integer>();
		time_column_position = new ArrayList<Integer>();
		length_column_position_and_value = new ArrayList<String>();
		primaryKey = new ArrayList<String>();
		partitionCol = new ArrayList<String>();
		srcSeqCount = 1;
		srcRowCount = 0;
		descSeqCount = 1;
		JSONObject excelFinalJSONObject = new JSONObject();
		try{
			ArrayList<ColumnStructure> excelColumnStructure = getSourceStructure(workbook,table);

			ArrayList<ColumnStructure> excelSrcHeaderColumnStructure = new ArrayList<ColumnStructure>();//  getSrcHeaderColumns(propertiesMap,metaInformation,classification);
			ArrayList<ColumnStructure> excelDescHeaderColumnStructure = new ArrayList<ColumnStructure>();//getDescHeaderColumns(propertiesMap,metaInformation,classification);

			primaryKey = getPrimaryKey(excelPropertyMap);
			partitionCol = getPartitionCol(excelPropertyMap);
			
			JSONObject excelSourceSchema = getSourceSchema(excelColumnStructure,excelSrcHeaderColumnStructure);
			JSONObject excelDestinationSchema = getDestinationSchema(excelColumnStructure, excelDescHeaderColumnStructure,datatypeMap);
			JSONArray partitionColArr = getPartitionColSchema(excelColumnStructure, excelDescHeaderColumnStructure,datatypeMap);
			excelDestinationSchema.put(JSONConstants.PARTITION_COLS, partitionColArr);
			JSONObject excelTransformationsSchema = ParserUtils.getTransformationsSchema(propertiesMap);

			date_column_position = ParserUtils.getDateColumnPosition(excelSourceSchema,datatypeMap);
			time_column_position = ParserUtils.getTimeColumnPosition(excelSourceSchema,datatypeMap);
			length_column_position_and_value = ParserUtils.getLengthColumnPosition(excelSourceSchema,datatypeMap);


			JSONObject modifiedMetaInfoObject = getModifiedMetaInfo(metaInformation,excelPropertyMap,propertiesMap,excelColumnStructure,excelSourceSchema);

			excelFinalJSONObject.put(JSONConstants.SOURCE_SCHEMA, excelSourceSchema);
			excelFinalJSONObject.put(JSONConstants.DESTINATION_SCHEMA, excelDestinationSchema);
			excelFinalJSONObject.put(JSONConstants.TRANSFORMATION_SCHEMA, excelTransformationsSchema);
			excelFinalJSONObject.put(JSONConstants.METAINFORMATION_SCHEMA,modifiedMetaInfoObject);
//			System.out.println(excelFinalJSONObject.toString());

		}catch(EmptyFileExcetion e){
			log.error(e.getMessage());
			return null;
		}catch(JSONException e){
			log.error(e.getMessage());
			return null;
		}
		return excelFinalJSONObject;
	}

	/** wrapper method to create modified metaInfo JSON
	 * @param metaInformation
	 * @param excelPropertyMap
	 * @param propertiesMap
	 * @param excelColumnStructure
	 * @param excelSourceSchema
	 * @return modified JSON
	 */
	private JSONObject getModifiedMetaInfo(JSONObject metaInformation,Map<String,String> excelPropertyMap,Map<String,String> propertiesMap,ArrayList<ColumnStructure> excelColumnStructure,JSONObject excelSourceSchema){

		JSONObject modifiedMetaInformationJsonObject = ParserUtils.getModifiedMetaInformation(metaInformation,srcRowCount,date_column_position,time_column_position,length_column_position_and_value,propertiesMap,excelSourceSchema);

//		JSONObject MetInfoWithHeaderandFooter = CommonUtils.updateHeaderAndFooter(modifiedMetaInformationJsonObject,excelPropertyMap);

//		JSONObject metaInfoWithDataColumnPosition = CommonUtils.updateDataColumnPosition(MetInfoWithHeaderandFooter,excelColumnStructure);
		
		JSONObject metaInfoWithDataColumnPosition = CommonUtils.updateDataColumnPosition(modifiedMetaInformationJsonObject,excelColumnStructure);

		return metaInfoWithDataColumnPosition;
	}

	/**
	 * get source schema excel file structure
	 * @param workbook
	 * @param table
	 * @return
	 * @throws EmptyFileExcetion
	 */
	private ArrayList<ColumnStructure> getSourceStructure(Workbook workbook,String table) throws EmptyFileExcetion{
		ArrayList<String> columnHeader = new ArrayList<String>();
		ArrayList<ColumnStructure> sourceColumnStructure = new ArrayList<ColumnStructure>();
		String sheetName = new String();
		Sheet columnSheet = null;
		Cell cellTableName = null;

		for ( int i = 0; i < workbook.getNumberOfSheets(); i++)
		{
			String columnSheetName = workbook.getSheetName(i).toLowerCase(); 
			if(columnSheetName.contains("column"))
			{
				sheetName = workbook.getSheetName(i);
				columnSheet = workbook.getSheet(sheetName);
				break;
			}
		}

		if(columnSheet != null){
			Iterator<Row> rowIterator = columnSheet.iterator();
			while (rowIterator.hasNext()) 
			{
				Row row = rowIterator.next();
				Iterator<Cell> cellIterator = row.cellIterator();
				cellTableName = row.getCell(0);
				ColumnStructure columnStructure = new ColumnStructure(); 
				int columnCount = 0;

				if(row.getRowNum() == 0){
					while (cellIterator.hasNext()) 
					{
						Cell cell = cellIterator.next();
						columnHeader.add(cell.getStringCellValue());		
					}
				}else{
					if(cellTableName != null ){
						String tempTableName = ParserUtils.removeControlChar(cellTableName.getStringCellValue());
						if(tempTableName.equalsIgnoreCase(table)){
							while (cellIterator.hasNext()) 
							{
								Cell cell = cellIterator.next();
								if(columnHeader.get(columnCount).equalsIgnoreCase(ExcelConstants.COLUMN_NAME))
								{
									columnStructure.setName(cell.getStringCellValue());
								}
								else if(columnHeader.get(columnCount).equalsIgnoreCase(ExcelConstants.COLUMN_TYPE))
								{
									columnStructure.setType(cell.getStringCellValue());
								}
								else if(columnHeader.get(columnCount).equalsIgnoreCase(ExcelConstants.COLUMN_SCALE))
								{
									Integer scale = 0;
									if( cell.getCellType() == Cell.CELL_TYPE_BLANK ){
										scale = 0;
									}
									else if(cell.getCellType() == Cell.CELL_TYPE_STRING){
										if(ParserUtils.removeControlChar(cell.getStringCellValue()).equalsIgnoreCase(""))
											scale = 0;
										else	
											scale = Integer.parseInt(cell.getStringCellValue());
									}else{
										scale = (int) cell.getNumericCellValue();      	
									}
									columnStructure.setScale(Integer.valueOf(scale));
								}
								else if(columnHeader.get(columnCount).equalsIgnoreCase(ExcelConstants.COLUMN_PRECISION))
								{
									Integer precision = 0;
									if( cell.getCellType() == Cell.CELL_TYPE_BLANK ){
										precision = 0;
									}
									else if(cell.getCellType() == Cell.CELL_TYPE_STRING){
										if(ParserUtils.removeControlChar(cell.getStringCellValue()).equalsIgnoreCase(""))
											precision = 0;
										else	
											precision = Integer.parseInt(cell.getStringCellValue());
									}else{
										precision = (int) cell.getNumericCellValue();      	
									}
									columnStructure.setPrecision(Integer.valueOf(precision));
								}
								else if(columnHeader.get(columnCount).equalsIgnoreCase(ExcelConstants.COLUMN_NULLABLE))
								{
									Boolean nullable = true;
									if( cell.getCellType() == Cell.CELL_TYPE_BLANK ){
										nullable = true;
									}
									else{
										if(ParserUtils.removeControlChar(cell.getStringCellValue()).equalsIgnoreCase(""))
											nullable = true;
										else	
											if(cell.getStringCellValue().equalsIgnoreCase("N")){
												nullable = false;
											}else{
												nullable = true;
											}
									}
									columnStructure.setNullable(nullable);
								}
								else if(columnHeader.get(columnCount).equalsIgnoreCase(ExcelConstants.COLUMN_DEFAULT))
								{
									columnStructure.setDefault(cell.getStringCellValue());
								}
								else if(columnHeader.get(columnCount).equalsIgnoreCase(ExcelConstants.COLUMN_LENGTH))
								{
									Integer length = 0;
									if( cell.getCellType() == Cell.CELL_TYPE_BLANK ){
										length = 0;
									}
									else if(cell.getCellType() == Cell.CELL_TYPE_STRING){
										if(ParserUtils.removeControlChar(cell.getStringCellValue()).equalsIgnoreCase(""))
											length = 0;
										else	
											length = Integer.parseInt(cell.getStringCellValue());
									}else{
										length = (int) cell.getNumericCellValue();      	
									}
									columnStructure.setLength(Integer.valueOf(length.intValue()));
								}
								else if(columnHeader.get(columnCount).equalsIgnoreCase(ExcelConstants.COLUMN_SEQUENCE)){
									Integer sequence = null;
									if( cell.getCellType() == Cell.CELL_TYPE_BLANK ){
										sequence = null;
									}
									else if(cell.getCellType() == Cell.CELL_TYPE_STRING){
										if(ParserUtils.removeControlChar(cell.getStringCellValue()).equalsIgnoreCase(""))
											sequence = null;
										else	
											sequence = Integer.parseInt(cell.getStringCellValue());
									}else{
										sequence = (int) cell.getNumericCellValue();      	
									}
									columnStructure.setSeq(sequence);

								}
								columnCount++;  
							}
							sourceColumnStructure.add(columnStructure);
						}
					}	
				}
			}
		}else{
			throw new EmptyFileExcetion("Column sheet is empty .....!");
		}
		return sourceColumnStructure;
	}

//	/**Generate Array list of header columns for source schema
//	 * @param propertiesMap
//	 * @param metaInformation
//	 * @param classification
//	 * @return
//	 */
//	private ArrayList<ColumnStructure> getSrcHeaderColumns(
//			Map<String, String> propertiesMap, JSONObject metaInformation,String classification) {
//		ArrayList<ColumnStructure> headerStructure = new ArrayList<ColumnStructure>();
//
//		//Add rowid in source schema (JSON)
//		ColumnStructure uniqueIdStructure = getHeaderColumnStructure(propertiesMap.get(Constants.UNIQUE_ID_COLUMN), true,true);
//		headerStructure.add(uniqueIdStructure);
//
//		//Add CDC columns if srcType is Cdc
//		if(metaInformation.has("srctype")){
//			try {
//				String systemType = metaInformation.getString("srctype");
//				if(systemType.equalsIgnoreCase("CDC")){
//					String[] cdcColumn = propertiesMap.get(Constants.CDC_COULMN).split(
//							"\\,");
//					for (int i = 0; i < cdcColumn.length; i++) {
//						ColumnStructure cdcColStructure = new ColumnStructure();
//						cdcColStructure = getHeaderColumnStructure(cdcColumn[i], false,true);
//						headerStructure.add(cdcColStructure);
//					}
//				}
//			} catch (JSONException e) {
//				log.error(e.getMessage());
//			}			
//		}
//		return headerStructure;
//	}

//	/** Get header columns
//	 * @param propertiesMap
//	 * @param metaInformation
//	 * @param classification
//	 * @return column structure for header columns
//	 */
	private ArrayList<ColumnStructure> getDescHeaderColumns(
			Map<String, String> propertiesMap, JSONObject metaInformation,String classification) {
		ArrayList<ColumnStructure> headerStructure = new ArrayList<ColumnStructure>();
		ColumnStructure uniqueIdStructure = getHeaderColumnStructure(propertiesMap.get(Constants.UNIQUE_ID_COLUMN), true,false);

		headerStructure.add(uniqueIdStructure);

		//add CDC columns if srcType is CDC	
//		String systemType = "";
//		if(metaInformation.has("srctype")){
//			try {
//				systemType = metaInformation.getString("srctype");
//				if(systemType.equalsIgnoreCase("CDC")){
//					String[] cdcColumn = propertiesMap.get(Constants.CDC_COULMN).split(
//							"\\,");
//					for (int i = 0; i < cdcColumn.length; i++) {
//						ColumnStructure cdcColStructure = new ColumnStructure();
//						cdcColStructure = getHeaderColumnStructure(cdcColumn[i],false,false);
//						headerStructure.add(cdcColStructure);
//					}
//				}
//			} catch (JSONException e) {
//				e.printStackTrace();
//			}			
//		}
		return headerStructure;
	}

//	/** Generate column structure for source header columns
//	 * @param columnName - column name
//	 * @param not_null - flag for not null check
//	 * @param is_source
//	 * @return
//	 */
	private ColumnStructure getHeaderColumnStructure(String columnName,Boolean not_null,Boolean is_source) {
		ColumnStructure headerColumnStructure = new ColumnStructure();
		headerColumnStructure.setName(columnName);
		headerColumnStructure.setType("string");
		headerColumnStructure.setLength(0);
		headerColumnStructure.setPrecision(0);
		headerColumnStructure.setScale(0);
		if (not_null)
			headerColumnStructure.setNullable(false);
		else
			headerColumnStructure.setNullable(true);

		if(is_source){
			headerColumnStructure.setSeq(srcSeqCount);
			srcRowCount++;
			srcSeqCount++;
		}else{
			headerColumnStructure.setSeq(descSeqCount);
			descSeqCount++;
		}
		return headerColumnStructure;
	}

	/** Get primary key for given table
	 * @param excelPropertyMap
	 * @return primary key
	 */
	private ArrayList<String> getPrimaryKey(Map<String,String> excelPropertyMap){

		ArrayList<String> primaryKeyArray = new ArrayList<String>();
		String primaryKey = excelPropertyMap.get(ExcelConstants.PRIMARY_KEY);
//		System.out.println("PRIMARY_KEY: " + primaryKey);
//		String[] key = primaryKey.split("\\s+");
		if(primaryKey==null)
			return primaryKeyArray;
		if(primaryKey.isEmpty()||primaryKey.equalsIgnoreCase("null")){
			return primaryKeyArray;
		}
		String[] key = primaryKey.split(",");
		for(int i =0;i<key.length;i++){
			String primaryKeyValue = ParserUtils.cleanString(key[i]);
			primaryKeyArray.add(primaryKeyValue);
		}
//		System.out.println("PRIMARY_KEY ARRAY: " + primaryKeyArray);
		return primaryKeyArray;
	}

	/** Get Partition Column for given table
	 * @param excelPropertyMap
	 * @return Partition columns array
	 */
	private ArrayList<String> getPartitionCol(Map<String,String> excelPropertyMap){
		ArrayList<String> partitionColArray= new ArrayList<String>();
		String partitionCol = excelPropertyMap.get(ExcelConstants.EXCEL_PARTITION_COL);
		if(partitionCol==null)
			return partitionColArray;
		if(partitionCol.isEmpty()||partitionCol.equalsIgnoreCase("null")){
			return partitionColArray;
		}
//		String[] key = partitionCol.split("\\s+");
		String[] key = partitionCol.split(",");
		for(int i =0;i<key.length;i++){
			String partitionColValue = ParserUtils.cleanString(key[i]);
			partitionColArray.add(partitionColValue);
		}
		return partitionColArray;
	}
	
	/**
	 * getSourceSchema: extract Source Schema
	 * @param ColumnStructure : ArrayList contains list of ColumnStructure object
	 * @param HeaderColumnStructure
	 * @return JSONObject : SourceSchema in json format
	 */
	private JSONObject getSourceSchema(
			ArrayList<ColumnStructure> ColumnStructure,
			ArrayList<ColumnStructure> HeaderColumnStructure) {

		ArrayList<Integer> src_primary_key_position = new ArrayList<Integer>();
		ArrayList<String> src_primary_key = new ArrayList<String>();

		JSONObject jObject = new JSONObject();
		try {
			JSONArray jArray = new JSONArray();
//			jArray = ParserUtils.getHeaderJsonArray(HeaderColumnStructure);
			for (ColumnStructure columnStructureList : ColumnStructure) {
				JSONObject dblJSON = new JSONObject();
				dblJSON.put(JSONConstants.COLUMN_NAME, columnStructureList.getName());
				dblJSON.put(JSONConstants.COLUMN_TYPE, columnStructureList.getType());
				dblJSON.put(JSONConstants.COLUMN_LENGTH, columnStructureList.getLength());
				dblJSON.put(JSONConstants.COLUMN_PRECISION, columnStructureList.getPrecision());
				dblJSON.put(JSONConstants.COLUMN_SCALE, columnStructureList.getScale());
				dblJSON.put(JSONConstants.COLUMN_SEQUENCE, srcSeqCount);
				if(!columnStructureList.getNullable()){
					dblJSON.put(JSONConstants.COLUMN_NULLABLE,JSONConstants.NOT_NULL_TRUE);
					if(!primaryKey.isEmpty()){
						for(int i=0;i<primaryKey.size();i++){

							if(ParserUtils.cleanString(columnStructureList.getName()).equalsIgnoreCase(primaryKey.get(i))){
								src_primary_key.add(columnStructureList.getName());
								src_primary_key_position.add(srcSeqCount-1);
							}
							
						}
					}
				}else{
					dblJSON.put(JSONConstants.COLUMN_NULLABLE,JSONConstants.NOT_NULL_FALSE);
				}
				srcSeqCount++;
				srcRowCount++;
				jArray.put(dblJSON);
			}

			JSONArray jArrayPrimaryKey = new JSONArray();
			if (!src_primary_key.isEmpty()) {
				for (String primaryKey : src_primary_key) {
					jArrayPrimaryKey.put(primaryKey);
				}
			}	

			JSONArray jKeyArray = new JSONArray();
			if(src_primary_key_position.size() > 0){
				String keyPositions = StringUtils.join(src_primary_key_position, ',');
				jKeyArray.put(keyPositions);
			}

			jObject.put(JSONConstants.COLUMNS_ARRAY, jArray);	
			jObject.put(JSONConstants.PRIMARY_KEY, jArrayPrimaryKey);
			jObject.put(JSONConstants.PRIMARY_KEY_POSITION, jKeyArray);
		} catch (JSONException jse) {
			jse.printStackTrace();
		}
		return jObject;
	}
	
	private ArrayList<String> getSourceColName(ArrayList<ColumnStructure> ColumnStructure){
		ArrayList<String> columnName = new ArrayList<String>();
		for (ColumnStructure columnStructureList : ColumnStructure) {
			String colName = ParserUtils.cleanString(columnStructureList.getName());
			columnName.add(colName);
		}
		
		return columnName;
	}

	/**
	 * getDestinationSchema: extract Destination Schema
	 * 
	 * @param ColumnStructure
	 *            : ArrayList contains list of ColumnStructure object
	 * @param HeaderColumnStructure
	 * @return JSONObject : DestinationSchema in json format
	 */
	private JSONArray getPartitionColSchema(
			ArrayList<ColumnStructure> ColumnStructure,
			ArrayList<ColumnStructure> HeaderColumnStructure,Map<String,String> datatypeMap) {
		JSONArray partitionInfo = new JSONArray();
		ArrayList<String> srcCols = getSourceColName(ColumnStructure);
		try {
			for(String partCol : partitionCol){
				boolean is_found = false;
				JSONObject dblJSON = new JSONObject();
				dblJSON.put(JSONConstants.COLUMN_NAME, partCol);
				for (ColumnStructure columnStructureList : ColumnStructure) {					
					if(ParserUtils.cleanString(columnStructureList.getName()).equalsIgnoreCase(partCol)){
						is_found = true;
						String orcDataType = ParserUtils.convertDataTypeForHive(columnStructureList.getType(), datatypeMap,columnStructureList.getPrecision(),columnStructureList.getScale());

						dblJSON.put(JSONConstants.COLUMN_TYPE, orcDataType);
						dblJSON.put(JSONConstants.COLUMN_LENGTH,ParserUtils.convertLengthForHive(columnStructureList.getType(),columnStructureList.getLength(), datatypeMap));

						//Set precision value for decimal datatype as 10 if in xml precison is zero
						String dataTypeMapKey = orcDataType + JSONConstants.ORC_PRECISION_SEPERATOR + JSONConstants.ORC_PRECISION_SUFFIX;
						if(datatypeMap.get(dataTypeMapKey) != null && columnStructureList.getPrecision() == 0)
						{					
							dblJSON.put(JSONConstants.COLUMN_PRECISION,
									datatypeMap.get(dataTypeMapKey));
						}
						else
						{
							dblJSON.put(JSONConstants.COLUMN_PRECISION,
									columnStructureList.getPrecision());
						}				

						dblJSON.put(JSONConstants.COLUMN_SCALE, columnStructureList.getScale());
						dblJSON.put(JSONConstants.COLUMN_NULLABLE,JSONConstants.NOT_NULL_FALSE); 
						break;
					}
				
				}
				if(!is_found)
				{
					dblJSON.put(JSONConstants.COLUMN_TYPE, "String");
					dblJSON.put(JSONConstants.COLUMN_LENGTH,100);
					dblJSON.put(JSONConstants.COLUMN_SCALE, 0);
					dblJSON.put(JSONConstants.COLUMN_PRECISION,10);
					dblJSON.put(JSONConstants.COLUMN_NULLABLE,JSONConstants.NOT_NULL_FALSE); 	
				}
				partitionInfo.put(dblJSON);
			}			
		} catch (JSONException jse) {
			jse.printStackTrace();
		}
		return partitionInfo;
	}

	/**
	 * getDestinationSchema: extract Destination Schema
	 * 
	 * @param ColumnStructure
	 *            : ArrayList contains list of ColumnStructure object
	 * @param HeaderColumnStructure
	 * @return JSONObject : DestinationSchema in json format
	 */
	private JSONObject getDestinationSchema(
			ArrayList<ColumnStructure> ColumnStructure,
			ArrayList<ColumnStructure> HeaderColumnStructure,Map<String,String> datatypeMap) {
		JSONObject jObject = new JSONObject();
		ArrayList<Integer> desc_primary_key_position = new ArrayList<Integer>();
		ArrayList<String> desc_primary_key = new ArrayList<String>();

		try {
			JSONArray jArray = new JSONArray();
			jArray = ParserUtils.getHeaderJsonArray(HeaderColumnStructure);
			for (ColumnStructure columnStructureList : ColumnStructure) {
				JSONObject dblJSON = new JSONObject();
				String colName = ParserUtils.cleanString(columnStructureList.getName());
				if(partitionCol.contains(colName)){
					continue;
				}
				dblJSON.put(JSONConstants.COLUMN_NAME, colName);

				String orcDataType = ParserUtils.convertDataTypeForHive(columnStructureList.getType(), datatypeMap,columnStructureList.getPrecision(),columnStructureList.getScale());

				dblJSON.put(JSONConstants.COLUMN_TYPE, orcDataType);
				dblJSON.put(JSONConstants.COLUMN_LENGTH,ParserUtils.convertLengthForHive(columnStructureList.getType(),columnStructureList.getLength(), datatypeMap));

				//Set precision value for decimal datatype as 10 if in xml precison is zero
				String dataTypeMapKey = orcDataType + JSONConstants.ORC_PRECISION_SEPERATOR + JSONConstants.ORC_PRECISION_SUFFIX;
				if(datatypeMap.get(dataTypeMapKey) != null && columnStructureList.getPrecision() == 0)
				{					
					dblJSON.put(JSONConstants.COLUMN_PRECISION,
							datatypeMap.get(dataTypeMapKey));
				}
				else
				{
					dblJSON.put(JSONConstants.COLUMN_PRECISION,
							columnStructureList.getPrecision());
				}				

				dblJSON.put(JSONConstants.COLUMN_SCALE, columnStructureList.getScale());
				dblJSON.put(JSONConstants.COLUMN_SEQUENCE, descSeqCount);

				if(!columnStructureList.getNullable()){
					dblJSON.put(JSONConstants.COLUMN_NULLABLE,JSONConstants.NOT_NULL_TRUE);
					if(!primaryKey.isEmpty()){
						for(int i=0;i<primaryKey.size();i++){
							if(columnStructureList.getName().equalsIgnoreCase(primaryKey.get(i))){
								desc_primary_key.add(columnStructureList.getName());
								desc_primary_key_position.add(descSeqCount-1);
							}
						}
					}
				}else{
					dblJSON.put(JSONConstants.COLUMN_NULLABLE,JSONConstants.NOT_NULL_FALSE); 
				}
				jArray.put(dblJSON);
				descSeqCount++;				
			}

			JSONArray jArrayPrimaryKey = new JSONArray();
			if (!desc_primary_key.isEmpty()) {
				for (String primaryKey : desc_primary_key) {
					jArrayPrimaryKey.put(primaryKey);
				}
			}	

			JSONArray jKeyArray = new JSONArray();
			if(desc_primary_key_position.size() > 0){
				String keyPositions = StringUtils.join(desc_primary_key_position, ',');
				jKeyArray.put(keyPositions);
			}

			jObject.put(JSONConstants.COLUMNS_ARRAY, jArray);	
			jObject.put(JSONConstants.PRIMARY_KEY, jArrayPrimaryKey);
			jObject.put(JSONConstants.PRIMARY_KEY_POSITION, jKeyArray);
		} catch (JSONException jse) {
			jse.printStackTrace();
		}
		return jObject;
	}

}