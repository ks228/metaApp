/**
 * JSONConstants
 * Contains all JSON required constants
 * @author Anurag Udasi
 */
package com.capgemini.mrapid.metaApp.constants;

public class JSONConstants {
	
	public static final String SOURCE_SCHEMA = "SourceSchema";
	public static final String DESTINATION_SCHEMA = "DestinationSchema";
	public static final String TRANSFORMATION_SCHEMA = "Transformations";
	public static final String METAINFORMATION_SCHEMA = "MetaInformation";
	public static final String COLUMN_NAME="name";
	public static final String COLUMN_TYPE = "type";
	public static final String COLUMN_LENGTH = "length";
	public static final String COLUMN_PRECISION = "precision";
	public static final String COLUMN_SCALE = "scale";
	public static final String COLUMN_NULLABLE = "nullable";
	public static final String NOT_NULL_TRUE = "NO";
	public static final String NOT_NULL_FALSE = "YES";
	public static final String COLUMN_DEFAULT = "default";
	public static final String COLUMN_SEQUENCE = "seq";
	public static final String COLUMNS_ARRAY = "columns";
	public static final String PRIMARY_KEY = "PrimaryKey";
	public static final String PRIMARY_KEY_POSITION = "PrimaryKeyPosition";
	public static final String XML_TABLE_TAG = "TableMapping";
	public static final String SOURCE_COLUMN_COUNT = "srccolumncount";
	public static final String SOURCE_DATE_COLUMN_POSITION = "srcdatecolumnposition";
	public static final String SOURCE_TIME_COLUMN_POSITION = "srctimeecolumnposition";
	public static final String SOURCE_COLUMN_LENGTH_POSITION_VALUE = "lengthcolumnpositionandvalue";
	public static final String AVRO_DATATYPE_PREFIX = "avro_";
	public static final String ORC_DATATYPE_PREFIX = "datatype";
	public static final String ORC_LENGTH_PREFIX = "length";
	public static final String ORC_PRECISION_SUFFIX = "precision";
	public static final String ORC_PRECISION_SEPERATOR = ".";
	public static final String SEPERATOR="\\.";	
	public static final String ORC_DEFAULT_PREFIX = "default";
	public static final String AVRO_NAME = "name";
	public static final String AVRO_TYPE = "type";
	public static final String AVRO_FIELDS = "fields";
	public static final String AVRO_LOGIC_TYPE = "logicalType";
	public static final String AVRO_RECORD= "record";
	public static final String AVRO_SCALE= "scale";
	public static final String AVRO_PRECISION = "precision";
	public static final String AVRO_PROP_LOGIC_TYPE = "logicaltype";
	public static final String TARGET_TABLE_TYPE = "targettabletype";
	public static final String TARGET_DB_FORMAT = "tgtdbformat";
	public static final String TARGET_TABLE_COMPRESSION = "datacompression";
	public static final String PARTITION_COLS = "partitioncol";
}
