/**
* The Constants class for declaring Application variables
* @author  Anurag Udasi 
*/

package com.capgemini.mrapid.metaApp.constants;

public class Constants {

	public static final String TABLE_LIST_PATH = "_TABLE_LIST_PATH";
	public static final String COUNTRY_PROPERTY_PATH = "_PROPERTIES_PATH";
	public static final String HADOOP_CONFIGURATION_PATH = "HADOOP_CONFIGURATION_PATH";
	public static final String PROPERTIES_PATH = "_PROPERTIES_PATH";
	public static final String DATATYPE_MAPPING_PATH="_DATATYPE_MAPPING_PATH";
	public static final String SCHEMA_EVAL_LOG_FILE_PATH="schemaevolution";
	public static final String INPUT_FILE_PATH="_INPUT_FILE_PATH";
	public static final String HDFS_URL = "HDFS_URL";
	public static final String ENV = "ENV";
	public static final String TYPE = "type";
	public static final String VERSION = "version";
	public static final String TABLELIST = ".tablelist";
	public static final String LOCAL_BASE_PATH = "LOCAL_BASE_PATH";
	public static final String LOCAL_INPUT_PATH = "LOCAL_INPUT_PATH";
	public static final String HDFS_BASE_PATH = "HDFS_BASE_PATH";
	public static final String HIVE_BASE_PATH = "HIVE_BASE_PATH";
	public static final String HIVE_BASE_PATH_STAGING = "HIVE_BASE_PATH_STAGGING";
	public static final String HIVE_BASE_PATH_PROCESSING = "HIVE_BASE_PATH_PROCESSING";
	public static final String HIVE_BASE_PATH_TEMP = "HIVE_BASE_PATH_TEMP";
	public static final String HIVE_STAGING_DB = "HIVE_STAGING_DB";
	public static final String HIVE_PROCESSING_DB = "HIVE_PROCESSING_DB";
	public static final String HIVE_TEMP_DB = "HIVE_TEMP_DB";
	public static final String SCHEMA_DIR = "INPUT_SCHEMA_DIR";
	public static final String JSON_OUTPUT_DIR = "JSON_OUTPUT_DIR";
	public static final String AVSC_OUTPUT_DIR = "AVSC_OUTPUT_DIR";
	public static final String DDL_OUTPUT_DIR = "HIVE_DDL_OUTPUT_DIR";
	public static final String ZOOKEEPER_PORT = "ZOOKEEPER_PORT";
	public static final String ZOOKEEPER_ZNODE = "ZOOKEEPER_ZNODE";
	public static final String MAXCLIENTCNXNS = "MAXCLIENTCNXNS";
	public static final String TARGET_DB_TYPE="TGT_DB_TYPE";
	public static final String TARGET_DB = "TGT_DB";
	public static final String TARGET_DELIMETED= "TGT_DELIM";
	public static final String TARGET_DB_FORMAT= "TGT_DB_FORMAT";
	public static final String TARGET_PROCESSED_FORMAT= "TGT_PROCESSED_FORMAT";
	public static final String TARGET_DATE_FORMAT= "TGT_DATE_FORMAT";
	public static final String TARGET_TIME_FORMAT="TGT_TIME_FORMAT";
	public static final String SOURCE_NAME="SRCNAME";
	public static final String SOURCE_TYPE = "SRCTYPE";
	public static final String SOURCE_DB_TYPE= "SRCDBTYPE";
	public static final String SOURCE_TABLE_SPACE= "SRCTABLESPACE";
	public static final String SOURCE_DELIMETED= "SRCDELIM";
	public static final String SOURCE_FREQUENCY= "SRCFREQ";
	public static final String SOURCE_DATE_FORMAT= "SRCDATEFORMAT";
	public static final String SOURCE_TIME_FORMAT= "SRCTIMEFORMAT";
	public static final String SOURCE_INPUT_TYPE= "SRCINPUTTYPE";
	public static final String UNIQUE_ID_COLUMN="UNIQUE_ID_COL";
	public static final String BUSS_DATE_COLS="BUSS_DATE_COLS";
	public static final String JOURNAL_TIME_COLS="JOURNAL_TIME_COLS";
	public static final String BUSS_JOURNAL_DATE_TIME_COLS="BUSS_JOURNAL_DATE_TIME_COLS";
	public static final String CDC_COULMN="CDC_COLS";
	public static final String DDL_SOURCE="DDL";
	public static final String XML_SOURCE="XML";
	public static final String LOG_FILE_PATH="log_file_path";
	public static final String EXCEL_SOURCE="EXCEL";
	public static final String RDBMS_SOURCE="RDBMS";
	public static final String FLAT_FILE_SOURCE="FLATFILE";
	public static final String JSON_FORMAT=".json";
	public static final String AVSC_FORMAT=".avsc";
	public static final String AVRO_FORMAT="_avro";
	public static final String TEXT_FORMAT="_text";
	public static final String ORC_FORMAT="_orc";
	public static final String DDL_FORMAT=".hql";
	public static final String SQL_FORMAT=".sql";
	public static final String HBASE_SITE_XML_PATH="HBASE_SITE_XML_PATH";
	public static final String TABLE_PARTITION="PARTITION_COL";
	public static final String PARTITION_TYPE="string";
	public static final String TIME_ZONE_VALUE="TIME_ZONE_VALUE";
	public static final String MRAPID_SUBSCRIPTION="MRAPID_SUBSCRIPTION";
	public static final String HIVE_CONNECTION_URL="HIVE_CONNECTION_URL";
	public static final String HIVE_USER_NAME="HIVE_USER_NAME";
	public static final String HIVE_CONNECTION_PWD="HIVE_CONNECTION_PWD";
	public static final String HIVE_SELECT_QUERY="HIVE_SELECT_QUERY";
	public static final String HIVE_SELECT_ALLCOUN_QUERY="HIVE_SELECT_ALLCOUN_QUERY";
	public static final String VALID_TO="VALID_TO";
	public static final String METADATA_PROPERTIES_PATH = "_METADATA_PROPERTIES_PATH";
	public static final String METADATA_HDFS_URL = "METADATA_HDFSURL";
	public static final String METADATA_DBNAME = "common_hive_db";
	public static final String METADATA_HDFS_BASE_PATH = "METADATA_HDFS_BASE_PATH";
	public static final String METADATA_ALL_COUNTRY = "_all_countries";
	public static final String METADATA_ALL_COLUMNS = "_all_tab_columns";
	public static final String METADATA_ALL_TABLES = "_all_tables";
	public static final String METADATA_DEL = "METADATA_DEL";
	public static final String ENCODINGCHARSET = "CHAR_SET_ENCODING";
	public static final String ORC_COMPRESSION = "ORC_COMPRESSION";
	public static final String XML_TABLE_TAG = "TableMapping";
	public static final String ERROR_FILE_PATH = "ERROR_FILE_PATH";
	public static final String HIVE_INPUT_FORMAT_PROPERTY= "HIVE_INPUT_FORMAT";
	public static final String HIVE_TXN_MANAGER_PROPERTY = "HIVE_TXN_MANAGER";
	public static final String HIVE_EXEC_DYNAMIC_PARTITION_MODE_PROPERTY = "HIVE_EXEC_DYNAMIC_PARTITION_MODE";
	public static final String HIVE_TXN_TIMEOUT_PROPERTY = "HIVE_TXN_TIMEOUT";
	public static final String HIVE_ENFORCE_BUCKETING_PROPERTY = "HIVE_ENFORCE_BUCKETING";
	public static final String HIVE_SUPPORT_CONCURRENCY_PROPERTY = "HIVE_SUPPORT_CONCURRENCY";
	public static final String HIVE_COMPACTOR_INITIATO_ON_PROPERTY = "HIVE_COMPACTOR_INITIATO_ON";
	public static final String HIVE_COMPACTOR_WORKER_THREADS_PROPERTY = "HIVE_COMPACTOR_WORKER_THREADS";
	public static final String DATANUCLEUS_CONNECTIONPOOLINGTYPE_PROPERTY  = "DATANUCLEUS_CONNECTIONPOOLINGTYPE";
	public static final String SUPPLEMENTARY_PK = "SUPPLEMENTARY_PK";
	public static final String FDVALUE="FDVALUE";
	public static final String FDFLAG = "FDFLAG";
	public static final String LENGTH_COLUMN= "lengthcolumn";
	public static final String DATE_COLUMN= "datecolumn";
	public static final String TIME_COLUMN= "timecolumn";
	public static final String IS_SCHEMA_EVOLUTION = "ISSCHEMAEVOLUTION";
	public static final String SCHEMA_EVAL_LOG_FILE_PATH1="SCHEMA_EVAL_LOG_FILE_PATH";
	public static final String HDFS_ARCHIVE_PATH="HDFS_ARCHIVE_PATH";
	public static final String DEPLOYMENT_PROPERTIES_PATH="_DEPLOYMENT_PROPERTIES_PATH";
	public static final String COMMON_BASE_PATH = "common_base_path";
	public static final String TABLE_TYPE_RECON = "recon";
	public static final String COMMON_HIVE_LOCATION = "common_hive_location_country";
	public static final String AVRO_DEFAULT = "AVRODEFAULT";
	public static final String AVRO_INCOMPATIBLE_TYPES = "avro.incompatible.types";
	public static final String INCOMPATIBLE_TYEP_VALUE = "incompatible";
	public static final String MYSQL_DRIVER_CLASS= "MYSQL_DRIVER_CLASS";
	public static final String MYSQL_CONNECTION_URL= "MYSQL_CONNECTION_URL";
	public static final String MYSQL_USER= "MYSQL_USER";
	public static final String MYSQL_PASSWORD= "MYSQL_PASSWORD";
	public static final String SOURCE_DEV_OPS= "SOURCE_DEV_OPS";
	public static final String SOURCE_METAAPP_DEV_OPS= "SOURCE_METAAPP_DEV_OPS";
	public static final String MYSQL_SRC_TYPE= "MYSQL_SRC_TYPE";
	public static final String MYSQL_SRC_DB_ID= "MYSQL_SRC_DB_ID";
	public static final String MYSQL_DB_NAME= "MYSQL_DB_NAME";
	public static final String IS_HEADER_FOOTER= "IS_HEADER_FOOTER";
}
