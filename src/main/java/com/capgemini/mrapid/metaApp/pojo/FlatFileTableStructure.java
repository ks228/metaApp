package com.capgemini.mrapid.metaApp.pojo;

import java.util.ArrayList;


/**
 * Flat file table structure POJO
 * @author Anurag Udasi
 *
 */
public class FlatFileTableStructure 
{
	private String source_table_name;
	private String source_name;
	private String source_type;
	private String source_dbtype;
	private String source_delimiter;
	private String source_charset_encoding;
	private String frequency;
//	private String date_format;
//	private Date date_format;
	private int time_format;
	private int date_format;
	private String table_type;
	private ArrayList<FlatFileColumnStructure> SourceColumn;
	
	
	
	public int getdate_format() {
		return date_format;
	}
	public void setdate_format(int date_format) {
		this.date_format = date_format;
	}
	
	
	
	public String getSourceTableName() {
		return source_table_name;
	}
	public void setSourceTableName(String source_table_name) {
		this.source_table_name = source_table_name;
	}
	
	public String getsource_name() {
		return source_name;
	}
	public void setsource_name(String source_name) {
		this.source_name = source_name;
	}
	
	public String getsource_type() {
		return source_type;
	}
	public void setsource_type(String source_type) {
		this.source_type = source_type;
	}
	
	public String getsource_dbtype() {
		return source_dbtype;
	}
	public void setsource_dbtype(String source_dbtype) {
		this.source_dbtype = source_dbtype;
	}
	
	public String getsource_delimiter() {
		return source_delimiter;
	}
	public void setsource_delimiter(String source_delimiter) {
		this.source_delimiter = source_delimiter;
	}
	
	public String getsource_charset_encoding() {
		return source_charset_encoding;
	}
	public void setsource_charset_encoding(String source_charset_encoding) {
		this.source_charset_encoding = source_charset_encoding;
	}
	
	public String getfrequency() {
		return frequency;
	}
	public void setfrequency(String frequency) {
		this.frequency = frequency;
	}

	public int gettime_format() {
		return time_format;
	}
	public void settime_format(int time_format) {
		this.time_format = time_format;
	}
	
	public String gettable_type() {
		return table_type;
	}
	public void settable_type(String table_type) {
		this.table_type = table_type;
	}
	
	public ArrayList<FlatFileColumnStructure> getSourceColumn() {
		return SourceColumn;
	}
	public void setSourceColumn(ArrayList<FlatFileColumnStructure> sourceColumn) {
		SourceColumn = sourceColumn;
	}
}
