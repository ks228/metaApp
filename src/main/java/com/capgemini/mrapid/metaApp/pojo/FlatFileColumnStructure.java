package com.capgemini.mrapid.metaApp.pojo;


/**
 * ColumnStructure POJO class DB2 SQL 
 * class member as column name
 * column datatype as length,scale,precision
 * column level Information nullable,Default 
 * @author Anurag Udasi
 */

public class FlatFileColumnStructure {
	
	public String columnName;
	public String dataType;
	public Boolean nullable;
	public String length;
	public String precision;
	public String  scale;
	public String Default="9999999999";
	public Integer seq;
	public Boolean key;
	public String Primarykey;
	

	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	public String getPrimarykey() {
		return Primarykey;
	}
	public void setPrimarykey(String primarykey) {
		Primarykey = primarykey;
	}
	public Boolean getKey() {
		return key;
	}
	public void setKey(Boolean key) {
		this.key = key;
	}
	public Integer getSeq() {
		return seq;
	}
	public void setSeq(Integer seq) {
		this.seq = seq;
	}
	public String getDefault() {
		return Default;
	}
	public void setDefault(String default1) {
		Default = default1;
	}
	public String getPrecision() {
	if(precision==null||precision.isEmpty())
		return "0";
		return precision;
	}
	public void setPrecision(String precision) {
		this.precision = precision;
	}
	public String getScale() {
		if(scale==null||scale.isEmpty())
			return "0";
		   return scale;
	}
	public void setScale(String scale) {
		this.scale = scale;
	}
	public String getLength() {
		if(length==null||length.isEmpty())
			return "0";
		return length;
	}
	public void setLength(String length) {
		this.length = length;
	}
	public String getName() {
		return columnName;
	}
	public void setName(String name) {
		this.columnName = name;
	}
	public String getType() {
		return dataType;
	}
	public void setType(String type) {
		this.dataType = type;
	}
	public Boolean getNullable() {
		return nullable;
	}
	public void setNullable(Boolean nullable) {
		this.nullable = nullable;
	}
	

}
