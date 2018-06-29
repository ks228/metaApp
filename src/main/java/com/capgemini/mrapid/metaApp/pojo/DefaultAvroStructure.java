package com.capgemini.mrapid.metaApp.pojo;

/**
 * Default Avro structure POJO
 * @author Pallavi Kadam
 *
 */
public class DefaultAvroStructure {
	
	public String columnname;
	public String Default;
	public String datatype;
	public String getColumnname() {
		return columnname;
	}
	public void setColumnname(String columnname) {
		this.columnname = columnname;
	}
	public String getDefault() {
		return Default;
	}
	public void setDefault(String default1) {
		Default = default1;
	}
	public String getDatatype() {
		return datatype;
	}
	public void setDatatype(String datatype) {
		this.datatype = datatype;
	}
	

}
