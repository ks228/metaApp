package com.capgemini.mrapid.metaApp.pojo;

/**
 * Schema Evolution pojo
 * @author Anurag Udasi
 *
 */

public class SchemaEvolutionPojo {

	String tableName;
	String version;
	
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getVersion() {
		return version;
	}
	public void setVersion(String version) {
		this.version = version;
	}
	
}
