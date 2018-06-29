package com.capgemini.mrapid.metaApp.pojo;

import java.util.ArrayList;

/**
 * Class XML table structure pojo
 * @author Anurag Udasi
 *
 */
public class XMLTableStructure {

	private String sourceTableName;
	private String targetUser;
	private String sourceUser;
	private ArrayList<ColumnStructure> SourceColumn;
	private String targetTableName;
	
	
	public String getSourceTableName() {
		return sourceTableName;
	}
	public void setSourceTableName(String sourceTableName) {
		this.sourceTableName = sourceTableName;
	}
	public String getTargetUser() {
		return targetUser;
	}
	public void setTargetUser(String targetUser) {
		this.targetUser = targetUser;
	}
	public String getSourceUser() {
		return sourceUser;
	}
	public void setSourceUser(String sourceUser) {
		this.sourceUser = sourceUser;
	}
	public ArrayList<ColumnStructure> getSourceColumn() {
		return SourceColumn;
	}
	public void setSourceColumn(ArrayList<ColumnStructure> sourceColumn) {
		SourceColumn = sourceColumn;
	}
	public String getTargetTableName() {
		return targetTableName;
	}
	public void setTargetTableName(String targetTableName) {
		this.targetTableName = targetTableName;
	}
}
