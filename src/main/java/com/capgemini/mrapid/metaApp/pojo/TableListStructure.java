package com.capgemini.mrapid.metaApp.pojo;

/**
 * TableListStructure POJO class contains Table List
 * Structure eg:TableName:Classification (TRANSACTION,DALTA)
 * @author Anurag Udasi
 */

public class TableListStructure {
	public String name;
	public String classification;
	public String threshold;
	public String partition_col;
	public int partition_col_val;

	
	public int getPartition_col_val() {
		return partition_col_val;
	}
	public void setPartition_col_val(int partition_col_val) {
		this.partition_col_val = partition_col_val;
	}
	public String getPartition_col() {
		return partition_col;
	}
	public void setPartition_col(String partition_col) {
		this.partition_col = partition_col;
	}
	public String getThreshold() {
		return threshold;
	}
	public void setThreshold(String threshold) {
		this.threshold = threshold;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getClassification() {
		return classification;
	}
	public void setClassification(String classification) {
		this.classification = classification;
	}

}
