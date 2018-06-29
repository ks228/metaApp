package com.capgemini.mrapid.metaApp.pojo;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * <srcSystem>_all_tables column structure Pojo class 
 * @author Anuradha Dede
 *
 */
public class AllTableColumnStructure {
	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	Date date = new Date();
	public String source;
	public String country_name;
	public String version="1";
	public String column_name;
	public String table_name;
	public int data_precision;
	public int data_length;
	public String data_type;
	public String primary_column_indicator;
	public String valid="Y";
	public String nullable;
	public String column_order;
	public String valid_from;
	public String valid_to;
	public String creation_date;
	public String last_update_date;
	public String current_version="Y";
	public String comment;
	
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	public String getCountry_name() {
		return country_name;
	}
	public void setCountry_name(String country_name) {
		this.country_name = country_name;
	}
	public String getVersion() {
		return version;
	}
	public void setVersion(String version) {
		this.version = version;
	}
	public String getColumn_name() {
		return column_name;
	}
	public void setColumn_name(String column_name) {
		this.column_name = column_name;
	}
	public String getTable_name() {
		return table_name;
	}
	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}
	public int getData_precision() {
		return data_precision;
	}
	public void setData_precision(int data_precision) {
		this.data_precision = data_precision;
	}
	public int getData_length() {
		return data_length;
	}
	public void setData_length(int data_length) {
		this.data_length = data_length;
	}
	public String getData_type() {
		return data_type;
	}
	public void setData_type(String data_type) {
		this.data_type = data_type;
	}
	public String getPrimary_column_indicator() {
		return primary_column_indicator;
	}
	public void setPrimary_column_indicator(String primary_column_indicator) {
		this.primary_column_indicator = primary_column_indicator;
	}
	public String getValid() {
		return valid;
	}
	public void setValid(String valid) {
		this.valid = valid;
	}
	public String getNullable() {
		return nullable;
	}
	public void setNullable(String nullable) {
		this.nullable = nullable;
	}
	public String getColumn_order() {
		return column_order;
	}
	public void setColumn_order(String column_order) {
		this.column_order = column_order;
	}
	public String getValid_from() {
		return valid_from;
	}
	public void setValid_from(String valid_from) {
		this.valid_from = valid_from;
	}
	public String getValid_to() {
		return valid_to;
	}
	public void setValid_to(String valid_to) {
		this.valid_to = valid_to;
	}
	public String getCreation_date() {
		return creation_date;
	}
	public void setCreation_date(String creation_date) {
		this.creation_date = creation_date;
	}
	public String getLast_update_date() {
		return last_update_date;
	}
	public void setLast_update_date(String last_update_date) {
		this.last_update_date = last_update_date;
	}
	public String getCurrent_version() {
		return current_version;
	}
	public void setCurrent_version(String current_version) {
		this.current_version = current_version;
	}
	public String getComment() {
		return comment;
	}
	public void setComment(String comment) {
		this.comment = comment;
	}
	
	
	

}
