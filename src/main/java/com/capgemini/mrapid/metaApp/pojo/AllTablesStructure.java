package com.capgemini.mrapid.metaApp.pojo;

import java.util.Date;

/**
 * <srcSystem>_all_tables table structure Pojo class 
 * @author Anurag Udasi
 *
 */
public class AllTablesStructure {
	
	public String source;
	public String country_name;
	public String version="1";
	public String current_version="Y";
	public String source_type;
	public String table_name;
	public String threshold_val="null";
	public String user="null";
	public String server_id;
	public int port;
	public String valid="Y";
	public String other_info="null";
	public String new_line_char="Y";
	public Date valid_from=null;
	public Date valid_to=null;
	public Date creation_date=null;
	public Date last_update_date=null;
	public String mrapid_subscription="Y";
	
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
	public String getCurrent_version() {
		return current_version;
	}
	public void setCurrent_version(String current_version) {
		this.current_version = current_version;
	}
	
	public String getSource_type() {
		return source_type;
	}
	public void setSource_type(String source_type) {
		this.source_type = source_type;
	}
	public String getTable_name() {
		return table_name;
	}
	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}
	public String getThreshold_val() {
		return threshold_val;
	}
	public void setThreshold_val(String threshold_val) {
		this.threshold_val = threshold_val;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getServer_id() {
		return server_id;
	}
	public void setServer_id(String server_id) {
		this.server_id = server_id;
	}
	
	public String getValid() {
		return valid;
	}
	public void setValid(String valid) {
		this.valid = valid;
	}
	public String getOther_info() {
		return other_info;
	}
	public void setOther_info(String other_info) {
		this.other_info = other_info;
	}
	public String getNew_line_char() {
		return new_line_char;
	}
	public void setNew_line_char(String new_line_char) {
		this.new_line_char = new_line_char;
	}
	
	public String getMrapid_subscription() {
		return mrapid_subscription;
	}
	public void setMrapid_subscription(String mrapid_subscription) {
		this.mrapid_subscription = mrapid_subscription;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	
	
	

}
