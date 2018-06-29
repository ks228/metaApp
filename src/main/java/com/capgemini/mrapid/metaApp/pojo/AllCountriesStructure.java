package com.capgemini.mrapid.metaApp.pojo;

/**
 * <srcSystem>_all_countries Pojo class 
 * @author Anuradha Dede
 *
 */
public class AllCountriesStructure {
	
	public String source;
	public String country_name;
	public String version="1";
	public String source_group;
	public String threshold_val="null";
	public int cut_off_hour=3;
	public int retention_period_hdfsfile=90;
	public int retention_period_edge_node=90;
	public int retention_period_ops=90;
	public int retention_period_backup=90;
	public int retention_period_eod_date_file=90;
	public String time_zone_value="null";
	public String data_path="null";
	public String mrapid_subscription="Y";
	
	public String getData_path() {
		return data_path;
	}
	public void setData_path(String data_path) {
		this.data_path = data_path;
	}
	public String getSource() {
		return source;
	}
	public int getCut_off_hour() {
		return cut_off_hour;
	}
	public void setCut_off_hour(int cut_off_hour) {
		this.cut_off_hour = cut_off_hour;
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
	public String getSource_group() {
		return source_group;
	}
	public void setSource_group(String source_group) {
		this.source_group = source_group;
	}
	public String getThreshold_val() {
		return threshold_val;
	}
	public void setThreshold_val(String threshold_val) {
		this.threshold_val = threshold_val;
	}
	
	public int getRetention_period_hdfsfile() {
		return retention_period_hdfsfile;
	}
	public void setRetention_period_hdfsfile(int retention_period_hdfsfile) {
		this.retention_period_hdfsfile = retention_period_hdfsfile;
	}
	public int getRetention_period_edge_node() {
		return retention_period_edge_node;
	}
	public void setRetention_period_edge_node(int retention_period_edge_node) {
		this.retention_period_edge_node = retention_period_edge_node;
	}
	public int getRetention_period_ops() {
		return retention_period_ops;
	}
	public void setRetention_period_ops(int retention_period_ops) {
		this.retention_period_ops = retention_period_ops;
	}
	public int getRetention_period_backup() {
		return retention_period_backup;
	}
	public void setRetention_period_backup(int retention_period_backup) {
		this.retention_period_backup = retention_period_backup;
	}
	public int getRetention_period_eod_date_file() {
		return retention_period_eod_date_file;
	}
	public void setRetention_period_eod_date_file(int retention_period_eod_date_file) {
		this.retention_period_eod_date_file = retention_period_eod_date_file;
	}
	public String getTime_zone_value() {
		return time_zone_value;
	}
	public void setTime_zone_value(String time_zone_value) {
		this.time_zone_value = time_zone_value;
	}
	public String getMrapid_subscription() {
		return mrapid_subscription;
	}
	public void setMrapid_subscription(String mrapid_subscription) {
		this.mrapid_subscription = mrapid_subscription;
	}


}
