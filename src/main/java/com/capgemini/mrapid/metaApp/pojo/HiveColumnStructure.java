package com.capgemini.mrapid.metaApp.pojo;

import com.google.gson.annotations.SerializedName;

/**
 * HiveColumnStructure POJO class contains Hive Column
 * Structure as column name
 * Column datatype as length,scale,precision
 * Column level Information NULL,Default 
 * @author Anurag Udasi
 */

public class HiveColumnStructure {
	
	
	public String scale;
	public String nullable;
	public String length;
	public String precision;
	public String seq;
	public String type;
	public String name;
	
	@SerializedName("default")
	public String Default = null;
	 
	public String getDefault() {
		return Default;
	}
	
	public void setDefault(String default1) {
		Default = default1;
	}
	public String getColscale() {
		return scale;
	}
	public void setColscale(String colscale) {
		this.scale = colscale;
	}
	public String getColnullable() {
		return nullable;
	}
	public void setColnullable(String colnullable) {
		this.nullable = colnullable;
	}
	public String getCollen() {
		return length;
	}
	public void setCollen(String collen) {
		this.length = collen;
	}
	public String getColprecision() {
		return precision;
	}
	public void setColprecision(String colprecision) {
		this.precision = colprecision;
	}
	public String getColseq() {
		return seq;
	}
	public void setColseq(String colseq) {
		this.seq = colseq;
	}
	public String getColtype() {
		return type;
	}
	public void setColtype(String coltype) {
		this.type = coltype;
	}
	public String getColname() {
		return name;
	}
	public void setColname(String colname) {
		this.name = colname;
	}
	

}
