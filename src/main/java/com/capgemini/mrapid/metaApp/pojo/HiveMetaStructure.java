package com.capgemini.mrapid.metaApp.pojo;


/**
 * HiveMetaStructure POJO class contains SQL MetaInformation
 * Structure Source and target table metaInformation
 * @author Anurag Udasi
 */


public class HiveMetaStructure {
	
	public String srcname;
	public String srctype;
	public String srcdbtype;
	public String country;
	public String tgtdbformat;
	public String tgtdbtype;
	public String tablename;
	public String tgtdelim;
	public String tgtdb;
	public String srctablespace;
	
	public String getTgtdb() {
		return tgtdb;
	}
	public void setTgtdb(String tgtdb) {
		this.tgtdb = tgtdb;
	}
	public String getSrctablespace() {
		return srctablespace;
	}
	public void setSrctablespace(String srctablespace) {
		this.srctablespace = srctablespace;
	}
	public String getTgtdelim() {
		return tgtdelim;
	}
	public void setTgtdelim(String tgtdelim) {
		this.tgtdelim = tgtdelim;
	}
	public String getSrcname() {
		return srcname;
	}
	public void setSrcname(String srcname) {
		this.srcname = srcname;
	}
	public String getSrctype() {
		return srctype;
	}
	public void setSrctype(String srctype) {
		this.srctype = srctype;
	}
	public String getSrcdbtype() {
		return srcdbtype;
	}
	public void setSrcdbtype(String srcdbtype) {
		this.srcdbtype = srcdbtype;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getTgtdbformat() {
		return tgtdbformat;
	}
	public void setTgtdbformat(String tgtdbformat) {
		this.tgtdbformat = tgtdbformat;
	}
	public String getTgtdbtype() {
		return tgtdbtype;
	}
	public void setTgtdbtype(String tgtdbtype) {
		this.tgtdbtype = tgtdbtype;
	}
	public String getTablename() {
		return tablename;
	}
	public void setTablename(String tablename) {
		this.tablename = tablename;
	}
	

}
