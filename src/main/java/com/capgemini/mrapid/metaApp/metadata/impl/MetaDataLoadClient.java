/**
 * @author Anuradha Dede
 * The MetaDataLoadClient class is entry point of metadata table load   
 */

package com.capgemini.mrapid.metaApp.metadata.impl;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;


public class MetaDataLoadClient {
	static{
	    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	    System.setProperty("current.date", dateFormat.format(new Date()));
	}
	final static Logger log = Logger.getLogger(MetaDataLoadClient.class);
	
	public static void main(String[] args) {

		if (args.length < 3 || args.length > 3) {
			log.error("Wrong parameter passed");
		}

		String srcSystem = args[0];
		String country = args[1];
		String configPath = args[2];
		log.info(srcSystem+":"+country+":"+"metadata table loading for country");


		MetaDataLoad load = new MetaDataLoad();
		try {
			 load.DataLoad(srcSystem, country,configPath);
		} catch (NumberFormatException e) {
			log.error("Wrong parameter passed");
		}
		}
}
