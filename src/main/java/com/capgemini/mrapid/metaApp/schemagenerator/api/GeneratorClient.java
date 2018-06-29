/**
* The GeneratorClient class is entry point of code which takes parameter srcSystem and country
* @author  Anurag Udasi 
*/
package com.capgemini.mrapid.metaApp.schemagenerator.api;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

import com.capgemini.mrapid.metaApp.schemagenerator.impl.Generator;

public class GeneratorClient {
	static{
	    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	    System.setProperty("current.date", dateFormat.format(new Date()));
	}
	final static Logger log = Logger.getLogger(GeneratorClient.class);
	public static void main(String[] args) {

		if (args.length < 3 || args.length > 3) {
			log.error("Wrong parameter passed");
		}

		String srcSystem = args[0];
		String country = args[1];
		String configPath = args[2];
		log.info(srcSystem+":"+country+":"+"passed parameter are correct");

		System.out.print("test");
		Generator generator = new Generator();
		generator.generate(srcSystem, country,configPath);
	}
}
