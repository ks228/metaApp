package com.capgemini.mrapid.metaApp.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Pallavi Kadam
 * Class : Json Utility class 
 * 1.check for valid Json
 * 2.Parse JSON string 
 * 3.Converts Map object to JSON String
 * 4.Check if key exists
 * 5.Converts String to Map
 */

public class JsonUtils {
	final static Logger log = Logger.getLogger(JsonUtils.class);
	
	/**
	 * Check passed string is valid json or not
	 * @param json:passed json string need to be validated
	 * @return returns boolean false invalid and true for valid json
	 */
	public static boolean isValidJson(JSONObject json) {
		ObjectMapper mapper = new ObjectMapper();
		@SuppressWarnings("unused")
		LinkedHashMap<String, Object> map = new LinkedHashMap<String, Object>();

		try {
			map = mapper.readValue(json.toString(), new TypeReference<Map<String, Object>>() {
			});
			return true;
		} catch (JsonParseException e) {
			log.error("JSON Parse Exception"+e);
		} catch (JsonMappingException e) {
			log.error("JSON Mapping Exception"+e);		
		} catch (IOException e) {
			log.error("IO Exception"+e);
		}

		return false;
	}

	/**
	 * Convert JSON string and convert it to LinkedHashMap  object
	 * @param json:passed json string need to be convert
	 * @return returns LinkedHashMap object which has json values
	 */
	public static LinkedHashMap<String, Object> parseJSON(JSONObject json) {
		ObjectMapper mapper = new ObjectMapper();
		LinkedHashMap<String, Object> map = new LinkedHashMap<String, Object>();

		try {
			map = mapper.readValue(json.toString(), new TypeReference<Map<String, Object>>() {
			});
		} catch (JsonParseException e) {
			log.error("JSON Parse Exception"+e);
		} catch (JsonMappingException e) {
			log.error("JSON Mapping Exception"+e);	
		} catch (IOException e) {
			log.error("IO Exception"+e);
		}
		return map;
	}

	/**
	 * Converts LinkedHashMap object to JSON String
	 * @param obj:LinkedHashMap object need to convert it to String
	 * @return returns jsonString
	 */
	public static String MaptoJSON(LinkedHashMap<String, Object> obj) {
		ObjectMapper objectMapper = new ObjectMapper();
		String jsonStr = null;
		try {
			jsonStr = objectMapper.writeValueAsString(obj);
		} catch (JsonProcessingException e) {
			log.error("Json Processing Exception"+e);
		}
		return jsonStr;
	}
	
	
	/**
	 * Converts String Map object to JSON String
	 * @param obj
	 * @return
	 */
	public static String MaptoJSON(Map<String, String> obj) {
		ObjectMapper objectMapper = new ObjectMapper();
		String jsonStr = null;
		try {
			jsonStr = objectMapper.writeValueAsString(obj);
		} catch (JsonProcessingException e) {
			log.error("Json Processing Exception"+e);
		}
		return jsonStr;
	}
	
	
	/**
	 * check if passed key exists in Map object
	 * @param obj:LinkedHashMap object to verify
	 * @param Key:key to check in Object
	 * @return returns boolean variable if key exists return true otherwise false
	 */
    public static Boolean isKeyExists(LinkedHashMap<String, Map<String, String>> obj, String key) {
          if (obj.containsKey(key)) {
                 return true;
          }
          return false;

    }
    /**
	 *check if passed key exists in Map object
	 * @param obj:Map object to verify
	 * @param Key:key to check in Object
	 * @return returns boolean variable if key exists return true otherwise false
	 */
    
    public static Boolean isKeyExists(Map<String,Object> obj, String key) {
        if (obj.get(key) != null) {
               return true;
        }
        return false;

  }

    /**
	 * Converts JSON String to HashMap object
	 * @param inputString:String to convert into Map
	 * @return returns Converted HashMap Object
	 */
   public static Map<String, String> getMapFromString(String inputString){
		
		inputString=inputString.substring(1,inputString.length()-1);
		String[] keyValuepairs=inputString.split(",");
		Map<String, String> map=new HashMap<String, String>();
		for(String pair:keyValuepairs)
		{
			String[] entry=pair.split("=");
			map.put(entry[0].trim(), entry[1].trim());
		}
		return map;	
	}

}
