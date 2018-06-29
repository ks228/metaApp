///**
// * AuditClient
// * It calls audit logger class and create runtime logs in HBase
// * @author Anurag Udasi
// *
// */
//package com.capgemini.mrapid.metaApp.audit;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import org.apache.log4j.Logger;
//import org.json.JSONObject;
//
//import com.capgemini.mrapid.auditlogs.integration.impl.AuditLoghandler;
//import com.capgemini.mrapid.auditlogs.pojo.AuditLogPojo;
//import com.capgemini.mrapid.metaApp.constants.AuditConstants;
//import com.capgemini.mrapid.metaApp.constants.Constants;
//import com.capgemini.mrapid.metaApp.constants.RemedyConstants;
//import com.capgemini.mrapid.metaApp.utils.RemedyLogsUtils;
//
//public class AuditClient {
//	final static Logger log = Logger.getLogger(AuditClient.class);
//	public static Map<String, String> detailedInfo = new HashMap<String, String>();
//	
//	/**
//	 * method to write audit log to HBase
//	 * @param schemaInformationObject
//	 * @param avscResult
//	 * @param avroResult
//	 * @param orcResult
//	 * @param hdfsWriteAvro
//	 * @param hdfsWriteAvsc
//	 * @param hdfsWriteJson
//	 * @param hdfsWriteOrc
//	 * @param auditLogsPojoObj
//	 * @param srcSystem
//	 * @param country
//	 * @param basicInfo
//	 * @param hadoopPropertiesMap
//	 * @param auditHandler
//	 * @param auditTableName
//	 * @param serverIP
//	 * @param deploymentProperties
//	 */
//	public static void writeToAuditLog(JSONObject schemaInformationObject,
//			String avscResult, String avroResult, String orcResult,
//			boolean hdfsWriteAvro, boolean hdfsWriteAvsc,
//			boolean hdfsWriteJson, boolean hdfsWriteOrc,
//			AuditLogPojo auditLogsPojoObj, String srcSystem, String country,Map<String, String> basicInfo,
//			Map<String, String> hadoopPropertiesMap,AuditLoghandler auditHandler,String auditTableName,String serverIP,Map<String, String> deploymentProperties) {
//
//		try {
//			if (schemaInformationObject != null && !avscResult.isEmpty()
//					&& !avroResult.isEmpty() && !orcResult.isEmpty()) {
//				detailedInfo.put(AuditConstants.COULMN_GENERATESCHEMA,
//						AuditConstants.SUCCESS);
//				auditLogsPojoObj.setGenerateSchema(AuditConstants.SUCCESS);
//			} else {
//				detailedInfo.put(AuditConstants.COULMN_GENERATESCHEMA,
//						AuditConstants.ERROR);
//				auditLogsPojoObj.setGenerateSchema(AuditConstants.ERROR);
//			}
//
//			// Store Schema AuditLogs
//			if (!hdfsWriteJson && !hdfsWriteAvsc && !hdfsWriteAvro
//					&& !hdfsWriteOrc) {
//				detailedInfo.put(AuditConstants.COULMN_STORE,
//						AuditConstants.ERROR);
//				auditLogsPojoObj.setStore(AuditConstants.ERROR);
//			} else {
//				detailedInfo.put(AuditConstants.COULMN_STORE,
//						AuditConstants.SUCCESS);
//				auditLogsPojoObj.setStore(AuditConstants.SUCCESS);
//			}
//
//			// Store Status AuditLogs
//			if (auditLogsPojoObj.getGenerateSchema().equalsIgnoreCase(
//					AuditConstants.SUCCESS)
//					&& auditLogsPojoObj.getReadSource().equalsIgnoreCase(
//							AuditConstants.SUCCESS)
//					&& auditLogsPojoObj.getStore().equalsIgnoreCase(
//							AuditConstants.SUCCESS)) {
//				basicInfo.put(AuditConstants.COULMN_STATUS,
//						AuditConstants.SUCCESS);
//				auditLogsPojoObj.setStatus(AuditConstants.SUCCESS);
//			} else {
//				basicInfo.put(AuditConstants.COULMN_STATUS,
//						AuditConstants.ERROR);
//				auditLogsPojoObj.setStatus(AuditConstants.ERROR);
//			}
//			if (auditLogsPojoObj.getStatus().equalsIgnoreCase(
//					AuditConstants.SUCCESS)) {
//				basicInfo.put(AuditConstants.COULMN_ERRORS,
//						AuditConstants.NOERROR);
//				auditLogsPojoObj.setErrors(AuditConstants.NOERROR);
//			} else {
//				basicInfo.put(AuditConstants.COULMN_ERRORS,
//						AuditConstants.ERRORMSG);
//				auditLogsPojoObj.setErrors(AuditConstants.ERRORMSG);
//			}
//
//			detailedInfo.put(AuditConstants.COULMN_READSOURCE,
//					auditLogsPojoObj.getReadSource());
//			auditLogsPojoObj
//					.setBasicInfoCF(AuditConstants.COULMN_FAMILY_BASIC_INFO);
//			auditLogsPojoObj
//					.setDetailedInfoCF(AuditConstants.COULMN_FAMILY_DETAILED_INFO);
//			auditLogsPojoObj.setBasicInfoCFMap(basicInfo);
//			auditLogsPojoObj.setDetailedInfoCFMap(detailedInfo);
//			if (auditLogsPojoObj.getKey() != null
//					|| auditLogsPojoObj.getBasicInfoCF() != null
//					|| auditLogsPojoObj.getDetailedInfoCF() != null
//					|| auditLogsPojoObj.getDate() != null
//					|| auditLogsPojoObj.getErrors() != null
//					|| auditLogsPojoObj.getLogs() != null
//					|| auditLogsPojoObj.getGenerateSchema() != null
//					|| auditLogsPojoObj.getReadSource() != null
//					|| auditLogsPojoObj.getStatus() != null
//					|| auditLogsPojoObj.getStore() != null
//					|| hadoopPropertiesMap.get(Constants.HBASE_SITE_XML_PATH) != null
//					|| hadoopPropertiesMap.get(Constants.ZOOKEEPER_PORT) != null
//					|| hadoopPropertiesMap.get(Constants.ZOOKEEPER_ZNODE) != null
//					|| hadoopPropertiesMap.get(Constants.MAXCLIENTCNXNS) != null)
//			auditHandler.insertLog(auditLogsPojoObj.getKey(), auditLogsPojoObj,
//					auditTableName, serverIP,
//					hadoopPropertiesMap.get(Constants.HBASE_SITE_XML_PATH),
//					hadoopPropertiesMap.get(Constants.ZOOKEEPER_PORT),
//					hadoopPropertiesMap.get(Constants.ZOOKEEPER_ZNODE),
//					hadoopPropertiesMap.get(Constants.MAXCLIENTCNXNS));
//			else {
//				log.info(srcSystem
//						+ ":"
//						+ country
//						+ ":"+
//						 ":we cannot insert audit check all required values are not null ");
//				log.info(srcSystem
//						+ ":"
//						+ country
//						+ ":"
//						+ "metaApp audit key"
//						+ auditLogsPojoObj.getKey()
//						+ " DetailedInfo column family"
//						+ auditLogsPojoObj.getDetailedInfoCF()
//						+ " BasicInfo column family"
//						+ auditLogsPojoObj.getBasicInfoCF()
//						+ " BasicInfoMap"
//						+ auditLogsPojoObj.getBasicInfoCFMap().toString()
//						+ "DetailedInfoMap"
//						+ auditLogsPojoObj.getDetailedInfoCFMap()
//								.toString());
//			}
//			
//
//		} catch (Exception e) {
//			log.error(e.getMessage());
//			RemedyLogsUtils.writeToRemedyLogs(
//					RemedyConstants.AUDIT_LOGS_COMPONENT, srcSystem, country,
//					RemedyConstants.PRODUCT_CODE_SEVENTEEN,
//					RemedyLogsUtils.remedyLogsPropertiesMap
//							.get(RemedyConstants.PRODUCT_CODE_SEVENTEEN), null,deploymentProperties);
//		}
//	}
//
//}
