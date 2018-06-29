package com.capgemini.mrapid.metaApp.integration.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.exceptions.HDFSFileOperationException;
import com.capgemini.mrapid.metaApp.integration.api.IIntegrate;

/**
 *
 * Class HDFSHandle : HDFS utilities 
 * 1.get HDFS FileSystem object to do hdfs filesystem operation 
 * 2.write to HDFS 
 * 3.Read from HDFS
 * @author Anuradha Dede
 */

public class HDFSHandle implements IIntegrate {
	final static Logger log = Logger.getLogger(HDFSHandle.class);

	StringWriter stack = new StringWriter();

	/**
	 * HDFSHandle: HDFSHandle default constructor
	 */
	public HDFSHandle() {
		// TODO Auto-generated constructor stub

	}

	/**
	 * getHandle:Gets Hdfs fileSystem Handler
	 * @param hdfsUrl:HDFS IP with port as string
	 * @return Hdfs Configuration object
	 */
	public FileSystem getHandle(String hdfsUrl) {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://" + hdfsUrl); //TODO - define hdfs in property only
		conf.set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",
				org.apache.hadoop.fs.LocalFileSystem.class.getName());
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(conf);
		} catch (Exception e) {
			log.error("IOException" + e.getMessage());
			return null;
		}

		return hdfs;
	}

	/**
	 * hdfsWrite : Writes file from local to HDFS Destination Path
	 * @param jsonString JSON String to write
	 * @param propertiesMap HDFS Configuration Properties Map
	 * @param filename local FileSystem path
	 * @param filepath HDFS path to write file
	 * @return String value as per Operation status
	 * @throws IOException
	 */

	public boolean hdfsWrite(String jsonString,
			Map<String, String> propertiesMap, String filename, String filepath)
			throws IOException {
		String hdfsPath = filepath + "/" + filename;
		String hdfsUrl = propertiesMap.get(Constants.HDFS_URL);
		FileSystem hdfs = null;
		hdfs = getHandle(hdfsUrl);
		FSDataOutputStream fin;
		try {
			if (hdfs != null) {
				fin = hdfs.create(new Path(hdfsPath));
				fin.writeBytes(jsonString);
				fin.close();
			} else {
				throw new HDFSFileOperationException(
						" Fail to write file to HDFS ...!");
			}

		}catch (HDFSFileOperationException e){
			log.error(e.getMessage());
			return false;
		}catch(Exception e){
			log.error(e.getMessage());
			return false;
		}
		return true;

	}

	/**
	 * hdfsFileExits : check if hdfs file exists
	 * @param filename
	 * @param hdfsURL
	 * @return String read content
	 * @throws IOException
	 */
	public boolean hdfsFileExits(String filename, String hdfsURL)
			throws IOException {
		log.info("filename" + filename + "hdfsURL" + hdfsURL);
		FileSystem hdfs = null;
		hdfs = getHandle(hdfsURL);
		boolean fileExists = false;
		try {
			if (hdfs.exists(new Path(filename))) {

				fileExists = true;
			} else {
				fileExists = false;
			}
		} catch(Exception e){
			log.error(e.getMessage());
		}
		return fileExists;

	}

	/**
	 * hdfsRead : Read files from hdfs
	 * @param filename
	 * @param hdfsURL
	 * @return String read content
	 * @throws IOException
	 */
	public String hdfsRead(String filename, String hdfsURL) throws IOException {
		log.info("filename" + filename + "hdfsURL" + hdfsURL);
		FileSystem hdfs = null;
		hdfs = getHandle(hdfsURL);
		String jsonString = "";
		StringBuffer filecontent = new StringBuffer();

		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(
					filename))));

			while ((jsonString = br.readLine()) != null) {

				filecontent.append(jsonString).append("\n");

			}
		} finally {
			if(br!=null)
				br.close();
		}

		return filecontent.toString();

	}

	/**
	 * hdfsMoveFile : HDFS move file from one hdfs location to another
	 * @param source
	 * @param destination
	 * @param sourceName
	 * @param destinationName
	 * @param hdfsURL
	 * @param extension
	 * @param tmpFilePath
	 * @param copyFlag
	 * @return
	 * @throws IOException
	 * @throws HDFSFileOperationException
	 */
	public boolean hdfsMoveFile(String source, String destination,
			String sourceName, String destinationName, String hdfsURL,
			String extension, String tmpFilePath, boolean copyFlag)
			throws IOException, HDFSFileOperationException {

		FileSystem hdfs = null;
		hdfs = getHandle(hdfsURL);

		sourceName = sourceName + extension;
		destinationName = destinationName + extension;
		try{
			if (hdfs != null) {
				Path sourcePath = new Path(source + "/" + sourceName);
				Path destinationPath = new Path(source + destination + "/"
						+ destinationName);
				log.info("File moving to " + tmpFilePath);
				hdfs.copyToLocalFile(copyFlag, sourcePath, new Path(tmpFilePath
						+ "/"), true);
				log.info("get file from " + tmpFilePath + "/" + sourceName);
				hdfs.copyFromLocalFile(copyFlag, new Path(tmpFilePath + "/"
						+ sourceName), destinationPath);
				log.info("File moved to " + destinationPath);
			} else {
			throw new HDFSFileOperationException(" Fail to move old version files ...!");
			}
		}catch(Exception e){
			log.error(e.getMessage());
		}
		return true;
	}
	
	/**
	 * hdfsRemoveFile : HDFS remove file method
	 * @param source
	 * @param sourceName
	 * @param hdfsURL
	 * @param extension
	 * @param copyFlag
	 * @return
	 */
	public boolean hdfsRemoveFile(String source, 
			String sourceName, String hdfsURL,
			String extension, boolean copyFlag)
	{
		FileSystem hdfs = null;
		hdfs = getHandle(hdfsURL);
		
		try
		{
			if (hdfs != null) 
			{
				Path sourcePath = new Path(source + "/" + sourceName + extension );
				log.info("deletion in progress");
				if(hdfs.delete(sourcePath, copyFlag))
				{
					log.info("File deleted");
				}
			}
		}	
		catch(Exception e)
		{
			log.error(e.getMessage());
		}
		return true;
	}
}
