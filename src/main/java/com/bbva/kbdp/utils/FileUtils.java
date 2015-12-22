package com.bbva.kbdp.utils;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FileUtils {

	private static final Logger logger = LoggerFactory.getLogger(FileUtils.class); 
    /**
     * Gets an in scope product types list the ./interceptor.properties file of
     * the base folder 
     *
     * @return values List
     * @throws IOException
     */
    public static String[] getProperty(String property, String iaHdfsPropertiesPath) {

    	String[] result = null; 
    	logger.info("entrando en getProperty");
    	Path inFile = new Path(iaHdfsPropertiesPath);
    	FSDataInputStream in = null;
    	
    	Configuration conf = new Configuration();
    	try {
			FileSystem fs = FileSystem.get(conf);
			in = fs.open(inFile);
			
			//to load application's properties, we use this class
	        Properties mainProperties = new Properties();
	      //load all the properties from this file
            mainProperties.load(in);
            in.close();
            
            logger.info("getProperty :: property :: " + property);
            
            //retrieve the property we are interested in, the app.version
            String line = mainProperties.getProperty(property);
            
            logger.info("getProperty :: line :: " + line);
            
            result = line.split(",");      
            
            logger.info("getProperty :: result :: " + result);
			
		} catch (IOException e) {
			logger.error("Error leyendo de HDFS :: file name " + iaHdfsPropertiesPath);
			e.printStackTrace();
		} finally {
            try {
                if (in != null) {
                	in.close();
                }
            } catch (Exception e) {
            	logger.error("Error getting property " + property + " from " + iaHdfsPropertiesPath, e);
            }
        }

        return result;
    }

}
