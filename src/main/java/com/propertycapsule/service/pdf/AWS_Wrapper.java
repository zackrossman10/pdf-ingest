/**
 * 
 * @author Zack Rossman
 * @version 7/5/18
 * 
 * Main handler function for lambda function
 * Called when an s3 object is created, will scrape this PDF
 * and create a new .json file with the results in another s3 bucket
 */

package com.propertycapsule.service.pdf;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

public class AWS_Wrapper implements RequestHandler<Map<String, Map<String, Object>[]>, String>{
    public static Keys properties = new Keys();
	public static BasicAWSCredentials awsCreds = new BasicAWSCredentials(properties.getKey("AWSAccessKeyId"), properties.getKey("AWSSecretKey"));
	public static final String s3InputBucket = "flyerdata";
	public static final String s3OutputBucket = "flyeroutput";
	public static final String s3NoAddressBucket = "noaddress";
	public static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCreds)).withRegion(Regions.US_EAST_1).build();
	public static String inputName;
    public static Geocode geocoder = new Geocode();


	//simple aws testing code
	public static void main(String[] args) {
	    System.out.println(properties.getKey("AWSAccessKeyId"));
	    S3Object o = s3Client.getObject(s3InputBucket, "30-29th-Street-Brochure-08.23.16.pdf");
		File tempPdfFile = new File("/Users/zacharycolerossman/Documents/ML_Flyer_Data/Complete_Test_Set/_20151113 Lonetree.pdf");
		File jsonResult = AWS_Scrape.scrape(tempPdfFile);
	    s3Client.putObject(s3OutputBucket, "test.json", jsonResult); 
	}
	
	/**
	 * Main function called by the lambda function
	 * Facilitates scraping of PDF and returning of JSON output
	 * @param input the Message passed in by SQS
	 * @param AWS context ??
	 */
	public String handleRequest(Map<String, Map<String, Object>[]> input, Context context) {
		String body = (String) input.get("Records")[0].get("body");
		JSONArray records = new JSONObject(body).getJSONArray("Records");
		String s3InputKey = records.getJSONObject(0).getJSONObject("s3").getJSONObject("object").getString("key").replace("+", " ").replace("%2C", ",");
		File tempPdfFile = createTmp("flyer", ".pdf");
		if(s3Client.doesObjectExist(s3InputBucket, s3InputKey)) {
			inputName = s3InputKey;
			writeObjToTmp(s3InputBucket, s3InputKey, tempPdfFile);
			File jsonResult = AWS_Scrape.scrape(tempPdfFile);
			String jsonOutputName = s3InputKey.substring(0, s3InputKey.length()-4)+".json";
		    s3Client.putObject(s3OutputBucket, jsonOutputName, jsonResult); 
		}else {
			s3Client.putObject(s3OutputBucket, s3InputKey+"_NOT_FOUND", tempPdfFile);
		}
	    return s3InputKey;
    }
	
	/**
	 * Write the data from an s3 object to a temporary file
	 * @param bucket where the object is stored
	 * @param key filename of s3 object
	 * @param tempFile temporary file to write to
	 */
	public static void writeObjToTmp(String bucket, String key, File tempFile) {
		S3Object o = s3Client.getObject(bucket, key);
	    S3ObjectInputStream s3is = o.getObjectContent();
	    FileOutputStream fos = null;
		try {
		    fos = new FileOutputStream(tempFile);
		    byte[] readBuf = new byte[1024];
		    int readLen = 0;
		    while ((readLen = s3is.read(readBuf)) > 0) {
		        fos.write(readBuf, 0, readLen);
		    }
		} catch (AmazonServiceException e) {
		    System.err.println(e.getErrorMessage());
		} catch (FileNotFoundException e) {
		    System.err.println(e.getMessage());
		} catch (IOException e) {
		    System.err.println(e.getMessage());
		}finally {
			if(fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			try {
				s3is.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Create a temporary file 
	 * @param name of the temp file
	 * @param extension of the temp file
	 * @return the temp
	 */
	public static File createTmp(String name, String extension) {
		File output = null;
		try {
			output = File.createTempFile(name, extension);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return output;
	}
	
	/**
	 * Put a placeholder doc in S3 to alert that no address was not found for the property
	 */
	public static void alertNoAddress() {
		s3Client.putObject(s3NoAddressBucket, inputName+"_ADDRESSNOTFOUND", createTmp("oops", ".txt"));
	}
}
