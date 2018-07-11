/**
 * 
 * @author Zack Rossman
 * @version 7/5/18
 * 
 * Main handler function for lambda function
 * Called when an s3 object is created, will scrape this PDF
 * and create a new .json file with the results in another s3 bucket
 */

package example;
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
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;

public class AWS_Wrapper implements RequestHandler<Map<String, Map<String, Object>[]>, String>{
	public static final BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAJAJLIOLQVD7ISY2A", "I5PYK60FgacyWuRbnDpPCFmgnuFu7gJlppgmwQeY");
	public static final String s3_input_bucket = "flyerdata";
	public static final String s3_output_bucket = "flyeroutput";
	public static final String s3_no_address_bucket = "noaddress";
	public static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCreds)).withRegion(Regions.US_EAST_1).build();
	public static String input_name;
	
	//simple aws testing code
	public static void main(String[] args) {
		File temp_pdf_file = createTmp("flyer", ".pdf");
	    if(s3Client.doesObjectExist(s3_input_bucket, "ki")) {
			writeObjToTmp(s3_input_bucket, "ki", temp_pdf_file);
		}
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
		String s3_input_key = records.getJSONObject(0).getJSONObject("s3").getJSONObject("object").getString("key").replace("+", " ").replace("%2C", ",");
		File temp_pdf_file = createTmp("flyer", ".pdf");
		if(s3Client.doesObjectExist(s3_input_bucket, s3_input_key)) {
			input_name = s3_input_key;
			writeObjToTmp(s3_input_bucket, s3_input_key, temp_pdf_file);
			File json_result = AWS_Scrape.scrape(temp_pdf_file);
			String json_output_name = s3_input_key.substring(0, s3_input_key.length()-4)+".json";
		    s3Client.putObject(s3_output_bucket, json_output_name, json_result); 
		}else {
			s3Client.putObject(s3_output_bucket, s3_input_key+"_NOT_FOUND", temp_pdf_file);
		}
	    return s3_input_key;
    }
	
	/**
	 * Write the data from an s3 object to a temporary file
	 * @param bucket where the object is stored
	 * @param key filename of s3 object
	 * @param temp_file temporary file to write to
	 */
	public static void writeObjToTmp(String bucket, String key, File temp_file) {
		S3Object o = s3Client.getObject(bucket, key);
	    S3ObjectInputStream s3is = o.getObjectContent();
	    FileOutputStream fos = null;
		try {
		    fos = new FileOutputStream(temp_file);
		    byte[] read_buf = new byte[1024];
		    int read_len = 0;
		    while ((read_len = s3is.read(read_buf)) > 0) {
		        fos.write(read_buf, 0, read_len);
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
		s3Client.putObject(s3_no_address_bucket, input_name+"_ADDRESSNOTFOUND", createTmp("oops", ".txt"));
	}
}
