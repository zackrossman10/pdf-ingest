/**
 * @author Zack Rossman
 * @version 7/9/18
 * 
 * Class used to test the scraping of PDFs in AWS_Scrape class
 */

package example;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;

public class TestOutput {
	public static final BasicAWSCredentials awsCreds = AWS_Wrapper.awsCreds;
	public static final String s3_output_bucket = AWS_Wrapper.s3_output_bucket;
	public static final AmazonS3 s3Client = AWS_Wrapper.s3Client;
	public static final String output_file_path = "/Users/zacharycolerossman/Documents/ML_Flyer_Data/Output_Txts/";
	public static String input_pdf_name = "";

	
	public static void main(String[] args) {
		//scrape the working set
		scrapeDirectory("/Users/zacharycolerossman/Documents/ML_Flyer_Data/Complete_Test_Set/");
	}
	
	/**
	 * Get the list of all files in the "input folder
	 * @param folder -> the folder containing pdf files to be scraped
	 * @return arraylist of file names for PDFs to be translated to txt and scraped
	 */
	public static ArrayList<String> getInputFilenames(final File folder) {
		ArrayList<String> input_file_names = new ArrayList<String>();
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				getInputFilenames(fileEntry);
			} else {
				String pdf_name = fileEntry.getName();
				if (pdf_name.substring(pdf_name.length() - 4, pdf_name.length()).equals(".pdf")) {
					input_file_names.add(fileEntry.getName());
				}
			}
		}
		return input_file_names;
	}
	
	public static void scrapeDirectory(String directory) {
		ArrayList<String> input_pdf_names = getInputFilenames(new File(directory));
		for(String pdf_name : input_pdf_names) {
			System.out.println(pdf_name);
			input_pdf_name = pdf_name;
			File file = new File(directory+pdf_name);
			File txt_output = AWS_Scrape.PDFToTxt(file);
			HashMap<String, ArrayList<String>> property_data = AWS_Scrape.scrapeTxt(txt_output);
			resultsToCsv(property_data);
//			AWS_Scrape.resultsToJson(property_data);
		}
	}
	
	/**
	 * Load the scraped information from all PDFs into one new .csv file
	 * @param results contains the property data for every pdf file processed
	 * @param input_pdf_names -> arraylist of names of pdfs that were scraped
	 * @throws IOException
	 */
	public static void resultsToCsv(HashMap<String, ArrayList<String>> results){
		File output_csv = new File(output_file_path+"output.csv");
		StringBuilder sb = new StringBuilder();
		FileWriter pw = null;
		try {
			if(!output_csv.exists()) {
				output_csv.createNewFile();
				pw = new FileWriter(new File(output_file_path+"output.csv"), true);
				sb.append("PDFName, Scraped Address, Geocoded Address, Latitude, Term, Emails, Longitude, Phone Numbers, Square Footage, Contact Names\n");
			}else {
				pw = new FileWriter(new File(output_file_path+"output.csv"), true);
			}
			sb.append("\""+input_pdf_name+"\"");
			for(HashMap.Entry<String, ArrayList<String>> entry : results.entrySet()) {
				ArrayList<String> property_data = entry.getValue();
				sb.append(',');
				if(property_data.size() > 0) {
					String data = property_data.get(0);
					for(int j = 1; j< property_data.size(); j++) {
						data += "/ "+property_data.get(j);
					}
					//avoid accidentally parsing for commas in the string
					sb.append("\""+data+"\"");
				}
			}
			pw.write(sb.append('\n').toString());
		}catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				pw.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
	}
}
