/**
 * @author Zack Rossman
 * @version 7/9/18
 * 
 * Class used to test the scraping of PDFs in AWS_Scrape class
 */

package com.propertycapsule.service.pdf;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;

public class TestOutput {
	public static final BasicAWSCredentials awsCreds = AWS_Wrapper.awsCreds;
	public static final String s3OutputBucket = AWS_Wrapper.s3OutputBucket;
	public static final AmazonS3 s3Client = AWS_Wrapper.s3Client;
	public static final String outputFilePath = "/Users/zacharycolerossman/Documents/ML_Flyer_Data/Output_Txts/";
	public static String inputPdfName = "";

	
	public static void main(String[] args) {
//		scrapeDirectory("/Users/zacharycolerossman/Documents/ML_Flyer_Data/Edge/");
		scrapeDirectory("/Users/zacharycolerossman/Documents/ML_Flyer_Data/Complete_Test_Set/");
	}
	
	/**
	 * Get the list of all files in the "input folder
	 * @param folder -> the folder containing pdf files to be scraped
	 * @return arraylist of file names for PDFs to be translated to txt and scraped
	 */
	public static ArrayList<String> getInputFilenames(final File folder) {
		ArrayList<String> inputFileNames = new ArrayList<String>();
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				getInputFilenames(fileEntry);
			} else {
				String pdfName = fileEntry.getName();
				if (pdfName.substring(pdfName.length() - 4, pdfName.length()).equals(".pdf")) {
					inputFileNames.add(fileEntry.getName());
				}
			}
		}
		return inputFileNames;
	}
	
	/**
	 * scrape all of the PDFs in a local directory
	 * @param directory path to directory
	 */
	public static void scrapeDirectory(String directory) {
		ArrayList<String> inputPdfNames = getInputFilenames(new File(directory));
		for(String pdfName : inputPdfNames) {
			System.out.println(pdfName);
			inputPdfName = pdfName;
			File file = new File(directory+pdfName);
			File messyTxt = AWS_Scrape.pdfToTxt(file);
			File cleanTxt = AWS_Scrape.cleanTxt(messyTxt);
			HashMap<String, ArrayList<String>> propertyData = AWS_Scrape.scrapeTxt(cleanTxt);
			resultsToCsv(propertyData);
//			AWS_Scrape.resultsToJson(propertyData);
		}
	}
	
	/**
	 * Load the scraped information from all PDFs into one new .csv file
	 * @param results contains the property data for every pdf file processed
	 * @param inputPdfNames -> arraylist of names of pdfs that were scraped
	 */
	public static void resultsToCsv(HashMap<String, ArrayList<String>> results) {
		File outputCsv = new File(outputFilePath+"output.csv");
		StringBuilder sb = new StringBuilder();
		FileWriter pw = null;
		try {
			if(!outputCsv.exists()) {
				outputCsv.createNewFile();
				pw = new FileWriter(new File(outputFilePath+"output.csv"), true);
				sb.append("PDFName, Type, Scraped Address, Lev Distance, Geocoded Address, Latitude, Term, Emails, Longitude, Phone Numbers, Square Footage, Contact Names\n");
			}else {
				pw = new FileWriter(new File(outputFilePath+"output.csv"), true);
			}
			sb.append("\""+inputPdfName+"\"");
			for(HashMap.Entry<String, ArrayList<String>> entry : results.entrySet()) {
				ArrayList<String> propertyData = entry.getValue();
				sb.append(',');
				if(propertyData.size() > 0) {
					String data = propertyData.get(0);
					for(int j = 1; j< propertyData.size(); j++) {
						data += "/ "+propertyData.get(j);
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
