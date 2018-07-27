/**
 * @author Zack Rossman
 * @version 7/9/18
 * 
 * Class used for testing the parallel scraper
 */

package com.propertycapsule.service.pdf.concurrent;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.maps.model.GeocodingResult;

public class TestOutput {
    public static final String s3OutputBucket = AWS_Wrapper.s3OutputBucket;
    public static final AmazonS3 s3Client = AWS_Wrapper.s3Client;
    public static final String outputFilePath = "/Users/zacharycolerossman/Documents/ML_Flyer_Data/National_Output_Txts/";
    public static String inputPdfName = "";
    public static Gson gson = new GsonBuilder().setPrettyPrinting().create();
    public static Geocode geocoder = new Geocode();




    public static void main(String[] args) {
//        scrapeDirectory("/Users/zacharycolerossman/Documents/ML_Flyer_Data/Complete_Test_Set/");
        scrapeDirectory("/Users/zacharycolerossman/Documents/ML_Flyer_Data/NationalFlyersAll/");
    }

    /**
     * Get the list of all files in the "input folder
     * 
     * @param folder
     *            the folder containing pdf files to be scraped
     * @return arraylist of file names for PDFs to be translated to txt and
     *         scraped
     */
    public static ArrayList<String> getInputFilenames(final File folder) {
        ArrayList<String> inputFileNames = new ArrayList<String>();
        System.out.println("hi2");

        for(final File fileEntry : folder.listFiles()) {
            if(fileEntry.isDirectory()) {
                System.out.println("hi");
                inputFileNames.addAll(getInputFilenames(fileEntry));
            } else {
                String pdfName = fileEntry.getName();
                if(pdfName.substring(pdfName.length() - 4, pdfName.length()).equals(".pdf")) {
                    inputFileNames.add(fileEntry.getName());
                }
            }
        }
        return inputFileNames;
    }

    /**
     * scrape all of the PDFs in a local directory
     * 
     * @param directory
     *            path to directory
     */
    public static void scrapeDirectory(String directory) {
        ArrayList<String> inputPdfNames = getInputFilenames(new File(directory));
        ParallelScraper pScraper = null;
        for(String pdfName : inputPdfNames) {
            inputPdfName = pdfName;
            pScraper = new ParallelScraper();
            File file = new File(directory + pdfName);
            pScraper.scrape(file);
            resultsToCsv();
        }
    }

    /**
     * Load the scraped information from all PDFs into one new .csv file
     * 
     * @param results
     *            contains the property data for every pdf file processed
     * @param inputPdfNames
     *            arraylist of names of pdfs that were scraped
     */
    public static void resultsToCsv() {
        File outputCsv = new File(outputFilePath + "output.csv");
        StringBuilder sb = new StringBuilder();
        FileWriter pw = null;
        try {
            if(!outputCsv.exists()) {
                outputCsv.createNewFile();
                pw = new FileWriter(new File(outputFilePath + "output.csv"), true);
                sb.append("PDFName, Scraped Address, Geocoded Address, Address Type, Latitude, Longitude, Term, Square Footage, Emails, Phone Numbers, Contact Names\n");
            } else {
                pw = new FileWriter(new File(outputFilePath + "output.csv"), true);
            }
            sb.append("\"" + inputPdfName + "\"");
            sb.append(',');
            Entry entry1 = ParallelScraper.address.peek();
            if(entry1 == null) {
                sb.append("\"" + "**" + "\"");
                sb.append(',');
                sb.append("\"" + "**" + "\"");
                sb.append(',');
                sb.append("\"" + "**" + "\"");
                sb.append(',');
                sb.append("\"" + "**" + "\"");
                sb.append(',');
                sb.append("\"" + "**" + "\"");
            }else {
                sb.append("\"" + entry1.getValue() + "\"");
                sb.append(',');
                GeocodingResult[] geoResults = geocoder.getParallelGeocodedInfo(entry1.getValue());
                if(geoResults.length > 0) {
                    sb.append("\"" + gson.toJson(geoResults[0].formattedAddress).replaceAll("\"", "") + "\"");
                    sb.append(',');
                    sb.append("\"" + gson.toJson(geoResults[0].addressComponents[0].types[0]).replaceAll("\"", "") + "\"");
                    sb.append(',');
                    sb.append("\"" + gson.toJson(geoResults[0].geometry.location.lat) + "\"");
                    sb.append(',');
                    sb.append("\"" + gson.toJson(geoResults[0].geometry.location.lng) + "\"");
                }else {
                    sb.append("\"" + "**" + "\"");
                    sb.append(',');
                    sb.append("\"" + "**" + "\"");
                    sb.append(',');
                    sb.append("\"" + "**" + "\"");
                    sb.append(',');
                    sb.append("\"" + "**" + "\"");
                }
            }
            sb.append(',');
            String entry = ParallelScraper.term.peek() != null ? ParallelScraper.term.peek().getValue() : "**";
            sb.append("\"" + entry + "\" ");
            sb.append(',');
            entry = ParallelScraper.squareFootage.peek() != null ? ParallelScraper.squareFootage.peek().getValue()
                    : "**";
            sb.append("\"" + entry + "\"");
            sb.append(',');

            //make sure parallel scraper is not also outputting results to .json
            String emailAcc = "";
            Entry email;
            while((email = ParallelScraper.emails.poll()) != null) {
                emailAcc += email.getValue() + "/";
            }
            entry = emailAcc.length() > 0 ? emailAcc : "**,";
            sb.append("\"" + entry + "\"");
            sb.append(',');
            String phoneAcc = "";
            Entry phone;
            while((phone = ParallelScraper.phoneNumbers.poll()) != null) {
                phoneAcc += phone.getValue() + " /";
            }
            entry = phoneAcc.length() > 0 ? phoneAcc : "**";
            sb.append("\"" + entry + "\"");
            sb.append(',');
            String contactAcc = "";
            Entry contact;
            while((contact = ParallelScraper.contactNames.poll()) != null) {
                contactAcc += contact.getValue() + "/";
            }
            entry = contactAcc.length() > 0 ? contactAcc : "**";
            sb.append("\"" + contactAcc + "\"");
            pw.write(sb.append('\n').toString());
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            try {
                pw.close();
            } catch(IOException e) {
                e.printStackTrace();
            }
        }
    }
}
