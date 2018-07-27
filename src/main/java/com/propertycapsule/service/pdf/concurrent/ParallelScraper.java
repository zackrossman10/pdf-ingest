/***
 * @author zacharycolerossman 
 * @version 7/17/18
 * 
 * Class which facilitates the scraping of PDF data using threads
 * 
 */

package com.propertycapsule.service.pdf.concurrent;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.maps.model.GeocodingResult;

public class ParallelScraper {
    // priority queues to accumulate property data scraped by threads operating
    // in parallel
    public static PriorityQueue<Entry> address;
    public static PriorityQueue<Entry> term;
    public static PriorityQueue<Entry> squareFootage;
    public static PriorityQueue<Entry> emails;
    public static PriorityQueue<Entry> phoneNumbers;
    public static PriorityQueue<Entry> contactNames;

    public static Geocode geocoder;
    public static Gson gson;
    public static final int parsedPageLength = 2000;
    public static final int poolSize = 5;

    public ParallelScraper() {
        // redeclare static vars to start from clean slate
        address = new PriorityQueue<Entry>();
        term = new PriorityQueue<Entry>();
        squareFootage = new PriorityQueue<Entry>();
        emails = new PriorityQueue<Entry>();
        phoneNumbers = new PriorityQueue<Entry>();
        contactNames = new PriorityQueue<Entry>();
        geocoder = new Geocode();
        gson = new GsonBuilder().setPrettyPrinting().create();
    }

    /**
     * Creates and manages threads that scrape a PDF file
     * 
     * @param input
     *            PDF file to be scraped
     * @return json file with scraped property data
     */
    public File scrape(File input) {
        PDDocument document = null;
        try {
            document = PDDocument.load(input);
            // translate PDF into multiple txt files, each with
            // <parsedPageLength> chars
            ArrayList<File> txtFiles = stringToMultipleTxts(pdfToString(document));
            // create pool for threading
            ExecutorService pool = Executors.newFixedThreadPool(poolSize);
            List<Runnable> arrTasks = new ArrayList<Runnable>();
            int pageCounter = 1;
            // accumulate tasks
            for(File file : txtFiles) {
                Runnable task = new ScrapeTask(file, pageCounter);
                arrTasks.add(task);
                pageCounter++;
            }
            //execute tasks
            for(Runnable task : arrTasks) {
                pool.execute(task);
            }
            // wait for threads to finish
            pool.shutdown();
            try {
                while(!pool.awaitTermination(24L, TimeUnit.HOURS)) {
                    System.out.println("Not yet. Still waiting for termination");
                }
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            if(document != null) {
                try {
                    document.close();
                } catch(IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
//        return resultsToJson();
    }

    /**
     * Extract text from a PDF to a string using PDF Box
     * 
     * @param input
     *            PDF file to scrape
     * @return txt file which contains PDF text
     */
    public String pdfToString(PDDocument document) {
        PDFTextStripper pdfStripper = null;
        String pdfString = "";
        try {
            pdfStripper = new PDFTextStripper();
            pdfString = pdfStripper.getText(document);
            // pdf text parses better when spaces replaced with special char
            // "~", will be
            pdfString = pdfString.replace(" ", "~");
        } catch(IOException e) {
            e.printStackTrace();
        }
        return pdfString;
    }

    /**
     * Write a string to multiple smaller txt files
     * 
     * @param pdfText
     *            the string containing PDF text from PDFStripper
     * @return ArrayList of txt files to be scraped
     */
    public ArrayList<File> stringToMultipleTxts(String pdfText) {
        ArrayList<File> outputTxts = new ArrayList<File>();
        int textLength = pdfText.length();
        int startIndex = 0;
        // use parsedPageLength to determine character size of each file
        int endIndex = textLength > parsedPageLength ? parsedPageLength : textLength;
        String textSegment = "";
        File outputTxt = null;
        FileWriter writer = null;
        int pageCounter = 1;
        while(true) {
            textSegment = pdfText.substring(startIndex, endIndex);
            try {
                outputTxt = File.createTempFile("Output" + Integer.toString(pageCounter), ".txt");
                writer = new FileWriter(outputTxt);
                writer.write(textSegment);
            } catch(IOException e) {
                e.printStackTrace();
            } finally {
                if(writer != null) {
                    try {
                        writer.close();
                    } catch(IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            outputTxts.add(outputTxt);
            if(endIndex == textLength)
                break;
            startIndex += parsedPageLength;
            endIndex = textLength > (startIndex + parsedPageLength) ? (startIndex + parsedPageLength) : textLength;
        }
        return outputTxts;
    }

    /**
     * Format scraped information from a PDF into .json file
     * 
     * @return json file containing property information
     */
    public File resultsToJson() {
        File jsonOutput = null;
        try {
            jsonOutput = File.createTempFile("output", ".json");
            JSONObject fileObject = new JSONObject();
            if(!address.isEmpty()) {
                String scrapedAddress = address.peek().getValue();
                fileObject.put("scraped_address", scrapedAddress);
                // Use Geocoder API to validate address, get property lat/long
                GeocodingResult[] geoResults = geocoder.getParallelGeocodedInfo(scrapedAddress);
                if(geoResults.length > 0) {
                    String geocodedAddress = gson.toJson(geoResults[0].formattedAddress).replaceAll("\"", "");
                    String addressType = gson.toJson(geoResults[0].addressComponents[0].types[0]).replaceAll("\"", "");
                    String latitude = gson.toJson(geoResults[0].geometry.location.lat);
                    String longitude = gson.toJson(geoResults[0].geometry.location.lng);
                    fileObject.put("geocoded_address", geocodedAddress);
                    fileObject.put("address_type", addressType);
                    fileObject.put("latitude", latitude);
                    fileObject.put("longitude", longitude);
                }
            }
            if(!term.isEmpty()) {
                fileObject.put("term", term.peek().getValue());
            }
            if(!squareFootage.isEmpty()) {
                fileObject.put("square_footage", squareFootage.peek().getValue());
            }
            // for fields that can include mulitple vals, put each val to a JSON
            // list
            if(!emails.isEmpty()) {
                JSONArray list = new JSONArray();
                while(!emails.isEmpty()) {
                    String data = emails.poll().getValue();
                    if(!list.contains(data)) {
                        list.add(data);
                    }
                }
                fileObject.put("emails", list);
            }
            if(!phoneNumbers.isEmpty()) {
                JSONArray list = new JSONArray();
                while(!phoneNumbers.isEmpty()) {
                    String data = phoneNumbers.poll().getValue();
                    if(!list.contains(data)) {
                        list.add(data);
                    }
                }
                fileObject.put("phone_numbers", list);
            }
            if(!contactNames.isEmpty()) {
                JSONArray list = new JSONArray();
                while(!contactNames.isEmpty()) {
                    String data = contactNames.poll().getValue();
                    if(!list.contains(data)) {
                        list.add(data);
                    }
                }
                fileObject.put("contact_names", list);
            }
            FileWriter filew = null;
            try {
                filew = new FileWriter(jsonOutput);
                Gson gson = new GsonBuilder().setPrettyPrinting().create();
                JsonParser jp = new JsonParser();
                JsonElement je = jp.parse(fileObject.toJSONString());
                filew.write(gson.toJson(je));
                filew.flush();
            } catch(IOException e) {
                e.printStackTrace();
            } finally {
                if(filew != null) {
                    try {
                        filew.close();
                    } catch(IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch(IOException e1) {
            e1.printStackTrace();
        }
        return jsonOutput;
    }
}
