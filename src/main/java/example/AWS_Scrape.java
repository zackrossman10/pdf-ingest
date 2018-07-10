/**
 * @author zacharycolerossman
 * @version 7/2/18
= * 
 * 1) translate multiple PDFs into .txt files (using PDFBox)
 * 2) scrape desired information related to each property
 * 3) export that scraped information as a .csv or .json file
 */

package example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class AWS_Scrape {
	public static final String[] property_criteria   = {"Address", "Geocoded Address", "Latitude", "Longitude", "Term", "Square Footage", "Emails", "Phone Numbers", "Contact Names"};
	public static final HashMap<String, String> abbreviations = new HashMap<String, String>(){{
		put("alabama", "AL");
		put("alaska", "AK");
		put("arizona", "AZ");
		put("arkansas", "AR");
		put("california", "CA");
		put("colorado", "CO");
		put("connecticut", "CT");
		put("delaware", "DE");
		put("florida", "FL");
		put("georgia", "GA");
		put("hawaii", "HI");
		put("idaho", "ID");
		put("illinois", "IL");
		put("indiana", "IN");
		put("iowa", "IA");
		put("kansas", "KS");
		put("kentucky", "KY");
		put("lousisiana", "LA");
		put("maine", "ME");
		put("maryland", "MD");
		put("massachusetts", "MA");
		put("michigan", "MI");
		put("minnesota", "MN");
		put("mississippi", "MS");
		put("missouri", "MO");
		put("montana", "MT");
		put("nebraska", "NE");
		put("nevada", "NV");
		put("new hampshire", "NH");
		put("new jersey", "NJ");
		put("new mexico", "NM");
		put("new york", "NY");
		put("north carolina", "NC");
		put("north dakota", "ND");
		put("ohio", "OH");
		put("oklahoma", "OK");
		put("oregon", "OR");
		put("pennsylvania", "PA");
		put("rhode island", "RI");
		put("south carolina", "SC");
		put("south dakota", "SD");
		put("tennessee", "TN");
		put("texas", "TX");
		put("utah", "UT");
		put("vermont", "VT");
		put("virginia", "VA");
		put("washington DC", "DC");
		put("washington", "WA");
		put("west virginia", "WV");
		put("wisconson", "WI");
		put("wyoming", "WY");
	}};
	public static Collection<String> list_abbrevs = abbreviations.values();
	public static Geocode geocoder = new Geocode();
	public static final String output_file_path = "/Users/zacharycolerossman/Documents/ML_Flyer_Data/";
	public static final String input_folder_path = "/Users/zacharycolerossman/Documents/ML_Flyer_Data/Working_Set/";
	
	/**
	 * Main wrapper function for scraping a pdf file
	 * @param input PDF file to scrape
	 * @return a .json file containing the results
	 */
	public static File scrape(File input){
		File txt_output = PDFToTxt(input);
		HashMap<String, ArrayList<String>> data = scrapeTxt(txt_output);
		return resultsToJson(data);
	}
	
	/**
	 * Use PDFBox to extract text from a PDF, write to a txt file
	 * @param input PDF file to scrape
	 * @return txt file which contains PDF text
	 * @throws IOException 
	 */
	public static File PDFToTxt(File input){
//		File output_txt = AWS_Wrapper.createTmp("output", ".txt");
		File output_txt = new File(TestOutput.output_file_path+TestOutput.input_pdf_name+".txt");
		PDDocument document = null;
		FileWriter writer = null;
		FileWriter writer2 = null;
		BufferedReader bufferreader = null;
		try {
			// 1) TRANSLATE PDF TO TXT DOC
			PDFTextStripper pdfStripper = new PDFTextStripper();
			document = PDDocument.load(input);
			String pdf_text = pdfStripper.getText(document);
			writer = new FileWriter(output_txt);
			// pdf text parses better when spaces replaced with special char "~", will be
			pdf_text = pdf_text.replace(" ", "~");
			writer.write(pdf_text);
			//must close this writer before opening writer2
			writer.close();
			
			// 2) REFORMAT NEW TXT DOC FOR BETTER PARSING
			bufferreader = new BufferedReader(new FileReader(output_txt));
			String oldContent = "";
			String line = bufferreader.readLine();
			while (line != null) {
				//edge case for unnecessary spaces between text (e.g. "4 2  A V E N U E")
				if (line.matches("[~]?[^~][~][^~][~][~]?([^~]?[^~][~][~]?)*[^~]?")) {
					line = line.replaceAll("~~", " ").replaceAll("~", "");
				}
				line = line.toLowerCase();
				//edge case for addresses ending in "... San Francisco" and missing state/zip
				if(line.matches(".*san~francisco")) {
					line += ", ca";
				}
				oldContent = oldContent + line + System.lineSeparator();
				line = bufferreader.readLine();
			}
			// remove/reformat special chars for better parsing
			String newContent = oldContent.replaceAll("~~", " ").replaceAll("~", " ").replaceAll("±", "").replace(" •", ",").replace(" |", ",");
			for(HashMap.Entry<String, String> entry : abbreviations.entrySet()) {
				//replace "florida" with "FL" for better address parsing
				newContent = newContent.replaceFirst(entry.getKey(), entry.getValue());
			}
			writer2 = new FileWriter(output_txt);
			writer2.write(newContent);
			writer2.write(" ");
			return output_txt;
		}catch (IOException e) {
			e.printStackTrace();
			return null;
		}finally {
			if (document != null) {
				try {
					document.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (bufferreader != null) {
				try {
					bufferreader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (writer2 != null) {
				try {
					writer2.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Scrape a txt file for property criteria
	 * @param filename name of the txt file to scrape
	 * @return a hashmap containing categorized property data for this file
	 */
	public static HashMap<String, ArrayList<String>> scrapeTxt(File txt_input) {
		//hashmap holding arrays of different information for this pdf
		HashMap<String, ArrayList<String>> property_data  = new HashMap<String, ArrayList<String>>();
		// arrays which accumulate data by category
		ArrayList<String> addresses 	   = new ArrayList<String>();
		ArrayList<String> geocoded_address = new ArrayList<String>();
		ArrayList<String> latitude 		   = new ArrayList<String>();
		ArrayList<String> longitude 	   = new ArrayList<String>();
		ArrayList<String> terms 		   = new ArrayList<String>();
		ArrayList<String> emails 		   = new ArrayList<String>();
		ArrayList<String> square_footages  = new ArrayList<String>();
		ArrayList<String> phone_nums 	   = new ArrayList<String>();
		BufferedReader bufferreader = null;
		// word stream supplies tokens from each line
		WordStream ws = new WordStream();
		try {
			bufferreader = new BufferedReader(new FileReader(txt_input));
			String line, previous_line;
			line = previous_line = "";
			boolean found_lease_term, found_sale_term;
			found_lease_term = found_sale_term = false;
			while ((line = bufferreader.readLine()) != null) {
				String last_token = "";
				ws.addLexItems(line);
				if (addresses.isEmpty() && (line.matches("[a-zA-Z'@ ]*[,][ ][A-Za-z]{2}?") || line.matches("[a-zA-Z'@ ]*[,][ ][A-Za-z]{2}[ ][0-9]{5}.*"))){
					// match ADDRESSES like "Orlando, FL" or "Round Rock, ca 91711" (with street address on the previous line)
					if(!previous_line.contains("suite") && !previous_line.contains("floor") && !line.contains("suite") && !previous_line.contains("floor")) {
						//avoid matching the office address
						if(line.length() < 30) {
							addresses.add((previous_line + ", " + line).replace("'", ""));
						}
					}
				}else if (addresses.isEmpty() && (line.matches(".*[,][ ][A-Za-z]{2}([ ].*|[.])?") || line.matches(".*[,][ ][A-Za-z]{2}[ ][0-9]{5}.*"))) {
					// match ADDRESSES like "222 W Avenida Valencia, Orlando, FL" or "222 W Avenida Valencia, Round Rock, TX"
					if(!previous_line.contains("suite") && !previous_line.contains("floor") && !line.contains("suite") && !previous_line.contains("floor")) {
						if(line.length() < 50) {
							addresses.add(line.replace("'", ""));
						}
					}	
				}
				while (ws.hasMoreTokens()) {
					// surround with try/catch to protect against weird character replacement/parsing issue
					try {
						String token = ws.nextToken();
						if (terms.isEmpty() && (token.toLowerCase().equals("lease") || token.toLowerCase().equals("leased"))) {
							// match LEASE TERMS of the property
							terms.add("Lease");
							found_lease_term = true;
						} else if (terms.isEmpty() && token.toLowerCase().equals("sale")) {
							// match SALE TERMS of the property
							terms.add("Sale");
							found_sale_term = true;
						} else if (token.matches("([a-zA-Z0-9]+[.])*[a-zA-Z0-9]+[@][a-zA-Z0-9]+.*") && !emails.contains(token)) {
							// match EMAILS
							emails.add(token);
						} else if (square_footages.isEmpty() && (token.toLowerCase().equals("sf") || token.toLowerCase().equals("square"))) {
							// match SQUARE FOOTAGES like "1,000,000", "1000000" ("1,00,00 or 1000,0 -> fail)
							if (last_token.matches("^(\\d+|\\d{1,3}(,\\d{3})*)(\\.\\d+)?$")) {
								square_footages.add(last_token);
							}
						} else if (token.matches("[0-9]{3}") && last_token.matches("[0-9]{3}") && ws.hasMoreTokens()) {
							// match PHONE NUMBERS like "425 241 7707"
							String next_four = ws.nextToken();
							String phone_number = last_token + "-" + token + "-" + next_four;
							if (!phone_nums.contains(phone_number)) {
								phone_nums.add(phone_number);
							}
						} else if (token.matches("[0-9]{3}[.][0-9]{3}[.][0-9]{4}") && !phone_nums.contains(token.replace(".", "-"))) {
							// match PHONE NUMBERS like "425.241.7707"
							String phone_number = token.replace(".", "-");
							phone_nums.add(phone_number);
						} else if (token.matches("[0-9]{3}[-][0-9]{3}[-][0-9]{4}") && !phone_nums.contains(token)) {
							// match PHONE NUMBERS like "425-241-7707"
							phone_nums.add(token);
						} else if (token.matches("[(][0-9]{3}[)]") && ws.hasMoreTokens()) {
							// match PHONE NUMBERS like "(425) 241-7707"
							String the_rest = ws.nextToken();
							if (the_rest.matches("[0-9]{3}[-][0-9]{4}") && !phone_nums.contains(token.substring(1, 4) + "-" + the_rest)) {
								String phone_number = token.substring(1, 4) + "-" + the_rest;
								phone_nums.add(phone_number);
							}
						}
						last_token = token;
					} catch (java.lang.IndexOutOfBoundsException e) {
						break;
					}
				}
				previous_line = line;
			}
			if(!addresses.isEmpty()) {
				System.out.println("Found address");
				HashMap<String, String> geocoded_info = geocoder.getGeocodedInfo(addresses.get(0));
				if(geocoded_info != null) {
					geocoded_address.add(geocoded_info.get("address").replaceAll("\"", ""));
					latitude.add(geocoded_info.get("latitude"));
					longitude.add(geocoded_info.get("longitude"));
				}else {
					addresses.remove(0);
					addresses.add("**Unknown**");
					latitude.add("**Unknown**");
					longitude.add("**Unknown**");
				}
			}else {
				System.out.println("Unfound address");
				AWS_Wrapper.alertNoAddress();
				System.out.println("done!");
				addresses.add("**Unknown**");
				geocoded_address.add("**Unknown**");
				latitude.add("**Unknown**");
				longitude.add("**Unknown**");
			}
			if (terms.isEmpty()) {
				terms.add("**Unknown**");
			}else if (found_lease_term & found_sale_term) {
				terms.remove("Lease");
				terms.remove("Sale");
				terms.add("Both");
			}
			if (square_footages.isEmpty()) {
				square_footages.add("**Unknown**");
			}
			if (emails.isEmpty()) {
				emails.add("**Unknown**");
			}
			if (phone_nums.isEmpty()) {
				phone_nums.add("**Unknown**");
			}
			bufferreader.close();
			property_data.put("Address", addresses);	
			property_data.put("Geocoded Address", geocoded_address);	
			property_data.put("Latitude", latitude);	
			property_data.put("Longitude", longitude);	
			property_data.put("Term", terms);	
			property_data.put("Square Footage", square_footages);	
			property_data.put("Emails", emails);	
			property_data.put("Phone Numbers", phone_nums);
			ArrayList<String> contacts = scrapeContactNames(emails, txt_input);
			property_data.put("Contact Names", contacts);
			return property_data;
		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
			return null;
		} catch (IOException ex) {
			ex.printStackTrace();
			return null;
		} finally {
			if(bufferreader != null) {
				try {
					bufferreader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Scraper function for contact names
	 * @param emails -> list of emails found in this pdf, will be parsed to find the name
	 * @param pdf_name -> pdf to scrape
	 * @return arrayList of contact names for the pdf
	 */
	public static ArrayList<String> scrapeContactNames(ArrayList<String> emails, File txt_input) {
		ArrayList<String> contacts = new ArrayList<String>();
		WordStream ws = new WordStream();
		BufferedReader bufferreader = null;
		try {
			bufferreader = new BufferedReader(new FileReader(txt_input));
			String line, entry, token, last_token;
			line = entry = token = last_token = "";
			boolean has_period, found_contact;
			has_period = found_contact = false;
			if(emails.get(0).equals("**Unknown**")) {
				contacts.add("**Unknown**");
			}else {
				for (String email : emails) {
					//isolate the name from the email address ("zackross@..." -> "zackross")
					String search_name = email.substring(0, email.indexOf("@")).toLowerCase();
					if (search_name.contains(".")) {
						//search for "zack" if email is "zack.rossman@..."
						search_name = search_name.substring(0, search_name.indexOf("."));
						has_period = true;
					}
					while (!found_contact && (line = bufferreader.readLine()) != null) {
						ws.addLexItems(line);
						while (ws.hasMoreTokens()) {
							try {
								token = ws.nextToken();
								if (token.toLowerCase().contains(search_name) && ws.hasMoreTokens()) {
									String last_name = ws.nextToken();
									entry = token.substring(0, 1).toUpperCase() + token.substring(1).toLowerCase() + " "
											+ last_name.substring(0, 1).toUpperCase()
											+ last_name.substring(1).toLowerCase();
									if (entry.contains("@")) {
										// in case the email itself is the only matched string, return the text before 
										entry = entry.substring(entry.indexOf(search_name), entry.indexOf("@"));
									}
									contacts.add(entry);
									found_contact = true;
								} else if (!has_period
										&& token.toLowerCase().contains(search_name.substring(1, search_name.length()))) {
									// serarch for last name for emails with format zrossman@...
									entry = last_token.substring(0, 1).toUpperCase() + last_token.substring(1).toLowerCase()
											+ " " + token.substring(0, 1).toUpperCase() + token.substring(1).toLowerCase();
									if (entry.contains("@")) {
										entry = entry.substring(entry.indexOf(0), entry.indexOf("@"));
									}
									contacts.add(entry);
									found_contact = true;
								}
								last_token = token;
							} catch (java.lang.IndexOutOfBoundsException e) {
								break;
							}
						}
					}
					if (!found_contact) {
						contacts.add("**Not found** " + search_name);
					}
					last_token = "";
					found_contact = false;
				}
			}
			return contacts;
		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
			return null;
		} catch (IOException ex) {
			ex.printStackTrace();
			return null;
		} finally {
			if(bufferreader != null) {
				try {
					bufferreader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * Format the scraped information from all PDFs into one new .json file
	 * @param results contains the property data for every pdf file processed
	 * @param input_pdf_names -> arraylist of names of pdfs that were scraped
	 * @throws IOException
	 */
	public static File resultsToJson(HashMap<String, ArrayList<String>> results){
		File json_output = AWS_Wrapper.createTmp("output", ".json");
		JSONObject file_object = new JSONObject();
		for(String criteria : property_criteria) {
			if(criteria.equals("Emails") || criteria.equals("Phone Numbers") || criteria.equals("Contact Names")) {
				JSONArray list = new JSONArray();
				for(String result : results.get(criteria)) {
					list.add(result);
				}
				file_object.put(criteria, list);
			}else {
				file_object.put(criteria, results.get(criteria).get(0));
			}
		}
		FileWriter filew = null;
		try {
			filew = new FileWriter(json_output);
			Gson gson = new GsonBuilder().setPrettyPrinting().create();
			JsonParser jp = new JsonParser();
			JsonElement je = jp.parse(file_object.toJSONString());
	        filew.write(gson.toJson(je));
	        filew.flush();
	        return json_output;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} finally {
			if(filew != null) {
				try {
					filew.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
