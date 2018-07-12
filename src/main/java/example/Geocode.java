/**
 * @author zacharycolerossman
 * @version 7/2/18
 * 
 * Handle the creating of Google Geocoder context and input of 
 * scraped property addresses
 */

package example;

import com.google.maps.*;
import com.google.maps.errors.ApiException;
import com.google.gson.*;
import com.google.maps.model.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.text.similarity.LevenshteinDistance;

public class Geocode {
	public GeoApiContext context;
	public Gson gson;
	public HashMap<String, String> map = new HashMap<String, String>();
	public final String api_key = "AIzaSyBAv1vghEx5gdzH7vHo3OLZTlFB_vm1b7U";
	public final String[] accurate_types = {"STREET_NUMBER", "PREMISE", "SUBPREMISE", "INTERSECTION"};
	public final String[] approximate_types = {"ROUTE", "LOCALITY", "POSTAL CODE", "NEIGHBORHOOD"};
	public LevenshteinDistance lev_distance = new LevenshteinDistance();
	
	public Geocode() {
		context = new GeoApiContext.Builder().apiKey(api_key).build();
		gson = new GsonBuilder().setPrettyPrinting().create();
	}
	
	/**
	 * @param scraped_address the address scraped from the pdf in Read_Text
	 * @return a Hashmap containing an address, latitude, and longitude supplied by Google Maps API
	 */
	public HashMap<String, String> getGeocodedInfo(String scraped_address) {
		map.clear();
		try {	
			GeocodingResult[] results = GeocodingApi.geocode(context, scraped_address).await();
			if(results.length > 0) {
				map.put("address", gson.toJson(results[0].formattedAddress));
				map.put("latitude", gson.toJson(results[0].geometry.location.lat));
				map.put("longitude", gson.toJson(results[0].geometry.location.lng));
				map.put("type", gson.toJson(results[0].addressComponents[0].types[0]).replaceAll("\"", ""));
				map.put("lev_distance", getLevDistance(scraped_address, results[0].formattedAddress));
			}
		}catch (IOException e) {
			e.printStackTrace();
		}catch (InterruptedException i) {
			i.printStackTrace();
		}catch (ApiException a) {
			a.printStackTrace();
		}
		return map;
	}
	
	/**
	 * Calculate the similarity of the scraped string and geocoded string using LevDistance alg
	 * @param scraped_addr 
	 * @param geo_addr
	 * @return the calculated lev distance as a string
	 */
	public String getLevDistance(String scraped_addr, String geo_addr) {
		//find indices for the state abbreviation in scraped_addr and geo_addr
		//will be used to get more comparable substrings
		Pattern scrape_pattern = Pattern.compile(".*, [a-zA-Z]{2}( |[.])?");
	    Matcher scrape_matcher = scrape_pattern.matcher(scraped_addr);
	    Pattern geo_pattern = Pattern.compile(", [A-Z]{2} ");
	    Matcher geo_matcher = geo_pattern.matcher(geo_addr);
	    int scraped_end_index;
	    int geo_index;
	    if (scrape_matcher.find()) {
	    	scraped_end_index = scrape_matcher.end();
	    }else {
	    	scraped_end_index = scraped_addr.length();
	    }    
	    if(geo_matcher.find()) {
	    	geo_index = geo_matcher.end();
	    }else {
	    	geo_index = geo_addr.length();
	    }
	    
	    //find lev distance between the two modified address strings
		int lev_d = lev_distance.apply(scraped_addr.substring(0, scraped_end_index), geo_addr.substring(0, geo_index).toLowerCase());
		return Integer.toString(lev_d);	
	}
			
}
