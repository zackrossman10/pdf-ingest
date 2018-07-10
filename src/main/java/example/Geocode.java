/**
 * @author zacharycolerossman
 * @version 7/2/18
 * 
 * Initialize a geocoder using Apple Maps API, use it to double-check the 
 * scraped property address and supply a lat/long
 */

package example;

import com.google.maps.*;
import com.google.maps.errors.ApiException;
import com.google.gson.*;
import com.google.maps.model.*;
import java.io.IOException;
import java.util.HashMap;

public class Geocode {
	public GeoApiContext context;
	public Gson gson;
	public HashMap<String, String> map = new HashMap<String, String>();
	public final String api_key = "AIzaSyBAv1vghEx5gdzH7vHo3OLZTlFB_vm1b7U";
	
	/**
	 * @param api_key for Google Maps API
	 */
	public Geocode() {
		context = new GeoApiContext.Builder().apiKey(api_key).build();
		gson = new GsonBuilder().setPrettyPrinting().create();
	}
	
	/**
	 * @param scraped_address the address scraped from the pdf in Read_Text
	 * @return a Hashmap containing an address, latitude, and longitude supplied by Google Maps API
	 */
	public HashMap<String, String> getGeocodedInfo(String scraped_address) {
		try {	
			GeocodingResult[] results = GeocodingApi.geocode(context, scraped_address).await();
			if(results.length > 0) {
				map.put("address", gson.toJson(results[0].formattedAddress));
				map.put("latitude", gson.toJson(results[0].geometry.location.lat));
				map.put("longitude", gson.toJson(results[0].geometry.location.lng));
			}
			return map;
		}catch (IOException e) {
			return map;
		}catch (InterruptedException i) {
			return null;
		}catch (ApiException a) {
			return null;
		}
	}
}
