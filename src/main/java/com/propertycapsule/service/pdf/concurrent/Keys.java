/**
 * @author zacharycolerossman
 * @version 7/17/18
 * 
 * Allows access to API keys in resources/config.properties file
 */

package com.propertycapsule.service.pdf.concurrent;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Keys {
    Properties prop = new Properties();
    String result = "";
    InputStream inputStream;

    //get the value of a key
    public String getKeyValue(String key) {
        try {
            inputStream = getClass().getClassLoader().getResourceAsStream("config.properties");
            prop.load(inputStream);
            return prop.getProperty(key);
        } catch(IOException e) {
            System.out.println("Exception: " + e);
        } finally {
            try {
                inputStream.close();
            } catch(IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }
}
