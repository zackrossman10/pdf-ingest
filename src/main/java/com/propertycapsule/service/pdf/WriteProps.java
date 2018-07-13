package com.propertycapsule.service.pdf;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

public class WriteProps {

    public static void main(String[] args) {
        try {
            Properties properties = new Properties();
            properties.setProperty("AWS_ACCESS_KEY", "AKIAJHKXWIYXC6HAQC3Q");
            properties.setProperty("AWS_SECRET_KEY", "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");
            properties.setProperty("GOOGLE_GEOCODE_API_KEY", "Nicole");
            File file = new File("config.properties");
            FileOutputStream fileOut = new FileOutputStream(file);
            properties.store(fileOut, "API Keys");
            fileOut.close();
            System.out.println("Done!");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
