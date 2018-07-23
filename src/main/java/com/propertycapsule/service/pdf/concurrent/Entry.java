/***
 * @author zacharycolerossman 
 * @version 7/17/18
 * 
 * Class for entries in a priority queue, sortable by the key (page number)
 */

package com.propertycapsule.service.pdf.concurrent;

public class Entry implements Comparable<Entry> {
    private Integer key;
    private String value;

    /**
     * 
     * @param pageNumber
     *            where this property data was found in the PDF
     * @param propertyData
     *            String representing an email, address, etc. from the PDF
     */
    public Entry(Integer pageNumber, String propertyData) {
        this.key = pageNumber;
        this.value = propertyData;
    }

    public Integer getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public int compareTo(Entry other) {
        return this.getKey().compareTo(other.getKey());
    }
}
