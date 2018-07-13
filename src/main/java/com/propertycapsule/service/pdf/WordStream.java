package com.propertycapsule.service.pdf;
/**
 * Class to break up input from lines into lexical items, namely
 * words and punctuation.  Lines are entered and decomposed into parts
 * by calling addLexItems.  The lexical items can be obtained one at a 
 * time by calling nextToken.  hasMoreTokens returns whether there are
 * still more lexical items to be obtained from the wordstream.
 * @author kim
 * @version 1/2011
 *  revised: added package --kpc 2/13
 *
 */

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class WordStream {
    // list of lexical items (words and punctuation) from input text
    private List<String> lexItems;
    public String lastToken = "";

    // index of next item to be returned from nextToken method
    private int nextItem = 0;

    // Characters that indicate the end of a word, including punctuation and
    // "white space"
    private static final String WORD_DELIMITERS = "~ '\";:!\n\t";

    /**
     * Construct an empty WordStream
     */
    public WordStream() {
        lexItems = new ArrayList<String>();
    }

    /**
     * @param line
     *            string containing words to be placed in list of lexical items
     * @post: all lexical items (including punctuation) have been added to list
     *        of items
     */
    public void addLexItems(String line) {
        // true in constructor says return delimiters as well as words
        StringTokenizer st = new StringTokenizer(line, WORD_DELIMITERS, true);
        while(st.hasMoreElements()) {
            lexItems.add(st.nextToken());
        }
    }

    /**
     * @param item
     *            character from input
     * @return true iff it corresponds to white space, i.e., space, newline, or
     *         tab.
     */
    private boolean isWhiteSpace(char item) {
        return item == ' ' || item == '\n' || item == '\t';
    }

    /**
     * @return next token from list of lexical items that is not whitespace
     */
    public String nextToken() {
        while(nextItem < lexItems.size() && isWhiteSpace(lexItems.get(nextItem).charAt(0))) {
            nextItem++;
        }
        if(nextItem < lexItems.size()) {
            // lastToken = lexItems.get(nextItem-1);
            nextItem++;
            return lexItems.get(nextItem - 1);
        } else {
            throw new IndexOutOfBoundsException("out of tokens");
        }
    }

    /**
     * @return true iff there are more tokens to be returned from list of
     *         lexical items
     */
    public boolean hasMoreTokens() {
        return nextItem < lexItems.size() - 1;
    }

    // Simple program to see if WordStream correctly returns tokens from input.
    public static void main(String[] args) {
        WordStream ws = new WordStream();
        ws.addLexItems("This is a test, to see if\n anything\t works.");
        while(ws.hasMoreTokens()) {
            System.out.println(":" + ws.nextToken() + ":");
        }
    }
}
