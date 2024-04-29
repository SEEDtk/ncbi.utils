/**
 *
 */
package org.theseed.ncbi.download;

import java.util.List;

/**
 * This object represents a partial sequence read.  It contains the read identifier and the four lines of input data.
 *
 * @author Bruce Parrello
 *
 */
public class SeqPart {

    /** enumeration for the various read types */
    public static enum Type {
        LEFT, RIGHT, SINGLETON;
    }

    // FIELDS
    /** read ID */
    private String id;
    /** read type */
    private Type type;
    /** data lines */
    private List<String> lines;


    /**
     *
     */

    // TODO data members for SeqPart

    // TODO constructors and methods for SeqPart
}
