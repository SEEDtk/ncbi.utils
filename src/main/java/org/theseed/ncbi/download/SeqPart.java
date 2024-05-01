/**
 *
 */
package org.theseed.ncbi.download;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * This object represents a partial sequence read.  It contains the read identifier and the four lines of input data.
 *
 * @author Bruce Parrello
 *
 */
public class SeqPart {

    // FIELDS
    /** read ID */
    private String id;
    /** read type */
    private Type type;
    /** data lines */
    private List<String> lines;

    /** enumeration for the various read types */
    public static enum Type {
        LEFT, RIGHT, SINGLETON;
    }

    /**
     * Create a partial read from the next records in an input stream.  An early end-of-file or
     * an invalid record will cause an IO exception; however, the first record must exist.
     *
     * @param sample	sample descriptor
     * @param iter		string iterator for the input stream
     *
     * @throws IOException
     */
    public static SeqPart read(ReadSample sample, Iterator<String> iter) throws IOException {
        // Get the header.
        String header = iter.next();
        SeqPart retVal = sample.processHeader(header);
        // Set up the line buffer.
        retVal.lines = new ArrayList<String>(4);
        // Save the header line.
        retVal.lines.add(header);
        try {
            // Save the sequence data line.
            retVal.lines.add(iter.next());
            // Verify the quality header.
            String qualHeader = iter.next();
            if (qualHeader.charAt(0) != '+')
                throw new IOException(retVal.type.toString() + " FASTQ record for " + retVal.id + " has invalid quality header.");
            retVal.lines.add(qualHeader);
            // Save the quality data line.
            retVal.lines.add(iter.next());
        } catch (NoSuchElementException e) {
            throw new IOException("End-of-file before full FASTQ record completed.");
        }
        return retVal;
    }

    /**
     * Construct a new sequence part with the specified ID and type.
     *
     * @param id2		sequence ID
     * @param type2		sequence type
     */
    protected SeqPart(String id2, Type type2) {
        this.id = id2;
        this.type = type2;
    }

    /**
     * @return TRUE if this sequence read part has the same ID as the other sequence read part
     *
     * @param other		other sequence read part to check
     */
    public boolean matches(SeqPart other) {
        return this.id.contentEquals(other.id);
    }

    /**
     * Write this read part to the specified output stream.
     *
     * @param stream	target output stream
     */
    public void write(PrintWriter stream) {
        for (String line : this.lines)
            stream.println(line);
    }

    /**
     * @return the read ID
     */
    public String getId() {
        return this.id;
    }

    /**
     * @return the read type
     */
    public Type getType() {
        return this.type;
    }

}
