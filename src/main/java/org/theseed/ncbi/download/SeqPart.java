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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

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
    /** parsing pattern for a paired-read header */
    private static final Pattern PAIRED_READ_HEADER = Pattern.compile("@(\\S+)(\\.[12])\\s+.+");
    /** parsing pattern for a singleton-read header */
    private static final Pattern SINGLE_READ_HEADER = Pattern.compile("@(\\S+).*");


    /** enumeration for the various read types */
    public static enum Type {
        LEFT, RIGHT, SINGLETON;
    }

    /**
     * Create a partial read from the next records in an input stream.  An early end-of-file or
     * an invalid record will cause an IO exception; however, the first record must exist.
     *
     * @param iter		string iterator for the input stream
     *
     * @throws IOException
     */
    public SeqPart(Iterator<String> iter) throws IOException {
        this.lines = new ArrayList<String>(4);
        // Get the header.
        String header = iter.next();
        Matcher m = PAIRED_READ_HEADER.matcher(header);
        if (m.matches()) {
            // Here we have a paired read.
            this.id = m.group(1);
            this.type = (m.group(2).endsWith("1") ? Type.LEFT : Type.RIGHT);
        } else {
            // Here we have a singleton read.
            m = SINGLE_READ_HEADER.matcher(header);
            if (! m.matches())
                throw new IOException("Invalid FASTQ record starting with \"" + StringUtils.abbreviate(header, 20) + "\".");
            else {
                this.id = m.group(1);
                this.type = Type.SINGLETON;
            }
        }
        // Save the header line.
        this.lines.add(header);
        try {
            // Save the sequence data line.
            this.lines.add(iter.next());
            // Verify the quality header.
            String qualHeader = iter.next();
            if (qualHeader.charAt(0) != '+')
                throw new IOException(this.type.toString() + " FASTQ record for " + this.id + " has invalid quality header.");
            this.lines.add(qualHeader);
            // Save the quality data line.
            this.lines.add(iter.next());
        } catch (NoSuchElementException e) {
            throw new IOException("End-of-file before full FASTQ record completed.");
        }
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
