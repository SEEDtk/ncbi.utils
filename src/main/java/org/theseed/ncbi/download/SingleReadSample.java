/**
 *
 */
package org.theseed.ncbi.download;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.theseed.ncbi.TagNotFoundException;
import org.w3c.dom.Element;

/**
 * This is a read-sample descriptor for singleton read streams.  Here every read is a separate, unrelated thing
 * and there is a single output stream.
 *
 * @author Bruce Parrello
 *
 */
public class SingleReadSample extends ReadSample {

    // FIELDS
    /** output file stream */
    private PrintWriter outStream;
    /** parsing pattern for a singleton-read header */
    private static final Pattern SINGLE_READ_HEADER = Pattern.compile("@(\\S+).*");

    /**
     * Create a single-read sample descriptor.
     *
     * @param sampleId
     * @param expElement
     * @throws TagNotFoundException
     */
    protected SingleReadSample(String sampleId, Element expElement) throws TagNotFoundException {
        super(sampleId, expElement);
        // Insure we know that no stream is open.
        this.outStream = null;
    }

    @Override
    public SeqPart processHeader(String header) throws IOException {
        Matcher m = SINGLE_READ_HEADER.matcher(header);
        if (! m.matches())
            throw new IOException("Invalid header for unpaired sample: " + StringUtils.abbreviate(header, 30));
        return new SeqPart(m.group(1), SeqPart.Type.SINGLETON);
    }

    @Override
    public void openStreams(File outDir, boolean zipped) throws IOException {
        // Open a single output file.
        this.outStream = this.openStream(outDir, zipped, "");
    }

    @Override
    public void writePair(SeqPart leftRead, SeqPart rightRead) {
        // Write both reads to the output file.
        leftRead.write(this.outStream);
        rightRead.write(this.outStream);
    }

    @Override
    public void writeSingle(SeqPart read) {
        read.write(this.outStream);
    }

    @Override
    public void flushStreams() {
        this.outStream.flush();

    }

    @Override
    public void closeStreams() {
        // Close the single output file.
        if (this.outStream != null)
            this.outStream.close();
    }

    @Override
    public String toString() {
        return this.getId() + "(" + this.getRuns().size() + " runs, single-stream)";
    }


}
