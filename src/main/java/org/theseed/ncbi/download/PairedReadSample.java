/**
 *
 */
package org.theseed.ncbi.download;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.theseed.ncbi.TagNotFoundException;
import org.w3c.dom.Element;

/**
 * @author Bruce Parrello
 *
 */
public class PairedReadSample extends ReadSample {

    // FIELDS
    /** left output stream */
    private PrintWriter leftStream;
    /** right output stream */
    private PrintWriter rightStream;
    /** singleton output stream */
    private PrintWriter singleStream;
    /** TRUE if we have zipped output */
    private boolean zipFlag;
    /** output directory for files */
    private File sampleDir;
    /** parsing pattern for a paired-read header */
    private static final Pattern PAIRED_READ_HEADER = Pattern.compile("@(\\S+)(\\.[12])\\s+.+");

    /**
     * Construct a new sample descriptor for a paired-read sample.
     *
     * @param sampleId		sample accession ID
     * @param expElement	experiment-package record from NCBI
     *
     * @throws TagNotFoundException
     */
    public PairedReadSample(String sampleId, Element expElement) throws TagNotFoundException {
        super(sampleId, expElement);
        // Insure we know the streams are closed.
        this.leftStream = null;
        this.rightStream = null;
        this.singleStream = null;
    }

    @Override
    public SeqPart processHeader(String header) throws IOException {
        Matcher m = PAIRED_READ_HEADER.matcher(header);
        if (! m.matches())
            throw new IOException("Invalid header for paired sample: " + StringUtils.abbreviate(header, 30));
        SeqPart.Type type = (m.group(2).contentEquals("1") ? SeqPart.Type.LEFT : SeqPart.Type.RIGHT);
        return new SeqPart(m.group(1), type);
    }

    @Override
    public void openStreams(File outDir, boolean zipped) throws IOException {
        // Open the paired output streams.
        this.leftStream = this.openStream(outDir, zipped, "_1");
        this.rightStream = this.openStream(outDir, zipped, "_2");
        // We only open this if we need it.  But we save the output directory and zip flag.
        this.sampleDir = outDir;
        this.zipFlag = zipped;
        this.singleStream = null;
    }

    @Override
    public void writePair(SeqPart leftRead, SeqPart rightRead) {
        leftRead.write(this.leftStream);
        rightRead.write(this.rightStream);
    }

    @Override
    public void writeSingle(SeqPart read) {
        // Insure the singleton stream is open.
        if (this.singleStream == null) {
            try {
                this.singleStream = this.openStream(this.sampleDir, this.zipFlag, "_s");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        read.write(this.singleStream);
    }

    @Override
    public void flushStreams() {
        this.leftStream.flush();
        this.rightStream.flush();
        if (this.singleStream != null)
            this.singleStream.flush();
    }

    @Override
    public void closeStreams() {
        if (this.leftStream != null)
            this.leftStream.close();
        if (this.rightStream != null)
            this.rightStream.close();
        if (this.singleStream != null)
            this.singleStream.close();
    }

    // TODO constructors and methods for PairedReadSample
}
