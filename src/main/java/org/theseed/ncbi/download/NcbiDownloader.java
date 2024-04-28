/**
 *
 */
package org.theseed.ncbi.download;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This method downloads one or more runs from the NCBI.  Each run is downloaded by starting the fastq-dump utility in a
 * separate process.  This program intercepts the output and sorts it into left, right, and singelton files appropriately.
 *
 * If multiple runs are downloaded, they are all concatenated.  The sequence IDs are prefixed with a run sequence number
 * to insure they are unique.
 *
 * The client provides a sample ID that is used to compute the output file names and also in log messages to identify the
 * sample.
 *
 * @author Bruce Parrello
 *
 */
public class NcbiDownloader implements AutoCloseable {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(NcbiDownloader.class);
    /** list of runs to download */
    private Set<String> runList;
    /** left output file stream */
    private OutputStream leftFastqStream;
    /** right output file stream */
    private OutputStream rightFastqStream;
    /** singleton output file stream */
    private OutputStream singleFastqStream;
    /** sample ID for tracing */
    private String sampleId;
    /** number of errors */
    private int errorCount;
    /** number of paired records */
    private int pairCount;
    /** number of singleton records */
    private int singleCount;
    /** number of runs processed */
    private int runCount;
    /** prefix for sequence IDs in current run */
    private String runPrefix;

    // TODO data members for NcbiDownloader

    /**
     * Construct a downloader for a list of runs.
     *
     * @param sampleId	ID to use for this set of runs
     * @param outDir	output directory for FASTQ files
     * @param zipped	TRUE if the output should be gzipped
     * @param runs		array of run accession IDs for the runs to download
     *
     * @throws IOException
     *
     */
    public NcbiDownloader(String sampleId, File outDir, boolean zipped, String... runs) throws IOException {
        this.runList = Set.of(runs);
        log.info("Sample {} with {} runs will be written to {}.", sampleId, this.runList.size(), outDir);
        this.sampleId = sampleId;
        // Create the output files.
        this.leftFastqStream = this.openStream(outDir, zipped, "_1");
        this.rightFastqStream = this.openStream(outDir, zipped, "_2");
        this.singleFastqStream = this.openStream(outDir, zipped, "_s");
        // Initialize the counters.
        this.errorCount = 0;
        this.runCount = 0;
        this.pairCount = 0;
        this.singleCount = 0;
    }

    /**
     * Open an output stream for one of the sample output files.
     *
     * @param outDir	directory to contain the files
     * @param zipped	TRUE if the stream should be GZIPped
     * @param suffix	suffix for the file name
     *
     * @return the open output stream
     *
     * @throws IOException
     */
    private OutputStream openStream(File outDir, boolean zipped, String suffix) throws IOException {
        // Compute the appropriate extension and build the file name.
        String ext = ".fastq";
        if (zipped) ext += ".gz";
        File outFile = new File(outDir, this.sampleId + suffix + ext);
        // Open the appropriate type of output stream.
        OutputStream retVal = new FileOutputStream(outFile);
        if (zipped)
            retVal = new GZIPOutputStream(retVal);
        return retVal;
    }

    // TODO execute method

    @Override
    public void close() {
        this.closeStream("left", this.leftFastqStream);
        this.leftFastqStream = null;
        this.closeStream("right", this.rightFastqStream);
        this.rightFastqStream = null;
        this.closeStream("single", this.singleFastqStream);
        this.singleFastqStream = null;
    }

    /**
     * Close one of the output streams.
     *
     * @param type		type of stream being closed
     * @param stream	stream to close
     */
    public void closeStream(String type, OutputStream stream) {
        if (stream != null) {
            try {
                stream.close();
            } catch (IOException e) {
                log.error("Could not close {} stream for {}: {}.", type, sampleId, e.toString());
                this.errorCount++;
            }
        }
    }
}
