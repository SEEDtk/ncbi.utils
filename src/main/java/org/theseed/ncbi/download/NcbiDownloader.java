/**
 *
 */
package org.theseed.ncbi.download;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.io.ErrorQueue;
import org.theseed.io.LineReader;
import org.theseed.reports.NaturalSort;
import org.theseed.sequence.fastq.SeqRead;
import org.theseed.utils.ProcessUtils;

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
 * Finally, the location of the fastq-dump binary must be specified in the environment variable SRALIB.
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
    /** directory containing FASTQ-DUMP binaries */
    private static final File CMD_PATH = checkSraLib();

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
        // Save the list of runs.  We want them in more-or-less numerical order.
        this.runList = new TreeSet<String>(new NaturalSort());
        for (String run : runs)
            this.runList.add(run);
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
     * This subclass manages output from the FASTQ-DUMP process and sorts the
     * results into the appropriate output files.
     */
    public class SeqConsumer extends Thread {

        /** FASTQ-DUMP output stream */
        private LineReader dumpOutput;

        /**
         * Construct a sequence consumer for a specified input stream.
         *
         * @param seqReader		input stream containing FASTQ-DUMP output
         */
        public SeqConsumer(LineReader seqReader) {
            this.dumpOutput = seqReader;
            // TODO initialize for sequence consumption
        }

        @Override
        public void run() {
            for (String seqLine : this.dumpOutput) {
                // Process output line
            }
        }

    }


    /**
     * @return the directory containing the SRA toolkit binaries
     */
    private static File checkSraLib() {
        File retVal = null;
        String pathName = System.getenv("SRALIB");
        if (! StringUtils.isBlank(pathName)) {
            File testDir = new File(pathName);
            if (testDir.isDirectory())
                retVal = testDir;
        }
        return retVal;
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

    /**
     * Execute the download of this object's runs.
     *
     * @throws IOException
     */
    public void execute() {
        log.info("Downloading sample {}.", this.sampleId);
        for (String run : this.runList) {
            this.runPrefix = String.format("R%02d.", this.runCount + 1);
            log.info("Processing run {} with prefix {}.", run, this.runPrefix);
            this.downloadRun(run);
        }
        log.info("Sample {} downloaded:  {} pairs, {} singles, {} errors.", this.sampleId, this.pairCount, this.singleCount, this.errorCount);
    }

    /**
     * Download a single run into the current output files.
     *
     * @param run	accession of the run to download
     * @throws IOException
     */
    private void downloadRun(String run) {
        try {
            List<String> command = this.formatCommand(run);
            ProcessBuilder dlCommand = new ProcessBuilder(command);
            dlCommand.redirectInput(Redirect.DISCARD);
            Process dlProcess = dlCommand.start();
            try (LineReader seqReader = new LineReader(dlProcess.getInputStream());
                    LineReader logReader = new LineReader(dlProcess.getErrorStream())) {
                // Create a thread to read the downloaded sequences.
                SeqConsumer seqProcessor = this.new SeqConsumer(seqReader);
                seqProcessor.start();
                // Create a thread to save error log messages.  Generally there are none.
                List<String> messages = new ArrayList<String>(30);
                ErrorQueue errorReader = new ErrorQueue(logReader, messages);
                errorReader.start();
                // Clean up the process.
                seqProcessor.join();
                errorReader.join();
                ProcessUtils.finishProcess("FASTQ-DUMP", dlProcess, messages);
            }
            // Flush the output.
            this.leftFastqStream.flush();
            this.rightFastqStream.flush();
            this.singleFastqStream.flush();
            log.info("Run {} downloaded.", run);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted BLAST execution: " + e.toString(), e);
        }

    }

    /**
     * Build the command string to dump the specified run.
     *
     * @param run	run accession ID
     *
     * @return a full command list for downloading the run
     *
     * @throws IOException
     */
    private List<String> formatCommand(String run) throws IOException {
        // Get the fully-qualified command name.
        if (CMD_PATH == null)
            throw new IOException("SRALIB environment variable is missing or invalid.");
        File commandFile = new File(CMD_PATH, "fastq-dump");
        // Format the full command.
        List<String> retVal = List.of(commandFile.toString(), "--readids", "--stdout", "--split-spot",
                "--skip-technical", "--clip",  "--read-filter", "pass", run);
        log.debug("Download command is: {}", StringUtils.join(retVal, " "));
        return retVal;
    }

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
