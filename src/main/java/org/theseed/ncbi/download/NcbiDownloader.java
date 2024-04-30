/**
 *
 */
package org.theseed.ncbi.download;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.Iterator;
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
import org.theseed.utils.ProcessUtils;

/**
 * This method downloads one or more runs from the NCBI.  Each run is downloaded by starting the fastq-dump utility in a
 * separate process.  This program intercepts the output and sorts it into left, right, and singelton files appropriately.
 *
 * If multiple runs are downloaded, they are all concatenated.
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
    private PrintWriter leftFastqStream;
    /** right output file stream */
    private PrintWriter rightFastqStream;
    /** singleton output file stream */
    private PrintWriter singleFastqStream;
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
    /** number of reads processed */
    private int readCount;
    /** directory containing FASTQ-DUMP binaries */
    private static final File CMD_PATH = checkSraLib();
    /** status summary string */
    private String summaryString;

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
        this.readCount = 0;
        // Set up the summary string.
        this.summaryString = "Sample " + sampleId + " being initialized.";
    }

    /**
     * This object manages output from the FASTQ-DUMP process and sorts the
     * results into the appropriate output files.
     */
    public class SeqConsumer extends Thread {

        /** FASTQ-DUMP output stream */
        private LineReader dumpOutput;
        /** buffered left read */
        private SeqPart leftRead;

        /**
         * Construct a sequence consumer for a specified input stream.
         *
         * @param seqReader		input stream containing FASTQ-DUMP output
         */
        public SeqConsumer(LineReader seqReader) {
            this.dumpOutput = seqReader;
            this.leftRead = null;
        }

        @Override
        public void run() {
            long lastMessage = System.currentTimeMillis();
            try {
                Iterator<String> dumpIter = this.dumpOutput.iterator();
                while (dumpIter.hasNext()) {
                    // Here we need to get the next read and determine the type.  In general, we will get paired reads
                    // next to each other, left followed by right.
                    SeqPart read = new SeqPart(dumpIter);
                    NcbiDownloader.this.readCount++;
                    // Determine the read type.
                    switch (read.getType()) {
                    case SINGLETON :
                        // A singleton is written immediately.
                        NcbiDownloader.this.writeSingleton(read);
                        break;
                    case RIGHT:
                        // If it is a right read, we emit both left and right if they match.  Otherwise, they go out as
                        // singles.
                        if (this.leftRead == null) {
                            // No left read, write this one as a single.
                            NcbiDownloader.this.writeSingleton(read);
                        } else if (this.leftRead.matches(read)) {
                            // Here we have the right pair for the current left read.  Write the pair, then clear the
                            // buffered read.
                            NcbiDownloader.this.writePair(this.leftRead, read);
                            this.leftRead = null;
                        } else {
                            // Here we have an unmatched right pair.  Write both as singles and clear the buffered read.
                            NcbiDownloader.this.writeSingleton(leftRead);
                            NcbiDownloader.this.writeSingleton(read);
                            NcbiDownloader.this.errorCount++;
                        }
                        break;
                    case LEFT :
                        // Here we have a left read.  If there is a buffered left read, write it as a singleton.
                        if (this.leftRead != null) {
                            NcbiDownloader.this.writeSingleton(this.leftRead);
                            NcbiDownloader.this.errorCount++;
                        }
                        // Buffer the current left read.
                        this.leftRead = read;
                        break;
                    }
                    long now = System.currentTimeMillis();
                    if (now - lastMessage >= 10000) {
                        NcbiDownloader.this.logProgress();
                        lastMessage = now;
                    }
                }
            } catch (IOException e) {
                // Convert IO exceptions to runtime.
                throw new UncheckedIOException(e);
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
     * Log our current progress.
     */
    protected void logProgress() {
        log.info("{} reads processed in sample {}.  {} pairs, {} singles, {} errors.",
                this.readCount, this.sampleId, this.pairCount, this.singleCount, this.errorCount);
    }

    /**
     * Write out a pair of read parts.
     *
     * @param leftRead	left read part
     * @param rightRead	right read part
     */
    private void writePair(SeqPart leftRead, SeqPart rightRead) {
        leftRead.write(this.leftFastqStream);
        rightRead.write(this.rightFastqStream);
        this.pairCount++;
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
    private PrintWriter openStream(File outDir, boolean zipped, String suffix) throws IOException {
        // Compute the appropriate extension and build the file name.
        String ext = ".fastq";
        if (zipped) ext += ".gz";
        File outFile = new File(outDir, this.sampleId + suffix + ext);
        // Open the appropriate type of output stream.
        PrintWriter retVal;
        if (! zipped)
            retVal = new PrintWriter(outFile);
        else {
            OutputStream outStream = new FileOutputStream(outFile);
            outStream = new GZIPOutputStream(outStream);
            retVal = new PrintWriter(outStream);
        }
        return retVal;
    }

    /**
     * Execute the download of this object's runs.
     *
     * @throws IOException
     */
    public void execute() {
        this.summaryString = "Download of sample " + this.sampleId + " in progress.";
        log.info("Downloading sample {}.", this.sampleId);
        for (String run : this.runList) {
            this.runCount++;
            log.info("Processing run {} of {}: {}.", this.runCount, this.runList.size(), run);
            this.downloadRun(run);
        }
        this.summaryString = String.format("Sample %s downloaded from %d runs, %d pairs, %d singletons, and %d errors.", this.sampleId,
                this.runCount, this.pairCount, this.singleCount, this.errorCount);
        log.info(this.summaryString);
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
            dlCommand.redirectInput(Redirect.INHERIT);
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
    private void closeStream(String type, PrintWriter stream) {
        if (stream != null)
            stream.close();
    }

    /**
     * Write a read to the singleton file.
     *
     * @param read	read to write
     */
    public void writeSingleton(SeqPart read) {
        read.write(this.singleFastqStream);
        this.singleCount++;
    }

    /**
     * @return a summary of the stats for this sample download
     */
    public String summaryString() {
        return this.summaryString;
    }


}
