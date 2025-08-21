/**
 *
 */
package org.theseed.ncbi.download;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.io.ErrorQueue;
import org.theseed.io.LineReader;
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
    private static final Logger log = LoggerFactory.getLogger(NcbiDownloader.class);
    /** descriptor for sample to download */
    private ReadSample sample;
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
     * Construct a downloader for a sample
     *
     * @param sampleIn	descriptor of the sample to download
     * @param outDir	output directory for FASTQ files
     * @param zipped	TRUE if the output should be gzipped
     * @param runs		array of run accession IDs for the runs to download
     *
     * @throws IOException
     *
     */
    public NcbiDownloader(ReadSample sampleIn, File outDir, boolean zipped) throws IOException {
        // Get the list of runs.
        this.sample = sampleIn;
        log.info("Sample {} will be written to {}.", sample, outDir);
        // Create the output files.
        sampleIn.openStreams(outDir, zipped);
        // Initialize the counters.
        this.errorCount = 0;
        this.runCount = 0;
        this.pairCount = 0;
        this.singleCount = 0;
        this.readCount = 0;
        // Set up the summary string.
        this.summaryString = "Sample " + sample.getId() + " being initialized.";
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
        /** error that terminated this consumer abnormally */
        private RuntimeException error;

        /**
         * Construct a sequence consumer for a specified input stream.
         *
         * @param seqReader		input stream containing FASTQ-DUMP output
         */
        public SeqConsumer(LineReader seqReader) {
            this.dumpOutput = seqReader;
            this.leftRead = null;
            this.error = null;
        }

        @Override
        public void run() {
            long lastMessage = System.currentTimeMillis();
            Iterator<String> dumpIter = this.dumpOutput.iterator();
            try {
                while (dumpIter.hasNext()) {
                    // Here we need to get the next read and determine the type.  In general, we will get paired reads
                    // next to each other, left followed by right.
                    SeqPart read = SeqPart.read(NcbiDownloader.this.sample, dumpIter);
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
                    if (now - lastMessage >= 5000) {
                        NcbiDownloader.this.logProgress();
                        lastMessage = now;
                    }
                }
            } catch (RuntimeException e) {
                // Convert an exception to a failure message.
                log.error("Error in sample download.", e);
                this.error = e;
            } catch (IOException e) {
                log.error("Error in sample download.", e);
                this.error = new UncheckedIOException(e);
            }
            // Clear the iterator.
            int residual = 0;
            while (dumpIter.hasNext()) {
                dumpIter.next();
                residual++;
            }
            if (residual > 0)
                log.error("{} downloaded lines discarded.", residual);
        }

        /**
         * @return TRUE if an error occurred during download.
         */
        public boolean failed() {
            return this.error != null;
        }

        /**
         * @return the error that terminated this consumer
         */
        public RuntimeException getError() {
            return this.error;
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
                this.readCount, this.sample.getId(), this.pairCount, this.singleCount, this.errorCount);
    }

    /**
     * Write out a pair of read parts.
     *
     * @param leftRead	left read part
     * @param rightRead	right read part
     */
    private void writePair(SeqPart leftRead, SeqPart rightRead) {
        this.sample.writePair(leftRead, rightRead);
        this.pairCount++;
    }


    /**
     * Execute the download of this object's runs.
     *
     * @throws IOException
     */
    public void execute() {
        this.summaryString = "Download of sample " + this.sample.getId() + " in progress.";
        log.info("Expecting {} spots in sample {}.", this.sample.getSpots(), this.sample.getId());
        Set<String> runs = this.sample.getRuns();
        for (String run : runs) {
            this.runCount++;
            log.info("Processing run {} of {}: {}.", this.runCount, runs.size(), run);
            this.downloadRun(run);
        }
        String summary = String.format("Sample %s downloaded from %d runs, %d pairs, %d singletons, and %d errors.",
                this.sample.getId(), this.runCount, this.pairCount, this.singleCount, this.errorCount);
        log.info(summary);
        this.summaryString = summary + "\n" + this.sample.getTitle();
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
            // Create a thread to read the downloaded sequences.
            try (LineReader seqReader = new LineReader(dlProcess.getInputStream());
                    LineReader logReader = new LineReader(dlProcess.getErrorStream())) {
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
                // If there was an error in the consumer thread, re-throw it here.
                if (seqProcessor.failed())
                    throw seqProcessor.getError();
            }
            // Flush the output.
            this.sample.flushStreams();
            log.info("Run {} downloaded.", run);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted FASTQ download execution: " + e.toString(), e);
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
        this.sample.closeStreams();
    }

    /**
     * Write a read to the singleton file.
     *
     * @param read	read to write
     */
    public void writeSingleton(SeqPart read) {
        this.sample.writeSingle(read);
        this.singleCount++;
    }

    /**
     * @return a summary of the stats for this sample download
     */
    public String summaryString() {
        return this.summaryString;
    }


}
