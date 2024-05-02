/**
 *
 */
package org.theseed.ncbi.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.MarkerFile;
import org.theseed.io.TabbedLineReader;
import org.theseed.ncbi.NcbiConnection;
import org.theseed.ncbi.NcbiListQuery;
import org.theseed.ncbi.NcbiTable;
import org.theseed.ncbi.TagNotFoundException;
import org.theseed.ncbi.XmlException;
import org.theseed.ncbi.download.NcbiDownloader;
import org.theseed.ncbi.download.ReadSample;
import org.theseed.utils.BaseInputProcessor;
import org.w3c.dom.Element;

/**
 * This command downloads multiple read samples from NCBI into an output directory.  Each sample will be
 * output to a subdirectory bearing the sample name.
 *
 * A sample consists of one or more runs.  The input file should be tab-delimited with headers, and
 * come in on the standard input.  One column should contain sample IDs and another the run IDs
 * separated by commas.  If the samples are in fast single-run samples, both columns can be the same,
 * and the sample ID is also the run ID.
 *
 * The positional parameter is the name of the output directory.  The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	input file containing sample and run IDs (if not STDIN)
 * -b	batch size for NCBI queries (default 100)
 *
 * --clear		erase the output directory before processing
 * --missing	only download samples not already present
 * --sampCol	index (1-based) or name of the input column containing sample and run IDs (default "sample_id")
 * --zip		GZIP the output to save space
 *
 * @author Bruce Parrello
 *
 */
public class NcbiFetchProcessor extends BaseInputProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(NcbiFetchProcessor.class);
    /** master sample list (sample ID -> sample descriptor */
    private Map<String, ReadSample> sampleMap;
    /** input list of samples */
    private Set<String> samples;
    /** input list of runs */
    private Set<String> runs;
    /** number of samples downloaded */
    private int downloadCount;
    /** number of pre-existing samples skipped */
    private int skipCount;
    /** number of downloads that failed */
    private int failCount;
    /** ncbi connection */
    private NcbiConnection ncbi;
    /** list query for processing accession identifiers */
    private NcbiListQuery query;

    // COMMAND-LINE OPTIONS

    /** if specified, the output directory will be erased before processing */
    @Option(name = "--clear", usage = "if specified, the output directory will be erased before processing")
    private boolean clearFlag;

    /** if specified, samples already downloaded will be skipped */
    @Option(name = "--missing", usage = "if specified, only new samples will be downloaded")
    private boolean missingFlag;

    /** specifier for input sample ID column */
    @Option(name = "--sampCol", metaVar = "sample", usage = "index (1-based) or name of sample ID input column")
    private String sampCol;

    /** if specified, the output files will be compressed */
    @Option(name = "--zip", usage = "if specified, output files will be GZIPped")
    private boolean zipFlag;

    /** batch size for NCBI queries */
    @Option(name = "--batchSize", aliases = { "-b" }, metaVar = "50", usage = "batch size for NCBI queries")
    private int batchSize;

    /** name of the output directory */
    @Argument(index = 0, metaVar = "outDir", usage = "output directory name", required = true)
    private File outDir;

    /**
     * This enum deals with the processing differences between samples and runs
     * during analysis.
     */
    private static enum AccessionType {
        SAMPLE {
            @Override
            protected String getId(Element expElement) throws TagNotFoundException {
                return ReadSample.getSampleId(expElement);
            }
        }, RUN {
            @Override
            protected String getId(Element expElement) throws TagNotFoundException {
                return ReadSample.getRunId(expElement);
            }
        };

        /**
         * @return the ID of the input for this record
         *
         * @param expElement	experiment-package element returned by NCBI
         *
         * @throws TagNotFoundException
         */
        protected abstract String getId(Element expElement) throws TagNotFoundException;
    }

    @Override
    protected void setReaderDefaults() {
        this.clearFlag = false;
        this.missingFlag = false;
        this.sampCol = "sample_id";
        this.zipFlag = false;
        this.batchSize = 100;
    }

    @Override
    protected void validateReaderParms() throws IOException, ParseFailureException {
        // Insure the batch size is valid.
        if (this.batchSize < 1)
            throw new ParseFailureException("Batch size must be at least 1.");
        // Validate the output directory.
        if (this.outDir.isFile())
            throw new FileNotFoundException("Output directory " + this.outDir + " is a file and cannot be used.");
        if (! this.outDir.isDirectory()) {
            // Here we must create the output directory.
            log.info("Creating output directory {}.", this.outDir);
            FileUtils.forceMkdir(this.outDir);
        } else if (this.clearFlag) {
            // Here we want the output directory erased.
            log.info("Erasing output directory {}.", this.outDir);
            FileUtils.cleanDirectory(this.outDir);
        } else {
            // Here we are just using the directory already there.
            log.info("Output samples will be put in {}.", this.outDir);
        }
        // Initialize the counters.
        this.downloadCount = 0;
        this.failCount = 0;
        this.skipCount = 0;
        // Connect to the NCBI.
        this.ncbi = new NcbiConnection();
        this.query = new NcbiListQuery(NcbiTable.SRA, "ACCN");
    }

    @Override
    protected void validateReaderInput(TabbedLineReader reader) throws IOException {
        // Locate the input column.
        int sampColIdx = reader.findField(this.sampCol);
        // Create the sample hash.
        this.sampleMap = new HashMap<String, ReadSample>();
        // Create the run and sample sets.
        log.info("Reading sample and run data from input.");
        this.runs = new TreeSet<String>();
        this.samples = new TreeSet<String>();
        int lineCount = 0;
        for (var line : reader) {
            lineCount++;
            String sampleId = line.get(sampColIdx);
            // A run ID is XXR, a sample ID is XXS.
            if (sampleId.length() < 4)
                throw new IOException("\"" + sampleId + "\" is not a valid sample or run ID.");
            if (sampleId.charAt(2) == 'R')
                this.runs.add(sampleId);
            else
                this.samples.add(sampleId);
        }
        log.info("{} lines read.  {} samples and {} runs found.", lineCount, this.samples.size(), this.runs.size());
    }

    @Override
    protected void runReader(TabbedLineReader reader) throws Exception {
        // Ask the NCBI for information about the samples.
        this.analyzeAccessions(this.samples, AccessionType.SAMPLE);
        log.info("{} samples found in input.", this.sampleMap.size());
        // Ask the NCBI for information about the runs.
        this.analyzeAccessions(this.runs, AccessionType.RUN);
        log.info("{} samples and runs found in input.", this.sampleMap.size());
        // Loop through the samples, processing them one at a time.
        this.sampleMap.values().forEach(x -> this.processSample(x));
        log.info("{} samples downloaded, {} failed, {} skipped.", this.downloadCount, this.failCount, this.skipCount);
    }

    /**
     * Analyze the accession identifiers to get the sample descriptors for each.  We ask for
     * experiment descriptors from NCBI, and use them to create the sample map.  We can
     * get multiple descriptors for the same sample, and this is handled.
     *
     * @param inSet		set of accession identifiers
     * @param type		type of accession identifiers
     *
     * @throws IOException
     * @throws XmlException
     */
    private void analyzeAccessions(Set<String> inSet, AccessionType type) throws XmlException, IOException {
        // Loop through the input set, forming batches.
        Set<String> idBatch = new HashSet<String>(this.batchSize * 5 / 4 + 1);
        for (String accnId : inSet) {
            if (idBatch.size() >= this.batchSize) {
                this.processAccessionBatch(idBatch, type);
                idBatch.clear();
            }
            idBatch.add(accnId);
        }
        if (! idBatch.isEmpty())
            this.processAccessionBatch(idBatch, type);
    }

    /**
     * Request data from NCBI about a set of accession IDs and analyze them so they can be
     * added to the sample map.
     *
     * @param idBatch	batch of accession identifiers to process
     * @param type		type of accession identifiers
     *
     * @throws IOException
     * @throws XmlException
     */
    private void processAccessionBatch(Set<String> idBatch, AccessionType type) throws XmlException, IOException {
        // Request SRA experiment data from NCBI.
        this.query.addIds(idBatch);
        List<Element> expElements = this.query.run(this.ncbi);
        // Loop through the experiment packages.
        for (Element expElement : expElements) {
            String inputId = type.getId(expElement);
            // Find a sample for this ID.
            ReadSample sample = this.sampleMap.get(inputId);
            if (sample == null) {
                // Here we have a new sample.
                sample = ReadSample.create(inputId, expElement);
                this.sampleMap.put(inputId, sample);
            } else {
                // Here we have an additional run set for a sample we've already processed.
                sample.addRuns(expElement);
            }
        }
    }

    /**
     * Download a single sample to the output directory.
     *
     * @param sample		sample to download
     */
    private void processSample(ReadSample sample) {
        try {
            // Compute the output directory.
            File sampleDir = new File(this.outDir, sample.getId());
            // Compute the marker file name.  This file is created after the download is successful.
            File markerFile = new File(sampleDir, "summary.txt");
            if (this.missingFlag && markerFile.exists()) {
                log.info("Skipping downloaded sample {}.", sample.getId());
                this.skipCount++;
            } else {
                log.info("Downloading sample #{}: {}.", this.downloadCount+1, sample.getId());
                // Insure the output directory exists.
                if (! sampleDir.isDirectory())
                    FileUtils.forceMkdir(sampleDir);
                try (NcbiDownloader downloader = new NcbiDownloader(sample, sampleDir, this.zipFlag)) {
                    // Download the sample.
                    downloader.execute();
                    // Mark it complete.
                    MarkerFile.write(markerFile, downloader.summaryString());
                    this.downloadCount++;
                }
            }
        } catch (Exception e) {
            log.error("Sample {} failed during download: {}.", sample.getId(), e.toString());
            this.failCount++;
        }
    }

}
