/**
 *
 */
package org.theseed.ncbi.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;
import org.theseed.utils.BaseInputProcessor;

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
 *
 * --clear		erase the output directory before processing
 * --missing	only download samples not already present
 * --sampCol	index (1-based) or name of the input column containing sample IDs
 * --runCol		index (1-based) or name of the input column containing run IDs
 * --zip		GZIP the output to save space
 *
 * @author Bruce Parrello
 *
 */
public class NcbiFetchProcessor extends BaseInputProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(NcbiFetchProcessor.class);
    /** master sample list (sample ID -> run ID list */
    private Map<String, List<String>> sampleMap;

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

    /** specified for input run list column */
    @Option(name = "--runCol", metaVar = "run_ids", usage = "index (1-based) or name of run ID list column")
    private String runCol;

    /** if specified, the output files will be compressed */
    @Option(name = "--zip", usage = "if specified, output files will be GZIPped")
    private boolean zipFlag;

    /** name of the output directory */
    @Argument(index = 0, metaVar = "outDir", usage = "output directory name", required = true)
    private File outDir;

    @Override
    protected void setReaderDefaults() {
        this.clearFlag = false;
        this.missingFlag = false;
        this.sampCol = "sample_id";
        this.runCol = "runs";
        this.zipFlag = false;
    }

    @Override
    protected void validateReaderParms() throws IOException, ParseFailureException {
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
    }

    @Override
    protected void validateReaderInput(TabbedLineReader reader) throws IOException {
        // TODO read input and build sample map

    }

    @Override
    protected void runReader(TabbedLineReader reader) throws Exception {
        // TODO code for runReader

    }

}
