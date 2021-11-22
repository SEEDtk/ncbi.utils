/**
 *
 */
package org.theseed.ncbi.utils;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.ncbi.NcbiConnection;
import org.theseed.ncbi.NcbiQuery;
import org.theseed.ncbi.NcbiTable;
import org.theseed.ncbi.XmlException;
import org.theseed.ncbi.reports.NcbiTableReporter;
import org.theseed.utils.BaseReportProcessor;
import org.theseed.utils.ParseFailureException;
import org.w3c.dom.Element;

/**
 * This is the base class for NCBI reports.  It is pretty limited, allowing the subclass to
 * do pre- and post-processing and build the query.  After that, the query is run and the
 * records run through the standard reporting process.
 *
 * The first positional parameter-- report type-- is supported at the base-class level, as
 * well as the following command-line options.
 *
 * -b	batch size for queries
 * -p	regex pattern for abstract filtering (multiple allowed)
 * -t	target table (for raw reporter only)
 *
 * --mode	minimum number of filters that must match in an abstract for a record to be output
 *
 * @author Bruce Parrello
 *
 */
public abstract class BaseNcbiProcessor extends BaseReportProcessor
        implements NcbiTableReporter.IParms, Iterator<NcbiQuery> {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(NcbiQueryProcessor.class);
    /** NCBI connection */
    private NcbiConnection ncbi;
    /** reporting object */
    private NcbiTableReporter reporter;

    // COMMAND-LINE OPTIONS

    /** query batch size */
    @Option(name = "--batchSize", aliases = { "-b" }, metaVar = "100", usage = "number of records per query batch")
    private int batchSize;

    /** keyword patterns for abstract filtering */
    @Option(name = "--pattern", aliases = { "-p" }, metaVar = "rna\\s*seq",
            usage = "regex pattern for filtering abstracts")
    private List<String> patterns;

    /** keyword pattern mode */
    @Option(name = "--mode", usage = "minimum number of keywords required to satisfy filter (0 == all)")
    private int filterMode;

    /** target table for raw report */
    @Option(name = "--table", aliases = { "-t" }, usage = "target table for RAW report")
    private NcbiTable targetTable;

    /** report type */
    @Argument(index = 0, metaVar = "reportType", usage = "type of report to write", required = true)
    private NcbiTableReporter.Type reportType;


    @Override
    protected final void setReporterDefaults() {
        this.setNcbiProcessDefaults();
        this.batchSize = 200;
        this.patterns = new ArrayList<String>();
        this.filterMode = 0;
    }

    @Override
    protected final void validateReporterParms() throws IOException, ParseFailureException {
        try {
            // Create the reporting object.
            this.reporter = this.reportType.create(this);
            // Verify the batch size.
            this.ncbi = new NcbiConnection();
            if (this.batchSize < 1)
                throw new ParseFailureException("Batch size must be greater than 0.");
            this.ncbi.setChunkSize(this.batchSize);
            // Process the subclass parms.
            this.validateNcbiProcessParms();
        } catch (XmlException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void runReporter(PrintWriter writer) throws Exception {
        NcbiTable table = this.getTable();
        String tableName = table.db();
        // Determine the mode.
        if (! this.processSpecial(writer)) {
            // Start the report.
            this.reporter.openReport(writer);
            // Loop through the queries.
            while (this.hasNext()) {
                NcbiQuery query = this.next();
                log.info("Submitting query for {} records.", tableName);
                List<Element> results = query.run(this.ncbi);
                log.info("{} records returned from {} query.", results.size(), tableName);
                // Loop through the records from this query.
                for (Element result : results)
                    this.reporter.writeRecord(result);
            }
            // Close off the report.
            this.reporter.closeReport();
            // Flush the report so that everything gets out before we update the since-date file.
            writer.flush();
            // Perform any necessary post-processing.
            this.postProcess();
        }
    }

    /**
     * @return the NCBI table for this processor
     */
    protected NcbiTable getTable() {
        return this.reporter.getTable();
    }

    /**
     * Set parameter defaults for the subclass.
     */
    protected abstract void setNcbiProcessDefaults();

    /**
     * Perform parameter validation for the subclass.
     */
    protected abstract void validateNcbiProcessParms() throws IOException, ParseFailureException,
            XmlException;

    /**
     * Process any special commands that bypass the report.
     *
     * @param writer	print writer for output
     *
     * @return TRUE if the report should be bypassed, else FALSE
     *
     * @throws XmlException
     * @throws IOException
     */
    protected abstract boolean processSpecial(PrintWriter writer) throws XmlException, IOException;

    /**
     * Perform any necessary post-processing.
     */
    protected abstract void postProcess();

    /**
     * @return the NCBI connection
     */
    protected NcbiConnection getNcbi() {
        return this.ncbi;
    }

    /**
     * @return the list of filtering fields in the current report's table
     *
     * @throws XmlException
     * @throws IOException
     */
    protected List<NcbiConnection.Field> getFieldList() throws XmlException, IOException {
        return this.ncbi.getFieldList(this.getTable());
    }

    /**
     * Compute the valid filtering field names, including the long and short forms.
     *
     * @return the set of valid field names for validation
     *
     * @throws XmlException
     * @throws IOException
     */
    protected Set<String> getFieldNames() throws XmlException, IOException {
        return this.ncbi.getFieldNames(this.getTable());
    }

    /**
     * @return the batch size for queries
     */
    protected int getBatchSize() {
        return this.batchSize;
    }

    /**
     * @return the list of abstract-filtering patterns
     */
    @Override
    public List<String> getPatterns() {
        return this.patterns;
    }

    /**
     * @return the minimum number of filters that must match for abstract-filtering
     */
    @Override
    public int getFilterMode() {
        return this.filterMode;
    }

    /**
     * @return the target table for a multi-table report
     */
    @Override
    public NcbiTable getTargetTable() {
        return this.targetTable;
    }

}
