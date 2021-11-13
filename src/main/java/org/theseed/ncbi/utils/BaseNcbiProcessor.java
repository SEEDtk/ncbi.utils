/**
 *
 */
package org.theseed.ncbi.utils;

import java.io.IOException;
import java.io.PrintWriter;
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
 * @author Bruce Parrello
 *
 */
public abstract class BaseNcbiProcessor extends BaseReportProcessor
        implements NcbiTableReporter.IParms, Iterator<NcbiQuery> {

    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(NcbiQueryProcessor.class);
    /** NCBI connection */
    private NcbiConnection ncbi;
    /** reporting object */
    private NcbiTableReporter reporter;
    /** query batch size */
    @Option(name = "--batchSize", aliases = { "-b" }, metaVar = "100", usage = "number of records per query batch")
    private int batchSize;
    /** report type */
    @Argument(index = 0, metaVar = "reportType", usage = "type of report to write", required = true)
    private NcbiTableReporter.Type reportType;


    @Override
    protected final void setReporterDefaults() {
        this.setNcbiProcessDefaults();
        this.batchSize = 200;
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
     */
    protected abstract boolean processSpecial(PrintWriter writer) throws XmlException;

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
     */
    protected List<NcbiConnection.Field> getFieldList() throws XmlException {
        return this.ncbi.getFieldList(this.getTable());
    }

    /**
     * Compute the valid filtering field names, including the long and short forms.
     *
     * @return the set of valid field names for validation
     *
     * @throws XmlException
     */
    protected Set<String> getFieldNames() throws XmlException {
        return this.ncbi.getFieldNames(this.getTable());
    }

    /**
     * @return the batch size for queries
     */
    protected int getBatchSize() {
        return this.batchSize;
    }

}
