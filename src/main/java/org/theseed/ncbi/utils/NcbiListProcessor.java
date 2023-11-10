/**
 *
 */
package org.theseed.ncbi.utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Option;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;
import org.theseed.ncbi.NcbiListQuery;
import org.theseed.ncbi.NcbiQuery;
import org.theseed.ncbi.XmlException;

/**
 * This command reads IDs from a file and produces a report based on the ENTREZ records with
 * those IDs.  The type of report determines the table that the records come from.
 *
 * The positional parameter is the name of the report.  The IDs should be in a tab-delimited
 * file, with headers.  The default location is the first column.  Blank column values will
 * be skipped.
 *
 * The following command-line options are supported.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	input file containing IDs (if not STDIN)
 * -o	output file for report (if not STDOUT)
 * -c	index (1-based) or name of input column containing IDs
 * -b	batch size for requests; the default is 200
 * -t	target table for RAW report
 *
 * --key	name of the ID field; the default is "accession"
 *
 * @author Bruce Parrello
 *
 */
public class NcbiListProcessor extends BaseNcbiProcessor {

    // FIELDS
    /** input stream */
    private Iterator<String> keyIter;
    /** total number of keys found in input */
    private int keyTotal;

    // COMMAND-LINE OPTIONS

    /** input file (if not STDIN) */
    @Option(name = "--input", aliases = { "-i" }, metaVar = "infile.tbl", usage = "input file containing keys (if not STDIN)")
    private File inFile;

    /** input column */
    @Option(name = "--col", aliases = { "-c" }, metaVar = "accn", usage = "index (1-based) or name of id column")
    private String colName;

    /** name of the key field in the ENTREZ database **/
    @Option(name = "--key", metaVar = "bioproject", usage = "name of key field in target database")
    private String filterName;

    @Override
    protected void setNcbiProcessDefaults() {
        this.inFile = null;
        this.colName = "1";
        this.filterName = "ACCN";
    }

    @Override
    protected void validateNcbiProcessParms() throws IOException, ParseFailureException, XmlException {
        // Set up the input file.
        TabbedLineReader inStream = null;
        try {
            if (this.inFile == null) {
                log.info("Keys will be read from the standard input.");
                inStream = new TabbedLineReader(System.in);
            } else {
                log.info("Keys will be read from {}.", this.inFile);
                inStream = new TabbedLineReader(this.inFile);
            }
            // Get the key column values.
            Set<String> keys = new TreeSet<String>();
            int keyIdx = inStream.findField(this.colName);
            for (TabbedLineReader.Line line : inStream) {
                String key = line.get(keyIdx);
                if (! StringUtils.isBlank(key))
                    keys.add(key);
            }
            // Record the size of the key request.
            this.keyTotal = keys.size();
            log.info("{} distinct keys found in input.", this.keyTotal);
            // Create the iterator through the keys.
            this.keyIter = keys.iterator();
        } finally {
            // Insure the input stream is closed.
            if (inStream != null)
                inStream.close();
        }
        // Verify the key field name.
        Set<String> fieldNames = this.getFieldNames();
        if (! fieldNames.contains(this.filterName))
            throw new ParseFailureException("Invalid key field name " + this.filterName + ".");
    }

    @Override
    protected boolean processSpecial(PrintWriter writer) throws XmlException {
        return false;
    }

    @Override
    protected void postProcess() {
    }

    @Override
    public boolean hasNext() {
        // If there are more IDs, there are more queries.
        return this.keyIter.hasNext();
    }

    @Override
    public NcbiQuery next() {
        // Fill a query until we are ready.
        NcbiListQuery retVal = new NcbiListQuery(this.getTable(), this.filterName);
        while (this.keyIter.hasNext() && retVal.size() < this.getBatchSize()) {
            String value = this.keyIter.next();
            retVal.addId(value);
        }
        return retVal;
    }

}
