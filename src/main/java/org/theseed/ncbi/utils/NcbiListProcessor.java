/**
 *
 */
package org.theseed.ncbi.utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;

import org.kohsuke.args4j.Option;
import org.theseed.io.TabbedLineReader;
import org.theseed.ncbi.NcbiListQuery;
import org.theseed.ncbi.NcbiQuery;
import org.theseed.ncbi.XmlException;
import org.theseed.utils.ParseFailureException;

/**
 * This command reads IDs from a file and produces a report based on the ENTREZ records with
 * those IDs.  The type of report determines the table that the records come from.
 *
 * The positional parameter is the name of the report.  The IDs should be in a tab-delimited
 * file, with headers.  The default location is the first column.
 *
 * The following command-line options are supported.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	input file containing IDs (if not STDIN)
 * -o	output file for report (if not STDOUT)
 * -c	index (1-based) or name of input column containing IDs
 * -b	batch size for requests; the default is 200
 *
 * --key	name of the ID field; the default is "accession"
 *
 * @author Bruce Parrello
 *
 */
public class NcbiListProcessor extends BaseNcbiProcessor {

    // FIELDS
    /** input stream */
    private TabbedLineReader inStream;
    /** input column index */
    private int keyIdx;

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
        if (this.inFile == null) {
            log.info("Keys will be read from the standard input.");
            this.inStream = new TabbedLineReader(System.in);
        } else {
            log.info("Keys will be read from {}.", this.inFile);
            this.inStream = new TabbedLineReader(this.inFile);
        }
        // Get the key column index.
        this.keyIdx = this.inStream.findField(this.colName);
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
        this.inStream.close();
    }

    @Override
    public boolean hasNext() {
        // If there are more IDs, there are more queries.
        return this.inStream.hasNext();
    }

    @Override
    public NcbiQuery next() {
        // Fill a query until we are ready.
        NcbiListQuery retVal = new NcbiListQuery(this.getTable(), this.filterName);
        while (this.inStream.hasNext() && retVal.size() < this.getBatchSize()) {
            TabbedLineReader.Line line = this.inStream.next();
            String value = line.get(this.keyIdx);
            retVal.addId(value);
        }
        return retVal;
    }

}
