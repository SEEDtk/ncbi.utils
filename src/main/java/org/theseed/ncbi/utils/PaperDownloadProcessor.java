/**
 *
 */
package org.theseed.ncbi.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;
import org.theseed.ncbi.DownloadException;
import org.theseed.ncbi.PaperConnection;
import org.theseed.utils.BaseMultiReportProcessor;
import org.theseed.web.ColSpec;
import org.theseed.web.HtmlTable;
import org.theseed.web.Key;
import org.theseed.web.Row;

import j2html.tags.ContainerTag;

import static j2html.TagCreator.*;
/**
 * This command downloads papers from PUBMED using the PUBMED-style report output.  An attempt will be made to download
 * each paper into a separate file ("XXXXX.html" or "XXXXX.pdf", where XXXXX is the PUBMED ID).  In addition, "failure.html" will contain
 * an HTML page for the papers that could not be downloaded successfully.  The input file will be the standard input.
 * The output files will be stored in the directory "Pubmed" under the current working directory.  Both these things
 * can be overridden by command-line options.
 *
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -D	output directory name
 * -i	input file containing the PUBMED report, if not STDIN
 *
 * --clear	erase the output directory before processing
 *
 *
 * @author Bruce Parrello
 *
 */
public class PaperDownloadProcessor extends BaseMultiReportProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(PaperDownloadProcessor.class);
    /** paper-downloading connection */
    private PaperConnection conn;
    /** input stream */
    private TabbedLineReader inStream;
    /** column index for PUBMED ID */
    private int idColIdx;
    /** column index for title */
    private int titleColIdx;
    /** column index for DOI link */
    private int linkColIdx;
    /** table of failed downloads */
    private HtmlTable<Key.Int> paperTable;
    /** pubmed link format */
    private static final String PUBMED_LINK = "https://pubmed.ncbi.nlm.nih.gov/%d/";

    // COMMAND-LINE OPTIONS

    /** input file name (if not STDIN) */
    @Option(name = "--input", aliases = { "-i", "--in" }, metaVar = "pubmed.tbl", usage = "input file containing PUBMED report (if not STDIN)")
    private File inFile;

    @Override
    protected File setDefaultOutputDir(File curDir) {
        return new File(curDir, "Pubmed");
    }

    @Override
    protected void setMultiReportDefaults() {
        this.inFile = null;
    }

    @Override
    protected void validateMultiReportParms() throws IOException, ParseFailureException {
        // Set up the input stream.
        if (this.inFile == null) {
            log.info("PUBMED data will be read from the standard input,");
            this.inStream = new TabbedLineReader(System.in);
        } else {
            log.info("PUBMED data will be read from {}.", this.inFile);
            this.inStream = new TabbedLineReader(this.inFile);
        }
        // Now get the column indices.
        this.idColIdx = this.inStream.findField("pubmed");
        this.titleColIdx = this.inStream.findField("title");
        this.linkColIdx = this.inStream.findField("doi_link");
    }

    @Override
    protected void runMultiReports() throws Exception {
        try {
            log.info("Connecting to web.");
            this.conn = new PaperConnection();
            // Initialize some counters.
            int linesIn = 0;
            int missingLinks = 0;
            int failures = 0;
            int papers = 0;
            // Create the failure table.
            this.paperTable = new HtmlTable<>(new ColSpec.Num("pubmed_id"), new ColSpec.Normal("Title and Link"), new ColSpec.Normal("Reason"));
            // Start logging our progress.
            log.info("Downloading papers from input stream.");
            long lastMsg = System.currentTimeMillis();
            long start = lastMsg;
            // Loop through the input.
            for (var line : this.inStream) {
                int pubmedId = line.getInt(this.idColIdx);
                String title = line.get(this.titleColIdx);
                String link = line.get(this.linkColIdx);
                linesIn++;
                // Insure we have a link.
                if (StringUtils.isBlank(link)) {
                    // Set up the title link and reason/
                    String linkUrl = String.format(PUBMED_LINK, pubmedId);
                    // Create the failure-table row.
                    this.createFailRow(pubmedId, title, linkUrl, "No DOI link");
                    missingLinks++;
                } else {
                    // Here we have a DOI link for the paper.
                    try {
                        // Try downloading the paper.
                        String paperHtml = this.conn.getPaper(link);
                        // The paper downloaded, so we need to find the actual text.
                        // TODO use Selenium to get the post-Javascript text
                        String paperUrl = conn.getPaperUrl();
                        if (paperUrl.contains("springer.com") || paperUrl.contains("nature.com")) {
                            // TODO get Springer PDF (find <meta name="citation_pdf_url" content=")
                        } else if (paperUrl.contains("elsevier.com") || paperUrl.contains("iucr.org")) {
                            // This journal requires payment.
                            this.createFailRow(pubmedId, title, paperUrl, "requires payment");
                        } else if (paperUrl.contains("microbiologyresearch.org")) {
                            // TODO get PDF URL and use POST protocol (find <form method="POST" action="
                        } else {
                            log.debug("Writing {} for {}.", paperUrl, pubmedId);
                            this.writeHtmlFile(String.valueOf(pubmedId), paperHtml);
                        }
                        papers++;
                    } catch (DownloadException e) {
                        // Here the download failed.  Add it to the missing-paper table.
                        this.createFailRow(pubmedId, title, e.getBadUrl(), e.getErrorType());
                        failures++;
                    }
                }
                long now = System.currentTimeMillis();
                if (now - lastMsg >= 10000) {
                    double rate = (now - start) / (double) (linesIn * 1000);
                    log.info("{} papers processed, {} seconds/paper, {} failures, {} missing links.", linesIn, rate, failures, missingLinks);
                    lastMsg = now;
                }
            }
            log.info("{} total papers processed, {} downloaded, {} failures, {} missing links.", linesIn, papers, failures, missingLinks);
            File failFile = this.getOutFile("failures.html");
            log.info("Writing failure table to {}.", failFile);
            // Convert the table to HTML.
            ContainerTag tableHtml = this.paperTable.output();
            // Enclose it in a page.
            ContainerTag failPage = html(head(title("Download Failures")), body(h1("Download Failures"), tableHtml));
            this.writeHtmlFile("failures", failPage.render());
        } finally {
            this.inStream.close();
        }

    }

    /**
     * Write an HTML string to a file.
     *
     * @param name		name of the file (without the extension)
     * @param html		HTML string to write
     *
     * @throws FileNotFoundException
     */
    private void writeHtmlFile(String name, String html) throws FileNotFoundException {
        File paperFile = this.getOutFile(String.valueOf(name) + ".html");
        try (PrintWriter paperStream = new PrintWriter(paperFile)) {
            paperStream.write(html);
        }
    }

    /**
     * Add a row to the failure table.
     *
     * @param pubmedId	PUBMED ID of the paper
     * @param title		title of the paper
     * @param linkUrl	link to the paper
     * @param reason	reason for the failure
     */
    private void createFailRow(int pubmedId, String title, String linkUrl,
            String reason) {
        Row<Key.Int> failRow = new Row<Key.Int>(this.paperTable, new Key.Int(pubmedId));
        failRow.add(pubmedId);
        ContainerTag linkHtml = a(title).withHref(linkUrl).withTarget("_blank");
        failRow.add(linkHtml);
        failRow.add(reason);
    }

}
