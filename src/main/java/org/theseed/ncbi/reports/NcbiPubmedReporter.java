/**
 *
 */
package org.theseed.ncbi.reports;

import org.apache.commons.lang3.StringUtils;
import org.theseed.ncbi.NcbiTable;
import org.theseed.ncbi.XmlUtils;
import org.theseed.utils.ParseFailureException;
import org.w3c.dom.Element;

/**
 * This report prints abstracts from the PUBMED database.  The abstracts are filtered
 * by keyword.
 *
 * @author Bruce Parrello
 *
 */
public class NcbiPubmedReporter extends NcbiTableReporter {

    // FIELDS
    /** array of headers */
    private static final String[] HEADINGS = new String[] { "pubmed", "title", "abstract" };


    /**
     * Construct a pubmed reporter. The filtering patterns for the abstract will be
     * retrieved from the command processor.
     *
     * @param processor		controlling command processor
     *
     * @throws ParseFailureException
     */
    public NcbiPubmedReporter(IParms processor) throws ParseFailureException {
        super(NcbiTable.PUBMED);
        this.setupKeywordFiltering(processor);
    }

    @Override
    protected String getHeader() {
        return StringUtils.join(HEADINGS, '\t');
    }

    @Override
    public void writeRecord(Element record) {
        // Get the abstract text.
        String abstractText = cleanHtml(XmlUtils.getXmlString(record, "Abstract"));
        boolean keep = this.checkAbstract(abstractText);
        if (keep) {
            // We are keeping this record, so get the other fields.
            String title = cleanHtml(XmlUtils.getXmlString(record, "ArticleTitle"));
            String pubmed = XmlUtils.getXmlString(record, "PMID");
            this.writeLine(pubmed, title, abstractText);
        }
    }

    @Override
    public void closeReport() {
    }

}
