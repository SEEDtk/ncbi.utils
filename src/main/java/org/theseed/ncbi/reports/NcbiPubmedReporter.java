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
    private static final String[] HEADINGS = new String[] { "pubmed", "doi_link", "pmc_id", "title", "abstract" };


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
            String doiLink = getDoiUrl(record);
            String pmcId = getArticleId(record, "pmc");
            this.writeLine(pubmed, doiLink, pmcId, title, abstractText);
        }
    }

    /**
     * Compute the DOI link for a pubmed article.  We find the DOI article ID (if any)
     * and form it into a URL.
     *
     * @param record	XML record for the pubmed article
     *
     * @return the link to the DOI, or an empty string if none exists
     */
    public static String getDoiUrl(Element record) {
        String retVal;
        String doiString = getArticleId(record, "doi");
        if (StringUtils.isBlank(doiString))
            retVal = "";
        else
            retVal = "https://dx.doi.org/" + doiString;
        return retVal;
    }

    /**
     * Locate the article ID of the specified type.
     *
     * @param record	XML record for the pubmed article
     * @param type		type of ID desired (e.g. "pmc", "doi")
     *
     * @return the desired article ID, or an empty string if none was found
     */
    public static String getArticleId(Element record, String type) {
        String retVal = "";
        // Find the article ID list.  It is always a direct child of the PubmedData element.
        // Note that we treat a malformed record as a no-id-found condition.
        Element pubmedDataElement = XmlUtils.findFirstByTagName(record, "PubmedData");
        if (pubmedDataElement != null) {
            Element idListElement = XmlUtils.findFirstByTagName(pubmedDataElement, "ArticleIdList");
            if (idListElement != null) {
                var iter = XmlUtils.descendantsOf(idListElement, "ArticleId").iterator();
                while (iter.hasNext() && retVal.isEmpty()) {
                    Element idElement = iter.next();
                    String typeCode = idElement.getAttribute("IdType");
                    if (typeCode.contentEquals(type))
                        retVal = idElement.getTextContent();
                }
            }
        }
        return retVal;
    }

    @Override
    public void closeReport() {
    }

}
