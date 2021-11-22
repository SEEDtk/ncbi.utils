/**
 *
 */
package org.theseed.ncbi.reports;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.ncbi.NcbiTable;
import org.theseed.ncbi.XmlUtils;
import org.theseed.utils.ParseFailureException;
import org.w3c.dom.Element;

/**
 * This report retrieves samples and displays the experiment abstract along with other
 * data relating to methods.  This is used to scan for metadata information that is
 * not in the standard SAMPLES report because it is hidden in text.  The report outputs
 * one record per project.  It is essentially a way to output project-related
 * information that is not available from the project database.
 *
 * @author Bruce Parrello
 *
 */
public class NcbiProjectReporter extends NcbiTableReporter {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(NcbiProjectReporter.class);
    /** map of projects to output maps */
    private Map<String, Map<String, String>> projectMap;

    /** heading array */
    private static final String[] HEADINGS = new String[] {
            "accession", "project", "strategy", "organism", "tax_id", "pubmed", "title", "description"
    };
    /** map for tag values in EXPERIMENT node */
    private static final Map<String, String> EXPERIMENT_TAGS = Map.of(
            "LIBRARY_STRATEGY", "strategy"
            );
    /** map for attribute values in STUDY node */
    private static final Map<String, String> STUDY_ATTRIBUTES = Map.of(
            "alias", "project", "accession", "accession"
            );
    /** map for tag values in SAMPLE node */
    private static final Map<String, String> SAMPLE_TAGS = Map.of(
            "SCIENTIFIC_NAME", "organism", "TAXON_ID", "tax_id");
    /** map for attribute values in POOL node */
    private static final Map<String, String> POOL_ATTRIBUTES = Map.of(
            "organism", "organism", "tax_id", "tax_id"
            );
    /** map for key/value pairs in STUDY_LINKS node */
    private static final Map<String, String> STUDY_LINKS_KEYS = Map.of(
            "pubmed", "pubmed"
            );

    public NcbiProjectReporter(IParms processor) throws ParseFailureException {
        super(NcbiTable.SRA);
        // Initialize the keyword filters.
        this.setupKeywordFiltering(processor);
        // Create the map for tracking experiments by project.
        this.projectMap = new TreeMap<String, Map<String, String>>();
    }

    @Override
    protected String getHeader() {
        return StringUtils.join(HEADINGS, '\t');
    }

    @Override
    public void writeRecord(Element record) {
        // Get the abstract and the title.
        Element study = XmlUtils.findFirstByTagName(record, "STUDY");
        if (study == null)
            log.warn("Missing STUDY node in EXPERIMENT_PACKAGE.");
        else {
            String abstractText = cleanHtml(XmlUtils.getXmlString(study, "STUDY_ABSTRACT"));
            String titleText = cleanHtml(XmlUtils.getXmlString(study, "STUDY_TITLE"));
            // We allow keywords in either the title or the abstract.
            boolean keep = this.checkAbstract(titleText + "\t" + abstractText);
            if (keep) {
                // We are keeping this record.  Get the rest of the fields.
                Map<String, String> outputMap = new TreeMap<String, String>();
                outputMap.put("description", abstractText);
                outputMap.put("title", titleText);
                this.processAttributes(study, STUDY_ATTRIBUTES, outputMap);
                // Get the organism name and taxon ID.
                Element sample = XmlUtils.findFirstByTagName(record, "SAMPLE");
                if (sample != null)
                    this.processTags(sample, SAMPLE_TAGS, outputMap);
                if (StringUtils.isBlank(outputMap.get("organism"))) {
                    // We didn't find the sample name, so we need to look for alternate data in the pool.
                    Element pool = XmlUtils.findFirstByTagName(record, "POOL");
                    if (pool != null)
                        this.processAttributes(pool, POOL_ATTRIBUTES, outputMap);
                }
                // Get the strategy from the experiment.
                Element experiment = XmlUtils.findFirstByTagName(record, "EXPERIMENT");
                if (experiment != null)
                    this.processTags(experiment, EXPERIMENT_TAGS, outputMap);
                // Get the pubmed ID (if any).
                Element studyLinks = XmlUtils.findFirstByTagName(study, "STUDY_LINKS");
                if (studyLinks != null)
                    this.processKeyValuePairs(studyLinks, "DB", "ID", STUDY_LINKS_KEYS, outputMap);
                // Save the record in the project map.
                this.projectMap.put(outputMap.get("accession"), outputMap);
            }
        }
    }

    @Override
    public void closeReport() {
        // This will be our output buffer.
        String[] outLine = new String[HEADINGS.length];
        // Loop through the projects, producing output.  We delay blank pubmeds
        // to the end.
        List<Map<String, String>> delayQueue =
                new ArrayList<Map<String, String>>(this.projectMap.size());
        for (Map<String, String> outputMap : this.projectMap.values()) {
            String pubmed = outputMap.get("pubmed");
            if (StringUtils.isBlank(pubmed))
                delayQueue.add(outputMap);
            else
                this.writeMap(outLine, outputMap);
        }
        for (Map<String, String> outputMap : delayQueue)
            this.writeMap(outLine, outputMap);
    }

    /**
     * Write the record in the specified output map using the specified output buffer.
     *
     * @param outLine		output buffer
     * @param outputMap		output map
     */
    private void writeMap(String[] outLine, Map<String, String> outputMap) {
        Arrays.fill(outLine, "");
        this.formatLine(HEADINGS, outputMap, outLine);
        this.writeLine(outLine);
    }

}
