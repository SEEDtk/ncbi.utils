/**
 *
 */
package org.theseed.ncbi.reports;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.ncbi.NcbiTable;
import org.theseed.ncbi.XmlUtils;
import org.w3c.dom.Element;

/**
 * This is a simple report that gets the project and pubmed IDs for all the samples found.  It is intended
 * for use in loading an RNA Seq expression database.
 *
 * @author Bruce Parrello
 *
 */
public class NcbiProjectTableReporter extends NcbiTableReporter {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(NcbiProjectReporter.class);

    /** heading array */
    private static final String[] HEADINGS = new String[] {
            "sample_id", "project", "pubmed"
    };
    /** map for attribute values in STUDY node */
    private static final Map<String, String> STUDY_ATTRIBUTES = Map.of(
            "accession", "project"
            );
    /** map for key/value pairs in STUDY_LINKS node */
    private static final Map<String, String> STUDY_LINKS_KEYS = Map.of(
            "pubmed", "pubmed"
            );
    /** map for attribute values in RUN node */
    private static final Map<String, String> RUN_ATTRIBUTES = Map.of(
            "accession", "sample_id");

    /**
     * Initialize this report.
     *
     * @param processor		controlling command processor
     */
    public NcbiProjectTableReporter(IParms processor) {
        super(NcbiTable.SRA);
    }

    @Override
    protected String getHeader() {
        return StringUtils.join(HEADINGS, '\t');
    }

    @Override
    public void writeRecord(Element record) {
        Map<String, String> outputMap = new TreeMap<String, String>();
        Element experiment = XmlUtils.findFirstByTagName(record, "EXPERIMENT");
        Element runSet = XmlUtils.findFirstByTagName(record, "RUN_SET");
        if (experiment == null)
            log.error("Missing EXPERIMENT node in experiment package.");
        else if (runSet == null || ! runSet.hasChildNodes())
            log.warn("Experiment {} has no runs.", experiment.getAttribute("accession"));
        else {
            Element study = XmlUtils.findFirstByTagName(record, "STUDY");
            if (study == null)
                log.warn("Missing STUDY node in EXPERIMENT_PACKAGE.");
            else {
                this.processAttributes(study, STUDY_ATTRIBUTES, outputMap);
                // Get the pubmed ID (if any).
                Element studyLinks = XmlUtils.findFirstByTagName(study, "STUDY_LINKS");
                if (studyLinks != null)
                    this.processKeyValuePairs(studyLinks, "DB", "ID", STUDY_LINKS_KEYS, outputMap);
                // Form the output line.
                String[] outLine = new String[HEADINGS.length];
                // Verify that we have data.
                this.formatLine(HEADINGS, outputMap, outLine);
                boolean found = IntStream.range(1, HEADINGS.length).anyMatch(i -> ! StringUtils.isBlank(outLine[i]));
                if (found) {
                    // Now we loop through the runs.  Each run gets an output record.  Only the first column
                    // changes.
                    List<Element> runs = XmlUtils.childrenOf(runSet);
                    for (Element run : runs) {
                        this.processAttributes(run, RUN_ATTRIBUTES, outputMap);
                        outLine[0] = outputMap.get("sample_id");
                        this.writeLine(outLine);
                    }
                }
            }
        }
    }

    @Override
    public void closeReport() {
    }

}
