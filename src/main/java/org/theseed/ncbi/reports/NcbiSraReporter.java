/**
 *
 */
package org.theseed.ncbi.reports;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.ncbi.NcbiTable;
import org.theseed.ncbi.XmlUtils;
import org.w3c.dom.Element;

/**
 * This is the basic report for samples from the SRA.  The records returned are experiments.
 * Experiments generally have a single run but there may be multiple runs.  One output line is
 * produced for each run.  The data in this report is large, with many columns, due to the
 * complexity of harvesting good samples and the many different sample types.
 *
 * @author Bruce Parrello
 *
 */
public class NcbiSraReporter extends NcbiTableReporter {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(NcbiSraReporter.class);

    /** headings */
    private static String[] HEADINGS = new String[] {
            "run_id", "experiment", "organism", "tax_id", "strategy", "source", "selector",
            "layout", "platform", "pubmed", "source_name", "strain", "medium",
            "growth_phase", "substrain", "total_spots", "total_bases", "size"
    };
    /** map for tag values in EXPERIMENT node */
    private static final Map<String, String> EXPERIMENT_TAGS = Map.of(
            "LIBRARY_STRATEGY", "strategy", "LIBRARY_SOURCE", "source",
            "LIBRARY_SELECTION", "selector"
            );
    /** map for attribute values in EXPERIMENT node */
    private static final Map<String, String> EXPERIMENT_ATTRIBUTES = Map.of(
            "accession", "experiment"
            );
    /** map for tag-tag values in EXPERIMENT node */
    private static final Map<String, String> EXPERIMENT_TAG_TAGS = Map.of(
            "LIBRARY_LAYOUT", "layout", "PLATFORM", "platform"
            );
    /** map for attribute values in RUN node */
    private static final Map<String, String> RUN_ATTRIBUTES = Map.of(
            "accession", "run_id", "total_spots", "total_spots",
            "total_bases", "total_bases", "size", "size"
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
    /** map for key/value pairs in SAMPLE_ATTRIBUTES node */
    private static final Map<String, String> SAMPLE_ATTRIBUTES_KEYS = Map.of(
            "source_name", "source_name", "strain", "strain", "medium", "medium",
            "growth phase", "growth_phase", "substrain", "substrain"
            );

    public NcbiSraReporter (IParms processor) {
        super(NcbiTable.SRA);
    }

    @Override
    protected String getHeader() {
        return StringUtils.join(HEADINGS, '\t');
    }

    @Override
    public void writeRecord(Element record) {
        Element experiment = XmlUtils.findFirstByTagName(record, "EXPERIMENT");
        Element runSet = XmlUtils.findFirstByTagName(record, "RUN_SET");
        if (experiment == null)
            log.error("Missing EXPERIMENT node in experiment package.");
        else if (runSet == null || ! runSet.hasChildNodes())
            log.warn("Experiment {} has no runs.", experiment.getAttribute("accession"));
        else {
            // We first gather all the experiment-level data.  Then we print an output list
            // for each run.
            Map<String, String> experimentOutputMap = new TreeMap<String, String>();
            this.processAttributes(experiment, EXPERIMENT_ATTRIBUTES, experimentOutputMap);
            this.processTagTags(experiment, EXPERIMENT_TAG_TAGS, experimentOutputMap);
            this.processTags(experiment, EXPERIMENT_TAGS, experimentOutputMap);
            Element studyLinks = XmlUtils.findFirstByTagName(record, "STUDY_LINKS");
            if (studyLinks != null)
                this.processKeyValuePairs(studyLinks, "DB", "ID", STUDY_LINKS_KEYS, experimentOutputMap);
            Element sample = XmlUtils.findFirstByTagName(record, "SAMPLE");
            if (sample != null)
                this.processTags(sample, SAMPLE_TAGS, experimentOutputMap);
            if (StringUtils.isBlank(experimentOutputMap.get("organism"))) {
                // We didn't find the sample name, so we need to look for alternate data in the pool.
                Element pool = XmlUtils.findFirstByTagName(record, "POOL");
                if (pool != null)
                    this.processAttributes(pool, POOL_ATTRIBUTES, experimentOutputMap);
            }
            Element sampleAttributes = XmlUtils.findFirstByTagName(sample, "SAMPLE_ATTRIBUTES");
            if (sampleAttributes != null)
                this.processKeyValuePairs(sampleAttributes, "TAG", "VALUE", SAMPLE_ATTRIBUTES_KEYS,
                        experimentOutputMap);
            // This will be our output buffer.  Fill in all the experiment stuff here.
            String[] outLine = new String[HEADINGS.length];
            Arrays.fill(outLine, "");
            this.formatLine(HEADINGS, experimentOutputMap, outLine);
            // Now we loop through the runs.  Each run gets an output record.  Only the run stuff
            // changes.
            List<Element> runs = XmlUtils.childrenOf(runSet);
            for (Element run : runs) {
                Map<String, String> runOutputMap = new TreeMap<String, String>();
                this.processAttributes(run, RUN_ATTRIBUTES, runOutputMap);
                this.formatLine(HEADINGS, runOutputMap, outLine);
                this.writeLine(outLine);
            }
        }
    }

    @Override
    public void closeReport() {
    }

}
