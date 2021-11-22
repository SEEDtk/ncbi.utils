/**
 *
 */
package org.theseed.ncbi.reports;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.theseed.ncbi.XmlException;
import org.theseed.ncbi.XmlUtils;
import org.theseed.utils.ParseFailureException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * @author Bruce Parrello
 *
 */
public class ReportUtilitiesTest {

    @Test
    public void testReportHelpers() throws IOException, XmlException, ParseFailureException {
        NcbiTableReporter reporter = NcbiTableReporter.Type.SAMPLE.create(null);
        Document doc = XmlUtils.readXmlFile(new File("data", "experiments.xml"));
        Element experiment = XmlUtils.getFirstByTagName(doc.getDocumentElement(), "EXPERIMENT_PACKAGE");
        assertThat(experiment, not(nullValue()));
        Element experimentActual = XmlUtils.getFirstByTagName(experiment, "EXPERIMENT");
        assertThat(experimentActual.getAttribute("accession"), equalTo("SRX474185"));
        Map<String, String> runMap = Map.of("total_spots", "read_count", "total_bases", "size",
                "size", "file_size", "is_private", "private");
        Element run = XmlUtils.getFirstByTagName(experiment, "RUN");
        Map<String, String> outputMap = new HashMap<String, String>();
        reporter.processAttributes(run, runMap, outputMap);
        assertThat(outputMap.get("read_count"), equalTo("8722953"));
        assertThat(outputMap.get("size"), equalTo("819957582"));
        assertThat(outputMap.get("file_size"), equalTo("527768397"));
        assertThat(outputMap.get("private"), equalTo(""));
        assertThat(outputMap.size(), equalTo(4));
        Map<String, String> libraryMap = Map.of("LIBRARY_STRATEGY", "strategy", "LIBRARY_SOURCE", "source",
                "LIBRARY_SELECTION", "selector", "LIBRARY_PROTOCOL", "protocol");
        reporter.processTags(experiment, libraryMap, outputMap);
        assertThat(outputMap.get("strategy"), equalTo("RNA-Seq"));
        assertThat(outputMap.get("source"), equalTo("TRANSCRIPTOMIC"));
        assertThat(outputMap.get("selector"), equalTo("cDNA"));
        assertThat(outputMap.get("protocol"), equalTo(""));
        assertThat(outputMap.size(), equalTo(8));
        Map<String, String> sampleMap = Map.of("source_name", "sample_source", "strain", "strain",
                "medium", "medium", "growth phase", "growth_phase", "substrain", "substrain",
                "pubmed", "pubmed");
        Element sampleAttributes = XmlUtils.getFirstByTagName(experiment, "SAMPLE_ATTRIBUTES");
        reporter.processKeyValuePairs(sampleAttributes, "TAG", "VALUE", sampleMap, outputMap);
        assertThat(outputMap.get("sample_source"), equalTo("Escherichia coli MG1655 cells"));
        assertThat(outputMap.get("strain"), equalTo("K-12"));
        assertThat(outputMap.get("medium"), equalTo("M63"));
        assertThat(outputMap.get("growth_phase"), equalTo("exponential"));
        assertThat(outputMap.get("substrain"), equalTo("MG1655"));
        assertThat(outputMap.get("pubmed"), equalTo(""));
        assertThat(outputMap.size(), equalTo(14));
        Map<String, String> expMap = Map.of("LIBRARY_LAYOUT", "layout", "PLATFORM", "platform",
                "TITLE", "title", "AUTHORITY", "authority");
        reporter.processTagTags(experimentActual, expMap, outputMap);
        assertThat(outputMap.get("layout"), equalTo("SINGLE"));
        assertThat(outputMap.get("platform"), equalTo("ILLUMINA"));
        assertThat(outputMap.get("title"), equalTo(""));
        assertThat(outputMap.get("authority"), equalTo(""));
        assertThat(outputMap.size(), equalTo(18));
    }

}
