/**
 *
 */
package org.theseed.ncbi.download;


import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.theseed.ncbi.TagNotFoundException;
import org.theseed.ncbi.XmlUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author Bruce Parrello
 *
 */
class NcbiDownloadTest {

    @Test
    void testDownloader() throws IOException, ParserConfigurationException, SAXException, TagNotFoundException {
        File outDir = new File("data", "dl_test");
        if (! outDir.isDirectory())
            FileUtils.forceMkdir(outDir);
        else
            FileUtils.cleanDirectory(outDir);
        File xmlFile = new File("data", "dltest.xml");
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(xmlFile);
        List<Element> children = XmlUtils.childrenOf(document.getDocumentElement());
        assertThat(children.size(), equalTo(3));
        String sampleId = ReadSample.getSampleId(children.get(2));
        ReadSample sample = ReadSample.create(sampleId, children.get(2));
        assertThat(sample instanceof PairedReadSample, equalTo(true));
        assertThat(sample.getTitle(), equalTo("WGS of neisseria meningitidis genome: strain MC58"));
        try (NcbiDownloader dl = new NcbiDownloader(sample, outDir, true)) {
            dl.execute();
        }
        File testFile = new File(outDir, "SRS1669092_1.fastq.gz");
        assertThat(testFile.canRead(), equalTo(true));
        testFile = new File(outDir, "SRS1669092_2.fastq.gz");
        assertThat(testFile.canRead(), equalTo(true));
        testFile = new File(outDir, "SRS1669092_s.fastq.gz");
        assertThat(testFile.canRead(), equalTo(false));
        sampleId = ReadSample.getSampleId(children.get(0));
        assertThat(sampleId, equalTo("SRS258413"));
        sample = ReadSample.create(sampleId, children.get(0));
        assertThat(sample instanceof SingleReadSample, equalTo(true));
        assertThat(sample.getTitle(), equalTo("Mesorhizobium ciceri biovar biserrulae WSM1271"));
        sample.addRuns(children.get(1));
        try (NcbiDownloader dl = new NcbiDownloader(sample, outDir, false)) {
            dl.execute();
        }
        testFile = new File(outDir, "SRS258413.fastq");
        assertThat(testFile.canRead(), equalTo(true));
    }

}
