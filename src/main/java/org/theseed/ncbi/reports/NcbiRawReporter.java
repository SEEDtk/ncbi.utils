/**
 *
 */
package org.theseed.ncbi.reports;

import java.io.IOException;
import java.io.StringWriter;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.ncbi.XmlUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * This report displays SRA records in raw XML form.  It is very basic.
 *
 * @author Bruce Parrello
 *
 */
public class NcbiRawReporter extends NcbiTableReporter {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(NcbiRawReporter.class);
    /** factory for transforming XML documents */
    private TransformerFactory xfactory;
    /** xml document being built */
    private Document document;
    /** xml document root */
    private Element root;


    public NcbiRawReporter(IParms processor) throws IOException {
        super(processor.getTargetTable());
        this.xfactory = TransformerFactory.newInstance();
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            this.document = builder.newDocument();
            this.root = this.document.createElement("RAW_DATA");
            this.document.appendChild(this.root);
        } catch (ParserConfigurationException e) {
            throw new IOException("Error in XML setup: " + e.toString());
        }
    }

    @Override
    protected String getHeader() {
        return null;
    }

    @Override
    public void writeRecord(Element record) {
        // Get a copy of this record in the current document.
        Element clone = (Element) this.document.importNode(record, true);
        // Remove empty text nodes.
        XmlUtils.cleanElement(clone);
        // Add it to the output document.
        this.root.appendChild(clone);
    }

    @Override
    public void closeReport() {
        // Convert the XML document to text.
        String output;
        try {
            Transformer xform = this.xfactory.newTransformer();
            xform.setOutputProperty(OutputKeys.INDENT, "yes");
            StringWriter writer = new StringWriter();
            xform.transform(new DOMSource(this.document), new StreamResult(writer));
            output = writer.toString();
        } catch (TransformerException e) {
            output = "<ERROR>" + e.toString() + "</ERROR>";
        }
        this.writeString(output);
    }

}
