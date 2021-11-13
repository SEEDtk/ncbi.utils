/**
 *
 */
package org.theseed.ncbi.reports;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.theseed.ncbi.NcbiTable;
import org.theseed.ncbi.XmlUtils;
import org.w3c.dom.Element;

/**
 * This is the base class for all NCBI table reports.
 *
 * @author Bruce Parrello
 *
 */
public abstract class NcbiTableReporter {

    // FIELDS
    /** output writer */
    private PrintWriter writer;
    /** target NCBI table */
    private NcbiTable table;

    /**
     * This interface defines the special parameters that must be provided by the client command.
     */
    public static interface IParms {

    }

    /**
     * This enum defines the supported report types.
     */
    public static enum Type {
        SAMPLE {

            @Override
            public NcbiTableReporter create(IParms processor) {
                return new NcbiSraReporter(processor);
            }

        };

        /**
         * @return a reporting object of this type
         *
         * @param processor		controlling command processor
         */
        public abstract NcbiTableReporter create(IParms processor);
    }

    /**
     * Construct an NCBI table reporting object.
     *
     * @param table			target table
     */
    public NcbiTableReporter(NcbiTable table) {
        this.table = table;
    }

    /**
     * Initialize the report and save the output writer.
     *
     * @param writer		output print writer
     */
    public void openReport(PrintWriter writer) {
        this.writer = writer;
        this.writer.println(this.getHeader());
    }

    /**
     * @return the table for this report
     */
    public NcbiTable getTable() {
        return this.table;
    }

    /**
     * @return the header line for this report
     */
    protected abstract String getHeader();

    /**
     * Write a data line for the specified NCBI record.
     *
     * @param record	XML element containing the record
     */
    public abstract void writeRecord(Element record);

    /**
     * Finish the report.
     */
    public abstract void closeReport();

    /**
     * This is a helper method.  The values array is filled in from the output map,
     * using the header array to guide the positioning.
     *
     * @param headers		the header array, in output order
     * @param outputMap		one or more maps, each keyed by header name with a value equal to the
     * 						output value
     * @param outLine		the output array
     */
    protected void formatLine(String[] headers, Map<String, String> outputMap, String[] outLine) {
        for (int i = 0; i < headers.length; i++) {
            String value = outputMap.get(headers[i]);
            if (value != null)
                outLine[i] = value;
        }
    }

    /**
     * Write a line of output that has been formatted in an output array.
     *
     * @param outLine		the output array
     */
    protected void writeLine(String[] outLine) {
        this.writer.println(StringUtils.join(outLine, '\t'));
    }

    /**
     * This is a helper method.  It takes as input an element, a map from attribute names to header
     * names, and an output map.  The output map will be updated with the values for the attributes,
     * keyed by the specified header names.
     *
     * @param element		source element for the data
     * @param helperMap		map of attribute names to output header names
     * @param outputMap		map of output header names to values, to be modified
     */
    protected void processAttributes(Element element, Map<String, String> helperMap,
            Map<String, String> outputMap) {
        for (Map.Entry<String, String> helper : helperMap.entrySet()) {
            // If the attribute doesn't exist, getAttribute helpfully returns an empty string.
            String value = element.getAttribute(helper.getKey());
            outputMap.put(helper.getValue(), value);
        }
    }

    /**
     * This is a helper method.  It takes as input an element, a map from tag names to header names,
     * and an output map.  The output map will be updated with the text content of the tag names, keyed
     * by the specified header names.
     *
     * @param element		source element for the data
     * @param helperMap		map of tag names to output header names
     * @param outputMap		map of output header names to values, to be modified
     */
    protected void processTags(Element element, Map<String, String> helperMap,
            Map<String, String> outputMap) {
        for (Map.Entry<String, String> helper : helperMap.entrySet()) {
            String value = XmlUtils.getXmlString(element, helper.getKey());
            outputMap.put(helper.getValue(), value);
        }
    }

    /**
     * This is a helper method.  It takes as input an element, a key tag name, a value tag name,
     * a map from keys to header names, and an output map.  For each key in the map, every child
     * of the input element will be checked for a key tag child with the specified key
     * as its text content.  The text content of the corresponding value tag will be associated with the
     * header name in the output map.
     *
     * @param element		source element for the data
     * @param keyName		name of the key tag
     * @param valueName		name of the value tag
     * @param helperMap		map of key values to output header names
     * @param outputMap		map of output header names to values, to be modified
     */
    protected void processKeyValuePairs(Element element, String keyName, String valueName,
            Map<String, String> helperMap, Map<String, String> outputMap) {
        // We update the map for all the children of the element.
        List<Element> children = XmlUtils.childrenOf(element);
        for (Element child : children) {
            // Get the key value.
            Element keyElement = XmlUtils.findFirstByTagName(child, keyName);
            if (keyElement != null) {
                // Is this an interesting key?
                String childKey = StringUtils.stripToEmpty(keyElement.getTextContent());
                if (helperMap.containsKey(childKey)) {
                    // Yes. Update the output map with our value.
                    String value = XmlUtils.getXmlString(child, valueName);
                    outputMap.put(helperMap.get(childKey), value);
                }
            }
        }
        // Now we need to make sure we have empty strings for the missing keys.
        for (String header : helperMap.values()) {
            if (! outputMap.containsKey(header))
                outputMap.put(header, "");
        }
    }

    /**
     * This is a helper method.  It takes as input an element, a map of descendant tag names to
     * header names, and an output map.  The tag name of the first child under each descendant is
     * the put into the output map with the header name as the key.
     *
     * @param element		source element for the data
     * @param helperMap		map of descendant tag names to header names
     * @param outputMap		output map, will be modified
     */
    protected void processTagTags(Element element, Map<String, String> helperMap,
            Map<String, String> outputMap) {
        for (Map.Entry<String, String> helper : helperMap.entrySet()) {
            String value = "";
            Element tagElement = XmlUtils.findFirstByTagName(element, helper.getKey());
            if (tagElement != null) {
                List<Element> children = XmlUtils.childrenOf(tagElement);
                if (children.size() >= 1)
                    value = children.get(0).getNodeName();
            }
            outputMap.put(helper.getValue(), value);
        }
    }

}
