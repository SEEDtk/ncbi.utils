/**
 *
 */
package org.theseed.ncbi.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.theseed.ncbi.NcbiTable;
import org.theseed.ncbi.XmlUtils;
import org.theseed.utils.ParseFailureException;
import org.w3c.dom.Element;

/**
 * This is the base class for all NCBI table reports.  It includes support for
 * methods that are shared between reports, though not all of these are used by
 * all reports.
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
    /** list of keywords to find, coded as regular expressions */
    private List<Pattern> keywords;
    /** minimum number of keyword matches required */
    private int minKeywords;
    /** pattern for cleaning HTML strings */
    private static final Pattern HTML_TAG = Pattern.compile("\\<.*?\\>");

    /**
     * This interface defines the special parameters that must be provided by the client command.
     */
    public static interface IParms {

        /**
         * @return the list of regex patterns for abstract filtering
         */
        public List<String> getPatterns();

        /**
         * @return the minimum number of matches for abstract filtering (0 == all)
         */
        public int getFilterMode();

        /**
         * @return the target table for a multi-table report
         */
        NcbiTable getTargetTable();

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

        },
        RAW {

            @Override
            public NcbiTableReporter create(IParms processor) throws IOException {
                return new NcbiRawReporter(processor);
            }

        },
        PUBMED {

            @Override
            public NcbiTableReporter create(IParms processor) throws ParseFailureException {
                return new NcbiPubmedReporter(processor);
            }

        },
        PROJTABLE {

            @Override
            public NcbiTableReporter create(IParms processor) {
                return new NcbiProjectTableReporter(processor);
            }

        },
        PROJECTS {

            @Override
            public NcbiTableReporter create(IParms processor) throws ParseFailureException {
                return new NcbiProjectReporter(processor);
            }

        };

        /**
         * @return a reporting object of this type
         *
         * @param processor		controlling command processor
         *
         * @throws ParseFailureException
         * @throws IOException
         */
        public abstract NcbiTableReporter create(IParms processor)
                throws ParseFailureException, IOException;
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
     * Initialize the filtering-keyword facility.
     *
     * @param processor		controlling command processor
     *
     * @throws ParseFailureException
     */
    protected void setupKeywordFiltering(IParms processor) throws ParseFailureException {
        // Create the list of patterns from the pattern strings.
        List<String> regexStrings = processor.getPatterns();
        this.keywords = new ArrayList<Pattern>(regexStrings.size());
        for (String regexString : regexStrings) {
            try {
                Pattern pattern = Pattern.compile(regexString, Pattern.CASE_INSENSITIVE);
                this.keywords.add(pattern);
            } catch (PatternSyntaxException e) {
                throw new ParseFailureException("Error in abstract-filtering pattern: " + e.getMessage());
            }
        }
        // Compute the minimum-match number.
        this.minKeywords = processor.getFilterMode();
        if (this.minKeywords == 0)
            this.minKeywords = this.keywords.size();
        if (this.minKeywords < 0)
            throw new ParseFailureException("Invalid filter mode:  cannot be negative");
        else if (this.minKeywords > this.keywords.size())
            throw new ParseFailureException("Invalid filter mode:  cannot be greater than number of patterns.");
    }

    /**
     * Initialize the report and save the output writer.
     *
     * @param writer		output print writer
     */
    public void openReport(PrintWriter writer) {
        this.writer = writer;
        String header = this.getHeader();
        if (header != null)
            this.writer.println(this.getHeader());
    }

    /**
     * @return the table for this report
     */
    public NcbiTable getTable() {
        return this.table;
    }

    /**
     * @return the header line for this report. or NULL if there is no header
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
    protected void writeLine(String... outLine) {
        this.writer.println(StringUtils.join(outLine, '\t'));
    }

    /**
     * Write a line of raw output.  A new-line is appended.
     *
     * @param outString		the output string
     */
    protected void writeString(String outString) {
        this.writer.println(outString);
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

    /**
     * Filter an abstract to insure it is acceptable.
     *
     * @param abstractText		abstract text to filter
     *
     * @return TRUE if we should keep the current record, else FALSE
     */
    protected boolean checkAbstract(String abstractText) {
        // Denote we are keeping this record.
        boolean retVal = true;
        // If we have filtering, do the filter search.
        if (this.minKeywords > 0) {
            int count = (int) this.keywords.stream().filter(x -> x.matcher(abstractText).find()).count();
            retVal = (count >= this.minKeywords);
        }
        return retVal;
    }

    /**
     * Remove HTML tags from a string
     *
     * @param text		text to clear
     *
     * @return the text with the HTML tags removed
     */
    public static String cleanHtml(String text) {
        String retVal = RegExUtils.removeAll(text, HTML_TAG);
        return retVal;
    }


}
