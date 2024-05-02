/**
 *
 */
package org.theseed.ncbi.download;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.ncbi.TagNotFoundException;
import org.theseed.ncbi.XmlUtils;
import org.theseed.reports.NaturalSort;
import org.w3c.dom.Element;

/**
 * This is the base class for samples.  The subclasses handle the differences between paired samples
 * and single-stream samples.
 *
 * @author Bruce Parrello
 *
 */
public abstract class ReadSample {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(ReadSample.class);

    /** ID of this sample */
    private String sampleId;
    /** list of run IDs */
    private NavigableSet<String> runs;
    /** estimated number of spots */
    private int spots;

    /**
     * Construct a new sample from an experiment-package descriptor.
     *
     * @param sampleId		sample accession ID
     * @param expElement	experiment-package document returned by NCBI
     *
     * @throws TagNotFoundException
     */
    protected ReadSample(String sampleId, Element expElement) throws TagNotFoundException {
        // Save the sample ID.
        this.sampleId = sampleId;
        // Denote no runs so far.
        this.spots = 0;
        this.runs = new TreeSet<String>(new NaturalSort());
        // Now add the run data.
        this.addRuns(expElement);
    }

    /**
     * Create a sample using an experiment-package descriptor.
     *
     * @param sampleId		sample accession ID
     * @param expElement	experiment-package document returned by NCBI
     *
     * @throws TagNotFoundException
     */
     public static ReadSample create(String sampleId, Element expElement) throws TagNotFoundException {
         // The library layout indicates whether we are paired.
         Element libraryLayout = XmlUtils.getFirstByTagName(expElement, "LIBRARY_LAYOUT");
         Element pairedNode = XmlUtils.findFirstByTagName(libraryLayout, "PAIRED");
         ReadSample retVal;
         if (pairedNode != null)
             retVal = new PairedReadSample(sampleId, expElement);
         else
             retVal = new SingleReadSample(sampleId, expElement);
         return retVal;
     }


    /**
     * Find the sample ID in an experiment package descriptor.
     *
     * @param expElement	experiment-package document returned by NCBI
     *
     * @return the sample accession string
     *
     * @throws TagNotFoundException
     */
    public static String getSampleId(Element expElement) throws TagNotFoundException {
        Element sampleTag = XmlUtils.getFirstByTagName(expElement, "SAMPLE");
        return sampleTag.getAttribute("accession");
    }

    /**
     * Find the run ID in an experiment package descriptor.
     *
     * @param expElement	experiment-package document returned by NCBI
     *
     * @return the sample accession string
     *
     * @throws TagNotFoundException
     */
    public static String getRunId(Element expElement) throws TagNotFoundException {
        Element runTag = XmlUtils.getFirstByTagName(expElement, "RUN");
        return runTag.getAttribute("accession");
    }

    /**
     * Process the header record for a FASTQ read.
     *
     * @param header	header record to parse
     *
     * @return sequence part with the ID and type filled in
     *
     * @throws IOException
     */
    protected abstract SeqPart processHeader(String header) throws IOException;

    /**
     * Add the runs from the specified experiment package to this sample.
     *
     * @param expElement	experiment-package document returned by NCBI
     *
     * @throws TagNotFoundException
     */
    public void addRuns(Element expElement) throws TagNotFoundException {
        // Get all the runs.
        Element runSetTag = XmlUtils.getFirstByTagName(expElement, "RUN_SET");
        List<Element> runTags = XmlUtils.descendantsOf(runSetTag, "RUN");
        // Loop through them, adding each one.
        for (Element run : runTags) {
            // Get the run ID and size.
            String runId = run.getAttribute("accession");
            int runSpots = Integer.valueOf(run.getAttribute("total_spots"));
            // Try to add the run.  If it's new, update the spot count.
            boolean newRun = this.runs.add(runId);
            if (newRun)
                this.spots += runSpots;
        }
    }

    /**
     * @return the sample accession ID
     */
    public String getId() {
        return this.sampleId;
    }

    /**
     * @return the set of runs
     */
    public NavigableSet<String> getRuns() {
        return this.runs;
    }

    /**
     * @return the spots
     */
    public int getSpots() {
        return this.spots;
    }

    /**
     * Open the output streams for this sample.
     *
     * @param outDir	output directory to use
     * @param zipped	TRUE if the output stream should be gzipped
     *
     * @throws IOException
     */
    public abstract void openStreams(File outDir, boolean zipped) throws IOException;

    /**
     * Write a sequence pair to the output.
     *
     * @param leftRead		left read part
     * @param rightRead		right read part
     */
    public abstract void writePair(SeqPart leftRead, SeqPart rightRead);

    /**
     * Write a singleton read to the output.
     *
     * @param read		read part to write
     */
    public abstract void writeSingle(SeqPart read);

    /**
     * Flush all the output streams.
     */
    public abstract void flushStreams();

    /**
     * Close the output streams for this sample.
     */
    public abstract void closeStreams();

    /**
     * Open an output stream for one of the sample output files.
     *
     * @param outDir	directory to contain the files
     * @param zipped	TRUE if the stream should be GZIPped
     * @param suffix	suffix for the file name
     *
     * @return the open output stream
     *
     * @throws IOException
     */
    protected PrintWriter openStream(File outDir, boolean zipped, String suffix) throws IOException {
        // Compute the appropriate extension and build the file name.
        String ext = ".fastq";
        if (zipped) ext += ".gz";
        File outFile = new File(outDir, this.getId() + suffix + ext);
        // Open the appropriate type of output stream.
        PrintWriter retVal;
        if (! zipped)
            retVal = new PrintWriter(outFile);
        else {
            OutputStream outStream = new FileOutputStream(outFile);
            outStream = new GZIPOutputStream(outStream);
            retVal = new PrintWriter(outStream);
        }
        return retVal;
    }

}
