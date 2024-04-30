/**
 *
 */
package org.theseed.ncbi.download;


import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.Test;

/**
 * @author Bruce Parrello
 *
 */
class NcbiDownloadTest {

    @Test
    void testDownloader() throws IOException {
        File outDir = new File("data", "dl_test");
        try (NcbiDownloader dl = new NcbiDownloader("SRS281652", outDir, false, "SRR387728", "SRR387730")) {
            dl.execute();
        }


    }

}
