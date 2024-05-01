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
        // TODO construct sample here
        try (NcbiDownloader dl = new NcbiDownloader(null, outDir, false)) {
            dl.execute();
        }


    }

}
