package org.theseed.ncbi.utils;

import java.util.Arrays;

import org.theseed.basic.BaseProcessor;

/**
 * Commands for retrieving data from the ENTREZ NCBI database.
 *
 * query	retrieve data from the NCBI Entrez database
 * list		list records from the NCBI Entrez database
 * fetch	download samples from NCBI
 */
public class App
{
    /** static array containing command names and comments */
    protected static final String[] COMMANDS = new String[] {
             "query", "retrieve data from the NCBI Entrez database",
             "list", "list records from the NCBI Entrez database",
             "fetch", "download samples from NCBI",
    };

    public static void main( String[] args )
    {
        // Get the control parameter.
        String command = args[0];
        String[] newArgs = Arrays.copyOfRange(args, 1, args.length);
        BaseProcessor processor;
        // Determine the command to process.
        switch (command) {
        case "query" :
            processor = new NcbiQueryProcessor();
            break;
        case "list" :
            processor = new NcbiListProcessor();
            break;
        case "fetch" :
            processor = new NcbiFetchProcessor();
            break;
        case "-h" :
        case "--help" :
            processor = null;
            break;
        default :
            throw new RuntimeException("Invalid command " + command + ".");
        }
        if (processor == null)
            BaseProcessor.showCommands(COMMANDS);
        else {
            boolean ok = processor.parseCommand(newArgs);
            if (ok) {
                processor.run();
            }
        }
    }
}
