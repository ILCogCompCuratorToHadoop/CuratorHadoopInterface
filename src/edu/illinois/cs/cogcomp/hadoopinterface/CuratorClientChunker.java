package edu.illinois.cs.cogcomp.hadoopinterface;

import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.CuratorClientArgParser;
import edu.illinois.cs.cogcomp.thrift.base.AnnotationFailedException;
import edu.illinois.cs.cogcomp.thrift.base.ServiceSecurityException;
import edu.illinois.cs.cogcomp.thrift.base.ServiceUnavailableException;
import org.apache.thrift.TException;

import java.io.File;
import java.io.IOException;

/**
 * @author Tyler Young
 */
public class CuratorClientChunker extends CuratorClient {
    public CuratorClientChunker( String localhost, int i ) {
        super(localhost, i);
    }

    /**
     * The main method for the external, "master" Curator client.
     * @param commandLineArgs  String arguments from the command line. Should
     *                         contain the host name for the (already-running)
     *                         Curator, the port number for connecting to the
     *                         Curator, the job input directory, and the mode at
     *                         minimum.
     */
    public static void main( String[] commandLineArgs )
            throws ServiceUnavailableException, TException,
            AnnotationFailedException, IOException, ServiceSecurityException {
        // Parse input
        CuratorClientArgParser args = new CuratorClientArgParser(commandLineArgs);
        args.printArgsInterpretation();

        // Set up local vars
        CuratorClientChunker theClient = new CuratorClientChunker( "localhost", 9010 );
        String msg = "Curator is running on localhost, port 9010? " +
                (theClient.curatorIsRunning() ? "Yes." : "No.");
        System.out.println(msg);


        // Create records from the input text files
        System.out.println( "Ready to reserialize records from serialized form in " +
                "the input directory." );
        theClient.addRecordsFromJobDirectory( new File("/shared/gargamel/undergrad/tyoun/hadoop-1.0.3/POS_output/"), false );

        System.out.println( "Turned " + theClient.getNumInputRecords()
                + " text files in the directory into records.");

        // Run a tool (for testing purposes)
        theClient.runNER();

    }
}
