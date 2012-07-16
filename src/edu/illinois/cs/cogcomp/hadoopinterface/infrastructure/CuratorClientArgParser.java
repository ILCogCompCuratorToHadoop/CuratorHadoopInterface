package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import java.io.File;

/**
 * @author Tyler Young
 */
public class CuratorClientArgParser {
    /**
     * Sets the many fields specifiable from the command line arguments
     * @param commandLineArgs Arguments passed to main() from the command line
     */
    public CuratorClientArgParser( String[] commandLineArgs ) {
        confirmArgsAreGood( commandLineArgs );

        host = commandLineArgs[0];
        port  = Integer.parseInt( commandLineArgs[1] );
        inputDir = new File( commandLineArgs[2] );
        testing = false;

        outputDir = new File( inputDir, "output" );

        if( commandLineArgs.length >= 4 ) {
            if( commandLineArgs[3].equals("-test") ) {
                testing = true;
            }
            else { // Must be an output directory
                outputDir = new File( commandLineArgs[3] );

                if( commandLineArgs.length == 5 ) { // final arg must be -test
                    testing = true;
                }
            }
        }
    }

    private static void confirmArgsAreGood( String[] args ) {
        if ( args.length < 3 )
        {
            System.err.println( "Usage: CuratorClient curatorHost curatorPort "
                                        + "inputDir [outputDir] [-test]" );

            StringBuilder argUsage = new StringBuilder();
            for( String arg : args ) {
                argUsage.append( arg );
                argUsage.append( " " );
            }

            System.err.println( "You tried to use this: CuratorClient "
                                        + argUsage.toString() );

            System.exit( -1 );
        }
    }

    /**
     * Prints our interpretation of the command line arguments to the
     * standard out
     */
    public void printArgsInterpretation() {
        StringBuilder usage = new StringBuilder();
        usage.append("You launched the CuratorClient with the following options:");
        usage.append("\n");
        usage.append("\tCurator host: ");
        usage.append(host);
        usage.append("\n");
        usage.append("\tCurator port: ");
        usage.append(port);
        usage.append("\n");
        usage.append("\tInput directory: ");
        usage.append(inputDir.toString());
        usage.append("\n");
        usage.append("\tOutput directory: ");
        usage.append(outputDir.toString());
        usage.append("\n");
        usage.append("\tRun in testing mode? ");
        usage.append(testing ? "Yes." : "No.");
        usage.append("\n");

        System.out.println( usage.toString() );
    }

    /**
     * @return The host name specified by the command line args
     */
    public String getHost() {
        return host;
    }

    /**
     * @return The port number specified by the command line args
     */
    public int getPort() {
        return port;
    }

    /**
     * @return The input directory specified by the command line args
     */
    public File getInputDir() {
        return inputDir;
    }

    /**
     * @return True if the command line args specified to run in test mode
     */
    public boolean isTesting() {
        return testing;
    }

    /**
     * @return The output directory determined by the command line args
     */
    public File getOutputDir() {
        return outputDir;
    }

    private String host;
    private int port;
    private File inputDir;
    private boolean testing;
    private File outputDir;
}
