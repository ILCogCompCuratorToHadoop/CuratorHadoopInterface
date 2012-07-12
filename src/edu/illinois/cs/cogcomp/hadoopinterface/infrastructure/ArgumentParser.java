package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import edu.illinois.cs.cogcomp.hadoopinterface.HadoopInterface;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.exceptions.BadCommandLineUsageException;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.exceptions.IllegalModeException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class is used to parse the command-line arguments used by the Hadoop
 * interface. It hides the "business logic" of the actual way that parameters
 * get passed.
 *
 * Note that the "mode" stored by this object is guaranteed to be valid, but
 * the directory is not---we do not check here to ensure that the directory
 * actually exists and has data.
 *
 * @author Tyler Young
 */
public class ArgumentParser {
    /**
     * Constructs a parser object
     * @param args Command line arguments received by the program's main method.
     *             Contains at least the input directory in HDFS whose documents
     *             will be annotated and the annotation mode to be used.
     * @throws BadCommandLineUsageException Indicates that what we got from the
     *                                      command line was not understood.
     */
    public ArgumentParser(String[] args)
            throws BadCommandLineUsageException {
        testing = false;

        if( args.length < 2 ) {
            String errorMsg = "Parameter usage: \n\t\t"
                    + "<document directory> <mode>\n\tor:\n\t\t"
                    + "-d <document directory> -m <mode> "
                    + "[-maps <number of maps>] [-reduces <number of reduces>] "
                    + "[-test]";
            HadoopInterface.logger.logError( errorMsg );
            System.err.println( errorMsg );
            ToolRunner.printGenericCommandUsage(System.err);

            throw new BadCommandLineUsageException( "Wrong number of parameters "
                    + "from command line. " + errorMsg );
        }

        // "Classic" usage: just directory and mode
        if( args.length == 2 ) {
            // Try parsing args[1] as a mode
            try {
                mode = AnnotationMode.fromString( args[1] );
                directory = args[0];
            } catch( IllegalModeException e ) {
                try {
                    // Try parsing args[0] as a mode
                    mode = AnnotationMode.fromString( args[0] );
                    directory = args[1];
                } catch ( IllegalModeException f ) {
                    // Die if neither args 0 nor 1 make sense as a mode
                    throw new BadCommandLineUsageException(
                            "Couldn't make sense of either " + args[0] + " or "
                            + args[1] + " as an annotation mode.");
                }
            }
        }

        // More robust usage, specifying which parms are which using CL flags
        else {
            for( int i = 0; i < args.length; ++i ) {
                // Allow either -d ("directory") or -i ("input")
                if( args[i].equals("-d") || args[i].equals("-i") ) {
                    directory = args[ ++i ];
                }
                else if( args[i].equals("-m") ) {
                    mode = AnnotationMode.fromString( args[++i] );
                }
                else if( args[i].equals("-maps") ) {
                    numMaps = new Integer( args[++i] );
                }
                else if( args[i].equals("-reduces") ) {
                    numReduces = new Integer( args[++i] );
                }
                else if( args[i].equals("-test") ) {
                    testing = true;
                }
            }

            if( mode == null ) {
                throw new BadCommandLineUsageException( "No mode specified. "
                        + "Since you're using the robust means of specifying "
                        + "parameters, add -m < some mode > to your argument "
                        + "list.");
            }
            if( directory == null ) {
                throw new BadCommandLineUsageException( "No directory specified."
                        + "Since you're using the robust means of specifying "
                        + "parameters, add -d < some directory > to your "
                        + "argument list.");
            }
            if( numReduces != null && numReduces < 1 ) {
                throw new IllegalArgumentException( "Number of reduce operations "
                        + "must be 1 or more. You specified "
                        + Integer.toString( numReduces ) + "." );
            }
            if( numMaps != null && numMaps < 1 ) {
                throw new IllegalArgumentException( "Number of map operations "
                        + "must be 1 or more. You specified "
                        + Integer.toString( numReduces ) + "." );
            }
        }
    }

    public void logResultsOfParsing() {
        String alertOfTesting = "";
        if( isTesting() ) {
            alertOfTesting = "\n\tJob started in test mode.";
        }
        HadoopInterface.logger.logStatus( "Parsed command-line arguments. "
                + "Using input directory " + directory + " and annotation mode "
                + mode.toString() + ", with " + getNumReduces() + " reduce ops."
                + alertOfTesting );
    }

    /**
     * @return The directory string from the command line parameters
     */
    public String getDirectory() {
        return directory;
    }

    /**
     * @return The Path object parsed from the command line parameters' input
     *         directory
     */
    public Path getPath() {
        return new Path( directory );
    }

    /**
     * @return The number of maps to be used in the job, either parsed from the
     *         command line or the default
     */
    public int getNumMaps()
    {
        if( numMaps == null ) {
            numMaps = new Integer(10);
        }
        return numMaps.intValue();
    }

    /**
     * @return The number of reduces to be used in the job, either parsed from the
     *         command line or the default
     */
    public int getNumReduces()
    {
        if( numReduces == null  ) {
            numReduces = new Integer(10);
        }

        return numReduces.intValue();
    }

    public boolean isTesting() {
        return testing;
    }

    /**
     * @return The annotation mode parsed from the command line parameters

     */
    public AnnotationMode getMode() {
        return mode;
    }

    private AnnotationMode mode;
    private String directory;
    private Integer numMaps;
    private Integer numReduces;
    private boolean testing = false;
}
