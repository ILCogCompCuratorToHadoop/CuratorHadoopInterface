package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import edu.illinois.cs.cogcomp.hadoopinterface.HadoopInterface;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.exceptions.BadCommandLineUsageException;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.exceptions.IllegalModeException;
import org.apache.hadoop.fs.Path;

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
        lib = "";

        if( args.length < 2 ) {
            StringBuilder err = new StringBuilder();
            err.append( "Parameter usage: \n\t\t" );
            err.append( "<document directory> <mode>\n\tor:\n\t\t" );
            err.append( "-d <document directory> -m <mode> [-out <output " );
            err.append( " directory>] [-maps <number of maps>] [-reduces " );
            err.append( "<number of reduces>] [-lib /path/to/lib/] [-test]\n" );
            err.append( "You tried to pass these parameters:\n\t" );

            for( String arg : args ) {
                err.append( arg );
                err.append( ' ' );
            }

            HadoopInterface.logger.logError( err.toString() );
            System.err.println( err );
            //ToolRunner.printGenericCommandUsage(System.err);

            throw new BadCommandLineUsageException( "Wrong number of parameters "
                    + "from command line. " + err );
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

        // More robust usage, specifying which params are which using CL flags
        else {
            for( int i = 0; i < args.length; ++i ) {
                // Allow either -d ("directory") or -i ("input")
                if( args[i].equals("-d") || args[i].equals("-i") ) {
                    directory = args[ ++i ];
                }
                else if( args[i].equals("-out") || args[i].equals("-o") ) {
                    outputDirectory = args[ ++i ];
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
                else if( args[i].equals("-cleanup") ) {
                    cleaning = true;
                }
                else if( args[i].equals("-lib") ) {
                    lib = args[ ++i ];
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
        StringBuilder usage = new StringBuilder();
        usage.append("You launched the HadoopInterface with the following options:");
        usage.append("\n");
        usage.append("\tInput directory: ");
        usage.append(directory);
        usage.append("\n");
        usage.append("\tAnnotation mode: ");
        usage.append(mode.toString());
        usage.append("\n");
        usage.append("\tNumber of reduces: ");
        usage.append(numReduces);
        usage.append("\n");

        usage.append("\tRun in testing mode? ");
        usage.append(testing ? "Yes." : "No.");
        usage.append("\n");

        HadoopInterface.logger.logStatus( usage.toString() );
    }

    /**
     * @return The directory string from the command line parameters. Should
     *         indicate where to find the <em>input</em> serialized records,
     *         which we will send to the Reducer and eventually the
     *         HadoopCuratorClient.
     */
    public String getDirectory() {
        return directory;
    }

    /**
     * @return The location to which we should write the MapReduce job's output.
     *         <strong>Note</strong>: If not specified by the user, this will be
     *         the empty string.
     */
    public String getOutputDirectory() {
        if( outputDirectory == null ) {
            return "";
        }
        return outputDirectory;
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

    public boolean isCleaning() {
        return cleaning;
    }

    /**
     * @return The directory (local to each Hadoop node) which should be used
     *         as the library during a MapReduce job. Should contain Thrift
     *         libraries.
     */
    public String getLibPath() {
        return lib;
    }

    /**
     * @return The annotation mode parsed from the command line parameters

     */
    public AnnotationMode getMode() {
        return mode;
    }

    private AnnotationMode mode;

    private String directory;

    private String outputDirectory;
    private String lib;
    private Integer numMaps;
    private Integer numReduces;
    private boolean testing = false;
    private boolean cleaning = false;
}
