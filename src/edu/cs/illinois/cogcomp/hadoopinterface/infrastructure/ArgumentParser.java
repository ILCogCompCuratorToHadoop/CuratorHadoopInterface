package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

import edu.cs.illinois.cogcomp.hadoopinterface.HadoopInterface;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import java.security.InvalidParameterException;

/**
 * This class is used to parse the command-line arguments used by the Hadoop
 * interface. It hides the "business logic" of the actual way that parameters
 * get passed.
 *
 * Note that the "mode" stored by this object is guaranteed to be valid, but
 * the directory is not---we do not check here to ensure that the directory
 * actually exists and has data.
 *
 * @TODO: Allow passing in number of maps and reductions?
 *
 * @author Tyler Young
 */
public class ArgumentParser {
    private String[] the_args;

    /**
     * Constructs a parser object
     * @param args Command line arguments received by the program's main method.
     *             Contains at least the input directory in HDFS whose documents
     *             will be annotated and the annotation mode to be used.
     * @throws InvalidParameterException
     */
    public ArgumentParser(String[] args) throws InvalidParameterException
    {
        the_args = args;

        if( args.length < 2 ) {
            String errorMsg = new String( "Usage: " + getClass().getName() +
                    "<document directory> <mode>" );
            HadoopInterface.logger.logError( errorMsg );
            System.err.println( errorMsg );
            ToolRunner.printGenericCommandUsage(System.err);

            throw new InvalidParameterException( "Wrong command-line usage.  "
                    + errorMsg );
        }

        // TODO: Check HDFS for the indicated directory, validate the mode

        // TODO: Make this more robust (allow parms in an arbitrary order using
        //       flags like -d (for "document") and -m (for "mode")
    }

    /**
     * Parse directory from the command line parameters
     *
     * @TODO: Document!
     * @return
     */
    public String getDirectory() {
        // Parse directory

        // TODO: Fill this method

        return new String("");
    }
    
    public Path getPath() {
        return new Path( getDirectory() );
    }

    /**
     * Parse mode from the command line parameters
     *
     * @TODO: Document!
     * @return
     */
    public AnnotationMode getMode() {
        // TODO: Fill this method

        return AnnotationMode.POS;
    }
}
