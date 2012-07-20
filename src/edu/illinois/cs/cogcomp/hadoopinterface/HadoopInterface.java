/**
* This program presents an interface for passing large batch jobs from the
* Curator to a Hadoop cluster.
*
* @example Call this tool from the command line like this:
*          ./hadoop jar CuratorHadoopInterface.jar <document_directory_in_hdfs> <mode>
*
* @author Tyler A. Young
* @author Lisa Bao
*/

package edu.illinois.cs.cogcomp.hadoopinterface;

import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.CuratorJob;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.MessageLogger;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.tests.DummyInputCreator;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.tests.FileSystemHandlerTest;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * A program that allows Hadoop to interface with Curator clients running on
 * the Hadoop nodes.
 *
 * @example (After compiling this program to a JAR)
 *          $ ./bin/hadoop jar CuratorHadoopInterface.jar -d input_dir -m parse
 * @example (After compiling this program to a JAR)
 *          $ ./bin/hadoop jar CuratorHadoopInterface.jar -d input_dir -m verb_srl -test
 * @precondition (Required by the CuratorReducer)
 *               The Hadoop node running this reduce() operation has a complete,
 *               compiled Curator distribution located (on the local file system)
 *               at `~/curator/dist/`.
 */
public class HadoopInterface {

    /**
     * Main method for launching the tool as a stand-alone command.
     * Structure modeled after Hadoop's examples.
     *
     * @param argv String arguments from the command line. Must contain a valid
     *             directory in the Hadoop Distributed File System (HDFS), as well
     *             as a valid mode.
     *
     *             The directory should contain all documents to be annotated,
     *             complete with their dependencies for this job type.
     */
    public static void main( String[] argv )
            throws IOException, ClassNotFoundException, InterruptedException {
        logger.beginWritingToDisk();
        logger.logLocation();
        logger.logStatus( "Setting up job.\n\n" );

        // Set up the job configuration that we will send to Hadoop
        final CuratorJob job = new CuratorJob( argv );

        if( job.isTesting() ) {
            DummyInputCreator.generateDocumentDirectories(job.getInputDirectory(),
                                                          job.getFileSystem() );
        }

        try {
            logger.logStatus( "Setting up IO directories" );
            logger.logStatus( "We will write our output to "
                                      + job.getOutputDirectory() );
            job.setUpIODirectories();
            logger.logStatus( "Checking file system" );
            job.checkFileSystem();

            // Start a map/reduce job -- runJob(jobConf) takes the job
            // configuration we just set up and distributes it to Hadoop nodes
            logger.logStatus( "Starting MapReduce job" );
            final long startTime = System.currentTimeMillis();
            logger.delayWritingToDisk();
            job.start();
            final double duration = ( System.currentTimeMillis() - startTime )
                                    / 1000.0;
            logger.log( "Job finished in " + duration + " seconds" );
        } finally {
            logger.continueWritingToDisk();
            job.cleanUpTempFiles();
        }

        if( job.isTesting() ) {
            logger.logStatus("Beginning tests of File System Handler.");
            FileSystemHandlerTest.main(new String[0]);
        }
    }

    // Temp directory for input/output
    static public final Path TMP_DIR = new Path(
            HadoopInterface.class.getSimpleName()+ "_TMP" );

    // A tool to standardize error logging. Prints the log to standard out.
    static public final MessageLogger logger = new MessageLogger( true );
}
