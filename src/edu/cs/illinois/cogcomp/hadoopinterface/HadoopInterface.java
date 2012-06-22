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

package edu.cs.illinois.cogcomp.hadoopinterface;

import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.CuratorJob;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.FileSystemHandler;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.MessageLogger;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.tests.DummyInputCreator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;

public class HadoopInterface {

    /**
     * Dummy main method for launching the tool as a stand-alone command.
     * Structure modeled after Hadoop's examples.
     *
     * @param argv String arguments from the command line. Must contain a valid
     *             directory in the Hadoop Distributed File System (HDFS), as well
     *             as a valid mode.
     *
     *             The directory should contain all documents to be annotated,
     *             complete with their dependencies for this job type.
     */
    public static void main( String[] argv ) throws IOException, ClassNotFoundException, InterruptedException {
        logger.logStatus( "Setting up job.\n\n" );

        // Set up the job configuration that we will send to Hadoop
        final CuratorJob job = new CuratorJob( argv );
        FileSystemHandler handler = new FileSystemHandler( job );

        if( job.isTesting() ) {
            DummyInputCreator.generateDocumentDirectory( job.getInputDirectory(),
                                                         job.getFileSystem() );
        }

        try {
            logger.logStatus( "Setting up IO directories" );
            handler.setUpIODirectories();
            logger.logStatus( "Checking file system" );
            handler.checkFileSystem();

            // Start a map/reduce job -- runJob(jobConf) takes the job
            // configuration we just set up and distributes it to Hadoop nodes
            logger.logStatus( "Starting MapReduce job" );
            final long startTime = System.currentTimeMillis();
            job.start();
            final double duration = ( System.currentTimeMillis() - startTime )
                                    / 1000.0;
            logger.log( "Job finished in " + duration + " seconds" );
        } finally {
            handler.cleanUpTempFiles();
        }
    }

    /**
     * Reads the output from the Reduce operation
     *
     * @TODO: Is this even necessary? (Depends on what class is responsible for moving output to the DB)
     * @param job The job configuration for this Hadoop job
     * @throws IOException If file not found
     */
    private static void readOutput(CuratorJob job) throws IOException {
        // Read outputs
        Path inFile = new Path( job.getOutputDirectory(), "reduce-out" );
        SequenceFile.Reader reader =
                new SequenceFile.Reader( job.getFileSystem(),
                                         inFile, job.getConfiguration() );
        // TODO: Oh yeah... we need real output. It's not clear how this works...
        reader.next("Lorem ipsum");
        reader.close();
    }

    // Temp directory for input/output
    static public final Path TMP_DIR = new Path(
            HadoopInterface.class.getSimpleName()+ "_TMP" );

    // A tool to standardize error logging. Prints the log to standard out.
    static public final MessageLogger logger = new MessageLogger( true );
}
