/**
* This program presents an interface for passing large batch jobs from the
* Curator to a Hadoop cluster.
*
* TODO: Document usage
*
* @author Tyler A. Young
* @author Lisa Bao
*/

package edu.cs.illinois.cogcomp.hadoopinterface;

// TODO: Check that we actually use all these... (We probably will not!)

import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HadoopInterface extends Configured implements Tool {

    /**
     * Dummy main method for launching the tool as a stand-alone command.
     * Structure modeled after Hadoop's examples.
     *
     * @param argv The command line arguments
     * @example Call this tool from the command line like this:
     *          ./hadoop jar CuratorHadoopInterface.jar document_directory_in_hdfs mode
     * @throws Exception For whatever reason, the Tool interface demands that
     *                   this throw a generic Exception. How helpfully vague.
     */
    public static void main( String[] argv ) throws Exception {
        System.exit(ToolRunner.run(null, new HadoopInterface(), argv));
    }

    /**
     * Parses arguments and then runs a map/reduce job.
     *
     * @param args String arguments from the command line. Must contain a valid
     *             directory in the Hadoop Distributed File System (HDFS), as well
     *             as a valid mode.
     *
     *             The directory should contain all documents to be annotated,
     *             complete with their dependencies for this job type.
     * @return Zero if we ran error-free, non-zero otherwise.
     */
    public int run( String[] args ) throws Exception
    {
        // Set up the job configuration that we will send to Hadoop
        final CuratorJobConf jobConf = new CuratorJobConf( getConf(), getClass(),
                                                           args );
        FileSystemHandler handler = new FileSystemHandler( jobConf );

        try {
            handler.setUpIODirectories();
            handler.checkFileSystem();

            // Start a map/reduce job -- runJob(jobConf) takes the job
            // configuration we just set up and distributes it to Hadoop nodes
            logger.log( "Starting MapReduce job" );
            final long startTime = System.currentTimeMillis();
            JobClient.runJob(jobConf);
            final double duration = ( System.currentTimeMillis() - startTime )
                                    / 1000.0;
            logger.log( "Job finished in " + duration + " seconds" );

            readOutput( jobConf );
        }
        finally {
            handler.cleanUpTempFiles();
        }

        return 0;
    }

    /**
     * Reads the output from the Reduce operation
     * @param jobConf The job configuration for this Hadoop job
     * @throws IOException
     */
    private void readOutput( CuratorJobConf jobConf ) throws IOException {
        // Read outputs
        Path inFile = new Path( jobConf.getOutputDirectory(), "reduce-out" );
        SequenceFile.Reader reader =
                new SequenceFile.Reader( jobConf.getFileSystem(),
                                         inFile, jobConf);
        reader.next("Lorem ipsum"); // TODO: Oh yeah... we need real output.
        reader.close();
    }

    // Temp directory for input/output
    static public final Path TMP_DIR = new Path(
            HadoopInterface.class.getSimpleName()+ "_TMP" );

    // A tool to standardize error logging. Prints the log to standard out.
    static public final MessageLogger logger = new MessageLogger( true );
}
