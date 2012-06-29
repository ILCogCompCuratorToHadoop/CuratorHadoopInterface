package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

import edu.cs.illinois.cogcomp.hadoopinterface.*;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.input.DirectoryInputFormat;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.tests.RecordTesterMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Random;

/**
 * A job configuration object for a Hadoop job that interfaces with the Curator.
 * This configuration "knows" what mapper and reducer will be used, and it also
 * knows how to access the file system for this job.
 *
 * The configuration provides the following variables to the rest of the program
 * (available via the Configuration's `get()` method):
 *
 *      - annotationMode: the tool that we're running over this batch of documents
 *      - inputDirectory: the directory in which you will find a number of
 *          directories named with document hashes (those directories will contain
 *          the actual text files for annotation)
 *      - outputDirectory: similar to inputDirectory
 *
 * @author Tyler Young
 */
public class CuratorJob extends org.apache.hadoop.mapreduce.Job {
    /**
     * Constructs a CuratorJobConf object
     * @param args The command-line arguments passed ot the tool
     */
    public CuratorJob( String[] args )
            throws ClassNotFoundException, InterruptedException, IOException {
        super( getBaselineConfiguration( args ), "Curator runner");

        ArgumentParser argParser = new ArgumentParser(args);
        argParser.logResultsOfParsing();

        inputDirectory = new Path( getConfiguration().get("inputDirectory") );
        outputDirectory = new Path( getConfiguration().get("outputDirectory") );
        mode = AnnotationMode.fromString(
                getConfiguration().get("annotationMode") );
        numReduces = argParser.getNumReduces();
        testing = argParser.isTesting();

        configureJob();

        this.fs = FileSystem.get(conf);

        logger.logStatus( "Job configuration successfully created." );
    }

    /**
     * Sets up the fields inherited from JobConf in the standard way for a
     * Curator job.
     */
    private void configureJob() {
        setJobName( "Curator job" );

        setJarByClass( HadoopInterface.class );

        // Specify various job-specific parameters
        if( testing ) {
            logger.log( "Set mapper to RecordTesterMapper" );
            setMapperClass( RecordTesterMapper.class );
        }
        else {
            logger.log( "Set mapper to CuratorMapper" );
            setMapperClass( CuratorMapper.class );
        }

        setReducerClass( CuratorReducer.class );
        setNumReduceTasks( numReduces );

        // We split the input at the document directory level
        setInputFormatClass( DirectoryInputFormat.class );

        // We output in (Text, Record) pairs
        setMapOutputKeyClass( Text.class );
        setMapOutputValueClass( HadoopRecord.class );
        setOutputKeyClass( Text.class );
        setOutputValueClass( HadoopRecord.class );

        // Turn off speculative execution, because DFS doesn't handle
        // multiple writers to the same file.
        setSpeculativeExecution( false );
    }

    /**
     * Submits this job to the Hadoop cluster, then polls for progress until
     * the job is complete
     * @return true if job completed successfully, false otherwise
     * @throws ClassNotFoundException . . . if the Mapper or Reducer are not
     *                               found.
     * @throws IOException thrown if the communication with the JobTracker is lost
     * @throws InterruptedException
     */
    public boolean start()
            throws ClassNotFoundException, IOException, InterruptedException {
        // Submit the job, then poll for progress until the job is complete
        return waitForCompletion( true );
    }

    /**
     * @return The number of reduce operations to be used for this Hadoop job
     */
    public int getNumReduces() {
        return numReduces;
    }

    /**
     * @return The directory (on the Hadoop Distributed File System) from which
     *         this Hadoop job will read its input
     */
    public Path getInputDirectory() {
        return inputDirectory;
    }

    /**
     * @return The directory (on the Hadoop Distributed File System) to which
     *         this Hadoop job will write its output
     */
    public Path getOutputDirectory() {
        return outputDirectory;
    }

    /**
     * @return The annotation mode to be used on this Hadoop job
     */
    public AnnotationMode getMode() {
        return mode;
    }

    /**
     * @return The Hadoop file system object
     */
    public FileSystem getFileSystem() {
        return fs;
    }

    /**
     * @return TRUE if the command-line arguments told us to run in test mode
     */
    public boolean isTesting() {
        return testing;
    }

    /**
     * After calling the superclass constructor, the configuration can't be
     * modified. Thus, we get build the configuration here before passing it to
     * the superclass constructor.
     * @param args The command line arguments, from which we take the annotation
     *             mode and I/O directories.
     * @return A configured Configuration option.
     */
    private static Configuration getBaselineConfiguration( String[] args ) {
        Configuration config = new Configuration();

        ArgumentParser argParser = new ArgumentParser(args);

        Random rng = new Random();
        String inputDirectory = argParser.getDirectory();
        if( argParser.isTesting() ) {
            inputDirectory = inputDirectory + "_"
                    + Integer.toString( rng.nextInt(1000) );
        }
        String outputDirectory = inputDirectory + "_out"
                + System.currentTimeMillis();
        AnnotationMode mode = argParser.getMode();

        config.set( "annotationMode", mode.toString() );
        config.set( "inputDirectory", inputDirectory );
        config.set( "outputDirectory", outputDirectory );
        return config;
    }

    private int numReduces;
    private Path inputDirectory;
    private Path outputDirectory;
    private boolean testing;
    private AnnotationMode mode;
    private MessageLogger logger = HadoopInterface.logger;
    private FileSystem fs;

}
