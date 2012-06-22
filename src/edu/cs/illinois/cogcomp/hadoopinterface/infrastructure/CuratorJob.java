package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

import edu.cs.illinois.cogcomp.hadoopinterface.CuratorMapper;
import edu.cs.illinois.cogcomp.hadoopinterface.CuratorReducer;
import edu.cs.illinois.cogcomp.hadoopinterface.HadoopInterface;
import edu.cs.illinois.cogcomp.hadoopinterface.TestMapper;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.input.DirectoryInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * A job configuration object for a Hadoop job that interfaces with the Curator.
 * This configuration "knows" what mapper and reducer will be used, and it also
 * knows how to access the file system for this job.
 *
 * @author Tyler Young
 */
public class CuratorJob extends org.apache.hadoop.mapreduce.Job {
    /**
     * Constructs a CuratorJobConf object
     * @param conf The job's Configuration (available to objects that implement
     *             Hadoop's Tool interface via the getConf() method)
     * @param args The command-line arguments passed ot the tool
     */
    public CuratorJob( Configuration conf, String[] args )
            throws IOException, ClassNotFoundException, InterruptedException {
        super( conf, "Curator runner");

        ArgumentParser argParser = new ArgumentParser(args);

        inputDirectory = argParser.getPath();
        outputDirectory = new Path( argParser.getDirectory() + "_out"
                                    + System.currentTimeMillis() );
        mode = argParser.getMode();
        numReduces = argParser.getNumReduces();
        testing = argParser.isTesting();

        config = conf;

        configureJob();

        this.fs = FileSystem.get(conf);

        logger.logStatus( "Job configuration successfully created." );
    }

    /**
     * Sets up the job configuration object, which specifies (among other
     * things) the map and reduce classes for Hadoop to use.
     *
     * @param conf The job's Configuration (available to objects that implement
     *             Hadoop's Tool interface via the getConf() method)
     * @param numReduces The number of reduce tasks for Hadoop to use. The
     *                   JobConf documentation says that the right number of
     *                   reduces seems to be 0.95 or 1.75 multiplied by
     *                   (num. nodes * mapreduce.tasktracker.reduce.tasks.maximum).
     *                   Increasing the number of reduces increases the framework
     *                   overhead, but increases load balancing and lowers the
     *                   cost of failures.
     * @param
     * @param mode The annotation mode that will be used on the document
     *             collection (i.e., the tool that will be called on each
     *             document).
     */
    public CuratorJob( Configuration conf, int numReduces, Path inputDirectory,
                       Path outputDirectory, AnnotationMode mode)
            throws IOException, ClassNotFoundException, InterruptedException {
        super( conf, "Curator runner" );

        this.config = conf;
        this.numReduces = numReduces;
        this.inputDirectory = inputDirectory;
        this.mode = mode;

        configureJob();

        this.fs = FileSystem.get(conf);

        logger.logStatus( "Job configuration created." );
    }


    /**
     * Sets up the fields inherited from JobConf in the standard way for a
     * Curator job.
     */
    private void configureJob() throws IOException {
        config.set( "annotationMode", mode.toString() );
        config.set( "inputDirectory", inputDirectory.toString() );

        setJobName( "Curator job" );

        setJarByClass( HadoopInterface.class );

        // Specify various job-specific parameters
        if( testing ) {
            setMapperClass( TestMapper.class );
        }
        else {
            setMapperClass( CuratorMapper.class );
        }

        setReducerClass( CuratorReducer.class );
        setNumReduceTasks( numReduces );

        // We split the input at the document directory level
        setInputFormatClass( DirectoryInputFormat.class );

        // We output in (Text, Record) pairs
        setMapOutputKeyClass( Text.class );
        setMapOutputValueClass( Record.class );
        setOutputKeyClass( Text.class );
        setOutputValueClass( Record.class );

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

    private int numReduces;
    private Path inputDirectory;
    private Path outputDirectory;
    private boolean testing;
    private AnnotationMode mode;
    private Configuration config;
    private MessageLogger logger = HadoopInterface.logger;
    private FileSystem fs;

}
