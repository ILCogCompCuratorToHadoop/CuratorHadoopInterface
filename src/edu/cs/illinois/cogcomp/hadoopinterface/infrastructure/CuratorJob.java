package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

import edu.cs.illinois.cogcomp.hadoopinterface.CuratorMapper;
import edu.cs.illinois.cogcomp.hadoopinterface.CuratorReducer;
import edu.cs.illinois.cogcomp.hadoopinterface.HadoopInterface;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;

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
    public CuratorJob( Configuration conf, String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        super( conf, "Curator runner");

        ArgumentParser argParser = new ArgumentParser(args);

        inputDirectory = argParser.getPath();
        mode = argParser.getMode();
        numMaps = argParser.getNumMaps();
        numReduces = argParser.getNumReduces();

        config = conf;

        configureJob();

        this.fs = FileSystem.get(conf);

        logger.logStatus( "Job configuration created." );
    }

    /**
     * Sets up the job configuration object, which specifies (among other
     * things) the map and reduce classes for Hadoop to use.
     *
     * @param conf The job's Configuration (available to objects that implement
     *             Hadoop's Tool interface via the getConf() method)
     * @param numMaps The number of map tasks for Hadoop to use.
     * @param numReduces The number of reduce tasks for Hadoop to use. The
     *                   JobConf documentation says that the right number of
     *                   reduces seems to be 0.95 or 1.75 multiplied by
     *                   (num. nodes * mapreduce.tasktracker.reduce.tasks.maximum).
     *                   Increasing the number of reduces increases the framework
     *                   overhead, but increases load balancing and lowers the
     *                   cost of failures.
     * @param mode The annotation mode that will be used on the document
     *             collection (i.e., the tool that will be called on each
     *             document).
     */
    public CuratorJob( Configuration conf, int numMaps,
                       int numReduces, Path inputDirectory,
                       AnnotationMode mode)
            throws IOException, ClassNotFoundException, InterruptedException {
        super( conf, "Curator runner" );

        this.config = conf;
        this.numMaps = numMaps;
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
    private void configureJob()
            throws ClassNotFoundException, IOException, InterruptedException {
        config.set( "annotationMode", mode.toString() );

        setJobName(HadoopInterface.class.getSimpleName());

        setJarByClass(CuratorJob.class);

        // Specify various job-specific parameters
        setJobName("myjob");

        /* TODO: This isn't present in the API. How do we set it?
        // At present, we output to the same place we take input from
        setInputPath( inputDirectory );
        setOutputPath( inputDirectory );*/

        setMapperClass( CuratorMapper.class );
        setReducerClass( CuratorReducer.class );
        setNumReduceTasks( numReduces );

        // Submit the job, then poll for progress until the job is complete
        waitForCompletion( true );

        // We output in (Text, Record) pairs
        setOutputKeyClass( Text.class );
        setOutputValueClass( Record.class );

        // Turn off speculative execution, because DFS doesn't handle
        // multiple writers to the same file.
        setSpeculativeExecution( false );
    }

    /**
     * @return The number of map operations to be used for this Hadoop job
     */
    public int getNumMaps() {
        return numMaps;
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
        return inputDirectory;
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

    private int numMaps;
    private int numReduces;
    private Path inputDirectory;
    private AnnotationMode mode;
    private Configuration config;
    private MessageLogger logger = HadoopInterface.logger;
    private FileSystem fs;
}
