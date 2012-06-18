package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

import edu.cs.illinois.cogcomp.hadoopinterface.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * A job configuration object for a Hadoop job that interfaces with the Curator.
 * This configuration "knows" what mapper and reducer will be used, and it also
 * knows how to access the file system for this job.
 * @author Tyler Young
 */
public class CuratorJobConf extends JobConf {

    /**
     * Constructs a CuratorJobConf object
     * @param conf The job's Configuration (available to objects that implement
     *             Hadoop's Tool interface via the getConf() method)
     * @param jobClass The job's Class (available to objects that implement
     *                 Hadoop's Tool interface via the getClass() method)
     * @param args The command-line arguments passed ot the tool
     */
    public CuratorJobConf( Configuration conf, Class jobClass,
                           String[] args) throws IOException {
        super( conf, jobClass );

        ArgumentParser argParser = new ArgumentParser(args);

        inputDirectory = argParser.getPath();
        mode = argParser.getMode();
        numMaps = argParser.getNumMaps();
        numReduces = argParser.getNumReduces();

        setInheritedFields();

        this.fs = FileSystem.get(this);
    }

    /**
     * Sets up the job configuration object, which specifies (among other
     * things) the map and reduce classes for Hadoop to use.
     *
     * @param conf The job's Configuration (available to objects that implement
     *             Hadoop's Tool interface via the getConf() method)
     * @param jobClass The job's Class (available to objects that implement
     *                 Hadoop's Tool interface via the getClass() method)
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
    public CuratorJobConf( Configuration conf, Class jobClass, int numMaps,
                           int numReduces, Path inputDirectory,
                           AnnotationMode mode) throws IOException {
        super( conf, jobClass );

        this.numMaps = numMaps;
        this.numReduces = numReduces;
        this.inputDirectory = inputDirectory;
        this.mode = mode;

        setInheritedFields();
    }


    /**
     * Sets up the fields inherited from JobConf in the standard way for a
     * Curator job.
     */
    private void setInheritedFields() {
        // Call all our inherited methods
        setJobName(HadoopInterface.class.getSimpleName());

        set( "annotationMode", mode.toString() );

        setInputFormat( SequenceFileInputFormat.class );

        // Output keys (the hashes that identify documents) will be string objects
        setOutputKeyClass( ObjectWritable.class );
        setOutputValueClass( ObjectWritable.class );
        setOutputFormat( SequenceFileOutputFormat.class );

        setMapperClass( CuratorMapper.class );
        setNumMapTasks( numMaps );

        setReducerClass( CuratorReducer.class );
        setNumReduceTasks( numReduces );

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
    private Path TMP_DIR = HadoopInterface.TMP_DIR;
    private MessageLogger logger = HadoopInterface.logger;
    private FileSystem fs;
}
