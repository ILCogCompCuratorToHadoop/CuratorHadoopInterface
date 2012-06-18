package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

import edu.cs.illinois.cogcomp.hadoopinterface.CuratorMapper;
import edu.cs.illinois.cogcomp.hadoopinterface.CuratorReducer;
import edu.cs.illinois.cogcomp.hadoopinterface.HadoopInterface;
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
    public CuratorJobConf( Configuration conf, Class jobClass, String[] args)
            throws IOException {
        super( conf, jobClass );

        ArgumentParser argParser = new ArgumentParser(args);

        inputDirectory = argParser.getPath();
        mode = argParser.getMode();

        numMaps = 10;
        numReduces = 10;

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


    private void setInheritedFields() throws IOException {
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

    public int getNumMaps() {
        return numMaps;
    }

    public int getNumReduces() {
        return numReduces;
    }

    public Path getInputDirectory() {
        return inputDirectory;
    }

    public Path getOutputDirectory() {
        return inputDirectory;
    }

    public AnnotationMode getMode() {
        return mode;
    }

    public FileSystem getFileSystem() {
        return fs;
    }

    private int numMaps;
    private int numReduces;
    private Path inputDirectory;
    private AnnotationMode mode;
    private Path TMP_DIR = HadoopInterface.TMP_DIR;
    private ErrorLogger logger = HadoopInterface.logger;
    private FileSystem fs;
}
