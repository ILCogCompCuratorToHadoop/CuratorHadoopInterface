package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import edu.illinois.cs.cogcomp.hadoopinterface.*;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.exceptions
        .BadInputDirectoryException;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.exceptions
        .EmptyInputException;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.input.DirectoryInputFormat;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.tests.RecordTesterMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * A job configuration object for a Hadoop job that interfaces with the Curator.
 * This configuration "knows" what mapper and reducer will be used, and it also
 * knows how to access the file system for this job. Because it handles setup for
 * the MapReduce job, <strong>it is conceptually quite different from, but related to,
 * the standard Hadoop job class.</strong>
 *
 * The configuration provides the following variables to the rest of the program
 * (available via the Configuration's <code>get()</code> method):
 *
 * <ul>
 *      <li>annotationMode: the tool that we're running over this batch of documents</li>
 *      <li>inputDirectory: the directory in which you will find a number of
 *          directories named with document hashes (those directories will contain
 *          the actual text files for annotation)</li>
 *      <li>outputDirectory: similar to inputDirectory</li>
 *      <li>libPath: the directory in which Hadoop nodes can find the
 *          Thrift libraries</li>
 * </ul>
 *
 * @author Tyler Young
 */
public class CuratorJob extends org.apache.hadoop.mapreduce.Job {

    /**
     * Constructs a CuratorJobConf object
     * @param args The command-line arguments passed to the tool
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
        numReduces = argParser.getNumReduces(); // TODO: Set this based on num input files!
        testing = argParser.isTesting();
        cleaning = argParser.isCleaning();

        configureJob();

        this.fs = FileSystem.get(conf);
        fsHandler = new FileSystemHandler( fs );

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

        if( isCleaning() ) {
            logger.log( "Set reducer to CuratorKillerReducer" );
            setReducerClass( CuratorKillerReducer.class );
        }
        else {
            logger.log( "Set reducer to CuratorReducer" );
            setReducerClass( CuratorReducer.class );
        }
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
     * Sets up the input and output directories for the Curator job in the Hadoop
     * Distributed File System (HDFS). This method resolves paths using the file
     * system object given to this object during its construction or created
     * based on the Curator job configuration it was given.
     * @throws IOException Possible IOException from file operations
     */
    public void setUpIODirectories() throws IOException {
        // Set up input/output directories. We will output the new annotation
        // to the same place in HDFS that we get the input from.
        FileInputFormat.addInputPath( this, getInputDirectory() );
        FileOutputFormat.setOutputPath( this, getMapReduceOutputDirectory() );

        // If the output path already exists, move it to another directory
        if( fsHandler.HDFSFileExists(outputDirectory) ) {
            Path destination =
                    new Path( outputDirectory.getParent().makeQualified(fs),
                              "old_jobs" );
            logger.logStatus("Trying to move old output directory");
            Path movedTo = fsHandler.moveFileOrDir( outputDirectory,
                                                    destination );
            logger.logStatus( "Moved the old contents of the output directory to "
                              + movedTo.toString() );

            // Now that the old contents of the directory are safely stored
            // away, we need to delete the directory so that MapReduce doesn't
            // throw a FileAlreadyExistsException
            fsHandler.delete( outputDirectory );
            fsHandler.mkdir( outputDirectory );
        }
    }

    /**
     * Confirms that the required directories exist (or don't exist, as the case
     * may be) and that we have valid inputs, and throws an IO Exception if we
     * do not. This method resolves paths using the file system object given to
     * this object during its construction.
     * @throws IOException Possible IOException from file operations
     */
    public void checkFileSystem( ) throws IOException {
        if( !isCleaning() ) {
            Path inputDirectory = getInputDirectory();

            if( fsHandler.HDFSFileExists( HadoopInterface.TMP_DIR ) ) {
                throw new IOException( "Temp directory "
                        + fs.makeQualified(HadoopInterface.TMP_DIR)
                        + " already exists.  Please remove it first.");
            }
            if( !fsHandler.HDFSFileExists( inputDirectory ) ) {
                throw new BadInputDirectoryException( "Input directory "
                        + fs.makeQualified( inputDirectory ) + " does not exist. "
                        + "Please create it in the Hadoop file system first." );
            }
            if( !fsHandler.isDir( inputDirectory ) ) {
                throw new BadInputDirectoryException( "The file "
                        + fs.makeQualified( inputDirectory )
                        + " is not a directory. Please check the documentation "
                        + "for this package for information on how to structure "
                        + "the input directory.");
            }

            List<Path> inputFiles =
                    fsHandler.getFilesOnlyInDirectory( inputDirectory );
            for( Path inputFile : inputFiles ) {
                if( fsHandler.getFileSizeInBytes( inputFile ) < 1 ) {
                    throw new EmptyInputException( "Input in document directory "
                            + fs.makeQualified( inputDirectory ) + " has no "
                            + "recognized input.  Please create input files in "
                            + "the Hadoop file system before starting this "
                            + "program." );
                }
            }
        }
    }

    /**
     * Removes the temp directory used by HadoopInterface
     * @throws IOException
     */
    public void cleanUpTempFiles() throws IOException {
        if( fsHandler.HDFSFileExists( HadoopInterface.TMP_DIR ) ) {
            fsHandler.delete( HadoopInterface.TMP_DIR );
        }
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
     * @return The directory that the MapReduce job will use for its
     *         miscellaneous output (not useful to the user)
     */
    private Path getMapReduceOutputDirectory() {
        return new Path( getOutputDirectory(), "mapreduce_out" );
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
     * Determines whether we are running in cleaning mode, where we don't
     * actually need to process input, we just need to shut down the Curator
     * running on each of the reducer nodes.
     * @return True if the command-line arguments told us to run in cleaning
     *         mode
     */
    public boolean isCleaning() {
        return cleaning;
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

        String inputDirectory = argParser.getDirectory();
        if( argParser.isTesting() ) {
            // Randomly assign input directory, since we will generate the input
            // ourselves.
            Random rng = new Random();
            inputDirectory = inputDirectory + "_"
                    + Integer.toString( rng.nextInt(1000) );
        }


        String outputDirectory;
        if( !argParser.getOutputDirectory().equals( "" ) ) {
            outputDirectory = argParser.getOutputDirectory();
        }
        else {
            outputDirectory = inputDirectory + "_out"
                    + System.currentTimeMillis();
        }

        // In cleaning mode, we ignore the user's output directory!
        if( argParser.isCleaning() ) {
            Random rng = new Random();
            outputDirectory = "/scratch/mapreduce_out_"
                    + Integer.toString( rng.nextInt(10000) );


        }

        String libPath = argParser.getLibPath();
        if( !libPath.equals( "" ) ) {
            config.set( "libPath", libPath );
        }

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
    private boolean cleaning;
    private AnnotationMode mode;
    private MessageLogger logger = HadoopInterface.logger;
    private FileSystem fs;

    private final FileSystemHandler fsHandler;
}
