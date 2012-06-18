package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

import edu.cs.illinois.cogcomp.hadoopinterface.HadoopInterface;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.exceptions.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

import java.io.IOException;

/**
 * A class to handle all the filesystem interactions in the Hadoop interface.
 *
 * @author Tyler Young
 */
public class FileSystemHandler {
    /**
     * Constructs a file system handler.
     * @param jobConf The job configuration for this Hadoop job
     */
    public FileSystemHandler( CuratorJobConf jobConf ) {
        this.jobConf = jobConf;
        fs = jobConf.getFileSystem();
    }

    /**
     * Sets up the input and output directories for this job in the Hadoop
     * Distributed File System (HDFS).
     * @throws IOException Possible IOException from file operations
     */
    public void setUpIODirectories() throws IOException {
        // Set up input/output directories. We will output the new annotation
        // to the same place in HDFS that we get the input from.
        FileInputFormat.setInputPaths(jobConf, jobConf.getInputDirectory());
        FileOutputFormat.setOutputPath(jobConf, jobConf.getOutputDirectory());

        // Ensure that directory structure is good
        checkFileSystem( );

        createInputFilesForMaps();
    }

    /**
     * Confirms that the required directories exist (or don't exist, as the case
     * may be) and that we have valid inputs, and throws an IO Exception if we
     * do not.
     * @throws IOException Possible IOException from file operations
     */
    public void checkFileSystem( ) throws IOException {
        Path inputDirectory = jobConf.getInputDirectory();

        if( fs.exists(HadoopInterface.TMP_DIR) ) {
            throw new IOException( "Temp directory "
                    + fs.makeQualified(HadoopInterface.TMP_DIR)
                    + " already exists.  Please remove it first.");
        }
        if( !fs.exists( inputDirectory ) ) {
            throw new BadInputDirectoryException( "Input directory "
                    + fs.makeQualified( inputDirectory ) + " does not exist. "
                    + "Please create it in the Hadoop file system first." );
        }
        // TODO: Check that we actually have input in that directory
        if( false /* files don't actually exist */ ) {
            throw new EmptyInputException( "Input directory "
                    + fs.makeQualified( inputDirectory ) + " has no recognized "
                    + "input.  Please create input files in the Hadoop file "
                    + "system before starting this program.");
        }
    }

    /**
     * Sets up the input files for the Map operations
     * @throws IOException Possible IOException from file operations
     */
    public void createInputFilesForMaps() throws IOException {
        // Generate an input file for each map task
        // TODO: Make this actually appropriate for our task (currently taken
        //       from the Hadoop example for approximating Pi)
        for (int i = 0; i < jobConf.getNumMaps(); ++i)
        {
            final Path file = new Path( jobConf.getInputDirectory(), "part" + i );
            final LongWritable offset = new LongWritable(i);
            final LongWritable size = new LongWritable(1);
            final SequenceFile.Writer writer = SequenceFile.createWriter(
                    fs, jobConf, file, LongWritable.class, LongWritable.class,
                    SequenceFile.CompressionType.NONE);
            try {
                writer.append( offset, size );
            } finally {
                writer.close();
            }

            HadoopInterface.logger.log(new String("Wrote input for Map #" + i));
        }
    }

    public void cleanUpTempFiles() throws IOException {
        if( fs.exists( HadoopInterface.TMP_DIR ) ) {
            fs.delete( HadoopInterface.TMP_DIR, true );
        }
    }

    private FileSystem fs;
    private CuratorJobConf jobConf;
}
