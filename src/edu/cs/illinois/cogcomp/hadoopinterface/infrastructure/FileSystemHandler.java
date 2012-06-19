package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

import edu.cs.illinois.cogcomp.hadoopinterface.HadoopInterface;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.exceptions.BadInputDirectoryException;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.exceptions.EmptyInputException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * A class to handle all the filesystem interactions in the Hadoop interface.
 *
 * @author Tyler Young
 */
public class FileSystemHandler {
    /**
     * Constructs a file system handler.
     * @param job The job configuration for this Hadoop job
     */
    public FileSystemHandler( CuratorJob job ) {
        this.job = job;
        fs = job.getFileSystem();
    }

    /**
     * Sets up the input and output directories for this job in the Hadoop
     * Distributed File System (HDFS).
     * @throws IOException Possible IOException from file operations
     */
    public void setUpIODirectories() throws IOException {
        // Set up input/output directories. We will output the new annotation
        // to the same place in HDFS that we get the input from.
        HadoopInterface.logger.logStatus( "Adding input path." );
        FileInputFormat.addInputPath( job, job.getInputDirectory());
        HadoopInterface.logger.logStatus( "Setting output path." );
        FileOutputFormat.setOutputPath( job, job.getOutputDirectory());

        //HadoopInterface.logger.logStatus( "Creating input files for map ops." );
        //createInputFilesForMaps();
    }

    /**
     * Confirms that the required directories exist (or don't exist, as the case
     * may be) and that we have valid inputs, and throws an IO Exception if we
     * do not.
     * @throws IOException Possible IOException from file operations
     */
    public void checkFileSystem( ) throws IOException {
        Path inputDirectory = job.getInputDirectory();

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
        if( false /* TODO: if files don't actually exist */ ) {
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
    /*
    public void createInputFilesForMaps() throws IOException {
        // Generate an input file for each map task
        // TODO: Make this actually appropriate for our task (currently taken from the Hadoop example for approximating Pi)
        for (int i = 0; i < job.getNumMaps(); ++i)
        {
            final Path file_path = new Path( job.getInputDirectory(), "part" + i );
            final LongWritable offset = new LongWritable(i);
            final LongWritable size = new LongWritable(1);

            HadoopInterface.logger.logStatus( "Creating Sequence file writer." );
            final SequenceFile.Writer writer = SequenceFile.createWriter(
                    fs, job.getConfiguration(), file_path,
                    Text.class, Record.class, SequenceFile.CompressionType.NONE);
            try {
                HadoopInterface.logger.logStatus( "Appending to sequence file" );
                writer.append( offset, size );
            } finally {
                writer.close();
            }

            HadoopInterface.logger.logStatus( "Wrote input for Map #" + i );
        }
    }*/

    public void cleanUpTempFiles() throws IOException {
        if( fs.exists( HadoopInterface.TMP_DIR ) ) {
            fs.delete( HadoopInterface.TMP_DIR, true );
        }
    }

    private FileSystem fs;
    private CuratorJob job;
}
