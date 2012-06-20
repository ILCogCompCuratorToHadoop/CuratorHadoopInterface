package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

import edu.cs.illinois.cogcomp.hadoopinterface.HadoopInterface;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.exceptions.BadInputDirectoryException;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.exceptions.EmptyInputException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

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
        if( !isDir(inputDirectory, fs) ) {
            throw new BadInputDirectoryException( "The file "
                    + fs.makeQualified( inputDirectory ) + " is not a directory. "
                    + "Please check the documentation for this package for "
                    + "information on how to structure the input directory.");
        }

        /*
        Path path = new Path(fileName);
        boolean fileExists = fs.exists(path);
        */

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

    /**
     * Returns the filename from a path. For instance, if p was /foo/bar/bas.txt,
     * it would return "bas.txt".
     * @param pathAsString A path that ends in a file
     * @return The final path component
     */
    public static String getFileNameFromPath( String pathAsString ) {
        return pathAsString.substring(
                pathAsString.lastIndexOf(Path.SEPARATOR) + 1,
                pathAsString.length());
    }

    public static String getFileNameFromPath( Path p ) {
        return getFileNameFromPath( p.toString().trim() );
    }

    /**
     * Removes a trailing slash from a path, if it exists.
     * @param p The path whose (possibly nonexistent) trailing slash you want to
     *          remove
     * @return The path with no trailing slash
     */
    public static String stripTrailingSlash( Path p ) {
        String s = p.toString();
        if( s.toString().lastIndexOf( Path.SEPARATOR ) == s.length() - 1 ) {
            return s.substring( 0, s.length() - 1 );
        }
        return s;
    }

    /**
     * Returns a string version of a file on the Hadoop Distributed File System
     * (HDFS).
     * @param locationOfFile The file's path
     * @param fileSystem The FileSystem object to resolve paths against
     * @param closeFileSystemOnCompletion True if we should close the file system
     *                                    object after reading, false otherwise.
     *                                    If you are still working with this FS
     *                                    object, you probably want to set this
     *                                    to false.
     * @return A string version of the requested file
     * @throws IOException
     */
    public static String readFileFromHDFS( Path locationOfFile,
                                           FileSystem fileSystem,
                                           boolean closeFileSystemOnCompletion )
            throws IOException {
        if ( !fileSystem.exists( locationOfFile ) ) {
            HadoopInterface.logger.logError( "File " + locationOfFile.toString()
                                             + " does not exists");
            return "";
        }

        FSDataInputStream in = fileSystem.open( locationOfFile );
        BufferedReader reader = new BufferedReader( new InputStreamReader(in) );

        String line;
        String fullOutput = "";
        while ( (line = reader.readLine()) != null ) {
            fullOutput = fullOutput + line + "\n";
        }
        reader.close();
        in.close();

        if( closeFileSystemOnCompletion ) {
            fileSystem.close();
        }

        return fullOutput;
    }

    public void cleanUpTempFiles() throws IOException {
        if( fs.exists( HadoopInterface.TMP_DIR ) ) {
            fs.delete( HadoopInterface.TMP_DIR, true );
        }
    }

    /**
     * Returns an array of strings naming the files and directories in the
     * directory denoted by this abstract path name.
     *
     * There is no guarantee that the name strings in the resulting array will
     * appear in any specific order; they are not, in particular, guaranteed to
     * appear in alphabetical order.
     *
     * @param dir The path whose files you want a list of
     * @return A list of all files and sub-directories found in the directory
     * @throws IOException
     */
    public static List<String> getFilesAndDirectoriesInDirectory( Path dir )
            throws IOException {
        return Arrays.asList(FileUtil.list(new File(dir.toUri())));
    }

    /**
     * Returns an array of strings naming the files and directories in the
     * directory denoted by this abstract path name.
     *
     * There is no guarantee that the name strings in the resulting array will
     * appear in any specific order; they are not, in particular, guaranteed to
     * appear in alphabetical order.
     *
     * @param dir The path whose files you want a list of
     * @return A list of all files and sub-directories found in the directory
     * @throws IOException
     */
    public static List<String> getFilesAndDirectoriesInDirectory( String dir )
            throws IOException {
        return getFilesAndDirectoriesInDirectory( new Path(dir) );
    }

    /**
     * Checks whether a given path refers to a directory.
     * @param pathAsString The location of the file/directory in question
     * @param fs A file system object to resolve paths relative to
     * @return True if path points to a directory, false otherwise.
     * @throws IOException
     */
    public static boolean isDir( String pathAsString, FileSystem fs )
            throws IOException {
        return isDir( new Path( pathAsString ), fs );
    }

    /**
     * Checks whether a given path refers to a directory.
     * @param path The location of the file/directory in question
     * @param fs A file system object to resolve paths relative to
     * @return True if path points to a directory, false otherwise.
     * @throws IOException
     */
    public static boolean isDir( Path path, FileSystem fs )
            throws IOException {
        return fs.getFileStatus( path ).isDir();
    }


    private FileSystem fs;

    private CuratorJob job;
}
