package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

import edu.cs.illinois.cogcomp.hadoopinterface.HadoopInterface;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.exceptions.BadInputDirectoryException;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.exceptions.EmptyInputException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * A class to handle all the filesystem interactions in the Hadoop interface.
 *
 * @TODO: Non-static versions of the static methods requiring a filesystem obj
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
    }

    /**
     * Confirms that the required directories exist (or don't exist, as the case
     * may be) and that we have valid inputs, and throws an IO Exception if we
     * do not.
     * @throws IOException Possible IOException from file operations
     */
    public void checkFileSystem( ) throws IOException {
        Path inputDirectory = job.getInputDirectory();

        if( HDFSFileExists(HadoopInterface.TMP_DIR, fs) ) {
            throw new IOException( "Temp directory "
                    + fs.makeQualified(HadoopInterface.TMP_DIR)
                    + " already exists.  Please remove it first.");
        }
        if( !HDFSFileExists(inputDirectory, fs) ) {
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

        Path originalTxt = new Path( inputDirectory, "original.txt" );

        if( getFileSizeInBytes( originalTxt, fs ) < 1 ) {
            throw new EmptyInputException( "Input directory "
                    + fs.makeQualified( inputDirectory ) + " has no recognized "
                    + "input.  Please create input files in the Hadoop file "
                    + "system before starting this program.");
        }
    }

    public void cleanUpTempFiles() throws IOException {
        if( HDFSFileExists(HadoopInterface.TMP_DIR, fs) ) {
            fs.delete( HadoopInterface.TMP_DIR, true );
        }
    }

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
        return getFileNameFromPath(p.toString().trim());
    }

    /**
     * Removes a trailing slash from a path, if it exists.
     * @param p The path whose (possibly nonexistent) trailing slash you want to
     *          remove
     * @return The path with no trailing slash
     */
    public static String stripTrailingSlash( Path p ) {
        String s = p.toString();
        if( s.lastIndexOf(Path.SEPARATOR) == s.length() - 1 ) {
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
        if ( !HDFSFileExists(locationOfFile, fileSystem) ) {
            HadoopInterface.logger.logError("File " + locationOfFile.toString()
                    + " does not exists");
            return "";
        }

        FSDataInputStream in = fileSystem.open( locationOfFile );
        BufferedReader reader = new BufferedReader( new InputStreamReader(in) );

        String line = "";
        String fullOutput = "";
        while ( line != null ) {
            fullOutput = fullOutput + line + "\n";
            line = reader.readLine();
        }
        reader.close();
        in.close();

        if( closeFileSystemOnCompletion ) {
            fileSystem.close();       // TODO: Should we allow this?
        }

        return fullOutput;
    }

    /**
     * Returns a string version of a file on the local file system (i.e., not in
     * the Hadoop Distributed File System).
     * @param locationOfFile The path of the file to be read
     * @return A string version of the requested file
     * @throws IOException
     */
    public static String readFileFromLocal( Path locationOfFile )
            throws IOException {
        if ( !localFileExists(locationOfFile) ) {
            HadoopInterface.logger.logError("File " + locationOfFile.toString()
                    + " does not exists");
            return "";
        }

        DataInputStream in = new DataInputStream(
                new FileInputStream( locationOfFile.toString() ) );
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        String line = "";
        String fullOutput = "";
        while ( line != null)   {
            fullOutput = fullOutput + line + "\n";
            line = reader.readLine();
        }

        in.close();
        return fullOutput;
    }

    /**
     * Writes a string to a text file to a given location on the Hadoop
     * Distributed File System (HDFS)
     * @param inputText The string to be written to the text file
     * @param locationForFile A path (complete with file name and extension) to
     *                        which we should write. If data already exists at
     *                        this location, it will be overwritten.
     * @param fs The FileSystem object against which we will resolve paths. Note
     *           that, technically, this need not be an HDFS path. However, if
     *           you want to write local files, reduce the risk of error for
     *           yourself by using #writeFileToLocal().
     * @param closeFileSystemOnCompletion TRUE if the filesystem should be closed
     *                                    when the I/O operations are finished.
     * @throws IOException
     */
    public static void writeFileToHDFS( String inputText,
                                        Path locationForFile,
                                        FileSystem fs,
                                        boolean closeFileSystemOnCompletion )
            throws IOException {
        FSDataOutputStream dos = fs.create(locationForFile, true);

        dos.writeChars( inputText );
        dos.close();

        if( closeFileSystemOnCompletion ) {
            fs.close();  // TODO: Should we allow this?
        }
    }

    /**
     * Writes a string to a specified file on the local (i.e., non-Hadoop
     * Distributed) file system.
     * @param inputText The string to be written to the file
     * @param locationForFile A path indicating where on the local disk the data
     *                        should be written
     * @throws IOException
     */
    public static void writeFileToLocal( String inputText,
                                         Path locationForFile )
            throws IOException {
        FileSystem localFS = FileSystem.getLocal( new Configuration() );
        Path qualifiedLoc = locationForFile.makeQualified( localFS );

        // Check to make sure parent dir exists; if not, create it
        if( !localFileExists( qualifiedLoc.getParent() )
                && isDir( qualifiedLoc, localFS ) ) {
            localFS.mkdirs( localFS,
                            qualifiedLoc.getParent(),
                            new FsPermission( (short)777 ) );
        }

        writeFileToHDFS( inputText, qualifiedLoc, localFS, true );
    }

    /**
     * Copies a file from the local file system into HDFS
     * @param inLocalFileSystem
     * @param inHDFS
     * @param fs
     * @throws IOException
     */
    public static void copyFileFromLocalToHDFS( Path inLocalFileSystem,
                                                Path inHDFS,
                                                FileSystem fs )
            throws IOException {
        fs.copyFromLocalFile(inLocalFileSystem, inHDFS);
    }

    public static void copyFileFromHDFSToLocal( Path inHDFS,
                                                Path inLocalFileSystem,
                                                FileSystem fs )
            throws IOException {
        fs.copyToLocalFile(inHDFS, inLocalFileSystem);
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
     * @param fs The filesystem that the directory should be resolved against
     *           (before doing anything with the path, we make it fully qualified
     *           against this filesystem).
     * @return A list of all files and sub-directories found in the directory
     * @throws IOException
     */
    public static List<String> getFilesAndDirectoriesInDirectory( Path dir,
                                                                  FileSystem fs )
            throws IOException {
        ArrayList<String> listOfPaths = new ArrayList<String>();

        FileStatus fileStatuses[] = fs.listStatus(dir);
        for( FileStatus status : fileStatuses ) {
            listOfPaths.add( status.getPath().toString() );
        }
        return listOfPaths;
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
     * @param fs The filesystem that the directory should be resolved against
     *           (before doing anything with the path, we make it fully qualified
     *           against this filesystem).
     * @return A list of all files and sub-directories found in the directory
     * @throws IOException
     */
    public static List<String> getFilesAndDirectoriesInDirectory( String dir,
                                                                  FileSystem fs )
            throws IOException {
        if( dir == null || dir.equals("") ) {
            throw new IllegalArgumentException(
                    "The empty string is not a valid path." );
        }
        return getFilesAndDirectoriesInDirectory( new Path(dir), fs );
    }

    /**
     * Returns TRUE if the file exists at the specified path, and false otherwise.
     * @param fileLocation A Path to the file in question
     * @return TRUE if and only if the specified file exists.
     */
    public static boolean localFileExists( Path fileLocation ) {
        return ( new File( fileLocation.toString() ) ).exists();
    }

    /**
     * Returns TRUE if the file exists at the specified path, and false otherwise.
     * @param fileLocation A Path to the file in question
     * @param fs The FileSystem object against which to resolve the path
     * @return TRUE if and only if the specified file exists.
     */
    public static boolean HDFSFileExists( Path fileLocation, FileSystem fs )
            throws IOException {
        return fs.exists( fileLocation );
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
     * Checks whether a given path refers to a directory. Note that the path does
     * *not* need to actually exist---if the file is not found, we simply return
     * false.
     * @param path The location of the file/directory in question
     * @param fs A file system object to resolve paths relative to
     * @return True if path points to a directory, false otherwise.
     * @throws IOException
     */
    public static boolean isDir( Path path, FileSystem fs )
            throws IOException {
        try {
            return fs.getFileStatus( path ).isDir();
        } catch( FileNotFoundException e ) {
            return false;
        }
    }

    public static long getFileSizeInBytes(Path path, FileSystem fs)
            throws IOException {
        return fs.getFileStatus( path ).getLen();
    }


    private FileSystem fs;

    private CuratorJob job;
}
