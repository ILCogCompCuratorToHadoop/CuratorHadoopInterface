package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import edu.illinois.cs.cogcomp.hadoopinterface.HadoopInterface;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.exceptions.BadInputDirectoryException;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.exceptions.EmptyInputException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A class to handle all the filesystem interactions in the Hadoop interface.
 *
 * @bug Writing files does *not* work when called from the
 *      InputSplit (DirectorySplit)
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
     * Constructs a file system handler
     * @param fs The file system against which we should resolve paths
     */
    public FileSystemHandler( FileSystem fs ) {
        this.fs = fs;
    }

    /**
     * Sets up the input and output directories for this job in the Hadoop
     * Distributed File System (HDFS). This method resolves paths using the file
     * system object given to this object during its construction or created
     * based on the Curator job configuration it was given.
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
     * do not. This method resolves paths using the file system object given to
     * this object during its construction.
     * @throws IOException Possible IOException from file operations
     */
    public void checkFileSystem( ) throws IOException {
        Path inputDirectory = job.getInputDirectory();

        if( HDFSFileExists(HadoopInterface.TMP_DIR) ) {
            throw new IOException( "Temp directory "
                    + fs.makeQualified(HadoopInterface.TMP_DIR)
                    + " already exists.  Please remove it first.");
        }
        if( !HDFSFileExists(inputDirectory) ) {
            throw new BadInputDirectoryException( "Input directory "
                    + fs.makeQualified( inputDirectory ) + " does not exist. "
                    + "Please create it in the Hadoop file system first." );
        }
        if( !isDir(inputDirectory) ) {
            throw new BadInputDirectoryException( "The file "
                    + fs.makeQualified( inputDirectory ) + " is not a directory. "
                    + "Please check the documentation for this package for "
                    + "information on how to structure the input directory.");
        }

        List<Path> dirsInInput = getSubdirectories(inputDirectory);
        for( Path dir : dirsInInput ) {
            Path originalTxt = new Path( dir, "original.txt" );

            if( getFileSizeInBytes( originalTxt  ) < 1 ) {
                throw new EmptyInputException( "Input in document directory "
                        + fs.makeQualified( inputDirectory ) + " has no "
                        + "recognized input.  Please create input files in the "
                        + "Hadoop file system before starting this program.");
            }
        }
    }

    /**
     * Removes the temp directory used by HadoopInterface
     * @throws IOException
     */
    public void cleanUpTempFiles() throws IOException {
        if( HDFSFileExists( HadoopInterface.TMP_DIR ) ) {
            delete( HadoopInterface.TMP_DIR );
        }
    }

    /**
     * Deletes a file or directory from this object's file system.
     * @param fileOrDirectoryToDelete The location of the thing to be deleted.
     *                                If this is a directory, we will perform a
     *                                recursive delete (deleting all files within
     *                                the directory as well).
     * @throws IOException
     */
    public void delete( Path fileOrDirectoryToDelete ) throws IOException {
        if( HDFSFileExists( fileOrDirectoryToDelete ) ) {
            fs.delete( fileOrDirectoryToDelete, true );
        }
    }

    /**
     * Deletes a file or directory from the local file system
     * @param fileOrDirectoryToDelete The location of the thing to be deleted.
     *                                If this is a directory, we will perform a
     *                                recursive delete (deleting all files within
     *                                the directory as well).
     * @throws IOException
     */
    public static void deleteLocal( Path fileOrDirectoryToDelete )
            throws IOException {
        if( localFileExists( fileOrDirectoryToDelete ) ) {
            FileSystem localFS = FileSystem.getLocal( new Configuration() );
            localFS.delete( fileOrDirectoryToDelete, true );
        }
    }

    /**
     * Creates a directory with read/write access to anyone (777 permissions).
     * For our purposes, this is fine. If the directory already exists, we simply
     * exit---it's up to you to make sure you're giving a good path. This method
     * resolves paths using the file system object given to this object during
     * its construction, or created based on the Curator job configuration it
     * was given.
     * @param directoryToCreate A path to the location where the directory should
     *                          be created.
     */
    public void mkdir( Path directoryToCreate ) throws IOException {
        if( !HDFSFileExists( directoryToCreate ) ) {
            fs.mkdirs( directoryToCreate );
        }
    }

    /**
     * Creates a directory on the local file system with 777 permissions.
     * For our purposes, this is fine. If the directory already exists, we simply
     * exit---it's up to you to make sure you're giving a good path.
     * @param directoryToCreate A path to the location where the directory should
     *                          be created.
     */
    public static void mkdirLocal(Path directoryToCreate) throws IOException {
        if( !localFileExists( directoryToCreate ) ) {
            FileSystem.getLocal( new Configuration() ).mkdirs( directoryToCreate );
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

    /**
     * Returns the filename from a path. For instance, if p was /foo/bar/bas.txt,
     * it would return "bas.txt".
     * @param p A path that ends in a file
     * @return The final path component
     */
    public static String getFileNameFromPath( Path p ) {
        String name = p.getName();
        return name.substring( 0, name.lastIndexOf('.') + 1 );
    }

    /**
     * Returns a string version of a file on the Hadoop Distributed File System
     * (HDFS).
     * @param locationOfFile The file's path
     * @return A string version of the requested file
     * @throws IOException
     */
    public byte[] readBytesFromHDFS( Path locationOfFile )
            throws IOException {
        FSDataInputStream in = fs.open( locationOfFile );
        BufferedReader reader = new BufferedReader( new InputStreamReader(in) );

        byte[] buffer = new byte[ (int)getFileSizeInBytes(locationOfFile) ];

        int i = 0;
        int c = reader.read();

        while (c != -1) {
            buffer[i++] = (byte)c;
            c = reader.read();
        }

        return buffer;
    }

    /**
     * Returns a string version of a file on the Hadoop Distributed File System
     * (HDFS).
     * @param locationOfFile The file's path
     * @return A string version of the requested file
     * @throws IOException
     */
    public String readFileFromHDFS( Path locationOfFile ) throws IOException {
        if ( !HDFSFileExists(locationOfFile) ) {
            HadoopInterface.logger.logError( "File " + locationOfFile.toString()
                    + " does not exists" );
            return "";
        }

        FSDataInputStream in = fs.open( locationOfFile );
        BufferedReader reader = new BufferedReader( new InputStreamReader(in) );

        String line = "";
        String fullOutput = "";
        try {
            while ( line != null ) {
                fullOutput = fullOutput + line + "\n";
                line = reader.readLine();
            }
        } catch ( OutOfMemoryError e ) {
            HadoopInterface.logger.logError( "Ran out of memory while writing "
                    + "file " + locationOfFile.toString() + ".");
            throw e;
        }
        reader.close();
        in.close();

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
     * Distributed File System (HDFS). Note that this will overwrite anything
     * currently present at the location. This method resolves paths using the
     * file system object given to this object during its construction, or
     * created based on the Curator job configuration it was given.
     *
     * Takes String. Defaults to override mode.
     *
     * @param inputText The string to be written to the text file
     * @param locationForFile A path (complete with file name and extension) to
     *                        which we should write. If data already exists at
     *                        this location, it will be overwritten.
     * @throws IOException
     */
    public void writeFileToHDFS( String inputText,
                                 Path locationForFile ) throws IOException {
        writeFileToHDFS( inputText, locationForFile, false );
    }
	
    /**
     * Writes a string to a text file to a given location on the Hadoop
     * Distributed File System (HDFS). Note that this is not thread-safe; Hadoop
     * recommends you not try to have multiple nodes writing to the same file,
     * ever.
     *
     * Takes String. Specifies append/overwrite mode. 
     *
     * @param inputText The string to be written to the text file
     * @param locationForFile A path (complete with file name and extension) to
     *                        which we should write. If data already exists at
     *                        this location, it will be overwritten.
     * @param appendInsteadOfOverwriting TRUE if we should append to whatever
     *                                   currently exists at the location, FALSE
     *                                   if it is okay to overwrite it.
     * @bug Writing files does *not* work when called from the
     *      InputSplit (DirectorySplit)
     * @throws IOException
     */
    public void writeFileToHDFS( String inputText,
                                 Path locationForFile,
                                 boolean appendInsteadOfOverwriting )
            throws IOException {
        writeBytesToHDFS( inputText.getBytes(), locationForFile,
                          appendInsteadOfOverwriting ); // call full method on bytes
    }

    /**
     * Writes a byte array to a text file to a given location on the Hadoop
     * Distributed File System (HDFS). Note that this will overwrite anything
     * currently present at the location. This method resolves paths using the
     * file system object given to this object during its construction, or
     * created based on the Curator job configuration it was given.
     *
     * Takes byte[]. Defaults to overwrite mode.
     *
     * @param input The byte array to be written to the text file
     * @param locationForFile A path (complete with file name and extension) to
     *                        which we should write. If data already exists at
     *                        this location, it will be overwritten.
     * @throws IOException
     */
    public void writeBytesToHDFS( byte[] input, Path locationForFile )
            throws IOException {
        writeBytesToHDFS( input, locationForFile, false );
    }

    /**
     * Writes a byte array to a text file to a given location on the Hadoop
     * Distributed File System (HDFS). Note that this is not thread-safe; Hadoop
     * recommends you not try to have multiple nodes writing to the same file,
     * ever.
     *
     * Takes byte[]. Specifies append/overwrite mode.
     *
     * @param input The byte array to be written to the text file
     * @param locationForFile A path (complete with file name and extension) to
     *                        which we should write. If data already exists at
     *                        this location, it will be overwritten.
     * @param appendInsteadOfOverwriting TRUE if we should append to whatever
     *                                   currently exists at the location, FALSE
     *                                   if it is okay to overwrite it.
     * @bug Writing files does *not* work when called from the
     *      InputSplit (DirectorySplit)
     * @throws IOException
     */
    public void writeBytesToHDFS( byte[] input,
                                  Path locationForFile,
                                  boolean appendInsteadOfOverwriting )
            throws IOException {
        // Qualify the location to avoid errors
        Path qualifiedLoc;
        if( locationForFile != null ) {
            qualifiedLoc = locationForFile.makeQualified( fs );
        }
        else {
            throw new NullPointerException( "Undefined location for file with "
                    + "contents \"" + Arrays.toString( input ) + "\".");
        }

        // Handle appends
        if( appendInsteadOfOverwriting && HDFSFileExists(qualifiedLoc) ) {
            // Append is *not* supported in Hadoop r1.0.3. Damn.
            // TODO: When 2.0 is final, change to use FileSystem.append() instead
            // dos = fs.append( locationForFile );

            byte[] old = readBytesFromHDFS( qualifiedLoc );
            byte[] combined = new byte[ old.length + input.length ];

            System.arraycopy( old, 0, combined, 0, old.length );
            System.arraycopy( input, 0, combined, old.length, input.length );

            input = combined;

            delete( qualifiedLoc );
        }

        FSDataOutputStream dos = fs.create( qualifiedLoc, true );
        // NOTE: Writing using FSDataOutputStream's writeChars() or writeUTF()
        //       methods writes megabytes worth of invisible control characters,
        //       very quickly leading to hundred megabyte log files. BAD.
        dos.write( input );
        dos.close();
    }

    /**
     * Writes a string to a specified file on the local (i.e., non-Hadoop
     * Distributed) file system. Note that this method overwrites any data
     * previously present in the requested location.
     * @param inputText The string to be written to the file
     * @param locationForFile A path indicating where on the local disk the data
     *                        should be written
     * @throws IOException
     */
    public void writeFileToLocal( String inputText,
                                  Path locationForFile )
            throws IOException {
        writeFileToLocal( inputText, locationForFile, false );
    }

    /**
     * Writes a string to a specified file on the local (i.e., non-Hadoop
     * Distributed) file system.
     * @param inputText The string to be written to the file
     * @param locationForFile A path indicating where on the local disk the data
     *                        should be written
     * @param appendInsteadOfOverwriting TRUE if we should append to whatever
     *                                   currently exists at the location, FALSE
     *                                   if it is okay to overwrite it.
     * @throws IOException
     */
    public static void writeFileToLocal( String inputText,
                                         Path locationForFile,
                                         boolean appendInsteadOfOverwriting )
            throws IOException {
        FileSystem localFS = FileSystem.getLocal( new Configuration() );
        FileSystemHandler localHandler = new FileSystemHandler( localFS );

        localHandler.writeFileToHDFS( inputText,
                                      locationForFile,
                                      appendInsteadOfOverwriting );
    }

    /**
     * Copies a file from the local file system into HDFS. Resolves paths using
     * the file system object given to this object during its construction, or
     * created based on the Curator job configuration it was given.
     * @param inLocalFileSystem The file in the local file system to be written
     *                          to HDFS.
     * @param inHDFS The location of the file to be written to. This will contain
     *               a copy of the file in the local file system.
     * @throws IOException
     */
    public void copyFileFromLocalToHDFS( Path inLocalFileSystem,
                                         Path inHDFS ) throws IOException {
        fs.copyFromLocalFile( inLocalFileSystem, inHDFS );
    }

    /**
     * Copies a file from HDFS to the local file system. Resolves paths using the
     * file system object given to this object during its construction, or
     * created based on the Curator job configuration it was given.
     * @param inHDFS The location of the file in HDFS that will be copied to
     *               the local file system.
     * @param inLocalFileSystem The file in the local file system that will
     *                          contain a copy of the file in HDFS.
     * @throws IOException
     */
    public void copyFileFromHDFSToLocal( Path inHDFS,
                                         Path inLocalFileSystem )
            throws IOException {
        fs.copyToLocalFile( inHDFS, inLocalFileSystem );
    }

    /**
     * Returns an array of strings naming the files and directories in the
     * directory denoted by this abstract path name. Resolves paths using the
     * file system object given to this object during its construction, or
     * created based on the Curator job configuration it was given.
     *
     * There is no guarantee that the name strings in the resulting array will
     * appear in any specific order; they are not, in particular, guaranteed to
     * appear in alphabetical order.
     *
     * @param dir The path whose files you want a list of
     * @return A list of all files and sub-directories found in the directory
     * @throws IOException
     */
    public List<String> getFilesAndDirectoriesInDirectory( Path dir )
            throws IOException {
        ArrayList<String> listOfPaths = new ArrayList<String>();

        FileStatus fileStatuses[] = fs.listStatus( dir );
        for( FileStatus status : fileStatuses ) {
            listOfPaths.add( status.getPath().toString() );
        }
        return listOfPaths;
    }

    /**
     * Gets a list of Paths that contains all subdirectories in the directory in
     * question. Resolves paths using the file system object given to this object
     * during its construction, or created based on the Curator job
     * configuration it was given.
     * @param dir The directory whose subdirectories we shall get the list of.
     * @return A list of Paths which point to the subdirectories of the input
     *         directory.
     * @throws IOException
     */
    public List<Path> getSubdirectories( Path dir )
            throws IOException {
        List<Path> listOfPaths = new ArrayList<Path>();

        FileStatus fileStatuses[] = fs.listStatus(dir);
        for( FileStatus status : fileStatuses ) {
            if( status.isDir() ) {
                listOfPaths.add( status.getPath() );
            }
        }
        return listOfPaths;
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
     *
     * This method resolves paths using the file system object given to this
     * object during its construction, or created based on the Curator job
     * configuration it was given.
     * @param fileLocation A Path to the file in question
     * @return TRUE if and only if the specified file exists.
     */
    public boolean HDFSFileExists( Path fileLocation )
            throws IOException {
        return fs.exists( fileLocation );
    }

    /**
     * Checks whether a given path refers to a directory. Note that the path does
     * *not* need to actually exist---if the file is not found, we simply return
     * false.
     *
     * This method resolves paths using the file system object given to this
     * object during its construction, or created based on the Curator job
     * configuration it was given.
     *
     * @param path The location of the file/directory in question
     * @return True if path points to a directory, false otherwise.
     * @throws IOException
     */
    public boolean isDir( Path path ) throws IOException {
        try {
            return fs.getFileStatus( path ).isDir();
        } catch( FileNotFoundException e ) {
            return false;
        }
    }

    /**
     * Checks the file to see if it was modified in the last (some number)
     * minutes.
     *
     * This method resolves paths using the file system object given to this
     * object during its construction, or created based on the Curator job
     * configuration it was given.
     *
     * @param path The location (file or directory) whose last modification
     *             time we should check
     * @param minutes The max allowed number of minutes since the last
     *                modification
     * @return True if the file was modified within the last (parameter: minutes)
     *         minutes.
     * @throws IOException
     */
    public boolean fileWasModifiedInLastXMins( Path path, int minutes )
            throws IOException {
        long timeXMinutesAgo = System.currentTimeMillis() - ( minutes * 60 * 1000);
        return ( fs.getFileStatus( path ).getAccessTime() > timeXMinutesAgo);
    }

    /**
     * Returns the size of the indicated file, in bytes
     *
     * This method resolves paths using the file system object given to this
     * object during its construction, or created based on the Curator job
     * configuration it was given.
     *
     * @param path The location of the file in question. Note that if this is a
     *             directory, you'll get the size of the actual Unix file
     *             representing the directory, *not* the size of the sum of
     *             the contents.
     * @return The size in bytes of the entity at the indicated path
     * @throws IOException
     */
    public long getFileSizeInBytes( Path path ) throws IOException {
        return fs.getFileStatus( path ).getLen();
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


    private FileSystem fs;

    private CuratorJob job;
}
