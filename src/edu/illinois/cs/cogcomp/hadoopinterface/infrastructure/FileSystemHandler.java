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

        List<Path> dirsInInput = getSubdirectories(inputDirectory, fs);
        for( Path dir : dirsInInput ) {
            Path originalTxt = new Path( dir, "original.txt" );

            if( getFileSizeInBytes( originalTxt, fs ) < 1 ) {
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
        if( HDFSFileExists( HadoopInterface.TMP_DIR, fs) ) {
            delete( HadoopInterface.TMP_DIR, fs );
        }
    }

    /**
     * Deletes a file or directory from a file system.
     * @param fileOrDirectoryToDelete The location of the thing to be deleted.
     *                                If this is a directory, we will perform a
     *                                recursive delete (deleting all files within
     *                                the directory as well).
     * @param fs The file system object against which we should resolve the path.
     *           May be constructed as either a local or HDFS file system.
     * @throws IOException
     */
    public static void delete(Path fileOrDirectoryToDelete, FileSystem fs)
            throws IOException {
        if( HDFSFileExists( fileOrDirectoryToDelete, fs ) ) {
            fs.delete( fileOrDirectoryToDelete, true );
        }
    }

    /**
     * Creates a directory with read/write access to anyone (777 permissions).
     * For our purposes, this is fine. If the directory already exists, we simply
     * exit---it's up to you to make sure you're giving a good path.
     * @param directoryToCreate A path to the location where the directory should
     *                          be created.
     * @param fs The file system against which to resolve the path
     */
    public static void mkdir(Path directoryToCreate, FileSystem fs)
            throws IOException {
        if( !HDFSFileExists( directoryToCreate, fs ) ) {
            fs.mkdirs( directoryToCreate );
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
        mkdir( directoryToCreate, fs );
    }

    /**
     * Creates a directory on the local file system with 777 permissions.
     * For our purposes, this is fine. If the directory already exists, we simply
     * exit---it's up to you to make sure you're giving a good path.
     * @param directoryToCreate A path to the location where the directory should
     *                          be created.
     */
    public static void mkdirLocal(Path directoryToCreate) throws IOException {
        mkdir( directoryToCreate, FileSystem.getLocal(new Configuration()) );
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
     * @param fs The FileSystem object to resolve paths against
     * @return A string version of the requested file
     * @throws IOException
     */
    public static byte[] readBytesFromHDFS( Path locationOfFile, FileSystem fs )
            throws IOException {
        FSDataInputStream in = fs.open( locationOfFile );
        BufferedReader reader = new BufferedReader( new InputStreamReader(in) );

        byte[] buffer = new byte[ (int)getFileSizeInBytes(locationOfFile, fs) ];

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
     * (HDFS). This method resolves paths using the file system object given to
     * this object during its construction, or created based on the Curator job
     * configuration it was given.
     * @param locationOfFile The file's path
     * @return A string version of the requested file
     * @throws IOException
     */
    public byte[] readBytesFromHDFS( Path locationOfFile ) throws IOException {
        return readBytesFromHDFS( locationOfFile, fs );
    }

    /**
     * Returns a string version of a file on the Hadoop Distributed File System
     * (HDFS).
     * @param locationOfFile The file's path
     * @param fileSystem The FileSystem object to resolve paths against
     * @return A string version of the requested file
     * @throws IOException
     */
    public static String readFileFromHDFS( Path locationOfFile,
                                           FileSystem fileSystem )
            throws IOException {
        if ( !HDFSFileExists(locationOfFile, fileSystem) ) {
            HadoopInterface.logger.logError( "File " + locationOfFile.toString()
                    + " does not exists" );
            return "";
        }

        FSDataInputStream in = fileSystem.open( locationOfFile );
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
     * Returns a string version of a file on the Hadoop Distributed File System
     * (HDFS). This method resolves paths using the file system object given to
     * this object during its construction, or created based on the Curator job
     * configuration it was given.
     * @param locationOfFile The file's path
     * @return A string version of the requested file
     * @throws IOException
     */
    public String readFileFromHDFS( Path locationOfFile ) throws IOException {
        return readFileFromHDFS( locationOfFile, fs );
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
     * currently present at the location.
     * @param inputText The string to be written to the text file
     * @param locationForFile A path (complete with file name and extension) to
     *                        which we should write. If data already exists at
     *                        this location, it will be overwritten.
     * @param fs The FileSystem object against which we will resolve paths. Note
     *           that, technically, this need not be an HDFS path. However, if
     *           you want to write local files, reduce the risk of error for
     *           yourself by using #writeFileToLocal().
     * @throws IOException
     */
    public static void writeFileToHDFS( String inputText,
                                        Path locationForFile,
                                        FileSystem fs )
            throws IOException {
        writeFileToHDFS( inputText, locationForFile, fs, false);
    }

    /**
     * Writes a string to a text file to a given location on the Hadoop
     * Distributed File System (HDFS). Note that this will overwrite anything
     * currently present at the location. This method resolves paths using the
     * file system object given to this object during its construction, or
     * created based on the Curator job configuration it was given.
     * @param inputText The string to be written to the text file
     * @param locationForFile A path (complete with file name and extension) to
     *                        which we should write. If data already exists at
     *                        this location, it will be overwritten.
     * @throws IOException
     */
    public void writeFileToHDFS( String inputText,
                                 Path locationForFile ) throws IOException {
        writeFileToHDFS( inputText, locationForFile, fs );
    }

    /**
     * Writes a byte array to a text file to a given location on the Hadoop
     * Distributed File System (HDFS). Note that this will overwrite anything
     * currently present at the location.
     * @param input The byte array to be written to the text file
     * @param locationForFile A path (complete with file name and extension) to
     *                        which we should write. If data already exists at
     *                        this location, it will be overwritten.
     * @param fs The FileSystem object against which we will resolve paths. Note
     *           that, technically, this need not be an HDFS path. However, if
     *           you want to write local files, reduce the risk of error for
     *           yourself by using #writeFileToLocal().
     * @throws IOException
     */
    public static void writeFileToHDFS( byte[] input,
                                        Path locationForFile,
                                        FileSystem fs )
            throws IOException {
        writeFileToHDFS( input, locationForFile, fs, false);
    }

    /**
     * Writes a byte array to a text file to a given location on the Hadoop
     * Distributed File System (HDFS). Note that this will overwrite anything
     * currently present at the location. This method resolves paths using the
     * file system object given to this object during its construction, or
     * created based on the Curator job configuration it was given.
     * @param input The byte array to be written to the text file
     * @param locationForFile A path (complete with file name and extension) to
     *                        which we should write. If data already exists at
     *                        this location, it will be overwritten.
     * @throws IOException
     */
    public void writeFileToHDFS( byte[] input,
                                        Path locationForFile )
            throws IOException {
        writeFileToHDFS( input, locationForFile, fs );
    }

    /**
     * Writes a string to a text file to a given location on the Hadoop
     * Distributed File System (HDFS). Note that this is not thread-safe; Hadoop
     * recommends you not try to have multiple nodes writing to the same file,
     * ever.
     * @param inputText The string to be written to the text file
     * @param locationForFile A path (complete with file name and extension) to
     *                        which we should write. If data already exists at
     *                        this location, it will be overwritten.
     * @param fs The FileSystem object against which we will resolve paths. Note
     *           that, technically, this need not be an HDFS path. However, if
     *           you want to write local files, reduce the risk of error for
     *           yourself by using #writeFileToLocal().
     * @param appendInsteadOfOverwriting TRUE if we should append to whatever
     *                                   currently exists at the location, FALSE
     *                                   if it is okay to overwrite it.
     * @bug Writing files does *not* work when called from the
     *      InputSplit (DirectorySplit)
     * @throws IOException
     */
    public static void writeFileToHDFS( String inputText,
                                        Path locationForFile,
                                        FileSystem fs,
                                        boolean appendInsteadOfOverwriting )
            throws IOException {
        writeFileToHDFS( inputText.getBytes(), locationForFile, fs,
                         appendInsteadOfOverwriting);
    }

    /**
     * Writes a string to a text file to a given location on the Hadoop
     * Distributed File System (HDFS). Note that this is not thread-safe; Hadoop
     * recommends you not try to have multiple nodes writing to the same file,
     * ever. This method resolves paths using the file system object given to
     * this object during its construction, or created based on the Curator job
     * configuration it was given.
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
        writeFileToHDFS( inputText, locationForFile, fs,
                         appendInsteadOfOverwriting );
    }

    /**
     * Writes a byte array to a text file to a given location on the Hadoop
     * Distributed File System (HDFS). Note that this is not thread-safe; Hadoop
     * recommends you not try to have multiple nodes writing to the same file,
     * ever.
     * @param input The byte array to be written to the text file
     * @param locationForFile A path (complete with file name and extension) to
     *                        which we should write. If data already exists at
     *                        this location, it will be overwritten.
     * @param fs The FileSystem object against which we will resolve paths. Note
     *           that, technically, this need not be an HDFS path. However, if
     *           you want to write local files, reduce the risk of error for
     *           yourself by using #writeFileToLocal().
     * @param appendInsteadOfOverwriting TRUE if we should append to whatever
     *                                   currently exists at the location, FALSE
     *                                   if it is okay to overwrite it.
     * @bug Writing files does *not* work when called from the
     *      InputSplit (DirectorySplit)
     * @throws IOException
     */
    public static void writeFileToHDFS( byte[] input,
                                        Path locationForFile,
                                        FileSystem fs,
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
        if( appendInsteadOfOverwriting && HDFSFileExists(qualifiedLoc, fs) ) {
            // Append is *not* supported in Hadoop r1.0.3. Damn.
            // TODO: When 2.0 is final, change to use FileSystem.append() instead
            // dos = fs.append( locationForFile );

            byte[] old = readBytesFromHDFS( qualifiedLoc, fs );
            byte[] combined = new byte[ old.length + input.length ];

            System.arraycopy( old, 0, combined, 0, old.length );
            System.arraycopy( input, 0, combined, old.length, input.length );

            input = combined;

            delete( qualifiedLoc, fs );
        }

        FSDataOutputStream dos = fs.create( qualifiedLoc, true );
        // NOTE: Writing using FSDataOutputStream's writeChars() or writeUTF()
        //       methods writes megabytes worth of invisible control characters,
        //       very quickly leading to hundred megabyte log files. BAD.
        dos.write( input );
        dos.close();
    }

    /**
     * Writes a byte array to a text file to a given location on the Hadoop
     * Distributed File System (HDFS). Note that this is not thread-safe; Hadoop
     * recommends you not try to have multiple nodes writing to the same file,
     * ever. Resolves paths using the file system object given to this object
     * during its construction, or created based on the Curator job
     * configuration it was given.
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
    public void writeFileToHDFS( byte[] input,
                                 Path locationForFile,
                                 boolean appendInsteadOfOverwriting )
            throws IOException {
        writeFileToHDFS( input, locationForFile, fs,
                         appendInsteadOfOverwriting );
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
    public static void writeFileToLocal( String inputText,
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

        writeFileToHDFS( inputText,
                         locationForFile,
                         localFS,
                         appendInsteadOfOverwriting );
    }

    /**
     * Copies a file from the local file system into HDFS
     * @param inLocalFileSystem The file in the local file system to be written
     *                          to HDFS.
     * @param inHDFS The location of the file to be written to. This will contain
     *               a copy of the file in the local file system.
     * @param fs An HDFS file system object which will be (used to access HDFS)
     * @throws IOException
     */
    public static void copyFileFromLocalToHDFS( Path inLocalFileSystem,
                                                Path inHDFS,
                                                FileSystem fs )
            throws IOException {
        fs.copyFromLocalFile( inLocalFileSystem, inHDFS );
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
        copyFileFromLocalToHDFS( inLocalFileSystem, inHDFS, fs );
    }

    /**
     * Copies a file from HDFS to the local file system
     * @param inHDFS The location of the file in HDFS that will be copied to
     *               the local file system.
     * @param inLocalFileSystem The file in the local file system that will
     *                          contain a copy of the file in HDFS.
     * @param fs An HDFS file system object which will be (used to access HDFS)
     * @throws IOException
     */
    public static void copyFileFromHDFSToLocal( Path inHDFS,
                                                Path inLocalFileSystem,
                                                FileSystem fs )
            throws IOException {
        fs.copyToLocalFile( inHDFS, inLocalFileSystem );
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
        copyFileFromHDFSToLocal( inHDFS, inLocalFileSystem, fs );
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
        return getFilesAndDirectoriesInDirectory( dir, fs );
    }

    /**
     * Gets a list of Paths that contains all subdirectories in the directory in
     * question.
     * @param dir The directory whose subdirectories we shall get the list of.
     * @param fs The file system against which the path will be resolved
     * @return A list of Paths which point to the subdirectories of the input
     *         directory.
     * @throws IOException
     */
    public static List<Path> getSubdirectories( Path dir, FileSystem fs )
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
        return getSubdirectories( dir, fs );
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
        return fs.exists(fileLocation);
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
        return HDFSFileExists( fileLocation, fs );
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
    public static boolean isDir( Path path, FileSystem fs ) throws IOException {
        try {
            return fs.getFileStatus( path ).isDir();
        } catch( FileNotFoundException e ) {
            return false;
        }
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
        return isDir(path, fs);
    }


    /**
     * Checks the file to see if it was modified in the last (some number)
     * minutes.
     * @param path The location (file or directory) whose last modification
     *             time we should check
     * @param fs The file system against which to resolve the path
     * @param minutes The max allowed number of minutes since the last
     *                modification
     * @return True if the file was modified within the last (parameter: minutes)
     *         minutes.
     * @throws IOException
     */
    public static boolean fileWasModifiedInLastXMins( Path path,
                                                      FileSystem fs,
                                                      int minutes )
            throws IOException {
        long timeXMinutesAgo = System.currentTimeMillis() - (minutes * 60 * 1000);
        return (fs.getFileStatus(path).getAccessTime() > timeXMinutesAgo);
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
        return fileWasModifiedInLastXMins( path, fs, minutes );
    }

    /**
     * Returns the size of the indicated file, in bytes
     * @param path The location of the file in question. Note that if this is a
     *             directory, you'll get the size of the actual Unix file
     *             representing the directory, *not* the size of the sum of
     *             the contents.
     * @param fs The file system against which to resolve paths
     * @return The size in bytes of the entity at the indicated path
     * @throws IOException
     */
    public static long getFileSizeInBytes(Path path, FileSystem fs)
            throws IOException {
        return fs.getFileStatus( path ).getLen();
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
        return getFileSizeInBytes( path, fs );
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
