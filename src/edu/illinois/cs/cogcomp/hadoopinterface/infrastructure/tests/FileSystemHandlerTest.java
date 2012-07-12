package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.tests;

import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.FileSystemHandler;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.MessageLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.util.UUID;

/**
 * A tester for the FileSystemHandler.
 *
 * NOTE: Must be run from within Hadoop. Since Hadoop doesn't enable assertions
 * (or provide a straightforward way of doing so), we just throw errors where
 * we might otherwise make assertions.
 * @author Tyler Young
 */
public class FileSystemHandlerTest {
    public FileSystemHandlerTest() {
        logger = new MessageLogger( true );
    }

    public void createsDirsProperly() throws IOException {
        logger.logStatus( "Testing that it creates directories correctly." );

        FileSystem hdfs = FileSystem.get( new Configuration() );
        FileSystem localFS = FileSystem.getLocal( new Configuration( ) );

        FileSystemHandler fsHandler = new FileSystemHandler( hdfs );

        // Create parent dir on both local FS and HDFS
        Path parent = new Path( "input_"  + System.currentTimeMillis() );
        fsHandler.mkdirLocal( parent );
        fsHandler.mkdir( parent );



        FileStatus hdfsStat = hdfs.getFileStatus( parent );
        FileStatus localStat = localFS.getFileStatus( parent );
        logger.logStatus( "Perms are: " + Short.toString(hdfsStat.getPermission().toShort())
                + "\t" + hdfsStat.getPermission().toString() );
        logger.logStatus("Perms are: " + Short.toString(hdfsStat.getPermission().toShort()));

        if( !fsHandler.HDFSFileExists( parent ) ) {
            throw new IOException( "HDFS dir doesn't exist!" );
        }
        if( !fsHandler.isDir( parent ) ) {
            throw new IOException( "HDFS Dir isn't a directory!" );
        }
        if( !fsHandler.localFileExists( parent ) ) {
            throw new IOException( "Local dir doesn't exist!" );
        }
        if( !fsHandler.isDir( parent ) ) {
            throw new IOException( "Local dir isn't a directory!" );
        }
        if( !hdfsStat.getPermission().equals( new FsPermission("755") ) ) {
            throw new IOException( "Bad permissions on the HDFS directory!\n"
                + "Perms are: " + Short.toString(hdfsStat.getPermission().toShort())
                + "\t" + hdfsStat.getPermission().toString() );
        }
        if( !localStat.getPermission().equals( new FsPermission( "755" ) ) ) {
            throw new IOException( "Bad permissions on the local directory!\n"
                    + "Perms are: " + Short.toString(hdfsStat.getPermission().toShort()) );
        }
    }

    public void readsAndWritesFilesCorrectly() throws IOException {
        logger.logStatus( "Testing that it reads and writes files correctly." );

        // Write file to HDFS
        FileSystem fs = FileSystem.get( new Configuration() );
        FileSystemHandler fsHandler = new FileSystemHandler(fs);

        // Create parent dir on both local FS and HDFS
        Path parent = new Path( "input_"  + System.currentTimeMillis() );
        fsHandler.mkdirLocal( parent );
        fsHandler.mkdir( parent );

        Path inputPath = new Path( parent, "bogus.txt" );

        String randomString = UUID.randomUUID().toString()
                + UUID.randomUUID().toString()
                + UUID.randomUUID().toString()
                + UUID.randomUUID().toString();

        fsHandler.writeFileToHDFS( randomString, inputPath, false);
        fsHandler.writeFileToLocal( randomString, inputPath);

        logger.log("The file is here: " + inputPath.toString());

        // Read it back
        String readVersion = fsHandler.readFileFromHDFS( inputPath );
        String readVersionLocal = fsHandler.readFileFromLocal( inputPath );
        if( !readVersion.equals( "\n" + randomString + "\n" )) {
            throw new IOException( "Read version of HDFS file doesn't match written!\n"
                + "Read: " + readVersion + " Wrote: " + randomString );
        }
        if( !readVersionLocal.equals( readVersion) ) {
            throw new IOException( "Read version of local file doesn't match written!" );
        }
    }

    public void properlyCopiesFilesAround() throws IOException {
        logger.logStatus( "Testing that it copies files around correctly." );

        Path inputPath = new Path( "input_"  + System.currentTimeMillis() );
        FileSystem fs = FileSystem.get( new Configuration() );
        FileSystemHandler fsHandler = new FileSystemHandler(fs);
        DummyInputCreator.createDocumentDirectory( inputPath, fs );

        // Copy file from HDFS to local
        Path origTxt = new Path( inputPath, "original.txt" );
        Path origTxtLocal = new Path( "originalFromHDFS.txt" );
        fsHandler.copyFileFromHDFSToLocal( origTxt, origTxtLocal );

        String HDFSVersion = fsHandler.readFileFromHDFS( origTxt );
        String localVersion = fsHandler.readFileFromLocal( origTxtLocal );
        if( !HDFSVersion.equals(localVersion) ) {
            throw new IOException( "HDFS version doesn't match local!" );
        }

        // Copy file from local to HDFS
        Path test = new Path( "local.txt" );
        Path testInHDFS = new Path( "local.txt" );
        fsHandler.writeFileToLocal( "lorem ipsum dolar sit amet", test);
        fsHandler.copyFileFromLocalToHDFS( test, testInHDFS );

        HDFSVersion = fsHandler.readFileFromHDFS( testInHDFS );
        localVersion = fsHandler.readFileFromLocal( test );
        if( !HDFSVersion.equals(localVersion) ) {
            throw new IOException( "HDFS version doesn't match local!" );
        }
    }

    public void properlyGetsFileNamesFromPathObjects() throws IOException {
        logger.logStatus( "Testing that it properly gets filenames from HDFS paths." );

        // Write file to HDFS
        FileSystem fs = FileSystem.get( new Configuration() );

        Path inputPath = new Path( "input_"  + System.currentTimeMillis()
                                   + Path.SEPARATOR + "bogus.txt" );

        // Read it back
        String readVersion = FileSystemHandler.getFileNameFromPath(inputPath);
        if( !readVersion.equals( "bogus.txt" )) {
            throw new IOException( "File name doesn't match original" );
        }
    }

    public void knowsWhatsADirectoryAndWhatIsnt() throws IOException {
        logger.logStatus( "Testing that it know what's a directory." );

        FileSystem fs = FileSystem.get( new Configuration() );
        FileSystemHandler fsHandler = new FileSystemHandler(fs);

        Path p = new Path( "input_"  + System.currentTimeMillis() );
        fsHandler.mkdir( p );

        if( !fsHandler.isDir( p )) {
            throw new IOException( "Doesn't think path " + p.toString() +
                                   " is a real directory!" );
        }
    }

    public void getsRightNumOfFilesAndDirectories() throws IOException {
        logger.logStatus( "Testing that it gets the right number of files and dirs." );

        Path p = new Path( "input_"  + System.currentTimeMillis() );
        FileSystem fs = FileSystem.get( new Configuration() );
        FileSystemHandler fsHandler = new FileSystemHandler(fs);
        fs.mkdirs( p );

        Path file = new Path( p, "bogus.txt" );
        Path dir = new Path( p, "some_dir" );

        fs.mkdirs(dir);

        FSDataOutputStream dos = fs.create( file, true);

        String randomString = UUID.randomUUID().toString()
                + UUID.randomUUID().toString()
                + UUID.randomUUID().toString()
                + UUID.randomUUID().toString();

        dos.writeChars(randomString);
        dos.close();

        logger.log( "Contents of directory are: "
                + fsHandler.getFilesAndDirectoriesInDirectory( p ).toString() );


        if( fsHandler.getFilesAndDirectoriesInDirectory( p ).size() != 2 ) {
            throw new IOException("Can't count!");
        }
    }

    public static void main( String[] args ) throws IOException {
        FileSystemHandlerTest tester = new FileSystemHandlerTest();

        tester.createsDirsProperly();
        tester.readsAndWritesFilesCorrectly();
        tester.properlyCopiesFilesAround();
        tester.properlyGetsFileNamesFromPathObjects();
        tester.knowsWhatsADirectoryAndWhatIsnt();
        tester.getsRightNumOfFilesAndDirectories();
    }

    MessageLogger logger;
}
