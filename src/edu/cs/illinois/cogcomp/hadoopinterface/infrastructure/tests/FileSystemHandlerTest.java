package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.tests;

import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.FileSystemHandler;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.MessageLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.UUID;

/**
 * @author Tyler Young
 */
public class FileSystemHandlerTest {
    public FileSystemHandlerTest() {
        logger = new MessageLogger( true );
    }

    public void getsFileNamesCorrectly() throws IOException {
        logger.logStatus( "Testing that it gets file names correctly." );

        // Write file to HDFS
        FileSystem fs = FileSystem.get( new Configuration() );

        Path inputPath = new Path( "input_"  + System.currentTimeMillis()
                + Path.SEPARATOR + "bogus.txt" );
        FSDataOutputStream dos = fs.create( inputPath, true);

        String randomString = UUID.randomUUID().toString()
                + UUID.randomUUID().toString()
                + UUID.randomUUID().toString()
                + UUID.randomUUID().toString();

        dos.writeChars(randomString);
        dos.close();

        logger.log("The file is here: " + inputPath.toString() );

        // Read it back
        String readVersion = FileSystemHandler.readFileFromHDFS( inputPath,
                fs, true );
        assert( readVersion.equals( randomString ));
    }

    public void properlyReadsFilesFromHDFS() throws IOException {
        logger.logStatus( "Testing that it properly reads from HDFS." );

        // Write file to HDFS
        FileSystem fs = FileSystem.get( new Configuration() );

        Path inputPath = new Path( "input_"  + System.currentTimeMillis()
                                   + Path.SEPARATOR + "bogus.txt" );

        // Read it back
        String readVersion = FileSystemHandler.getFileNameFromPath(inputPath);
        assert( readVersion.equals( "bogus.txt" ));
    }

    public void knowsWhatsADirectoryAndWhatIsnt() throws IOException {
        logger.logStatus( "Testing that it know what's a directory." );

        FileSystem fs = FileSystem.get( new Configuration() );

        Path p = new Path( "input_"  + System.currentTimeMillis() );
        fs.create( p );

        assert( FileSystemHandler.isDir( p, fs ));
        assert( FileSystemHandler.isDir( p.toString(), fs ));
    }

    public void getsRightNumOfFilesAndDirectories() throws IOException {
        logger.logStatus( "Testing that it gets the right number of files and dirs." );

        Path p = new Path( "input_"  + System.currentTimeMillis() );
        FileSystem fs = FileSystem.get( new Configuration() );
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
                + FileSystemHandler.
                getFilesAndDirectoriesInDirectory(p, fs).toString() );


        assert( FileSystemHandler.
                getFilesAndDirectoriesInDirectory(p, fs).size() == 2 );
    }

    public static void main( String[] args ) throws IOException {
        FileSystemHandlerTest tester = new FileSystemHandlerTest();

        tester.getsFileNamesCorrectly();
        tester.properlyReadsFilesFromHDFS();
        tester.knowsWhatsADirectoryAndWhatIsnt();
        tester.getsRightNumOfFilesAndDirectories();
    }

    MessageLogger logger;
}
