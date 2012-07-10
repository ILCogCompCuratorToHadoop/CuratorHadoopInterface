package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.tests;

import edu.illinois.cs.cogcomp.hadoopinterface.CuratorClient;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.FileSystemHandler;
import edu.illinois.cs.cogcomp.thrift.curator.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runners.model.InitializationError;

import java.io.File;
import java.io.IOException;

/**
 * A tester class for the CuratorClient. Focuses on the client's handling of
 * Records, especially serialization of them.
 * @author Tyler Young
 */
public class CuratorClientTester {
    public CuratorClientTester() {

    }

    @Test
    public static void deserializesARecord() throws Exception {
        CuratorClient.deserializeRecord()

    }

    @Test
    public static void addsRecordsFromJobDirectory() throws Exception {
        // TODO: Write this test once we have real records

    }

    @Test
    public static void takesNewRawInputFilesFromDirectory() throws IOException {
        int numFiles = 10;
        DummyInputCreator.createRawTextInputDirectory( dummyInputDir,
                                                       localFS,
                                                       numFiles );
        File in = new File( dummyInputDir.toString() );
        CuratorClient.createRecordsFromRawInputFiles( in );

        int actualSize = CuratorClient.getInputList().size();
        if( actualSize != numFiles ) {
            throw new IOException("Failed to create the right number of records"
                + "from the raw text directory. There should have been "
                + Integer.toString(numFiles) + " but instead we found "
                + Integer.toString(actualSize) + "." );
        }

        FileSystemHandler.delete( dummyInputDir, localFS );

        // TODO: check the actual contents of the Records
    }

    @Test
    public static void generatesNewRecord() throws Exception {
        String test = "This is a test string to generate a record for.";
        Record r = CuratorClient.generateNewRecord(test);
        if( CuratorClient.recordHasAnnotations( r ) ) {
            throw new InitializationError( "Record wrongly claims to "
                                            + "have annotations." );
        }
        if( !r.getRawText().equals( test ) ) {
            throw new InitializationError( "Record's raw text is wrong." );
        }
    }

    @Test
    public static void writesSerializedInput() throws Exception {
        int numFiles = 10;
        DummyInputCreator.createRawTextInputDirectory( dummyInputDir,
                localFS,
                numFiles );

        File in = new File( dummyInputDir.toString() );
        CuratorClient.createRecordsFromRawInputFiles( in );

        File outDir = new File( dummyInputDir.toString(), "output" );
        CuratorClient.writeSerializedRecords( outDir );

        Path outDirPath = new Path( outDir.toString() );
        int numOutputFiles = FileSystemHandler.getFilesAndDirectoriesInDirectory(
                outDirPath, localFS ).size();
        if( numOutputFiles != numFiles ) {
            throw new IOException("Failed to serialize the right number of "
                    + "files. We found " + Integer.toString(numOutputFiles)
                    + " files, but we expected " + Integer.toString(numFiles) );
        }

        FileSystemHandler.delete( dummyInputDir, localFS );
    }

    public static void main( String[] args ) throws Exception {
        generatesNewRecord();

        localFS = FileSystem.getLocal( new Configuration() );
        dummyInputDir = new Path( "samplejob" );
        DummyInputCreator.generateDocumentDirectories( dummyInputDir, localFS );

        generatesNewRecord();
        takesNewRawInputFilesFromDirectory();
        System.out.println("Successfully got new raw input files from directory.");
        CuratorClient.clearInputList();
        writesSerializedInput();
        System.out.println("Successfully wrote serialized input.");

        FileSystemHandler.delete( dummyInputDir, localFS );
    }

    private static FileSystem localFS;
    private static Path dummyInputDir;
}
