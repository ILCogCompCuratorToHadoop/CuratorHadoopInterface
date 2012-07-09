package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.tests;

import edu.illinois.cs.cogcomp.hadoopinterface.CuratorClient;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.FileSystemHandler;
import edu.illinois.cs.cogcomp.thrift.curator.Record;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runners.model.InitializationError;

import java.io.IOException;

/**
 * A tester class for the CuratorClient. Focuses on the client's handling of
 * Records, especially serialization of them.
 * @author Tyler Young
 */
public class CuratorClientTester extends TestCase {
    public CuratorClientTester() {

    }

    @Test
    public static void serializesAndDeserializesARecord() throws Exception {
        // TODO: Write this with real record data??

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
        CuratorClient.takeNewRawInputFilesFromDirectory(dummyInputDir.toString());

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

        CuratorClient.takeNewRawInputFilesFromDirectory(dummyInputDir.toString());

        Path outDir = new Path( dummyInputDir, "output" );
        CuratorClient.writeSerializedInput( outDir.toString() );

        int numOutputFiles = FileSystemHandler.getFilesAndDirectoriesInDirectory(
                outDir, localFS ).size();
        if( numOutputFiles != numFiles ) {
            throw new IOException("Failed to serialize the right number of files.");
        }

        FileSystemHandler.delete( dummyInputDir, localFS );
    }

    public static void main( String[] args ) throws Exception {
        generatesNewRecord();

        localFS = FileSystem.getLocal( new Configuration() );
        dummyInputDir = new Path( "testJob123" );
        DummyInputCreator.generateDocumentDirectories( dummyInputDir, localFS );

        generatesNewRecord();
        takesNewRawInputFilesFromDirectory();
        writesSerializedInput();

        FileSystemHandler.delete( dummyInputDir, localFS );
    }

    private static FileSystem localFS;
    private static Path dummyInputDir;
}
