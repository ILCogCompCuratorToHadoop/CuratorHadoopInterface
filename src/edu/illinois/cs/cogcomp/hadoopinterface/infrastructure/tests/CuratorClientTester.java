package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.tests;

import edu.illinois.cs.cogcomp.hadoopinterface.CuratorClient;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.FileSystemHandler;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.RecordTools;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure
        .SerializationHandler;
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
        File outputDir = new File( "samplejob", "output" );

        if( !outputDir.isDirectory() ) {
            throw new IOException(outputDir.toString() + " is not a directory.");
        }

        SerializationHandler serializer = new SerializationHandler();
        for( File serializedRec : outputDir.listFiles() ) {
            // If it is a real serialized record . . .
            if( !serializedRec.isHidden() ) {
                System.out.println("Deserializing a record");
                serializer.deserializeFromFile( serializedRec );
            }
        }

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
        CuratorClient client = new CuratorClient("localhost", 9010);
        client.createRecordsFromRawInputFiles( in );

        int actualSize = client.getNumberOfInputRecords();
        if( actualSize != numFiles ) {
            throw new IOException("Failed to create the right number of records"
                + "from the raw text directory. There should have been "
                + Integer.toString(numFiles) + " but instead we found "
                + Integer.toString(actualSize) + "." );
        }

        FileSystemHandler.deleteLocal( dummyInputDir );

        // TODO: check the actual contents of the Records
    }

    @Test
    public static void generatesNewRecord() throws Exception {
        String test = "This is a test string to generate a record for.";
        Record r = RecordTools.generateNew( test );
        if( RecordTools.hasAnnotations( r ) ) {
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
        CuratorClient client = new CuratorClient("localhost", 9010);
        client.createRecordsFromRawInputFiles( in );

        File outDir = new File( dummyInputDir.toString(), "output" );
        client.writeSerializedRecords( outDir );

        Path outDirPath = new Path( outDir.toString() );
        int numOutputFiles =
                fsHandler.getFilesAndDirectoriesInDirectory(outDirPath ).size();
        if( numOutputFiles != numFiles ) {
            throw new IOException("Failed to serialize the right number of "
                    + "files. We found " + Integer.toString(numOutputFiles)
                    + " files, but we expected " + Integer.toString(numFiles) );
        }

        fsHandler.deleteLocal( dummyInputDir );
    }

    public static void main( String[] args ) throws Exception {
        fsHandler =
                new FileSystemHandler( FileSystem.getLocal(new Configuration()) );
        generatesNewRecord();

        localFS = FileSystem.getLocal( new Configuration() );
        dummyInputDir = new Path( "testjob" );
        DummyInputCreator.generateDocumentDirectories( dummyInputDir, localFS );

        generatesNewRecord();
        takesNewRawInputFilesFromDirectory();
        System.out.println("Successfully got new raw input files from directory.");
        writesSerializedInput();
        System.out.println("Successfully wrote serialized input.");

        deserializesARecord();
    }

    private static FileSystemHandler fsHandler;
    private static FileSystem localFS;
    private static Path dummyInputDir;
}
