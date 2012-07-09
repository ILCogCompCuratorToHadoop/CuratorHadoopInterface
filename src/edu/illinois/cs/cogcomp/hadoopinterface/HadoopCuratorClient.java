package edu.illinois.cs.cogcomp.hadoopinterface;

import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.AnnotationMode;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.HadoopRecord;
import edu.illinois.cs.cogcomp.thrift.base.AnnotationFailedException;
import edu.illinois.cs.cogcomp.thrift.base.ServiceUnavailableException;
import edu.illinois.cs.cogcomp.thrift.curator.Curator;
import edu.illinois.cs.cogcomp.thrift.curator.Record;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.FileSystemHandler.writeFileToHDFS;

/**
 * Similar in functionality to the CuratorClient.java (packaged with the Curator,
 * in the `curator-clients` directory), this class simply provides a way to
 * "talk" to a locally running Curator.
 *
 * This class is designed to annotate exactly one document at a time. The
 * expected use pattern goes like this:
 *
 *   1. Create a HadoopCuratorClient object
 *   2. Call annotateSingleDoc() on your document record. The Curator Client will
 *      handle all the type conversions for the record and ask the locally
 *      running Curator to annotate it.
 *   3. Call writeOutputFromLastAnnotate() to write that new annotation to the
 *      Hadoop Distributed File System (HDFS).
 *
 * Note that it is *your* responsibility to make sure there is a Curator running
 * on this node, and that the annotation tool you want to use is running on
 * this node and is able to communicate with your Curator.
 *
 * @author Tyler Young
 */
public class HadoopCuratorClient extends CuratorClient {
    /**
     * Constructs a Curator Client.
     */
    public HadoopCuratorClient( FileSystem hdfs, FileSystem local ) {
        this.hdfs = hdfs;
        this.localFS = local;

        // Create a Thrift transport
        transport = new TSocket( "localhost", PORT );
        // Use a framed transport so that we have a non-blocking server
        transport = new TFramedTransport( transport );
        // Define a protocol which will use the transport
        TProtocol protocol = new TBinaryProtocol( transport );
        // Create the client
        client = new Curator.Client( protocol );
    }

    /**
     * Requests an annotation from the indicated annotation tool (running on the
     * local node) for the indicated document record. Stores the result in this
     * object for later output through the writeOutputFromLastAnnotate() method.
     * @param record The HadoopRecord on which we will run the annotation tool
     * @param toolToRun The type of annotation that we should get for the record
     */
		public void annotateSingleDoc( HadoopRecord record,
                                   AnnotationMode toolToRun )
            throws IOException, TException {
        // De-serialize the Hadoop Record
        Record curatorFriendlyRec = deserializeHadoopRecord( record );

        // Call performAnnotation() on the de-serialized record
        lastAnnotatedRecord = performAnnotation( curatorFriendlyRec, toolToRun );
    }

    /**
     * Writes the results of the last call to annotate() to the specified
     * directory. This is equivalent to serializing the results of a call to
     * performAnnotation() to the directory.
     * @param docOutputDir The directory to which the results of the last call
     *                     to annotate() should be written. This directory will
     *                     most likely be named with the document's hash, and it
     *                     will likely be a subdirectory of the overall job
     *                     output directory.
     */
    public void writeOutputFromLastAnnotate( Path docOutputDir ) throws TException {
        serializeCuratorRecord( lastAnnotatedRecord, docOutputDir, localFS );
    }

    /**
     * Calls the specified annotation tool (assumed to be running locally on
     * this node) on the specified Record.
     * @param curatorRecord A Curator Record for the document to be annotated.
     * @param annotationToGet The type of annotation that should be requested
     *                        for the input Record. This must correspond to an
     *                        annotation tool currently running on the local node.
     * @return The Curator-friendly Record, modified to include the new
     *         annotation (assuming no errors, of course!). If there was an error
     *         thrown, this Record may contain no new annotations; in this case,
     *         you will get back the same Record you passed in, so it's up to you
     *         to check that you have the requested annotation.
     */
    private Record performAnnotation( Record curatorRecord,
                                      AnnotationMode annotationToGet ) {
        try {
            // Ask the Curator to perform the annotation
            transport.open();
            String annotationMode =
                    AnnotationMode.toCuratorString(annotationToGet);

            client.performAnnotation( curatorRecord, annotationMode, true );
        } catch (ServiceUnavailableException e) {
            HadoopInterface.logger.logError( annotationToGet.toString()
                    + " annotations are not available.\n" + e.getReason());
        } catch (TException e) {
            HadoopInterface.logger.logError( "Transport exception when getting "
                    + annotationToGet.toString() + " annotation.\n"
                    + Arrays.toString( e.getStackTrace() ) );
        } catch (AnnotationFailedException e) {
            HadoopInterface.logger.logError( "Failed attempting annotation "
                    + annotationToGet.toString() + ".\n" + e.getReason());
        } finally {
            if( transport.isOpen() ) {
                transport.close();
            }
        }

        // Should contain all of the previous annotations, along with
        // the new one
        return curatorRecord;
    }

    /**
     * Takes a Curator-friendly Record (as might be returned by the annotation
     * tools) and writes it to the Hadoop Distributed File System. (If you wish,
     * it is easy to later create a HadoopRecord based on the output directory.)
     * @param curatorRecord The Curator-friendly, Thrift-based Record to write
     *                      to HDFS.
     * @param docOutputDir The location in HDFS to which the Record should be
     *                     written.
     * @param fs The filesystem against which the path will be resolved
     */
    private void serializeCuratorRecord( Record curatorRecord,
                                         Path docOutputDir,
                                         FileSystem fs ) throws TException {
        Map<String, String> recordAsText;
        recordAsText = CuratorClient.serializeRecord( curatorRecord );

        for( String key : recordAsText.keySet() ) {
            try {
                writeFileToHDFS( recordAsText.get(key),
                        new Path( docOutputDir, key + ".txt" ),
                        fs );
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Reads a number of text files in from the Hadoop File System (the location
     * of which is provided by the HadoopRecord that is passed in) and turns them
     * into a Curator-friendly Record.
     * @param record The HadoopRecord that should be converted to a
     *               Curator-friendly, Thrift-based Record.
     * @return A Thrift/Curator Record containing the annotations (and original
     *         text, of course) that were present in the HDFS version of
     *         the record.
     */
    private Record deserializeHadoopRecord( HadoopRecord record )
            throws IOException, TException {
        return CuratorClient.deserializeRecord( record.toMap() );
    }

    private edu.illinois.cs.cogcomp.thrift.curator.Record lastAnnotatedRecord;

    private FileSystem hdfs;
    private FileSystem localFS;
    private Curator.Client client;
    private TTransport transport;
    public static final int PORT = 9010;
}
