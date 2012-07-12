package edu.illinois.cs.cogcomp.hadoopinterface;

import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.AnnotationMode;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.SerializationHandler;
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
        super();
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

        serializationHandler = new SerializationHandler();
    }

    /**
     * Requests an annotation from the indicated annotation tool (running on the
     * local node) for the indicated document record. Stores the result in this
     * object for later output through the writeOutputFromLastAnnotate() method.
     * @param record The document record on which we will run the annotation tool
     * @param toolToRun The type of annotation that we should get for the record
     */
	public void annotateSingleDoc( Record record,
                                   AnnotationMode toolToRun )
            throws IOException, TException {
        try {
            // Ask the Curator to perform the annotation
            transport.open();
            String annotationMode =
                    AnnotationMode.toCuratorString( toolToRun );

            client.annotate( record, annotationMode );
        } catch (ServiceUnavailableException e) {
            HadoopInterface.logger.logError( toolToRun.toString()
                    + " annotations are not available.\n" + e.getReason());
        } catch (TException e) {
            HadoopInterface.logger.logError( "Transport exception when getting "
                    + toolToRun.toString() + " annotation.\n"
                    + Arrays.toString( e.getStackTrace() ) );
        } catch (AnnotationFailedException e) {
            HadoopInterface.logger.logError( "Failed attempting annotation "
                    + toolToRun.toString() + ".\n" + e.getReason());
        } finally {
            if( transport.isOpen() ) {
                transport.close();
            }
        }

        // Should contain all of the previous annotations, along with
        // the new one
        lastAnnotatedRecord = record;
    }

    /**
     * Writes the results of the last call to annotate() to the specified
     * directory in HDFS. This is equivalent to serializing the results of a call to
     * annotate() to the directory.
     * @param outputDir The directory to which the results of the last call
     *                  to annotate() should be written. Each serialized document
     *                  should be named with the document's hash.
     */
    public void writeOutputFromLastAnnotate( Path outputDir )
            throws TException, IOException {
        serializeCuratorRecord( lastAnnotatedRecord, outputDir, localFS );
    }

    /**
     * Takes a Curator-friendly Record (as might be returned by the annotation
     * tools) and writes it to the Hadoop Distributed File System. (If you wish,
     * it is easy to later create a HadoopRecord based on the output directory.)
     * @param curatorRecord The Curator-friendly, Thrift-based Record to write
     *                      to HDFS.
     * @param outputDir The location in HDFS to which the Record should be
     *                  written.
     * @param fs The filesystem against which the path will be resolved
     */
    private void serializeCuratorRecord( Record curatorRecord,
                                         Path outputDir,
                                         FileSystem fs )
            throws TException, IOException {
        byte[] recordAsBytes = serializationHandler.serialize( curatorRecord );
		String docName = curatorRecord.getIdentifier();
		
        try {
            writeFileToHDFS( recordAsBytes,
                             new Path( outputDir, docName.concat(".txt") ),
                             fs );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Record lastAnnotatedRecord;

    private FileSystem hdfs;
    private FileSystem localFS;
    private Curator.Client client;
    private TTransport transport;
    public static final int PORT = 9010;
    private SerializationHandler serializationHandler;
}
