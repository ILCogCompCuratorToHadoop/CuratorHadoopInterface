package edu.illinois.cs.cogcomp.hadoopinterface;

import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.AnnotationMode;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.HadoopSerializationHandler;
import edu.illinois.cs.cogcomp.thrift.base.AnnotationFailedException;
import edu.illinois.cs.cogcomp.thrift.base.ServiceUnavailableException;
import edu.illinois.cs.cogcomp.thrift.curator.Record;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;

import java.io.IOException;
import java.util.Arrays;

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
     * @param localFS A FileSystem object to handle interactions with the local
     *                file system
     */
    public HadoopCuratorClient( FileSystem localFS ) {
        super("local", PORT);

        this.localFS = localFS;
        transport = super.getTransport();

        serializer = new HadoopSerializationHandler();
    }

    /**
     * Requests an annotation from the indicated annotation tool (running on the
     * local node) for the indicated document record. Stores the result in this
     * object for later output through the writeOutputFromLastAnnotate() method.
     * @param record The document record on which we will run the annotation tool
     * @param toolToRun The type of annotation that we should get for the record
     */
	public void annotateSingleDoc( Record record,
                                   AnnotationMode toolToRun ) {
        try {
            // Ask the Curator to perform the annotation
            transport.open();
            annotate( record, toolToRun );
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
        Path fileLoc = getLocForSerializedForm( lastAnnotatedRecord, outputDir );
        serializer.serialize( lastAnnotatedRecord, fileLoc, localFS );
    }

    /**
     * Returns the location of the serialized form of the indicated record, which
     * depends on the location that it should be written to. At present, this will
     * always be a text file, named with the record's hash, within the containing
     * directory. However, this is subject to change. As an example, the current
     * structure simply contains a number of [document_hash].txt files within the
     * output directory.
     *
     * @param r The record in question
     * @param containingDir The directory containing this serialized record
     * @return The location at which the serialized form of the record should
     *         be found
     */
    private Path getLocForSerializedForm( Record r, Path containingDir ) {
        return new Path( containingDir, r.getIdentifier() + ".txt" );
    }

    private Record lastAnnotatedRecord;

    private FileSystem localFS;
    private TTransport transport;
    public static final int PORT = 9010;
    private HadoopSerializationHandler serializer;
}
