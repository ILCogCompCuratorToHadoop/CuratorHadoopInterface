package edu.cs.illinois.cogcomp.hadoopinterface;

import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.AnnotationMode;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.HadoopRecord;
import edu.illinois.cs.cogcomp.thrift.curator.Record;
import org.apache.hadoop.fs.Path;

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
public class HadoopCuratorClient {
    /**
     * Constructs a Curator Client.
     */
    public HadoopCuratorClient() {

    }

    /**
     * Requests an annotation from the indicated annotation tool (running on the
     * local node) for the indicated document record. Stores the result in this
     * object for later output through the writeOutputFromLastAnnotate() method.
     * @param record
     * @param toolToRun
     */
    public void annotateSingleDoc( HadoopRecord record,
                                   AnnotationMode toolToRun ) {
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
     *                     to annotate() should be written.
     * @TODO: Write this method
     */
    public void writeOutputFromLastAnnotate( Path docOutputDir ) {


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
     * @TODO: Write method
     */
    private Record deserializeHadoopRecord( HadoopRecord record ) {


        return new edu.illinois.cs.cogcomp.thrift.curator.Record();
    }

    /**
     * Takes a Curator-friendly Record (as might be returned by the annotation
     * tools) and writes it to the Hadoop Distributed File System. (If you wish,
     * it is easy to create a HadoopRecord based on the output directory.)
     * @param curatorRecord The Curator-friendly, Thrift-based Record to write
     *                      to HDFS.
     * @param docOutputDir The location in HDFS to which the Record should be
     *                     written.
     * @TODO: Write method
     */
    private void serializeCuratorRecord( Record curatorRecord,
                                        Path docOutputDir ) {
    }

    /**
     * Calls the specified annotation tool (assumed to be running locally on
     * this node) on the specified Record.
     * @param curatorRecord A Curator Record for the document to be annotated.
     * @param annotationToGet The type of annotation that should be requested
     *                        for the input Record. This must correspond to an
     *                        annotation tool currently running on the local node.
     * @return The Curator-friendly Record, modified to include the new
     *         annotation (assuming no errors, of course!).
     *
     * @TODO: Write method
     */
    private Record performAnnotation( Record curatorRecord,
                                     AnnotationMode annotationToGet ) {

        return new edu.illinois.cs.cogcomp.thrift.curator.Record();
    }

    private edu.illinois.cs.cogcomp.thrift.curator.Record lastAnnotatedRecord;
}
