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
 * It has the ability to serialize and deserialize Curator Records, for the
 * purpose of transfering them across file systems.
 *
 * @author Tyler Young
 */
public class HadoopCuratorClient {
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
    public Record deserializeHadoopRecord( HadoopRecord record ) {

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
     */
    public void serializeCuratorRecord( Record curatorRecord,
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
     */
    public Record performAnnotation( Record curatorRecord,
                                     AnnotationMode annotationToGet ) {

        return new edu.illinois.cs.cogcomp.thrift.curator.Record();
    }
}
