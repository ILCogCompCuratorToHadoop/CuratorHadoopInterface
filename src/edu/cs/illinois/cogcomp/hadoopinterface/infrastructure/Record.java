package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A version of the Curator's document record for use on the Hadoop Distributed
 * File System (HDFS) which does not rely on database calls.
 *
 * Implements WritableComparable so that it can be used as a key or value in a
 * MapReduce job.
 * @author Tyler Young
 */
public class Record implements WritableComparable< Record > {
    /**
     * Constructs a record object
     * @param documentHash The hash for the document whose annotation this
     *                     record stores
     */
    public Record( String documentHash ) {
        this.documentHash = documentHash;

    }

    /**
     * Gets the location in HDFS of a particular annotation for a document
     *
     * @TODO: Fill this method in
     * @param typeOfAnnotation The type of annotation to retrieve for the
     *                         document (chunking, parsing, named entity
     *                         recognition, etc.).
     * @return An HDFS path to the requested annotation
     */
    public Path getAnnotation( AnnotationMode typeOfAnnotation ) {

        return new Path("");
    }

    /**
     * Adds a particular annotation for a document. Will write the provided text
     * to HDFS.
     *
     * @TODO: Fill this method in
     * @param typeOfAnnotation The type of annotation to retrieve for the
     *                         document (chunking, parsing, named entity
     *                         recognition, etc.).
     * @param annotationBody The text of the annotation being provided
     */
    public void addAnnotation( AnnotationMode typeOfAnnotation,
                               String annotationBody ) {

    }

    /**
     * Adds a particular annotation for a document for a file that already exists
     * in HDFS.
     *
     * @TODO: Fill this method in
     * @param typeOfAnnotation The type of annotation to retrieve for the
     *                         document (chunking, parsing, named entity
     *                         recognition, etc.).
     * @param annotationLocation The location in HDFS of the annotation being
     *                           added
     */
    public void addAnnotation( AnnotationMode typeOfAnnotation,
                               Path annotationLocation ) {

    }

    @Override
    public int compareTo( Record record )
    {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    private String documentHash;
}
