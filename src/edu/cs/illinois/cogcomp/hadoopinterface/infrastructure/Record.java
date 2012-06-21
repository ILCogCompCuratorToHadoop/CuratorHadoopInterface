package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;

import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import java.nio.file.*;

/**
 * A version of the Curator's document record, for use on the Hadoop Distributed
 * File System (HDFS), which does not rely on database calls.
 *
 * Implements WritableComparable so that it can be used as a key or value in a
 * MapReduce job.
 * @author Tyler Young
 * @author Lisa Bao
 */
public class Record implements WritableComparable< Record > {
    
    private String inputDir;
    private ArrayList annotations;

    /**
     * Constructs a record object
     * @param documentHash The hash for the document whose annotation this
     *                     record stores
     * @param fs A filesystem object with which this Record can access the
     *           Hadoop Distributed File System
     * @param config Hadoop Configuration for this job containing HDFS file path
     */
    public Record( String documentHash, FileSystem fs, Configuration config ) {
        this.fs = fs;
        this.documentHash = documentHash;
        this.config = config;
        inputDir = config.get("inputDirectory");
        annotations = new ArrayList();
    }

    /**
     * Gets the location in HDFS of a particular annotation for a document
     *
     * @param typeOfAnnotation The type of annotation to retrieve for the
     *                         document (chunking, parsing, named entity
     *                         recognition, etc.).
     * @return An HDFS path to the requested annotation
     */
    public Path getAnnotation( AnnotationMode typeOfAnnotation ) {
        String annotation = typeOfAnnotation.toString();
        Path path = Paths.get(inputDir, documentHash, annotation + ".txt");
        // NOTE: annotation filenames must conform to enumerated type names in all caps
        if (!annotations.contains(annotation)) {    
            System.out.println("Error: No existing annotation at this path!");
        }
        return path;
    }

    /**
     * Adds a particular annotation for a document. Will write the provided text
     * to HDFS.
     *
     * @param typeOfAnnotation The type of annotation to retrieve for the
     *                         document (chunking, parsing, named entity
     *                         recognition, etc.).
     * @param annotationBody The text of the annotation being provided
     */
    public void addAnnotation( AnnotationMode typeOfAnnotation,
                               String annotationBody ) {
        String annotation = typeOfAnnotation.toString();
        Path path = Paths.get(inputDir, documentHash, annotation + ".txt");
        File file = path.toFile();
        FileUtils.writeStringToFile(file, annotationBody);
        annotations.add(annotation);
    }

    /**
     * Adds an already-existing annotation for a document in HDFS
     * by copying it to the appropriate directory.
     *
     * @param typeOfAnnotation The type of annotation to retrieve for the
     *                         document (chunking, parsing, named entity
     *                         recognition, etc.).
     * @param annotationLocation The location in HDFS of the annotation being
     *                           added
     */
    public void addAnnotation( AnnotationMode typeOfAnnotation,
                               Path annotationLocation ) {
        String annotation = typeOfAnnotation.toString();
        Path source = annotationLocation;
        Path dest = Paths.get(inputDir, documentHash, annotation + ".txt");
        Files.copy(source, dest);
        annotations.add(annotation);
    }

    /**
     * Validates the required dependencies for a particular annotation.
     *
     * @param typeOfAnnotation The type of annotation to validate for the
     *                         document (chunking, parsing, named entity
     *                         recognition, etc.).
     */
    public bool checkDependencies( AnnotationMode typeOfAnnotation ) {
        String annotation = typeOfAnnotation.toString();
        bool valid = true;
        if (annotation == "CHUNK") {
            bool token = annotations.contains("TOKEN");
            bool pos = annotations.contains("POS");
            if ( !(token || pos) ) {
                valid = false;
            }
        }
        else if (annotation == "COREF") {
            bool token = annotations.contains("TOKEN");
            bool pos = annotations.contains("POS");
            bool ner = annotations.contains("NER");
            if ( !(token || pos || ner) ) {
                valid = false;
            }
        }
        else if ( annotation == ("NOM_SRL" || "VERB_SRL") ) {
            bool token = annotations.contains("TOKEN");
            bool pos = annotations.contains("POS");
            bool chunk = annotations.contains("CHUNK");
            book parse = annotations.contains("PARSE"); // Charniak parser
            if ( !(token || pos || chunk || parse) ) {
                valid = false;
            }
        }
        else if ( annotation == ("PARSE" || "POS") ) {
            bool token = annotations.contains("TOKEN");
            if (!token) {
                valid = false;
            }
        }
        else if (annotation == "WIKI") {
            bool token = annotations.contains("TOKEN");
            bool pos = annotations.contains("POS");
            bool chunk = annotations.contains("CHUNK");
            bool ner = annotations.contains("NER"); // Charniak parser
            if (!(token || pos || chunk || ner) ) {
                valid = false;
            }
        }
        // else: TOKEN has no dependencies
        return valid;
    }

    @Override
    public int compareTo( Record record )
    {
        return getDocumentHash().compareTo( record.getDocumentHash() );
    }

    @Override
    public void write( DataOutput out ) throws IOException
    {
        // TODO: Real writing
        out.writeUTF( "Test0 is " + test0 );
        out.writeUTF( "Test1 is " + test1 );
    }

    @Override
    public void readFields( DataInput in ) throws IOException
    {
        test0 = in.readUTF();
        test1 = in.readUTF();
    }

    /**
     * @return The hash identifying the document that this record describes
     */
    public String getDocumentHash()
    {
        return documentHash;
    }

    @Override
    public String toString() {
        return "Record for document whose ID is " + documentHash;
    }

    private String documentHash;

    private String test0;
    private String test1;
    private FileSystem fs;
}
