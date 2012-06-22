package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

//import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
//import java.nio.file.*;
import java.util.ArrayList;

import static edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.FileSystemHandler.*;


/**
 * A version of the Curator's document record, for use on the Hadoop Distributed
 * File System (HDFS), which does not rely on database calls.
 *
 * Implements WritableComparable so that it can be used as a key or value in a
 * MapReduce job.
 * @author Lisa Bao
 */
public class Record implements WritableComparable< Record > {
    
    private String inputDir;
    private ArrayList annotations;
    private Configuration config;
    private String documentHash;
    private String test0;
    private String test1;
    private FileSystem fs;

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
        Path path = new Path(inputDir + Path.SEPARATOR + documentHash + Path.SEPARATOR + annotation + ".txt");
        // NOTE: annotation filenames must conform to enumerated type names in all caps
        if (!annotations.contains(annotation)) {    
            System.out.println("Error: No existing annotation at this path!");
            // path = null;
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
                               String annotationBody ) throws IOException {
        String annotation = typeOfAnnotation.toString();
        if ( annotations.contains(annotation) ) {
            System.out.println("Error: This annotation already exists; not adding");
        }
        else {
            Path path = new Path(inputDir + Path.SEPARATOR + documentHash + Path.SEPARATOR + annotation + ".txt");
            writeFileToHDFS((String) annotationBody, (Path) path, (FileSystem) fs, (boolean) true);
            annotations.add(annotation);
        }
    }

    /**
     * Adds an already-existing annotation for a document in HDFS
     * to the Record's arraylist of available annotations.
     *
     * @param typeOfAnnotation The type of annotation to retrieve for the
     *                         document (chunking, parsing, named entity
     *                         recognition, etc.).
     */
    public void informAnnotation( AnnotationMode typeOfAnnotation ) {
        String annotation = typeOfAnnotation.toString();
        if (annotations.contains(annotation) ) {
            System.out.println("Error: This annotation already exists; not informing");
        }
        else {
            annotations.add(annotation);
        }
    }

	/**
	 * Removes an annotation and its corresponding HDFS file from the Record.
	 * For use when forcing Curator to reprocess an annotation regardless of cache.
	 *
     * @param typeOfAnnotation The type of annotation to retrieve for the
     *                         document (chunking, parsing, named entity
     *                         recognition, etc.).
	 */
	public void removeAnnotation( AnnotationMode typeofAnnotation ) {
	    String annotation = typeOfAnnotation.toString();
		if (!annotations.contains(annotation)) {
		    System.out.println("Error: This annotation does not exist; not removing");
		}
		else {
		    Path path = new Path(inputDir + Path.SEPARATOR + documentHash + Path.SEPARATOR + annotation + ".txt");
			delete(path, fs);
			annotations.remove(annotation);
		}
	}
	
    /**
     * Prints a list of the available annotations
     * as stored in ArrayList annotations.
     */
    public void listAnnotations() {
        for (int i = 0; i < annotations.size(); i++) {
            System.out.println(annotations.get(i) + " ");
        }
    }


    /**
     * Validates the required dependencies for a particular annotation.
     *
     * @param typeOfAnnotation The type of annotation to validate for the
     *                         document (chunking, parsing, named entity
     *                         recognition, etc.).
     */
    public boolean checkDependencies( AnnotationMode typeOfAnnotation ) {
        String annotation = typeOfAnnotation.toString();
        boolean valid = true;
        if ( annotation.equals("CHUNK") ) {
            boolean token = annotations.contains("TOKEN");
            boolean pos = annotations.contains("POS");
            if ( !(token || pos) ) {
                valid = false;
            }
        }
        else if ( annotation.equals("COREF") ) {
            boolean token = annotations.contains("TOKEN");
            boolean pos = annotations.contains("POS");
            boolean ner = annotations.contains("NER");
            if ( !(token || pos || ner) ) {
                valid = false;
            }
        }
        else if ( annotation.equals("NOM_SRL") || (annotation.equals("VERB_SRL") ) ) {
            boolean token = annotations.contains("TOKEN");
            boolean pos = annotations.contains("POS");
            boolean chunk = annotations.contains("CHUNK");
            boolean parse = annotations.contains("PARSE"); // Charniak parser
            if ( !(token || pos || chunk || parse) ) {
                valid = false;
            }
        }
        else if ( annotation.equals("PARSE") || (annotation.equals("POS") ) ) {
            boolean token = annotations.contains("TOKEN");
            if (!token) {
                valid = false;
            }
        }
        else if ( annotation.equals("WIKI") ) {
            boolean token = annotations.contains("TOKEN");
            boolean pos = annotations.contains("POS");
            boolean chunk = annotations.contains("CHUNK");
            boolean ner = annotations.contains("NER"); // Charniak parser
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

} // THE END
