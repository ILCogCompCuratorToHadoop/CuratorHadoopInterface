package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

import edu.cs.illinois.cogcomp.hadoopinterface.HadoopInterface;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import static edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.AnnotationMode.*;
import static edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.FileSystemHandler.delete;
import static edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.FileSystemHandler.readFileFromHDFS;
import static edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.FileSystemHandler.writeFileToHDFS;

/**
 * A version of the Curator's document record, for use on the Hadoop Distributed
 * File System (HDFS), which does not rely on database calls.
 *
 * Implements WritableComparable so that it can be used as a key or value in a
 * MapReduce job.
 * @author Lisa Bao
 * @author Tyler Young
 */
public class HadoopRecord implements WritableComparable< HadoopRecord > {

    private HashSet<AnnotationMode> annotations; // Takes care of duplicates
    private Configuration config;
    private String documentHash;
    private FileSystem fs;
    private MessageLogger logger;
    private Path docDir;
    private boolean isInitialized;

    /**
     * Zero-argument constructor for use by the Hadoop backend. It calls this
     * constructor, then reads the fields in using the Writable interface.
     * (Within the readFields() method, we call the initializeAllVars() method
     * so that you wind up with a normal, filled object.)
     */
    public HadoopRecord() {
    }

    /**
     * Constructs a record object and fills it with all annotations for this
     * document which already exist on the Hadoop Distributed File System (HDFS).
     * @param documentHash The hash for the document whose annotation this
     *                     record stores
     * @param fs A filesystem object with which this Record can access the
     *           Hadoop Distributed File System
     * @param config Hadoop Configuration for this job containing HDFS file path
     */
    public HadoopRecord( String documentHash, FileSystem fs, Configuration config ) {
        initializeAllVars( documentHash, fs, config );
    }

    /**
     * Essentially the "real" constructor. Handles the initialization of all
     * variables. The reason for separating this from the actual constructor is
     * that the Writable interface works by creating new Record objects using
     * the zero-argument constructor, then passing the arguments later. A bit
     * of a pain.
     * @param documentHash The hash for the document whose annotation this
     *                     record stores
     * @param fs A filesystem object with which this Record can access the
     *           Hadoop Distributed File System
     * @param config Hadoop Configuration for this job containing HDFS file path
     */
    private void initializeAllVars( String documentHash, FileSystem fs,
                                    Configuration config ) {
        this.fs = fs;
        this.documentHash = documentHash;
        this.config = config;
        String inputDir = config.get("inputDirectory");
        docDir = new Path( inputDir + Path.SEPARATOR + documentHash );
        annotations = new HashSet<AnnotationMode>();
        initializeKnownAnnotations();

        logger = new MessageLogger( true );
        logger.log( "Initialized record for document with hash " + documentHash );

    }

    /**
     * Checks this document's directory for each possible annotation type, and
     * adds to the list of known annotations all annotation files that it finds
     * there.
     */
    private void initializeKnownAnnotations() {
        for( AnnotationMode mode : AnnotationMode.values() ) {
            try {
                if( annotationExistsOnDisk( mode ) ) {
                    informAnnotation( mode );
                }
            } catch (IOException e) {
                logger.logError("Error checking disk for extant annotations!");
            }
        }
        isInitialized = true;
    }

    /**
     * Gets the location in HDFS of a particular annotation for a document. If
     * the requested annotation doesn't exist, returns NULL.
     *
     * @param typeOfAnnotation The type of annotation to retrieve for the
     *                         document (chunking, parsing, named entity
     *                         recognition, etc.).
     * @return An HDFS path to the requested annotation, if and only if the
     *         annotation exists in the document directory. Else, returns a NULL
     *         pointer.
     */
    public Path getAnnotation( AnnotationMode typeOfAnnotation ) {
        // NOTE: annotation filenames must conform to enumerated type names in all caps
        if ( !annotations.contains( typeOfAnnotation ) ) {
            logger.logError("No existing annotation at this path!");
            return null;
        }

        return constructAnnotationPath( typeOfAnnotation );
    }

    /**
     * Gets what *would* be the annotation's location in HDFS if that annotation
     * exists. Doesn't care whether it actually exists or not.
     * @param typeOfAnnotation The annotation in question
     * @return A path containing that annotation's location
     *         (if it happens to exist)
     */
    private Path constructAnnotationPath( AnnotationMode typeOfAnnotation ) {
        return new Path( docDir, typeOfAnnotation.toString() + ".txt" );
    }

    /**
     * Returns a string version of the requested annotation file.
     * @param typeOfAnnotation The annotation type in question
     * @return The annotation's file, as read from HDFS. If this annotation
     *         doesn't exist in HDFS, returns the empty string.
     * @throws IOException
     */
    public String getAnnotationString( AnnotationMode typeOfAnnotation )
            throws IOException {
        if( annotationExists( typeOfAnnotation ) ) {
            return FileSystemHandler.
                    readFileFromHDFS( getAnnotation( typeOfAnnotation ), fs );
        }
        else {
            return "";
        }
    }

    /**
     * Gets the location in HDFS of the file containing the document's original
     * text.
     *
     * @return An HDFS path to the `original.txt` file.
     */
    public Path getOriginal() {
        return new Path( docDir, "original.txt" );
    }

    /**
     * Returns a string version of the document's original text
     * @return The `original.txt` file, as read from HDFS.
     * @throws IOException
     */
    public String getOriginalString() throws IOException {
        readFileFromHDFS( getOriginal(), fs );
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
        if ( annotations.contains( typeOfAnnotation ) ) {
            logger.logError( "This annotation already exists; not adding" );
            throw new IllegalArgumentException( "Annotation "
                    + typeOfAnnotation.toString() + " already exists for"
                    + " document with hash " + getDocumentHash() + ". "
                    + "Can't add a new one until you delete the old one." );
        }
        else {
            Path path = constructAnnotationPath( typeOfAnnotation );
            writeFileToHDFS( annotationBody, path, fs );
            annotations.add( typeOfAnnotation );
        }
    }

    /**
     * Adds an already-existing annotation for a document in HDFS
     * to the Record's arraylist of available annotations. This will add the
     * annotation regardless of whether we currently have the dependencies
     * for that annotation in HDFS.
     *
     * If we already know of this annotation, we silently accept it---informing of
     * an already-known annotation is silly, but it doesn't damage anything,
     * so we don't throw an error.
     *
     * @param typeOfAnnotation The type of annotation to retrieve for the
     *                         document (chunking, parsing, named entity
     *                         recognition, etc.).
     */
    public void informAnnotation( AnnotationMode typeOfAnnotation ) {
        annotations.add( typeOfAnnotation );
    }

	/**
	 * Removes an annotation and its corresponding HDFS file from the Record.
	 * For use when forcing Curator to reprocess an annotation regardless of cache.
	 *
     * @param typeOfAnnotation The type of annotation to retrieve for the
     *                         document (chunking, parsing, named entity
     *                         recognition, etc.).
	 */
	public void removeAnnotation( AnnotationMode typeOfAnnotation )
            throws IOException {
		if (!annotations.contains( typeOfAnnotation )) {
            logger.logError( "Annotation " + typeOfAnnotation.toString()
                            + " is not known to exist; nothing to remove" );
		}
		else {
		    Path path = constructAnnotationPath( typeOfAnnotation );
			delete(path, fs);
			annotations.remove( typeOfAnnotation );
		}
	}

    /**
     * Checks to see if a particular annotation for this document exists in HDFS
     * @param typeOfAnnotation The annotation type in question
     * @return TRUE if the annotation exists in the document directory,
     *         FALSE otherwise.
     */
    public boolean annotationExists( AnnotationMode typeOfAnnotation ) {
        return ( annotations.contains( typeOfAnnotation ) );
    }

    /**
     * Checks if the annotation exists on disk, regardless of whether we're aware
     * of it or not.
     * @param typeOfAnnotation The annotation in question
     * @return TRUE if the annotation exists on disk, false otherwise
     */
    private boolean annotationExistsOnDisk( AnnotationMode typeOfAnnotation )
            throws IOException {
        return FileSystemHandler.
                HDFSFileExists(constructAnnotationPath(typeOfAnnotation), fs);

    }

    /**
     * Gets a list of Paths which point to the annotations that are known to
     * exist for this document---that is, all annotations that the Master Curator
     * has seen fit to copy in to HDFS.
     * @return A list of paths to the annotations in the document's directory.
     */
    public List<Path> getKnownAnnotationLocations() {
        ArrayList<Path> knownAnnotations = new ArrayList<Path>();
        for( AnnotationMode mode : AnnotationMode.values() ) {
            if( annotationExists( mode ) ) {
                knownAnnotations.add( getAnnotation(mode) );
            }
        }
        return knownAnnotations;
    }

    /**
     * @return The directory in HDFS containing all this document's text files
     *         (the original.txt along with the annotations). This will simply
     *         be [job input directory]/[this document's hash]/.
     */
    public Path getDocumentDirectory() {
        return docDir;
    }


    /**
     * Prints a list of the available annotations
     * as stored in ArrayList annotations.
     */
    public void dumpAnnotationsToStdOut() {
        System.out.println( toString() );
    }

    /**
     * Validates the required dependencies for a particular annotation.
     *
     * @param typeOfAnnotation The type of annotation to validate for the
     *                         document (chunking, parsing, named entity
     *                         recognition, etc.).
     */
    public boolean checkDependencies( AnnotationMode typeOfAnnotation ) {
        HadoopInterface.logger.log( "Checking if document with hash "
                + getDocumentHash() + " satisfies the dependency requirements "
                + "for annotation type " + typeOfAnnotation.toString() );

        boolean valid = true;
        if ( typeOfAnnotation == CHUNK ) {
            boolean token = annotations.contains(TOKEN);
            boolean pos = annotations.contains(POS);
            if ( !(token || pos) ) {
                valid = false;
            }
        }
        else if ( typeOfAnnotation == COREF ) {
            boolean token = annotations.contains(TOKEN);
            boolean pos = annotations.contains(POS);
            boolean ner = annotations.contains(NER);
            if ( !(token || pos || ner) ) {
                valid = false;
            }
        }
        else if ( typeOfAnnotation == NOM_SRL || typeOfAnnotation == VERB_SRL ) {
            boolean token = annotations.contains(TOKEN);
            boolean pos = annotations.contains(POS);
            boolean chunk = annotations.contains(CHUNK);
            boolean parse = annotations.contains(PARSE); // Charniak parser
            if ( !(token || pos || chunk || parse) ) {
                valid = false;
            }
        }
        else if ( typeOfAnnotation == PARSE || typeOfAnnotation == POS ) {
            boolean token = annotations.contains(TOKEN);
            if (!token) {
                valid = false;
            }
        }
        else if ( typeOfAnnotation == WIKI ) {
            boolean token = annotations.contains(TOKEN);
            boolean pos = annotations.contains(POS);
            boolean chunk = annotations.contains(CHUNK);
            boolean ner = annotations.contains(NER); // Charniak parser
            if (!(token || pos || chunk || ner) ) {
                valid = false;
            }
        }
        // else: TOKEN has no dependencies
        return valid;
    }

    @Override
    public int compareTo( HadoopRecord record ) {
        logger.log( "Comparing rec for " + getDocumentHash()
                                    + "to rec for " + record.getDocumentHash() );
        return getDocumentHash().compareTo( record.getDocumentHash() );
    }

    @Override
    public void write( DataOutput out ) throws IOException {
        if( isInitialized ) {
            logger.log( "Writing data output for " + getDocumentHash() );
            String stringRep = getDocumentHash() + "\n";
            out.write( stringRep.getBytes() );
            // Have the configuration serialize its data
            config.write( out );
        }
    }

    @Override
    public void readFields( DataInput in ) throws IOException {
        String newDocumentHash = in.readLine();
        Configuration newConfig = new Configuration();
        newConfig.readFields( in );
        FileSystem newFileSystem = FileSystem.get( newConfig );
        if( newDocumentHash != null ) {
            initializeAllVars( newDocumentHash, newFileSystem, newConfig );
        }
    }

    /**
     * @return The hash identifying the document that this record describes
     */
    public String getDocumentHash() {
        return documentHash;
    }

    @Override
    public String toString() {
        // Record string is the document hash plus the known annotations
        String stringRep = "Document with hash " + getDocumentHash()
                           + " has annotations:\n";

        for( Path annotationPath : getKnownAnnotationLocations() ) {
            stringRep = stringRep + "\t\t - " + annotationPath.toString() + "\n";
        }
        return stringRep;
    }

    public Map<String, String> toMap() throws IOException {
        Map< String, String > mapVersion = new HashMap<String, String>();
        for( AnnotationMode annotation : annotations ) {
            mapVersion.put( annotation.toString(),
                            getAnnotationString( annotation ) );
        }
        mapVersion.put( "original", getOriginalString() );

        return mapVersion;
    }

    /**
     * Primarily for testing purposes. Specify the message logger to use for
     * logging messages.
     * @param logger The message logger to which we should output.
     */
    public void setMessageLogger( MessageLogger logger ) {
        this.logger = logger;
    }
} // THE END
