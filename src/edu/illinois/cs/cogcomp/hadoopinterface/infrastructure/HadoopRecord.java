package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import edu.illinois.cs.cogcomp.thrift.base.Clustering;
import edu.illinois.cs.cogcomp.thrift.base.Forest;
import edu.illinois.cs.cogcomp.thrift.base.Labeling;
import edu.illinois.cs.cogcomp.thrift.base.View;
import edu.illinois.cs.cogcomp.thrift.curator.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A version of the Curator's document record, for use on the Hadoop Distributed
 * File System (HDFS), which does not rely on database calls.
 *
 * Implements WritableComparable so that it can be used as a key or value in a
 * MapReduce job.
 * @author Lisa Bao
 * @author Tyler Young
 */
public class HadoopRecord extends Record implements WritableComparable< Record > {

    private MessageLogger logger;
    private HadoopSerializationHandler serializer;

    /**
     * Zero-argument constructor for use by the Hadoop backend. It calls this
     * constructor, then reads the fields in using the Writable interface.
     * (Within the readFields() method, we call the initializeAllVars() method
     * so that you wind up with a normal, filled object.)
     */
    public HadoopRecord() {
        super();

        logger = new MessageLogger( true );
        serializer = new HadoopSerializationHandler();
    }

    /**
     * Constructs a record object and fills it with all annotations for this
     * document which already exist on the Hadoop Distributed File System (HDFS).
     * @param documentHash The hash for the document (i.e. hash.txt) 
     *                     whose annotations this record stores
     * @param fs A filesystem object with which this Record can access the
     *           Hadoop Distributed File System (HDFS)
     * @param config Hadoop Configuration for this job, containing HDFS file path
     */
    public HadoopRecord( String documentHash, FileSystem fs,
                         Configuration config )
            throws IOException {
        super();
        logger = new MessageLogger( true );

        try {
            initializeAllVars( documentHash, fs, config );
        } catch ( TException e ) {
            logger.logError("Error initializing Hadoop Record");
        }
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
                                    Configuration config )
            throws IOException, TException {
        Path doc = new Path( config.get( "inputDirectory" ),
                             documentHash + ".txt" );

        logger.logStatus( "Reconstructing this HadoopRecord from file "
                          + doc.toString() );
        serializer = new HadoopSerializationHandler();
        Record reconstructed = serializer.deserialize( doc, fs );

        configureThisFromOther( reconstructed );

        logger.log( "Initialized record for document with hash " + documentHash );
    }

    /**
     * Sets this record's original text, views, and all other properties to
     * those of the other record
     * @param other The Record whose properties we will copy
     */
    private void configureThisFromOther( Record other ) {
        setRawText( other.getRawText() );

        setClusterViews( other.getClusterViews() );
        setLabelViews( other.getLabelViews() );
        setParseViews( other.getParseViews() );
        setViews( other.getViews() );

        setIdentifier( other.getIdentifier() );
        setWhitespaced( other.isWhitespaced() );
    }

    /**
     * @return The hash identifying the document that this
     *         record describes
     */
    public String getDocumentHash() {
        return getIdentifier();
    }

    /**
     * Primarily for testing purposes. Specify the message logger to use for
     * logging messages.
     * @param logger The message logger to which we should output.
     */
    public void setMessageLogger( MessageLogger logger ) {
        this.logger = logger;
    }

    /**
     * Adds an annotation to the record. If that annotation type already exists
     * in the record, it will be silently overwritten.
     * @param type The type of annotation being added
     * @param annotation The Thrift data structure representing the annotation
     */
    public void addAnnotation( AnnotationMode type, TBase annotation ) {
        ViewType viewType = type.getViewType();
        String curatorAnnoType = type.toCuratorString();

        switch ( viewType ) {
            case PARSE:
                putToParseViews( curatorAnnoType, (Forest)annotation );
                break;
            case CLUSTER:
                putToClusterViews( curatorAnnoType, (Clustering)annotation );
                break;
            case LABEL:
                putToLabelViews( curatorAnnoType, (Labeling)annotation );
                break;
            case VIEW:
                putToViews( curatorAnnoType, (View)annotation );
                break;
        }
    }

    /**
     * Checks that this record provides all annotations required by a particular
     * annotation tool.
     * @param annotationToPerform The type of annotation to be performed on the
     *                            record.
     * @return True if this record contains all annotations on which the
     *         annotation to be performed depends, false otherwise.
     */
    public boolean meetsDependencyReqs( AnnotationMode annotationToPerform ) {
        return RecordTools.meetsDependencyReqs( this, annotationToPerform );
    }
	
    @Override
    public int compareTo( Record record ) {
        logger.log( "Comparing record for " + getDocumentHash()
                                    + "to record for " + record.getIdentifier() );
        return getDocumentHash().compareTo( record.getIdentifier() );
    }

    @Override
    public void write( DataOutput out ) throws IOException {
        if( !isSetWhitespaced() ) {
            setWhitespaced(false);
        }

        try {
            serializer.serializeToDataOutput( this, out );
        } catch ( TException e ) {
            logger.logError( "Thrift exception serializing Record "
                             + getIdentifier() );
        }
    }

    @Override
    public void readFields( DataInput in ) throws IOException {
        Record readVersion = new Record();

        try {
            readVersion = serializer.deserializeFromDataInput( in );
        } catch ( TException e ) {
            logger.logError( "Error deserializing record!" );
        }

        configureThisFromOther( readVersion );
    }

    @Override
    /**
     * Same functionality as getDocumentHash(), since the HadoopRecord cannot access
     * document annotations (only Thrift can).
     */
    public String toString() {
        return getDocumentHash();
    }
} // THE END!
