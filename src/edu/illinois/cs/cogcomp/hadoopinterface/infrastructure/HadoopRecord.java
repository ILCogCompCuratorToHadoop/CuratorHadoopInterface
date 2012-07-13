package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import edu.illinois.cs.cogcomp.hadoopinterface.HadoopInterface;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.exceptions.*;
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
import java.io.EOFException;
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

    private Configuration config;
    private FileSystem fs;
    private MessageLogger logger;
    private Path doc;
    private SerializationHandler serializer;

    /**
     * Zero-argument constructor for use by the Hadoop backend. It calls this
     * constructor, then reads the fields in using the Writable interface.
     * (Within the readFields() method, we call the initializeAllVars() method
     * so that you wind up with a normal, filled object.)
     */
    public HadoopRecord() {
        super();

        serializer = new SerializationHandler();
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
    public HadoopRecord( String documentHash, FileSystem fs, Configuration config ) {
        super();

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
        FileSystemHandler fsHandler = new FileSystemHandler( fs );

        this.config = config;
        doc = new Path( config.get("inputDirectory"), documentHash + ".txt" );

        logger = new MessageLogger( true );

        serializer = new SerializationHandler();

        byte[] byteForm = new byte[0];
        try {
            byteForm = fsHandler.readBytesFromHDFS( doc );
            configureFromSerialized( byteForm );
        } catch ( IOException e ) {
            logger.logError( "IOException reading serial form of document "
                                     + documentHash + " from HDFS." );
            logger.logError( e.getMessage() );
        }

        logger.log( "Initialized record for document with hash " + documentHash );
    }

    /**
     * Sets up this object using the annotations (views) from the serialized form
     * of the Record that this object should mirror. After this executes, this
     * Record should have all cluster, label, and parse views that the original
     * had.
     * @param byteForm The serialized form of the record that this one will mirror
     */
    private void configureFromSerialized( byte[] byteForm )
            throws EmptyInputException {
        try {
            Record master = serializer.deserialize( byteForm );
            setClusterViews( master.getClusterViews() );
            setLabelViews( master.getLabelViews() );
            setParseViews( master.getParseViews() );
            setViews( master.getViews() );

            setIdentifier( master.getIdentifier() );
            setWhitespaced( master.isWhitespaced() );
        } catch ( TException e ) {
            if( logger == null ) {
                logger = HadoopInterface.logger;
            }
            logger.logError("Thrift error in deserializing Record for a document"
                                     + " from HDFS." );

        }
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
            if( serializer == null ) {
                serializer = new SerializationHandler();
            }
            out.write( serializer.serialize(this) );
        } catch ( TException e ) {
            e.printStackTrace();
        }
    }

    @Override
    public void readFields( DataInput in ) throws IOException {
        byte[] serializedForm = new byte[10240];

        int i = 0;
        int b = in.readByte();

        while ( b != -1 ) {
            serializedForm[i++] = (byte) b;

            try {
                b = in.readByte();
            } catch( EOFException e ) {
                b = -1;
            }

            if( i == serializedForm.length ) {
                byte[] newBuffer = new byte[serializedForm.length * 2];
                System.arraycopy(serializedForm, 0, newBuffer, 0, serializedForm.length);
                serializedForm = newBuffer;
            }
        }

        configureFromSerialized( serializedForm );
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
