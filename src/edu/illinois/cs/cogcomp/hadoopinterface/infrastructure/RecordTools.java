package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import edu.illinois.cs.cogcomp.archive.Identifier;
import edu.illinois.cs.cogcomp.thrift.base.Clustering;
import edu.illinois.cs.cogcomp.thrift.base.Forest;
import edu.illinois.cs.cogcomp.thrift.base.Labeling;
import edu.illinois.cs.cogcomp.thrift.base.View;
import edu.illinois.cs.cogcomp.thrift.curator.Record;

import java.util.HashMap;
import java.util.Set;

public class RecordTools {
    /**
     * Checks to see if a Record object contains annotations
     *
     * @param r The record in question
     * @return True if the record contains some annotation, false otherwise
     */
    public static boolean recordHasAnnotations( Record r ) {
        return getNumViews( r ) > 0;
    }

    /**
     * Counts the number of known views for the indicated record
     *
     * @param r The record in question
     * @return The total number of parse, label, cluster, and general views known
     */
    public static int getNumViews( Record r ) {
        return r.getClusterViewsSize() + r.getLabelViewsSize()
                + r.getParseViewsSize() + r.getViewsSize();
    }

    /**
     * This should really be a constructor in Record. Whatever.
     *
     * Constructs a new Record object with no annotations at all to the original
     * document text.
     *
     * @param originalText The document's raw text.
     * @return A record object for the document whose original text was passed
     *         in.
     */
    public static Record generateNewRecord( String originalText ) {
        return generateNewRecord( Identifier.getId( originalText, false ),
                                  originalText );
    }

    public static Record generateNewRecord(
            String documentHash, String originalText ) {
        Record r = new Record();
        r.setRawText( originalText );
        r.setWhitespaced( false );
        r.setLabelViews( new HashMap<String, Labeling>() );
        r.setClusterViews( new HashMap<String, Clustering>() );
        r.setParseViews( new HashMap<String, Forest>() );
        r.setViews( new HashMap<String, View>() );
        r.setIdentifier( documentHash );

        return r;
    }

    /**
     * Checks a record for a given annotation type
     *
     * @param r          The record to check
     * @param annotation The annotation type to search the record for
     * @return True if the record contains the indicated annotation, false
     *         otherwise
     */
    public static boolean hasAnnotation( Record r, AnnotationMode annotation ) {
        String annotationString = annotation.toCuratorString();
        return r.getLabelViews().containsKey( annotationString )
                || r.getClusterViews().containsKey( annotationString )
                || r.getParseViews().containsKey( annotationString )
                || r.getViews().containsKey( annotationString );
    }

    /**
     * Gets a long string that represents the contents of a record, including the
     * raw text and the views present therein.
     *
     * @param record The records whose (string-version) contents you want to get
     * @return A string version of the record's contents
     */
    public static String getRecordContents( Record record ) {
        StringBuilder result = new StringBuilder();
        result.append( "Annotations present in the record:\n" );
        result.append( "- rawText: " );
        result.append( record.isSetRawText() ? "Yes" : "No" );
        result.append( "\nThe following Label Views: " );
        for ( String key : record.getLabelViews().keySet() ) {
            result.append( key );
            result.append( " " );
        }
        result.append( "\n" );
        result.append( "The following Cluster Views: " );
        for ( String key : record.getClusterViews().keySet() ) {
            result.append( key );
            result.append( " " );
        }
        result.append( "\n" );
        result.append( "The following Parse Views: " );
        for ( String key : record.getParseViews().keySet() ) {
            result.append( key );
            result.append( " " );
        }
        result.append( "\n" );
        result.append( "The following general Views: " );
        for ( String key : record.getViews().keySet() ) {
            result.append( key );
            result.append( " " );
        }
        result.append( "\n" );
        return result.toString();
    }

    /**
     * Checks that the record provides the required dependencies for a particular
     * annotation tool. For instance, if you indicate that the annotation to be
     * performed is tokenization, this will always return true (there are no
     * dependencies for tokenization). If, however, you indicate the annotation
     * to perform is chunking, it will only return true if the record contains
     * both part of speech (POS) and tokenization annotations.
     *
     * @param r The record to check for dependencies
     * @param annoToPerform The type of annotation to validate the record's
     *                      existing views against.
     * @return True if the record provides all annotations required by the
     *         annotation to be performed, false otherwise.
     */
    public boolean meetsDependencyReqs( Record r,
                                        AnnotationMode annoToPerform ) {
        Set<AnnotationMode> dependencies = annoToPerform.getDependencies();
        for( AnnotationMode dep : dependencies ) {
            if( !RecordTools.hasAnnotation( r, dep ) ) {
                return false;
            }
        }

        return true;
    }
}
