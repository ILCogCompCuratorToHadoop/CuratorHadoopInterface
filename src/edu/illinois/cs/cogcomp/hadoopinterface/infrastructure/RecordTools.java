package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import edu.illinois.cs.cogcomp.archive.Identifier;
import edu.illinois.cs.cogcomp.thrift.base.Clustering;
import edu.illinois.cs.cogcomp.thrift.base.Forest;
import edu.illinois.cs.cogcomp.thrift.base.Labeling;
import edu.illinois.cs.cogcomp.thrift.base.View;
import edu.illinois.cs.cogcomp.thrift.curator.Record;

import java.util.*;

/**
 * A utility class for working with Records. Most of these should really
 * be a part of the Record's interface, but changing that is a pain.
 * @author Tyler Young
 */
public class RecordTools {
    /**
     * Checks to see if a Record object contains annotations
     *
     * @param r The record in question
     * @return True if the record contains some annotation, false otherwise
     */
    public static boolean hasAnnotations( Record r ) {
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
    public static Record generateNew( String originalText ) {
        return generateNew( Identifier.getId( originalText, false ),
                            originalText );
    }

    /**
     * This should really be a constructor in Record. Whatever.
     *
     * Constructs a new Record object with no annotations at all to the original
     * document text.
     *
     * @param documentHash The identifier to be used in this Record, obtained
     *                     from a call to Identifier.getId()
     * @param originalText The document's raw text.
     * @return A record object for the document whose original text was passed
     *         in.
     */
    public static Record generateNew( String documentHash,
                                      String originalText ) {
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
    public static String getContents( Record record ) {
        StringBuilder result = new StringBuilder();
        result.append( "Annotations present in the record:\n" );
        result.append( "- rawText: " );
        result.append( record.isSetRawText() ? "Yes" : "No" );
        result.append( "\nThe following Label Views: " );
        for ( String key : record.getLabelViews().keySet() ) {
            result.append( key );
            result.append( ", " );
        }
        result.append( "\n" );
        result.append( "The following Cluster Views: " );
        for ( String key : record.getClusterViews().keySet() ) {
            result.append( key );
            result.append( ", " );
        }
        result.append( "\n" );
        result.append( "The following Parse Views: " );
        for ( String key : record.getParseViews().keySet() ) {
            result.append( key );
            result.append( ", " );
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
     * Gets a string that represents the annotations contained in the record.
     *
     * @param record The records whose (string-version) contents you want to get
     * @return A string version of the record's annotations. Example:
     *             "TOKEN SENTENCE POS CHUNK"
     */
    public static String getAnnotationsString( Record record ) {
        Set<String> allAnnotations = new HashSet<String>();

        allAnnotations.addAll( record.getLabelViews().keySet() );
        allAnnotations.addAll( record.getClusterViews().keySet() );
        allAnnotations.addAll( record.getParseViews().keySet() );
        allAnnotations.addAll( record.getViews().keySet() );

        StringBuilder result = new StringBuilder();

        for ( String key : allAnnotations ) {
            result.append( AnnotationMode.fromString(key).toString() );
            result.append( " " );
        }
        return result.toString();
    }
	
	/**
	 * Based on getAnnotationsString() method
	 *
	 * @return a list of all existing annotations stored in this record
	 */
	public ArrayList<AnnotationMode> getAnnotationsList() {
        Set<String> allAnnotations = new HashSet<String>();
        allAnnotations.addAll( this.getLabelViews().keySet() );
        allAnnotations.addAll( this.getClusterViews().keySet() );
        allAnnotations.addAll( this.getParseViews().keySet() );
        allAnnotations.addAll( this.getViews().keySet() );

        ArrayList<AnnotationMode> result = new ArrayList<AnnotationMode>();
        for ( String key : allAnnotations ) {
            result.add( AnnotationMode.fromString(key) );
        }
        return result;
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
    public static boolean meetsDependencyReqs(
            Record r,
            AnnotationMode annoToPerform ) {
        List<AnnotationMode> dependencies = annoToPerform.getDependencies();
        for( AnnotationMode dep : dependencies ) {
            if( !RecordTools.hasAnnotation( r, dep ) ) {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns a bit of the original text (useful for identifying a record to
     * a human, where a hash is meaningless and hard to use)
     * @param r The record in question
     * @return The first 10 words or so of the record's original (raw) text
     */
    public static String getBeginningOfOriginalText( Record r ) {
        StringBuilder text = new StringBuilder();
        String fullText = r.getRawText();

        Scanner scanner = new Scanner(fullText);
        for( int i = 0; i < 10; i++ ) {
            if( scanner.hasNext() ) {
                text.append( scanner.next() );
                text.append(' ');
            }
        }

        if( scanner.hasNext() ) {
            text.append(". . . " );
        }

        return text.toString();
    }
}
