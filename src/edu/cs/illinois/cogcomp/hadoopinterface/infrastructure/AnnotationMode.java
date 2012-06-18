package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.exceptions.IllegalModeException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Defines "modes" for the Curator-to-Hadoop interface (i.e., annotation types
 * which the tool will run with.
 * @author Tyler Young
 */
public enum AnnotationMode {
    CHUNK, COREF, NOM_SRL, POS, TOKEN, VERB_SRL, WIKI, PARSE;

    public static AnnotationMode fromString( String s ) {
        Pattern pattern = Pattern.compile( "chunk", Pattern.CASE_INSENSITIVE );
        Matcher matcher = pattern.matcher(s);
        if( matcher.matches() ) {
            return CHUNK;
        }

        pattern = Pattern.compile("ref", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(s);
        if( matcher.matches() ) {
            return COREF;
        }

        pattern = Pattern.compile("nom", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(s);
        if( matcher.matches() ) {
            return NOM_SRL;
        }

        pattern = Pattern.compile("pos|part", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(s);
        if( matcher.matches() ) {
            return POS;
        }

        pattern = Pattern.compile("tok", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(s);
        if( matcher.matches() ) {
            return TOKEN;
        }

        pattern = Pattern.compile("verb", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(s);
        if( matcher.matches() ) {
            return VERB_SRL;
        }

        pattern = Pattern.compile("wiki", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(s);
        if( matcher.matches() ) {
            return WIKI;
        }

        pattern = Pattern.compile("pars|stanford", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(s);
        if( matcher.matches() ) {
            return PARSE;
        }
        
        throw new IllegalModeException( "Parse mode " + s + " not recognized. "
                                        + "Please try one of the following: \n"
                                        + "    CHUNK, COREF, NOM_SRL, POS, TOKEN, "
                                        + "VERB_SRL, WIKI, or PARSE." );
    }
}
