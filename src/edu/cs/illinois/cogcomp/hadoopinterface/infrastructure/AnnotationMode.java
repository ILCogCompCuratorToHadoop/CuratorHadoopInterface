package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.exceptions.IllegalModeException;

/**
 * Defines "modes" for the Curator-to-Hadoop interface (i.e., annotation types
 * which the tool will run with.
 * @author Tyler Young
 */
public enum AnnotationMode {
    CHUNK, COREF, NER, NOM_SRL, PARSE, POS, TOKEN, VERB_SRL, WIKI;

    public static AnnotationMode fromString( final String s ) {
        try { 
            return AnnotationMode.valueOf( s );
        } catch ( IllegalArgumentException e ) {
            // TODO: Use regexes instead?
            if( s.contains("hunk") || s.contains("HUNK") ) {
                return CHUNK;
            }

            if( s.contains("oref") || s.contains("OREF") ) {
                return COREF;
            }

			if( s.contains("ner") || s.contains("NER") || s.contains("amed") || s.contains("AMED") ) {
				return NER;
			}

            if( s.contains("nom") || s.contains("NOM") ) {
                return NOM_SRL;
            }

            if( s.contains("arse") || s.contains("PARS") ) {
                return PARSE;
            }
			
            if( s.contains("pos") || s.contains("art") || s.contains("POS")
                    || s.contains("ART") ) {
                return POS;
            }

            if( s.contains("oken") || s.contains("TOKEN") ) {
                return TOKEN;
            }

            if( s.contains("erb") || s.contains("VERB") ) {
                return VERB_SRL;
            }

            if( s.contains("iki") || s.contains("WIKI") ) {
                return WIKI;
            }

            throw new IllegalModeException( "Parse mode " + s + " not recognized. "
                                            + "Please try one of the following: \n"
                                            + "    CHUNK, COREF, NOM_SRL, POS, TOKEN, "
                                            + "VERB_SRL, WIKI, PARSE, or NER." );
        }
    }
}
