package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.exceptions.IllegalModeException;

/**
 * Defines "modes" for the Curator-to-Hadoop interface (i.e., annotation types
 * which the tool will run with.
 * @author Tyler Young
 */
public enum AnnotationMode {
    CHUNK, COREF, NER, NOM_SRL, PARSE, POS, TOKEN, VERB_SRL, WIKI;

    /**
     * Takes a string version of an annotation mode and returns the equivalent
     * value in the enumerated type
     * @param s A string version of an annotation mode (e.g.,
     *          "named entity recognition", "NER", "pos", "tokenizer")
     * @return The AnnotationMode enumerated type version of the input string
     *         (e.g., if you passed in "tokenizer", you get back
     *         AnnotationMode.PARSE)
     */
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

    /**
     * Converts our own enumerated type to the string identifier used in the
     * Curator. E.g., if you pass in AnnotationMode.NER, you get back "ner-ext".
     * @param mode The enumerated type to convert to a Curator-friendly string
     * @return A string version of the annotation mode
     */
    public static String toCuratorString( AnnotationMode mode ) {
        switch (mode) {
            case CHUNK:
                return "chunk";
            case COREF:
                return "coref";
            case NER:
                return "ner-ext";
            case NOM_SRL:
                return "nom";
            case PARSE:
                return "stanfordParse";
            case POS:
                return "pos";
            case TOKEN:
                return "tokens";
            case VERB_SRL:
                return "srl";
            case WIKI:
                return "wikifier";
            default:
                throw new IllegalModeException( "Mode " + mode.toString()
                                                + " is not recognized.");
        }

    }
}
