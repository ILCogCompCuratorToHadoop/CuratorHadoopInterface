package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.exceptions
        .IllegalModeException;

import java.util.HashSet;
import java.util.Set;

/**
 * Defines "modes" for the Curator-to-Hadoop interface (i.e., annotation types
 * which the tool will run with.
 * @author Tyler Young
 * @TODO: Make sure this actually represents all the tools we need!
 */
public enum AnnotationMode {
    CHUNK, COREF, NER, NOM_SRL, PARSE, POS, SENTENCE, TOKEN, VERB_SRL, WIKI;

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
            // TODO: Use regexes instead
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

            if( s.contains("arse") || s.contains("PARS")
                    || s.contains("tanford") || s.contains("STANFORD") ) {
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

            if( s.contains("entence") || s.contains("SENTENCE") ) {
                return SENTENCE;
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
            case SENTENCE:
                return "sentences";
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

    /**
     * The equivalent of calling the static version of toCuratorString() and
     * passing it "this" AnnotationMode object
     * @return A string version of this annotation mode
     */
    public String toCuratorString() {
        return toCuratorString(this);
    }

    /**
     * Returns the annotation mode's view type. For instance, NER is a Labeling
     * view, but SRL is a Parse view.
     * @param mode The annotation mode whose view type you want to know
     * @return The view type corresponding to the indicated annotation mode
     */
    public static ViewType getViewType( AnnotationMode mode ) {
        if( mode == PARSE || mode == VERB_SRL || mode == NOM_SRL ) {
            return ViewType.PARSE;
        }
        else if( mode == TOKEN || mode == NER || mode == POS || mode == CHUNK
                || mode == WIKI || mode == SENTENCE ) {
            return ViewType.LABEL;
        }
        else if( mode == COREF ) {
            return ViewType.CLUSTER;
        }
        else {
            throw new IllegalModeException("Mode " + mode.toString()
                    + " was not recognized.");
        }
    }

    /**
     * The equivalent of calling the static version of getViewType() and
     * passing it "this" AnnotationMode object
     * @return he view type corresponding to this annotation mode
     */
    public ViewType getViewType() {
        return  getViewType( this );
    }

    /**
     * Gets the set of annotations required in order to perform an annotation.
     * @return The set of dependencies for the given annotation type. For example,
     *         if the annotation is TOKEN (tokenization), this will return the
     *         empty set (there are no dependencies for tokenization). If,
     *         however, the annotation is CHUNK (chunking), it will return
     *         a set containing POS and TOKEN, since chunking depends on both
     *         part of speech and tokenization.
     */
    public Set<AnnotationMode> getDependencies(AnnotationMode typeOfAnnotation) {
        HashSet<AnnotationMode> deps = new HashSet<AnnotationMode>();

        // Note: a set of nested `if` statements would be more efficient (less
        // duplicated code), but a switch makes it super easy to confirm that we
        // have accounted for all cases. This is important for maintainability.
        switch ( typeOfAnnotation ) {
            case CHUNK:
                deps.add(TOKEN);
                deps.add(POS);
                break;
            case COREF:
                deps.add(TOKEN);
                deps.add(POS);
                deps.add(NER);
                break;
            case NER:
                // No dependencies.
                break;
            case NOM_SRL:
                deps.add(TOKEN);
                deps.add(POS);
                deps.add(CHUNK);
                deps.add(PARSE);
                break;
            case PARSE:
                deps.add(TOKEN);
                break;
            case POS:
                deps.add(TOKEN);
                break;
            case SENTENCE:
                // No dependencies.
                break;
            case TOKEN:
                // No dependencies.
                break;
            case VERB_SRL:
                deps.add(TOKEN);
                deps.add(POS);
                deps.add(CHUNK);
                deps.add(PARSE);
                break;
            case WIKI:
                deps.add(TOKEN);
                deps.add(POS);
                deps.add(CHUNK);
                deps.add(NER);
                break;
        }

        return deps;
    }

    /**
     * Gets the set of annotations required in order to perform this annotation.
     * @return The set of dependencies for this annotation type. For instance,
     *         if this annotation is TOKEN (tokenization), this will return the
     *         empty set (there are no dependencies for tokenization). If,
     *         however, this annotation is CHUNK (chunking), it will return
     *         a set containing POS and TOKEN, since chunking depends on both
     *         part of speech and tokenization.
     */
    public Set<AnnotationMode> getDependencies() {
        return getDependencies( this );
    }
}
