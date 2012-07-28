package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.exceptions
        .IllegalModeException;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Defines "modes" for the Curator-to-Hadoop interface (i.e., annotation types
 * which the tool will run with.
 * @author Tyler Young
 */
public enum AnnotationMode {
    CHUNK, COREF, NER, NOM_SRL, PARSE, POS, SENTENCE, STANFORD_PARSE, TOKEN, VERB_SRL, WIKI;

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
            // This map will contain a bunch of strings which we will turn into
            // case insensitive regular expressions. If we match one of them,
            // we will return the AnnotationMode that the string is mapped to.
            Map<String, AnnotationMode> regexes =
                    new HashMap<String, AnnotationMode>();
            regexes.put( "chunk", CHUNK );
            regexes.put( "coref", COREF );
            regexes.put( "ner", NER );
            regexes.put( "named", NER );
            regexes.put( "parse", PARSE );
            regexes.put( "charniak", PARSE );
            regexes.put( "stanford", STANFORD_PARSE );
            regexes.put( "pos", POS );
            regexes.put( "part", POS );
            regexes.put( "sentence", SENTENCE );
            regexes.put( "token", TOKEN );
            regexes.put( "nom", NOM_SRL );
            regexes.put( "verb", VERB_SRL );
            regexes.put( "wiki", WIKI );

            for( String key : regexes.keySet() ) {
                Matcher matcher = Pattern.compile( Pattern.quote( key ),
                                                   Pattern.CASE_INSENSITIVE ).matcher(
                        s );
                if( matcher.find() ) {
                    return regexes.get(key);
                }
            }

            // Special case. We don't want to use a regex formed from "srl"
            // because it's ambiguous. However, the Curator uses "srl" to identify
            // specifically Verb SRL, and it uses "nom" to identify Nominal SRL.
            if( s.equals( "srl" ) ) {
                return VERB_SRL;
            }

            throw new IllegalModeException(
                    "Parse mode " + s + " not recognized. Please try one of "
                    + "the following: \n\t" + getKnownAnnotations() );
        }
    }

    /**
     * @return A comma-separated list of the known annotation modes
     */
    public static String getKnownAnnotations() {
        StringBuilder sBuilder = new StringBuilder();
        List<AnnotationMode> allModes = Arrays.asList( AnnotationMode.values() );
        Iterator itr = allModes.iterator();
        while( itr.hasNext() ) {
            String mode = itr.next().toString();

            if( !itr.hasNext() ) {
                sBuilder.append("and ");
            }

            sBuilder.append( mode );

            if( itr.hasNext() ) {
                sBuilder.append(", ");
            }
        }

        return sBuilder.toString();
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
                return "ner";
            case NOM_SRL:
                return "nom";
            case PARSE:
                return "charniak";
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
     * Gets the list of annotations required in order to perform an annotation.
     * @return The list of dependencies for the given annotation type. For instance,
     *         if this annotation is TOKEN (tokenization), this will return the
     *         empty list (there are no dependencies for tokenization). If,
     *         however, this annotation is CHUNK (chunking), it will return
     *         a set containing TOKEN and POS (in this order), since chunking 
     *         depends on both tokenization and part of speech.
	 *
	 * NOTE: This method should return dependencies in run-time order (i.e. resolving
	 *       inter-dependencies automatically).
     */
    public ArrayList<AnnotationMode> getDependencies(AnnotationMode typeOfAnnotation) {
        ArrayList<AnnotationMode> deps = new ArrayList<AnnotationMode>();

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
                break ;
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
     * Gets the list of annotations required in order to perform this annotation.
     * @return The list of dependencies for this annotation type. For instance,
     *         if this annotation is TOKEN (tokenization), this will return the
     *         empty list (there are no dependencies for tokenization). If,
     *         however, this annotation is CHUNK (chunking), it will return
     *         a set containing TOKEN and POS (in this order), since chunking 
     *         depends on both tokenization and part of speech.
     */
    public ArrayList<AnnotationMode> getDependencies() {
        return getDependencies( this );
    }
}
