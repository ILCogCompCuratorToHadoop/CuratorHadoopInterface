package edu.cs.illinois.cogcomp.hadoopinterface;

/**
 * Defines "modes" for the Curator-to-Hadoop interface (i.e., annotation types
 * which the tool will run with.
 * @author Tyler Young
 */
public enum AnnotationMode {
    CHUNK, COREF, NOM_SRL, POS, TOKEN, VERB_SRL, WIKI, PARSE
}
