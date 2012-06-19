package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.tests;

import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.AnnotationMode;
import org.junit.Test;

/**
 * @author Tyler Young
 */



public class AnnotationModeTester
{
    public void AnnotationModeTester() { }

    @Test
    public void fromStringMethodWorks() {
        assert( AnnotationMode.fromString( "chunker" ) == AnnotationMode.CHUNK );
        assert( AnnotationMode.fromString( "coreference" ) == AnnotationMode.COREF );
        assert( AnnotationMode.fromString( "nominal semantic role labeling" ) == AnnotationMode.NOM_SRL );
        assert( AnnotationMode.fromString( "part of speech" ) == AnnotationMode.POS );
        assert( AnnotationMode.fromString( "tokenizer" ) == AnnotationMode.TOKEN );
        assert( AnnotationMode.fromString( "verb SRL" ) == AnnotationMode.VERB_SRL );
        assert( AnnotationMode.fromString( "wikifier" ) == AnnotationMode.WIKI );
        assert( AnnotationMode.fromString( "parser" ) == AnnotationMode.PARSE );

        assert( AnnotationMode.fromString( "CHUNKER" ) == AnnotationMode.CHUNK );
        assert( AnnotationMode.fromString( "COREFERENCE" ) == AnnotationMode.COREF );
        assert( AnnotationMode.fromString( "NOMINAL_SRL" ) == AnnotationMode.NOM_SRL );
        assert( AnnotationMode.fromString( "PART_OF_SPEECH" ) == AnnotationMode.POS );
        assert( AnnotationMode.fromString( "TOKENIZER" ) == AnnotationMode.TOKEN );
        assert( AnnotationMode.fromString( "VERB_SEMANTIC_ROLE_LABELER" ) == AnnotationMode.VERB_SRL );
        assert( AnnotationMode.fromString( "WIKIFIER" ) == AnnotationMode.WIKI );
        assert( AnnotationMode.fromString( "PARSER" ) == AnnotationMode.PARSE );
    }
}
