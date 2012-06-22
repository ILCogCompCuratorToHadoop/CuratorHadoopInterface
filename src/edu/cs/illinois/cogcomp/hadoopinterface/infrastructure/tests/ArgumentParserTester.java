package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.tests;

import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.AnnotationMode;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.ArgumentParser;
import org.junit.Test;

import java.io.IOException;

/**
 * Test class for ArgumentParser
 */
public class ArgumentParserTester
{
    public ArgumentParserTester()
    {

    }

    @Test
    public void parsesPlainInputCorrectly() throws IOException {
        String[] args = new String[2];
        args[0] = "some_folder_name";
        args[1] = "Verb SRL";
        ArgumentParser plainStyle1 = new ArgumentParser(args);
        assert (plainStyle1.getDirectory().equals("some_folder_name"));
        assert (plainStyle1.getMode() == AnnotationMode.VERB_SRL);
        assert (plainStyle1.getNumMaps() == 10);
        assert (plainStyle1.getNumReduces() == 10);


        args = new String[2];
        args[0] = "chunker";
        args[1] = "another_directory";
        ArgumentParser plainStyle2 = new ArgumentParser(args);
        assert (plainStyle2.getDirectory().equals("another_directory"));
        assert (plainStyle2.getMode() == AnnotationMode.CHUNK);
        assert (plainStyle2.getNumMaps() == 10);
        assert (plainStyle2.getNumReduces() == 10);

    }



    @Test
    public void parsesRobustInputCorrectly() throws IOException {
        String[] args;
        args = new String[4];
        args[0] = "-d";
        args[1] = "dir1234";
        args[2] = "-m";
        args[3] = "parser";
        ArgumentParser robustStyle1 = new ArgumentParser(args);
        assert( robustStyle1.getDirectory().equals( "dir1234" ) );
        assert( robustStyle1.getMode() == AnnotationMode.PARSE );
        assert( robustStyle1.getNumMaps() == 10 );
        assert( robustStyle1.getNumReduces() == 10 );

        args = new String[4];
        args[0] = "-m";
        args[1] = "coreference";
        args[2] = "-d";
        args[3] = "some_dir1234";
        ArgumentParser robustStyle2 = new ArgumentParser(args);
        assert( robustStyle2.getDirectory().equals( "some_dir1234" ) );
        assert( robustStyle2.getMode() == AnnotationMode.COREF );
        assert( robustStyle2.getNumMaps() == 10 );
        assert( robustStyle2.getNumReduces() == 10 );

        args = new String[6];
        args[0] = "-m";
        args[1] = "tokenizer";
        args[2] = "-d";
        args[3] = "some_dir1234";
        args[4] = "-maps";
        args[5] = "99";
        ArgumentParser robustStyle3 = new ArgumentParser(args);
        assert( robustStyle3.getDirectory().equals( "some_dir1234" ) );
        assert( robustStyle3.getMode() == AnnotationMode.TOKEN );
        assert( robustStyle3.getNumMaps() == 99 );
        assert( robustStyle3.getNumReduces() == 10 );

        args = new String[8];
        args[0] = "-reduces";
        args[1] = "3";
        args[2] = "-m";
        args[3] = "wikifier";
        args[4] = "-d";
        args[5] = "asdf1234";
        args[6] = "-maps";
        args[7] = "1";
        ArgumentParser robustStyle4 = new ArgumentParser(args);
        assert( robustStyle4.getDirectory().equals( "asdf1234" ) );
        assert( robustStyle4.getMode() == AnnotationMode.WIKI );
        assert( robustStyle4.getNumMaps() == 1 );
        assert( robustStyle4.getNumReduces() == 3 );
    }
}
