/**
 * Provides the map() method to Hadoop's MapReduce. 
 * TESTING VERSION
 * Called by HadoopInterface
 * 
 * @author Tyler A. Young
 * @author Lisa Y. Bao
 */

package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.tests;

import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.AnnotationMode;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.MessageLogger;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.Record;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;


public class RecordTesterMapper extends Mapper<Text, Record, Text, Record> {


    /**
     * The map method in a map/reduce cycle. All nodes in the Hadoop
     * job cluster run this on their own portions of the input.
     * @param testKey = key
     * @param testValue = value
     * @param testContext The configuration context
     */
    public void map( Text testKey, 
                     Record testValue, 
                     Context testContext) throws IOException, InterruptedException {

        MessageLogger logger = new MessageLogger(true);
        testValue.setMessageLogger( logger );

        logger.logStatus( "Beginning map operation. Attempting to access vars." );
        logger.logStatus( "Got test key " + testKey.toString()
                          + "\n\tand test value: " + testValue.toString() );

        // Test rec's add and remove capabilities //
        logger.logStatus( "Adding POS annotation. "
                          + "Should throw an already-exists error." );
        testValue.addAnnotation( AnnotationMode.POS, "This is the POS annotation body." );

        logger.logStatus( "Removing POS annotation, then re-adding. "
                          + "Should be silent." );
        testValue.removeAnnotation( AnnotationMode.POS );
        testValue.addAnnotation( AnnotationMode.POS, "[Some POS annotation body]" );

        logger.logStatus( "That new POS annotation is: "
                          + testValue.getAnnotationString( AnnotationMode.POS ) );


        logger.logStatus("Informing of an already existing annotation ");
        testValue.informAnnotation( AnnotationMode.NER );

        for( int i = 0; i < 4; i++ ) {
            AnnotationMode mode = getRandomMode();
            logger.log( "Dumping the contents of file " + mode.toString() );
            logger.log( testValue.getAnnotationString( mode ) );
        }

        Boolean pos_bool = testValue.checkDependencies( AnnotationMode.POS );
        Boolean ner_bool = testValue.checkDependencies( AnnotationMode.NER );
        if( pos_bool ) {
            logger.log("We have satisfied the deps for POS");
        }
        else {
            logger.logError("We have NOT satisfied the deps for POS");
        }
        if( ner_bool ) {
            logger.log("We have satisfied the deps for NER");
        }
        else {
            logger.logError("We have NOT satisfied the deps for NER");
        }

        testContext.write(testKey, testValue);

        logger.beginWritingToDisk();
    }


    private AnnotationMode getRandomMode() {
        List<AnnotationMode> values = Collections.unmodifiableList(
                Arrays.asList( AnnotationMode.values() ) );
        Random rng = new Random();
        return values.get( rng.nextInt( values.size() ) );
    }

}
