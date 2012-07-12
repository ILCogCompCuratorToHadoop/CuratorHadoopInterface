/**
 * Provides the map() method to Hadoop's MapReduce. 
 * TESTING VERSION
 * Called by HadoopInterface
 * 
 * @author Tyler A. Young
 * @author Lisa Y. Bao
 */

package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.tests;

import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.AnnotationMode;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.HadoopRecord;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.MessageLogger;
import edu.illinois.cs.cogcomp.thrift.base.Labeling;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;


/**
 * A Mapper class to test the Record input extensively. Also, since running the
 * Mapper relies on the CuratorRecordReader, DirectoryInputFormat, and
 * DirectorySplit classes to function properly, successfully running this mapper
 * also serves as a test for those classes.
 *
 * @author Lisa Bao
 * @author Tyler Young
 */
public class RecordTesterMapper extends Mapper<Text, HadoopRecord, Text, HadoopRecord> {


    /**
     * The map method in a map/reduce cycle. All nodes in the Hadoop
     * job cluster run this on their own portions of the input.
     * @param testKey = key
     * @param testValue = value
     * @param testContext The configuration context
     */
    public void map( Text testKey, 
                     HadoopRecord testValue,
                     Context testContext) throws IOException, InterruptedException {

        MessageLogger logger = new MessageLogger(true);
        testValue.setMessageLogger( logger );

        logger.logStatus( "Beginning map operation. Attempting to access vars." );
        logger.logStatus( "Got test key " + testKey.toString()
                          + "\n\tand test value: " + testValue.toString() );

        // Test rec's add and remove capabilities //

        logger.logStatus( "Re-adding POS annotation. "
                          + "Should be silent." );
        testValue.addAnnotation( AnnotationMode.POS, new Labeling() );

        for( int i = 0; i < 4; i++ ) {
            AnnotationMode mode = getRandomMode();
            logger.log( "Dumping the contents of file " + mode.toString() );
        }

        Boolean pos_bool = testValue.meetsDependencyReqs( AnnotationMode.POS );
        Boolean ner_bool = testValue.meetsDependencyReqs( AnnotationMode.NER );
        if( pos_bool ) {
            logger.log("We have satisfied the deps for POS");
        }
        else {
            logger.logError("We have NOT satisfied the deps for POS");
            throw new Error( "Error checking dependencies in Record." );
        }
        if( ner_bool ) {
            logger.log("We have satisfied the deps for NER");
        }
        else {
            logger.logError("We have NOT satisfied the deps for NER");
            throw new Error( "Error checking dependencies in Record." );
        }

        testContext.write(testKey, testValue);

        logger.beginWritingToDisk();
    }


    /**
     * Simply returns a random annotation mode for the sake of testing the
     * record's ability to work with that mode.
     * @return A random AnnotationMode
     */
    private AnnotationMode getRandomMode() {
        List<AnnotationMode> values = Collections.unmodifiableList(
                Arrays.asList( AnnotationMode.values() ) );
        Random rng = new Random();
        return values.get( rng.nextInt( values.size() ) );
    }

}
