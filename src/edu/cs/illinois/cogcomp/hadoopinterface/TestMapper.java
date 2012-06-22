/**
 * Provides the map() method to Hadoop's MapReduce. 
 * TESTING VERSION
 * Called by HadoopInterface
 * 
 * @author Tyler A. Young
 * @author Lisa Y. Bao
 */

package edu.cs.illinois.cogcomp.hadoopinterface;

import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.AnnotationMode;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.Record;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class TestMapper extends Mapper<Text, Record, Text, Record> {


    /**
     * The map method in a map/reduce cycle. All nodes in the Hadoop
     * job cluster run this on their own portions of the input.
     * @param inKey = key
     * @param inValue = value
     * @param context The configuration context
     */
    public void map( Text testKey, 
                     Record testValue, 
                     Context testContext) throws IOException, InterruptedException {
    
        HadoopInterface.logger.logStatus( "Beginning map phase.\n"
                                          + "Attempting to access vars." );
        HadoopInterface.logger.logStatus( "\tGot test key " + testKey.toString()
                                          + "\n\tand test value: "
                                          + testValue.toString() );

        testValue.addAnnotation(AnnotationMode.fromString("POS"), "This is the POS annotation body.");
        testValue.informAnnotation(AnnotationMode.fromString("NER")); // should throw a dependencies error
        Path pos = testValue.getAnnotation(AnnotationMode.fromString("POS"));
        Path ner = testValue.getAnnotation(AnnotationMode.fromString("NER"));
        System.out.println(pos.toString() + "\n");
        System.out.println(ner.toString() + "\n");
        testValue.listAnnotations();
        Boolean pos_bool = testValue.checkDependencies(AnnotationMode.fromString("POS"));
        Boolean ner_bool = testValue.checkDependencies(AnnotationMode.fromString("NER"));
        System.out.println("Dependencies for POS: " + pos_bool.toString());
        System.out.println("Dependencies for NER: " + ner_bool.toString());

        testContext.write(testKey, testValue);
        
    }


}
