/**
 * Provides the map() method to Hadoop's MapReduce.
 * Called by HadoopInterface
 * 
 * @author Tyler A. Young
 * @author Lisa Y. Bao
 */

package edu.cs.illinois.cogcomp.hadoopinterface;

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
    public void map( Text inKey, 
                     Record inValue, 
                     Context context) throws IOException, InterruptedException {
    
        HadoopInterface.logger.logStatus( "Beginning map phase.\n"
                                          + "Attempting to acces vars." );
        HadoopInterface.logger.logStatus( "\tGot input key " + inKey.toString()
                                          + "\n\tand input value: "
                                          + inValue.toString() );


        // TODO write our own Record class, w/ writeable/comparable interface


    	/*transform input to output as (key, value) = (hash ID, Record)
          String key = id;
          Record value = record;

          String key = new String("0xdeadbeef"); // The document's hash (unique)
          String value = new String("This is my document text.");
        */
        

        // context.write replaces out.collect()
        context.write(inKey, inValue);
        
    }


}
