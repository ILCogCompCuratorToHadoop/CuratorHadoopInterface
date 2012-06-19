/**
 * Provides the map() method to Hadoop's MapReduce.
 * Called by HadoopInterface
 * 
 * @author Tyler A. Young
 * @author Lisa Bao
 */

package edu.cs.illinois.cogcomp.hadoopinterface;

import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.Record;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class CuratorMapper extends Mapper<Text, Record, Text, Record> {

    /**
     * The map method in a map/reduce cycle. All nodes in the Hadoop
     * job cluster run this on their own portions of the input.
     * @param inKey = key
     * @param inValue = value
     * @param context The configuration context
     */
    public void map( Text inKey,
                     Record inValue,
                     Context context ) throws IOException {

        HadoopInterface.logger.logStatus( "Beginning map phase.\n"
                                          + "\t\tGot input key " + inKey.toString()
                                          + "\n\t\tand input value "
                                          + inValue.toString() );


        // Output the map results

        // TODO
        // 1. write our own Record class
        // 2. modify params of class/method
        // 3. add writeable/comparable interfaces to id and record objs
        // 4. change id type from String to "text"?


        // TODO print out input that has been passed in

        

    	// transform input to output as (key, value) = (hash ID, Record)
        //String key = id;
        //Record value = record;

        //String key = new String("0xdeadbeef"); // The document's hash (unique)
        //String value = new String("This is my document text.");
        
    }


}
