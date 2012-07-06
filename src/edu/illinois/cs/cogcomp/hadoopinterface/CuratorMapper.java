/**
 * Provides the map() method to Hadoop's MapReduce.
 * Called by HadoopInterface
 * 
 * @author Tyler A. Young
 * @author Lisa Y. Bao
 */

package edu.illinois.cs.cogcomp.hadoopinterface;

import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.HadoopRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class CuratorMapper extends Mapper<Text, HadoopRecord, Text, HadoopRecord> {


    /**
     * The map method in a map/reduce cycle. All nodes in the Hadoop
     * job cluster run this on their own portions of the input.
     * @param inKey = key
     * @param inValue = value
     * @param context The configuration context
     */
    public void map( Text inKey, 
                     HadoopRecord inValue, 
                     Context context) throws IOException, InterruptedException {
    
        HadoopInterface.logger.logStatus( "Beginning map phase.\n"
                                          + "Attempting to access vars." );
        HadoopInterface.logger.logStatus( "\tGot input key " + inKey.toString()
                                          + "\n\tand input value: "
                                          + inValue.toString() );
        
        context.write(inKey, inValue);
        
    }


}
