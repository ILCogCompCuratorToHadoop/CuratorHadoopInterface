package edu.cs.illinois.cogcomp.hadoopinterface;

import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.Record;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Tyler Young
 */
public class CuratorReducer extends Reducer<Text, Record, Text, Record> {
    /**
     * Accumulate number of points inside/outside results from the mappers.
     * @param inKey
     * @param inValue
     * @param context
     */
    public void reduce( Text inKey, Record inValue, Context context)
            throws IOException {

        // TODO write input document to HDFS
    	//Path file = ; // TODO pull filepath from Record object
        
	    // TODO while loop, wait for output in appropriate directory to "magically" appear
        // (thanks to the local Curator instance)
        boolean done = false;

        /*
        while (!done) {
            Path output = ; // TODO check for Curator output in local dir
            if (output == ) {
                done = true;
                // TODO do we need to process this Curator output before close()?
            }
        }              */
    }
}
