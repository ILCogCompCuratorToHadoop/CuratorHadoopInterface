package edu.cs.illinois.cogcomp.hadoopinterface;

import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.Record;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Tyler A. Young
 * @author Lisa Y. Bao
 */
public class CuratorReducer extends Reducer<Text, Record, Text, Record> {
    /**
     * Accumulate number of points inside/outside results from the mappers.
     * @param inKey
     * @param inValue
     * @param context
     */
    public void reduce( Text inKey, 
                        Record inValue, 
                        Context context ) throws IOException {

        // write input document to HDFS
        Path file = inValue.getPath(); // pulls Hadoop-HDFS filepath from Record object
        
	    // while loop, wait for output in appropriate directory to "magically" appear
        // (thanks to the local Curator instance)
        boolean done = false;
        Path output = null;
        while (!done) {
            try {
                // attempts to get java.io path to curator output, if it exists
                output = FileSystems.getDefault().getPath("out", "curator_out.txt");
            } catch (IOException e) {
                System.out.println("Waiting on Curator output");
            } 
            if (output != null) {
                done = true;
                // pass Curator output back to Hadoop as Record
                BufferReader reader = Files.newBufferedReader(output, StandardCharsets.UTF_8);
                Record curatorRecord = new Record(reader); // TODO does Record allow optional params in creation?
                context.write(inKey, curatorRecord);
            }
        }              
    }
}
