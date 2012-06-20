package edu.cs.illinois.cogcomp.hadoopinterface;

import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.Record;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;
import java.nio.charset.Charset;
import java.nio.file.*;


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

        // write input document to local dir
        Path source = inValue.getAnnotation(); // pulls Hadoop-HDFS filepath from Record object
        Path dest = Paths.get(System.getProperty("user.dir"), "out", "curator_in.txt");
        Files.copy(source, dest); // Java 7 finally implements this function natively...
        
	    // while loop, wait for output in appropriate directory to "magically" appear
        // (thanks to the local Curator instance)
        boolean done = false;
        Path output = null;
        while (!done) {
            try {
                // attempts to get java.io path to curator output, if it exists
                output = Paths.get(System.getProperty("user.dir"), "out", "curator_out.txt");
                //output = FileSystems.getDefault().getPath("out", "curator_out.txt");
            } 
            catch (IOException e) {
                System.out.println("Waiting on Curator output");
            } 
            if (output != null) {
                done = true;
                // read output file from path to string
                String text = Files.readAllLines(output, Charset.defaultCharset());
                inValue.addAnnotation(text);
            }
            // pass Curator output back to Hadoop as Record
            context.write(inKey, inValue);
            
        }              
    }
}
