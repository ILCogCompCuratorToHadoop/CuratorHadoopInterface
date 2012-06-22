package edu.cs.illinois.cogcomp.hadoopinterface;

import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.Record;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import infrastructure.*;

//import java.nio.charset.Charset;
//import java.nio.file.*;


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
        FileSystem fs = FileSystem.get(context.getConfiguration());
        String text = readFileFromHDFS(source, fs, true);
        Path dest = new Path("/temp/hadoop/curator_in.txt");
        writeFileToLocal(text, dest);
        
	    // while loop, wait for output in appropriate directory to "magically" appear
        // (put there by the local Curator instance)
        boolean done = false;
        output = new Path("/temp/hadoop/curator_out.txt");
        while (!done) { // TODO add time delay
            if ( !localFileExists(output) ) {
                System.out.println("Waiting on Curator output");
            }
            else { // only if output file exists, read file from path to a string
                done = true;
                text = readFileFromLocal(output);
                inValue.addAnnotation(text);
            }
        }
        
        // pass Curator output back to Hadoop as Record
        context.write(inKey, inValue);              
    }
}
