package edu.cs.illinois.cogcomp.hadoopinterface;

import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.AnnotationMode;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.Record;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.FileSystemHandler.*;

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
                        Context context ) throws IOException, InterruptedException {

        // write input document to local dir
        String annotation = context.getConfiguration().get("annotationMode");
        Path source = inValue.getAnnotation(AnnotationMode.fromString(annotation)); // pulls Hadoop-HDFS filepath from Record object
        FileSystem fs = FileSystem.get(context.getConfiguration());
        String text = readFileFromHDFS(source, fs, true);
        Path dest = new Path("/temp/hadoop/curator_in.txt");
        writeFileToLocal(text, dest);
        
	    // while loop, wait for output in appropriate directory to "magically" appear
        // (put there by the local Curator instance)
        boolean done = false;
        Path output = new Path("/temp/hadoop/curator_out.txt");
        while (!done) {
            try {
                Thread.sleep(1000); // sleep for 1 sec
            }
            catch (InterruptedException e) {
                System.out.println("Time delay interrupted");
            }

            if ( !localFileExists(output) ) {
                System.out.println("Waiting on Curator output");
            }
            else { // only if output file exists, read file from path to a string
                done = true;
                text = readFileFromLocal(output);
                inValue.addAnnotation(AnnotationMode.fromString(annotation), text);
            }
        }
        
        // pass Curator output back to Hadoop as Record
        context.write(inKey, inValue);
    }
}
