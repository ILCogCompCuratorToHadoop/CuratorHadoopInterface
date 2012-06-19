/**
 * Provides the map() method to Hadoop's MapReduce.
 * Called by HadoopInterface
 * 
 * @author Tyler A. Young
 * @author Lisa Bao
 */

package edu.cs.illinois.cogcomp.hadoopinterface;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;


public class CuratorMapper extends MapReduceBase implements
        Mapper<LongWritable, LongWritable, ObjectWritable, ObjectWritable> {

    /**
     * The map method in a map/reduce cycle. All nodes in the Hadoop
     * job cluster run this on their own portions of the input.
     * @param id = key 
     * @param record = value
     * @param out output {true->numInside, false->numOutside}
     * @param reporter Where does this come from, and how do we use it?
     */
    public void map(LongWritable id,
                    Record record,
                    OutputCollector<ObjectWritable, ObjectWritable> out,
                    Reporter reporter) throws IOException {

        reporter.setStatus("Beginning map phase.");
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
        
	out.collect(new ObjectWritable(id), new ObjectWritable(record));
    }


}
