package edu.cs.illinois.cogcomp.hadoopinterface

import org.apache.hadoop.io.ObjectWritable;

import java.lang.String;

/**
 * Provides the map() method to Hadoop's MapReduce.
 * Called by HadoopInterface
 * 
 * @author Tyler A. Young
 * @author Lisa Bao
 */

public class CuratorMapper extends MapReduceBase implements
        Mapper<LongWritable, LongWritable, BooleanWritable, LongWritable> {

    /**
     * The map method in a map/reduce cycle. All nodes in the Hadoop
     * job cluster run this on their own portions of the input.
     * @param offset samples starting from the (offset+1)th sample.
     * @param size the number of samples for this map
     * @param out output {true->numInside, false->numOutside}
     * @param reporter TODO: Where does this come from, and how do we use it?
     */
    public void map(LongWritable offset,
                    LongWritable size,
                    OutputCollector<BooleanWritable, LongWritable> out,
                    Reporter reporter) throws IOException {

        reporter.setStatus("Beginning map phase.");

        // Output the map results
        String key = new String("0xdeadbeef"); // The document's hash (unique)
        String value = new String("This is my document text.");
        out.collect(new ObjectWritable( key ), new ObjectWritable( value ));
    }


}