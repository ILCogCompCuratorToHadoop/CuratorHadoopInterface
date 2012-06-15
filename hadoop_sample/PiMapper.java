package com.gmail.s3cur3.hadoop_test;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Mapper class for Pi estimation.
 * Generate points in a unit square
 * and then count points inside/outside of the inscribed circle of the square.
 */
public class PiMapper extends MapReduceBase
        implements Mapper<LongWritable, LongWritable, BooleanWritable, LongWritable> {

    /** Map method.
     * @param offset samples starting from the (offset+1)th sample.
     * @param size the number of samples for this map
     * @param out output {true->numInside, false->numOutside}
     * @param reporter
     */
    public void map(LongWritable offset,
                    LongWritable size,
                    OutputCollector<BooleanWritable, LongWritable> out,
                    Reporter reporter) throws IOException {

        final HaltonSequence haltonsequence = new HaltonSequence(offset.get());
        long numInside = 0L;
        long numOutside = 0L;

        for(long i = 0; i < size.get(); ) {
            //generate points in a unit square
            final double[] point = haltonsequence.nextPoint();

            //count points inside/outside of the inscribed circle of the square
            final double x = point[0] - 0.5;
            final double y = point[1] - 0.5;
            if (x*x + y*y > 0.25) {
                numOutside++;
            } else {
                numInside++;
            }

            //report status
            i++;
            if (i % 1000 == 0) {
                reporter.setStatus("Generated " + i + " samples.");
            }
        }

        //output map results
        out.collect(new BooleanWritable(true), new LongWritable(numInside));
        out.collect(new BooleanWritable(false), new LongWritable(numOutside));
    }
}
