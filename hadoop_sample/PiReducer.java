package com.gmail.s3cur3.hadoop_test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * Reducer class for Pi estimation.
 * Accumulate points inside/outside results from the mappers.
 */
public class PiReducer extends MapReduceBase
        implements Reducer<BooleanWritable, LongWritable, WritableComparable<?>, Writable>
{

    private long numInside = 0;
    private long numOutside = 0;
    private JobConf conf; //configuration for accessing the file system
    private Path TMP_DIR = new Path( PiEstimator.class.getSimpleName()
            + "_TMP_3_141592654");
    
    
    /** Store job configuration. */
    @Override
    public void configure(JobConf job) {
        conf = job;
    }

    /**
     * Accumulate number of points inside/outside results from the mappers.
     * @param isInside Is the points inside?
     * @param values An iterator to a list of point counts
     * @param output dummy, not used here.
     * @param reporter
     */
    public void reduce(BooleanWritable isInside,
                       Iterator<LongWritable> values,
                       OutputCollector<WritableComparable<?>, Writable> output,
                       Reporter reporter) throws IOException
    {
        if (isInside.get()) {
            for(; values.hasNext(); numInside += values.next().get());
        } else {
            for(; values.hasNext(); numOutside += values.next().get());
        }
    }

    /**
     * Reduce task done, write output to a file.
     */
    @Override
    public void close() throws IOException {
        //write output to a file
        Path outDir = new Path(TMP_DIR, "out");
        Path outFile = new Path(outDir, "reduce-out");
        FileSystem fileSys = FileSystem.get(conf);
        SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf,
                outFile, LongWritable.class, LongWritable.class,
                SequenceFile.CompressionType.NONE);
        writer.append(new LongWritable(numInside), new LongWritable(numOutside));
        writer.close();
    }
}
