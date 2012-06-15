package edu.cs.illinois.cogcomp.hadoopinterface;

import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Tyler Young
 */
public class CuratorReducer extends MapReduceBase
        implements Reducer<BooleanWritable, LongWritable, WritableComparable<?>, Writable>
{
    /** Store job configuration. */
    @Override
    public void configure( JobConf job ) {
        conf = job;
    }

    /**
     * Accumulate number of points inside/outside results from the mappers.
     * @param isInside Is the points inside?
     * @param values An iterator to a list of point counts
     * @param output dummy, not used here.
     * @param reporter
     */
    public void reduce( BooleanWritable isInside,
                        Iterator<LongWritable> values,
                        OutputCollector<WritableComparable<?>, Writable> output,
                        Reporter reporter) throws IOException
    {
        return; // TODO: We have to actually do something.
    }

    /**
     * Reduce task done, write output to a file.
     */
    @Override
    public void close() throws IOException {
        // Write output to a file
        Path outDir = new Path(HadoopInterface.TMP_DIR, "out");
        Path outFile = new Path(outDir, "reduce-out");
        FileSystem fileSys = FileSystem.get(conf);
        SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf,
                outFile, LongWritable.class, LongWritable.class,
                SequenceFile.CompressionType.NONE);
        writer.append(new LongWritable(1), new LongWritable(1));
        writer.close();
    }

    private JobConf conf; //configuration for accessing the file system

}
