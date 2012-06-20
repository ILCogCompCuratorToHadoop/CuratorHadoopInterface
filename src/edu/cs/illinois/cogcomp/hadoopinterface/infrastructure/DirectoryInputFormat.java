package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * This defines an input type for a Map operation. It splits an input by
 * files---that is, we will have (roughly) one map operation per file in
 * the input directory.
 *
 * Responsibilities:
 *
 *  1. Validate the input-specification of the job.
 *  2. Split-up the input file(s) into logical InputSplit instances, each of
 *     which is then assigned to an individual Mapper.
 *  3. Provide the RecordReader implementation used to glean input records from
 *     the logical InputSplit for processing by the Mapper.
 *
 * @author Tyler Young
 */
public class DirectoryInputFormat extends InputFormat< Text, Record > {
    @Override
    public List<InputSplit> getSplits(JobContext jobContext)
            throws IOException, InterruptedException {
        // Get location of document directory from job context

        // For each document in the directory, create a new split, which contains
        // the text of the original document


        LinkedList< InputSplit > l = new LinkedList< InputSplit >();
        l.add( new InputSplit() {
            @Override
            public long getLength() throws IOException, InterruptedException {
                return "test".length();
            }

            @Override
            public String[] getLocations() throws IOException, InterruptedException {
                return new String[0];  //To change body of implemented methods use File | Settings | File Templates.
            }
        });
        return l;
    }

    @Override
    public RecordReader<Text, Record>
            createRecordReader( InputSplit inputSplit,
                                TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return null;
    }
}
