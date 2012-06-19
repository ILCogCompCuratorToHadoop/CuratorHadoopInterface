package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.List;

/**
 * @author Tyler Young
 */
public class DirectoryInputFormat extends InputFormat< Text, Record > {
    @Override
    public List<InputSplit> getSplits(JobContext jobContext)
            throws IOException, InterruptedException {
        return null;
    }

    @Override
    public RecordReader<Text, Record>
            createRecordReader( InputSplit inputSplit,
                                TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return null;
    }
}
