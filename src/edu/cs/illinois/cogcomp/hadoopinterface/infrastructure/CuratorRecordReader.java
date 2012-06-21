package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Tyler Young
 */
public class CuratorRecordReader extends RecordReader {
    public CuratorRecordReader() {

    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        config = context.getConfiguration();
        nextKey = new Text( split.toString() );
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // Read the next key, value pair from the input split
        // Effectively, construct the Record that we will pass out as a value
        nextRecord = new Record( nextKey.toString() );
        return true;
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
        returnedKey = true;
        return nextKey;
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        returnedRecord = true;
        return nextRecord;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        float progress = (float)0.0;

        if( returnedKey || returnedRecord ) {
            progress = (float)0.5;
            if( returnedKey && returnedRecord ) {
                progress = (float)1.0;
            }
        }

        return progress;
    }

    @Override
    public void close() throws IOException {
        // TODO: Fill me
    }

    Configuration config;

    //  The hash identifying the document for which this object generates records
    Text nextKey;
    Record nextRecord;
    private boolean returnedRecord;
    private boolean returnedKey;
}
