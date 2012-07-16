package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.input;

import edu.illinois.cs.cogcomp.hadoopinterface.HadoopInterface;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.FileSystemHandler;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.HadoopRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * This class transforms a DirectorySplit into a (Text key, HadoopRecord value)
 * pair (where HadoopRecord is of our own type, denoting a single document in the
 * corpus together with all its annotations).
 *
 * @author Tyler Young
 */
public class CuratorRecordReader extends RecordReader {
    @Override
    public void initialize( InputSplit split, TaskAttemptContext context )
            throws IOException, InterruptedException {
        HadoopInterface.logger.log( "Initializing record reader" );
        config = context.getConfiguration();

        Path inputPath = new Path( split.toString() );
        String docHash =
                FileSystemHandler.getFileNameWithoutExtension( inputPath );

        nextKey = new Text( docHash );
        progress = 0.0f;
    }

    /**
     * Checks to see if there is another (key, value) pair to be read; if there
     * is, this method prepares that pair for future access.
     * @return TRUE if a (key, value) pair was created, and thus if there are
     *         further (key, value) pairs to emit. FALSE if all (key, value)
     *         pairs from this input split have already been emitted.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        HadoopInterface.logger.log( "Checking next KV in RecordReader" );
        if( progress < 0.9 ) {
            // Read the next key, value pair from the input split
            // Effectively, construct the Record that we will pass out as a value
            nextRecord = new HadoopRecord( nextKey.toString(),
                                     FileSystem.get( config ),
                                     config );
            return true;
        }
        return false;
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
        progress += 0.5f;
        return nextKey;
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        progress += 0.5f;
        return nextRecord;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return progress;
    }

    @Override
    public void close() throws IOException {
        // No files to close (Record should close its stuff as soon as
        // it's constructed)
    }

    //  The hash identifying the document for which this object generates records
    Text nextKey;
    Configuration config;
    HadoopRecord nextRecord;
    float progress;
}
