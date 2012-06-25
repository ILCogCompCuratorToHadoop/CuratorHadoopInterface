package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.input;

import edu.cs.illinois.cogcomp.hadoopinterface.HadoopInterface;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.FileSystemHandler;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * This defines an input type for a Map operation. It splits an input by
 * document collections---that is, we will have (roughly) one map operation per
 * document (original plus annotations) in the input directory.
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
public class DirectoryInputFormat extends InputFormat< Text, Record> {

    @Override
    public List<InputSplit> getSplits(JobContext context)
                throws IOException, InterruptedException {

        HadoopInterface.logger.logStatus( "Getting splits." );
        LinkedList<InputSplit> jobSplits = new LinkedList<InputSplit>();

        // Get location of the input document directory from job context
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        HadoopInterface.logger.log("Input dir is " + conf.get("inputDirectory"));

        List<Path> subDirsOfInputDirs = FileSystemHandler.getSubdirectories(
                new Path( conf.get("inputDirectory") ), fs);

        HadoopInterface.logger.log( "Found " + subDirsOfInputDirs.size()
                + " documents in the input directory. "
                + " These are as follows: "
                + HadoopInterface.logger.getPrettifiedList(subDirsOfInputDirs) );


        // For each document directory in the directory . . .
        for( Path filePath : subDirsOfInputDirs ) {
            // Add a directory split for this document directory
            jobSplits.add( new DirectorySplit( filePath, fs, conf ) );
        }
        HadoopInterface.logger.log( "Finished creating splits." );

        return jobSplits;
    }

    @Override
    public RecordReader<Text, Record>
            createRecordReader( InputSplit inputSplit,
                                TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        // Because the MapReduce framework calls initialize() (i.e., the REAL
        // constructor, we don't need to pass any params. Weird.
        //HadoopInterface.logger.log( "Returning record reader." );
        return new CuratorRecordReader();
    }

}
