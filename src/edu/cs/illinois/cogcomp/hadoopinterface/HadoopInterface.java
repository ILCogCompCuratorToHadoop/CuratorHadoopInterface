/**
* This program presents an interface for passing large batch jobs from the
* Curator to a Hadoop cluster.
*
* TODO: Document usage
*
* @author Tyler A. Young
* @author Lisa Bao
*/

package edu.cs.illinois.cogcomp.hadoopinterface;

// TODO: Check that we actually use all these... (We probably will not!)

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HadoopInterface extends Configured implements Tool {
    /**
     * Dummy main method for launching the tool as a stand-alone command.
     * Structure modeled after Hadoop's examples.
     *
     * For whatever reason, Tool demands that this throws a generic Exception.
     * How helpfully vague.
     */
    public static void main( String[] argv ) throws Exception {
        System.exit(ToolRunner.run(null, new HadoopInterface(), argv));
    }

    /**
     * Parses arguments and then runs a map/reduce job.
     *
     * @return 0 if we ran error-free, non-zero otherwise.
     */
    public int run( String[] args ) throws Exception {
        // The number of map tasks we will use in this Hadoop job
        // TODO: Make this user-specifiable (?)
        int numMaps = 10;

        if (args.length < 1) {
            String errorMsg = new String( "Usage: " + getClass().getName() +
                                          "< document directory>" );
            logger.logError( errorMsg );
            System.err.println( errorMsg );
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        // Set up the job configuration that we will send to Hadoop
        // Javadoc for JobConf:
        //   http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/mapred/JobConf.html

        // Specifies the map and reduce classes to be used (and more)
        final JobConf jobConf = new JobConf(getConf(), getClass());
        jobConf.setJobName(HadoopInterface.class.getSimpleName());

        jobConf.setInputFormat(SequenceFileInputFormat.class);

        // Output keys (the hashes identifying documents) will be string objects
        jobConf.setOutputKeyClass(ObjectWritable.class);
        jobConf.setOutputValueClass(ObjectWritable.class);
        jobConf.setOutputFormat(SequenceFileOutputFormat.class);

        jobConf.setMapperClass(CuratorMapper.class);
        jobConf.setNumMapTasks(numMaps);

        jobConf.setReducerClass(CuratorReducer.class);
        // From JobConf docs: The right number of reduces seems to be 0.95 or
        // 1.75 multiplied by
        // (<no. of nodes> * mapreduce.tasktracker.reduce.tasks.maximum)
        // Increasing the number of reduces increases the framework overhead, but
        // increases load balancing and lowers the cost of failures.
        jobConf.setNumReduceTasks(1);

        // Turn off speculative execution, because DFS doesn't handle
        // multiple writers to the same file.
        jobConf.setSpeculativeExecution(false);


        // Set up input/output directories
        // TODO: Eventually, we want to read and write to a directory that
        //       the Master Curator has access to
        final Path inDir = new Path(TMP_DIR, "in");
        final Path outDir = new Path(TMP_DIR, "out");
        FileInputFormat.setInputPaths(jobConf, inDir);
        FileOutputFormat.setOutputPath(jobConf, outDir);

        final FileSystem fs = FileSystem.get(jobConf);
        if (fs.exists(TMP_DIR)) {
            throw new IOException("Tmp directory " + fs.makeQualified(TMP_DIR)
                    + " already exists.  Please remove it first.");
        }
        if (!fs.mkdirs(inDir)) {
            throw new IOException("Cannot create input directory " + inDir);
        }

        try {
            // Generate an input file for each map task
            // TODO: Make this actually appropriate for our task (currently taken
            //       from the Hadoop example for approximating Pi)
            for( int i = 0; i < numMaps; ++i ) {
                final Path file = new Path(inDir, "part"+i);
                final LongWritable offset = new LongWritable(i);
                final LongWritable size = new LongWritable(1);
                final SequenceFile.Writer writer = SequenceFile.createWriter(
                        fs, jobConf, file,
                        LongWritable.class, LongWritable.class, CompressionType.NONE);
                try {
                    writer.append(offset, size);
                } finally {
                    writer.close();
                }
                logger.log( new String( "Wrote input for Map #" + i ) );
            }

            // Start a map/reduce job -- runJob(jobConf) takes the job
            // configuration we just set up and distributes it to Hadoop nodes
            logger.log( new String( "Starting MapReduce job" ) );
            final long startTime = System.currentTimeMillis();
            JobClient.runJob(jobConf);
            final double duration = (System.currentTimeMillis() - startTime)/1000.0;
            logger.log( new String( "Job finished in " + duration + " seconds" ) );

            // Read outputs
            Path inFile = new Path(outDir, "reduce-out");
            SequenceFile.Reader reader =
                    new SequenceFile.Reader(fs, inFile, jobConf);
            reader.next("Lorem ipsum"); // TODO: Oh yeah... we need real output.
            reader.close();
        } finally {
            fs.delete(TMP_DIR, true);
        }

        return 0;
    }

    /** tmp directory for input/output */
    static public final Path TMP_DIR = new Path(
            HadoopInterface.class.getSimpleName()+ "_TMP" );

    // A tool to standardize error logging. Prints the log to standard out.
    static public final ErrorLogger logger = new ErrorLogger( true );
}
