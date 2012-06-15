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
     * Parse arguments and then runs a map/reduce job.
     * Print output in standard out.
     *
     * @return a non-zero if there is an error.  Otherwise, return 0.
     */
    public int run( String[] args ) throws Exception {
        // TODO: Log errors in a standardized way
        if (args.length != 2) {
            System.err.println( "Usage: " + getClass().getName() +
                    " <document directory>");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        // The number of map tasks we will set
        // TODO: Make this user-specifiable (?)
        int numMaps = 10;

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
                System.out.println("Wrote input for Map #"+i);
            }

            // Start a map/reduce job -- runJob(jobConf) takes the job
            // configuration we just set up and distributes it to Hadoop nodes
            // TODO: Send this to a proper log file/standardized logging class
            System.out.println("Starting Job");
            final long startTime = System.currentTimeMillis();
            JobClient.runJob(jobConf);
            final double duration = (System.currentTimeMillis() - startTime)/1000.0;
            System.out.println("Job Finished in " + duration + " seconds");

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
}
