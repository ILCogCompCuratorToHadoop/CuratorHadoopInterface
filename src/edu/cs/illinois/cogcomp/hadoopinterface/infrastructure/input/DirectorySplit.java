package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.input;

import edu.cs.illinois.cogcomp.hadoopinterface.HadoopInterface;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.FileSystemHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

/**
 * A document directory within the input directory.
 * Returned by DirectoryInputFormat.getSplits() and passed to
 * DirectoryInputFormat.createRecordReader().  Note that a split doesn’t contain
 * the input data, but is just a reference to the data.
 *
 * Represents the data to be processed by an individual Map process.
 * @author Tyler Young
 */
public class DirectorySplit extends InputSplit implements Writable {
    public DirectorySplit() throws IOException {
        HadoopInterface.logger.logError("Who the hell called the zero-arg constructor?");
    }

    /**
     * Constructs a DirectorySplit object
     * @param docDirectoryInHDFS The location (in HDFS) of the
     *            document's directory, complete with all annotations.
     *
     *            This directory should
     *            be named with the document's hash, and should contain both an
     *            original.txt and an < annotation name >.txt for each dependency.
     * @param fs The filesystem associated with this job
     */
    public  DirectorySplit( Path docDirectoryInHDFS, FileSystem fs,
                            Configuration config )
            throws IOException {
        this.config = config;
        this.inputPath = docDirectoryInHDFS;

        hash = FileSystemHandler.getFileNameFromPath(
                FileSystemHandler.stripTrailingSlash(inputPath));

        HadoopInterface.logger.log("Creating directory split for " + hash);

        this.fs = fs;
    }

    /**
     * Get the size of this split so that the input splits can be sorted by
     * size. Here, we calculate the size to be the number of bytes in the
     * original document (i.e., ignoring all annotations).
     *
     * @return The number of characters in the original document
     */
    @Override
    public long getLength() throws IOException, InterruptedException {
        Path origTxt = new Path( inputPath, "original.txt" );
        String msg = "Getting length of split for " + toString() + ". Requesting "
                + "size for file at " + origTxt.toString();
        HadoopInterface.logger.log( msg );
        return FileSystemHandler.getFileSizeInBytes(origTxt, fs);
    }

    /**
     * Get the list of nodes where the data for this split would be local.
     * This list includes all nodes that contain any of the required data---it's
     * up to Hadoop to decide which one to use.
     *
     * @return An array of the nodes for whom the split is local
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        HadoopInterface.logger.log( "Getting locations for " + toString() + "." );
        FileStatus status = fs.getFileStatus(inputPath);

        BlockLocation[] blockLocs = fs.getFileBlockLocations( status, 0,
                                                              status.getLen() );

        HashSet<String> allBlockHosts = new HashSet<String>();
        for( BlockLocation blockLoc : blockLocs ) {
            allBlockHosts.addAll( Arrays.asList( blockLoc.getHosts() ) );
        }

        // Passing the String array causes toArray() to return an array of the
        // same type
        return allBlockHosts.toArray( new String[0] );
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        HadoopInterface.logger.logStatus( "Writing to data output in dir split" );
        System.out.println( "Writing to data output in dir split" );
        // Serialize our data
        String stringRep = toString() + "\n";
        dataOutput.write(stringRep.getBytes());
        // Have the configuration serialize its data
        config.write( dataOutput );

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        HadoopInterface.logger.logStatus( "Reading from data input in dir split" );
        System.out.println( "Reading data input in dir split" );
        hash = dataInput.readLine();
        config = new Configuration();
        config.readFields( dataInput );
        fs = FileSystem.get(config);
    }

    /**
     * @return The hash of the document that this split handles
     */
    public String toString() {
        return hash;
    }

    private Path inputPath;
    private Configuration config;
    private String hash;
    private FileSystem fs;
}
