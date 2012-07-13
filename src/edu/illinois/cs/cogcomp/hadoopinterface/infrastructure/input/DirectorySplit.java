package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.input;

import edu.illinois.cs.cogcomp.hadoopinterface.HadoopInterface;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.FileSystemHandler;
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
import java.util.Set;

/**
 * A document within the input directory.
 * Returned by DirectoryInputFormat.getSplits() and passed to
 * DirectoryInputFormat.createRecordReader().  Note that a split doesn't contain
 * the input data, but is just a reference to the data.
 *
 * Represents the data to be processed by an individual Map process.
 * @author Tyler Young
 */
public class DirectorySplit extends InputSplit implements Writable {
    public DirectorySplit() throws IOException {
        HadoopInterface.logger.logError(
                "Someone called the DirectorySplit's zero-arg constructor. (?)");
    }

    /**
     * Constructs a DirectorySplit object
     * @param serializedRecInHDFS The location (in HDFS) of the
     *            document's serialized record, complete with all annotations.
     *
     *            This file should be in the job directory, and it should be named
     *            < record hash/ID >.txt.
     * @param fs The filesystem associated with this job
     */
    public  DirectorySplit( Path serializedRecInHDFS, FileSystem fs,
                            Configuration config )
            throws IOException {
        this.config = config;
        this.inputPath = serializedRecInHDFS;
        this.fs = fs;
        hash = FileSystemHandler.getFileNameWithoutExtension( inputPath );
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
        FileSystemHandler fsHandler = new FileSystemHandler(fs);
        return fsHandler.getFileSizeInBytes( inputPath );
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
        FileStatus status = fs.getFileStatus(inputPath);

        BlockLocation[] blockLocs = fs.getFileBlockLocations( status, 0,
                                                              status.getLen() );

        Set<String> allBlockHosts = new HashSet<String>();
        for( BlockLocation blockLoc : blockLocs ) {
            allBlockHosts.addAll( Arrays.asList( blockLoc.getHosts() ) );
        }

        // Passing the String array causes toArray() to return an array of the
        // same type
        return allBlockHosts.toArray( new String[0] );
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // Serialize our data
        String stringRep = toString() + "\n";
        dataOutput.write(stringRep.getBytes());
        // Have the configuration serialize its data
        config.write( dataOutput );

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        inputPath = new Path( dataInput.readLine() );
        hash = FileSystemHandler.getFileNameWithoutExtension( inputPath );
        config = new Configuration();
        config.readFields( dataInput );
        fs = FileSystem.get(config);
    }

    /**
     * @return The input path for this document
     */
    public String toString() {
        return inputPath.toString();
    }

    private Path inputPath;
    private Configuration config;
    private String hash;
    private FileSystem fs;
}
