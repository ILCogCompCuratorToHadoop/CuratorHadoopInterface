package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * A reducer used to shut down the Curator running on Hadoop nodes. Run a job
 * using this reducer (being sure to assign at least 1 reduce task per node
 * in the cluster) after running a Curator job on the Hadoop cluster. This will
 * shut down both the client and the annotator that it was launched with.
 * @author Tyler Young
 */
public class CuratorKillerReducer extends
        Reducer<Text, HadoopRecord, Text, HadoopRecord> {
    /**
     * Dummy method. When run on a Hadoop node, it will shut down any
     * Curator Servers running locally on that node.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce( Text inKey,
                        Iterable<HadoopRecord> inValues,
                        Context context )
            throws IOException, InterruptedException {
        // Command to do this: "jps -l | grep edu.illinois.cs.cogcomp.curator.CuratorServer | cut -d ' ' -f 1 | xargs -n1 kill"
        //      See here: http://stackoverflow.com/questions/2131874/shell-script-to-stop-a-java-program

        String cmd = "jps -l | " + // Get the list of all running Java processes
                // Select the line for the Curator server
                "grep edu.illinois.cs.cogcomp.curator.CuratorServer | " +
                // Split the line on spaces
                "cut -d ' ' -f 1 | " +
                // Send the first element of the split line (i.e., the process
                // ID) to the kill command
                "xargs -n1 kill";

        Runtime.getRuntime().exec( cmd );
    }
}
