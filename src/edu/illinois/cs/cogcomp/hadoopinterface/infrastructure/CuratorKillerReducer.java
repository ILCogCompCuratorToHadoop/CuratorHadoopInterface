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

        /* Preserved in case I mess things up below:
        String curator = "jps -l | " + // Get the list of all running Java processes
                // Select the line for the Curator server
                "grep edu.illinois.cs.cogcomp.curator.CuratorServer | " +
                // Split the line on spaces
                "cut -d ' ' -f 1 | " +
                // Send the first element of the split line (i.e., the process ID) to the kill command
                "xargs -n1 kill";
        Runtime.getRuntime().exec(curator);
        */

        while (true) { //TODO triple-check that this loop can't get stuck in infinity...
            String tool = "jps -l | " + // Get the list of all running Java processes
                // Select the first matching process
                "grep edu.illinois.cs.cogcomp | head -n 1 | " +
                // Split the line on spaces
                "cut -d ' ' -f 1 | " +
                // Send the first element of the split line (i.e., the process ID) to the kill command
                "xargs -n1 kill";
            try {
                Runtime.getRuntime().exec(tool);
            } catch (RuntimeException e) {
                break; // stop when grep fails to find a matching process
            }
        }

    }
}
