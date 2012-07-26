package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import edu.illinois.cs.cogcomp.hadoopinterface.CuratorReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * A reducer used to shut down the Curator and NLP annotation tools
 * running on Hadoop nodes. Run a job using this reducer (being sure to assign at
 * least 1 reduce task per node in the cluster) after running a Curator job on
 * the Hadoop cluster. This will shut down both the client and the annotator that
 * it was launched with.
 * @author Tyler Young
 * @author Lisa Bao
 */
public class CuratorKillerReducer extends
        Reducer<Text, HadoopRecord, Text, HadoopRecord> {
    private static final int MAX_RUNNING_TOOLS = 5;

    /**
     * Dummy method. When run on a Hadoop node, it will shut down any
     * Curator Servers and annotators running locally on that node.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce( Text inKey,
                        Iterable<HadoopRecord> inValues,
                        Context context )
            throws IOException, InterruptedException {
        // Command to kill the Curator specifically:
        //      "jps -l | grep edu.illinois.cs.cogcomp.curator.CuratorServer | cut -d ' ' -f 1 | xargs -n1 kill"
        //      See here: http://stackoverflow.com/questions/2131874/shell-script-to-stop-a-java-program

        // We should never have more than MAX_RUNNING_TOOLS to kill.
        for( int attempts = 0; attempts < MAX_RUNNING_TOOLS; ++attempts ) {
            String killCmd = "jps -l | " + // Get the list of all running Java processes
                // Select the first matching process
                "grep edu.illinois.cs.cogcomp | head -n 1 | " +
                // Split the line on spaces
                "cut -d ' ' -f 1 | " +
                // Send the first element of the split line
                // (i.e., the process ID) to the kill command
                "xargs -n1 kill";

            String[] cmd = {
                    "/bin/sh",
                    "-c",
                    killCmd
            };

            try {
                Process p = Runtime.getRuntime().exec(cmd);

                StreamGobbler err = new StreamGobbler( p.getErrorStream(),
                        "ERR: ", true );
                StreamGobbler out = new StreamGobbler( p.getInputStream(),
                        "", true );
                err.start();
                out.start();

                if( p.waitFor() == 0 ) {
                    System.out.println( "Successfully shut down a process." );
                }
                else {
                    // stop when grep fails to find a matching process
                    System.out.println( "Failed to shut down a process. "
                                        + "Exiting . . ." );
                    break;
                }
            } catch( RuntimeException e ) {
                throw new IOException( "Runtime exception shutting down a "
                        + "process! \n" + e.getMessage() );
            }
        }

        // Make sure future Reducers don't think their tools are already running
        CuratorReducer.setToolHasBeenLaunched( false );
    }
}
