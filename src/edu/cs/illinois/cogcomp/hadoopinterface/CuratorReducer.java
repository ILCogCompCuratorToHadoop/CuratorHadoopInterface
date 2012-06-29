package edu.cs.illinois.cogcomp.hadoopinterface;

import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.AnnotationMode;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.HadoopRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Tyler A. Young
 * @author Lisa Y. Bao
 */
public class CuratorReducer extends Reducer<Text, HadoopRecord, Text, HadoopRecord> {
    /**
     * Accumulate number of points inside/outside results from the mappers.
     * @param inKey
     * @param inValue
     * @param context
     */
    public void reduce( Text inKey, 
                        HadoopRecord inValue,
                        Context context ) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get( context.getConfiguration() );
        // Pseudo-code per discussion with Mark on 27 June

        AnnotationMode toolToRun = AnnotationMode
                .fromString( context.getConfiguration().get("annotationMode") );

        // Check if annotation tool is running locally (i.e., by checking log
        // file located at CURATOR_DIR/logs/).
        if( !toolIsRunning( toolToRun, fs ) ) {
            // If not, start it and sleep repeatedly until it's ready to go
            startTool( toolToRun );
            Thread.sleep(1000); // Give it 1 second at minimum to start up

            while( !toolIsRunning( toolToRun, fs ) ) {
                Thread.sleep(5000);
            }
        }

        // Check if Curator is running locally (again, via log file)
        if( !curatorIsRunning() ) {
            // If not, start it and sleep repeatedly until it's ready to go
            startCurator();
            Thread.sleep(1000); // Give it 1 second at minimum to start up

            while( !curatorIsRunning() ) {
                Thread.sleep(5000);
            }
        }

        // Create a new Curator client object
        HadoopCuratorClient client = new HadoopCuratorClient();

        client.annotateSingleDoc(inValue, toolToRun);

        // Serialize the updated record to the output directory
        Path generalOutputDir =
                new Path( context.getConfiguration().get("outputDirectory") );
        Path docOutputDir =
                new Path( generalOutputDir, inValue.getDocumentHash() );
        client.writeOutputFromLastAnnotate(docOutputDir);


        // TODO: As another MR job (?): after all jobs are through, kill all tools

        
        // pass Curator output back to Hadoop as Record
        context.write(inKey, inValue);
    }

    /**
     * Checks the log files in your Curator directory (`~/curator/dist/logs`)
     * to see if the indicated tool has finished launching.
     * @param toolToCheck The annotation tool in question (more accurately,
     *                    the type of annotation provided by the tool)
     * @return TRUE if the log file indicates the tool is running. FALSE if
     *         the log file *should* indicate the tool is running successfully,
     *         but it does not.
     *
     *         Note that if the annotation tool, through an aggravating design
     *         decision, does *not* indicate explicitly when it's ready to go,
     *         we make a best guess based on when the log file was last modified.
     *
     * @TODO: Make this less brittle (don't rely on tools to indicate readiness in log)
     */
    public boolean toolIsRunning( AnnotationMode toolToCheck,
                                  FileSystem fs )
            throws EnumConstantNotPresentException {
        // Get the tool's log location
        Path generalLogDir = new Path( "~/curator/dist/logs" );
        Path logLocation;
        switch (toolToCheck) {
            case CHUNK:
                logLocation = new Path( generalLogDir, "chunk.log" );
                break;
            case COREF:
                logLocation = new Path( generalLogDir, "coref.log" );
                break;
            case NER:
                logLocation = new Path( generalLogDir, "ner-ext-conll.log" );
                break;
            case NOM_SRL:
                logLocation = new Path( generalLogDir, "nom-srl.log" );
                break;
            case PARSE:
                logLocation = new Path( generalLogDir, "stanford.log" );
                break;
            case POS:
                logLocation = new Path( generalLogDir, "pos.log" );
                break;
            case TOKEN:
                // TODO: Seriously? The Tokenizer doesn't keep a log file?
                // No log file, so we're stuck assuming it's running.
                return true;
                break;
            case VERB_SRL:
                logLocation = new Path( generalLogDir, "verb-srl.log" );
                break;
            case WIKI:
                logLocation = new Path( generalLogDir, "wikifier.log" );
                break;
            default:
                throw new IllegalArgumentException( "Tool"
                        + toolToCheck.toString() + " is not known." );
        }

        // If the tool to check gives us an explicit "hello world" or something
        // in it's log file
        // TODO: Figure out which ones do this, and how to check it. . .

            // Check the log file

        // Else, check the log file's time of last modification. If it's within
        // the last half hour, assume all is well.
        try {
            long thirtyMinsAgo = System.currentTimeMillis() - ( 30*60*1000 );
            if( fs.getFileStatus(logLocation).getAccessTime() > thirtyMinsAgo ) {
                return true;
            }
            else {
                return false;
            }
        } catch( IOException e ) {
            // Can't access the file system. Just assume it works (else we could
            // have an infinite loop where we just keep trying to check the log).
            return true;
        }

        return false;
    }


    /**
     * Checks the log files in your Curator directory (`~/curator/dist/logs`)
     * to see if the indicated tool has finished launching.
     * @return TRUE if the log file indicates the Curator is running.
     * @TODO: Write method
     */
    public boolean curatorIsRunning() {
        // Basically, since we assume that the Curator isn't launched until all
        // tools are good to go, we can assume the Curator is running as long as
        // its log file has been modified in the last half hour.

        return false;
    }

    /**
     * Runs the shell script required to launch the indicated annotation tool.
     * If this script is not found in your Curator directory (i.e., at
     * `~/curator/dist/scripts/launch_annotator_on_this_node.sh`), we'll simply
     * create it.
     * @param toolToLaunch The annotation tool to launch (more accurately,
     *                     the type of annotation provided by the tool to be
     *                     launched)
     * @TODO: Write method
     */
    public void startTool( AnnotationMode toolToLaunch ) {
        // Figure out location of shell script based on tool in use

        //Runtime.getRuntime().exec(myShellScript);

    }

    /**
     * Runs the shell script required to launch the indicated annotation tool.
     * If this script is not found in your Curator directory (i.e., at
     * `~/curator/dist/scripts/launch_curator_on_this_node.sh`), we'll simply
     * create it.
     *
     * @TODO: Write method
     */
    public void startCurator() {
        //Runtime.getRuntime().exec(myShellScript);
    }


    private Path curatorDir;
}
