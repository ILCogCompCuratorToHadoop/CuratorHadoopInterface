package edu.cs.illinois.cogcomp.hadoopinterface;

import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.AnnotationMode;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.FileSystemHandler;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.HadoopRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.FileSystemHandler.fileWasModifiedInLastXMins;

/**
 * A Reducer that serves as a wrapper for the document annotation tool. It
 * handles creating a locally-running Curator, launching and calling the
 * annotation tool, and so on. After running the annotation tool, new annotation
 * will be written to HDFS.
 *
 * @precondition The Hadoop node running this reduce() operation has a complete,
 *               compiled Curator distribution located (on the local file system)
 *               at `~/curator/dist/`.
 * @author Tyler A. Young
 * @author Lisa Y. Bao
 */
public class CuratorReducer extends Reducer<Text, HadoopRecord, Text, HadoopRecord> {
    /**
     * Constructs a CuratorReducer
     */
    public CuratorReducer() {
        distDir = new Path( "~" + Path.SEPARATOR + "curator"
                + Path.SEPARATOR + "dist" );
        logDir = new Path( distDir, "logs" );
        binDir = new Path( distDir, "bin" );
        configDir = new Path( distDir, "configs" );
    }

    /**
     * Asks the Curator get an annotation (the type of which is specified in the
     * context's configuration) for the document record in inValue.
     * @param inKey The document's hash
     * @param inValue The record for the document, which includes both the
     *                original text file and the known annotations.
     * @param context The job context
     */
    public void reduce( Text inKey, 
                        HadoopRecord inValue,
                        Context context ) throws IOException, InterruptedException {
        fs = FileSystem.get( context.getConfiguration() );

        // Pseudo-code per discussion with Mark on 27 June

        AnnotationMode toolToRun = AnnotationMode
                .fromString( context.getConfiguration().get("annotationMode") );

        // Check if annotation tool is running locally (i.e., by checking log
        // file located at CURATOR_DIR/logs/).
        if( !toolIsRunning( toolToRun ) ) {
            // If not, start it and sleep repeatedly until it's ready to go
            startTool( toolToRun );
            Thread.sleep(1000); // Give it 1 second at minimum to start up

            while( !toolIsRunning( toolToRun ) ) {
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
    public boolean toolIsRunning( AnnotationMode toolToCheck )
            throws EnumConstantNotPresentException {
        // Tokenizer is a special case: no log file // TODO: No log file, right?
        if( toolToCheck == AnnotationMode.TOKEN ) {
            return true;
        }

        // Get the tool's log location
        Path logLocation = getLogLocation( toolToCheck );

        // If the tool to check gives us an explicit "hello world" or something
        // in it's log file
        // TODO: Figure out which ones do this, and how to check it. . .

            // Check the log file

        // Else, check the log file's time of last modification. If it's within
        // the last half hour, assume all is well.
        try {
            if( fileWasModifiedInLastXMins( logLocation, fs, 30 ) ) {
                return true;
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
     */
    public boolean curatorIsRunning() {
        // Basically, since we assume that the Curator isn't launched until all
        // tools are good to go, we can assume the Curator is running as long as
        // its log file has been modified in the last half hour.
        Path logLocation = getCuratorLogLocation();

        try {
            if( fileWasModifiedInLastXMins( logLocation, fs, 30 ) ) {
                return true;
            }
        } catch( IOException e ) {
            // Can't access the file system. Just assume it works (else we could
            // have an infinite loop where we just keep trying to check the log).
            return true;
        }

        return false;
    }

    /**
     * @return The location, on the local file system, of the Curator log file
     */
    private Path getCuratorLogLocation() {
        return new Path( logDir, "curator.log" );
    }

    /**
     * Runs the shell script required to launch the indicated annotation tool.
     * If this script is not found in your Curator directory (i.e., at
     * `~/curator/dist/scripts/launch_annotator_on_this_node.sh`), we'll simply
     * create it.
     * @param toolToLaunch The annotation tool to launch (more accurately,
     *                     the type of annotation provided by the tool to be
     *                     launched)
     */
    public void startTool( AnnotationMode toolToLaunch )
            throws IOException {
        // Tokenizer is a special case (started with Curator . . .?)
        // TODO: Confirm Tokenizer starts with Curator
        if( toolToLaunch == AnnotationMode.TOKEN ) {
            return;
        }

        // Make sure log directory exists
        try {
            FileSystemHandler.mkdir( logDir, fs );
        } catch( IOException ignored ) { }

        // Figure out location of shell script based on tool in use
        Path scriptLocation;
        int port = -1;
        switch( toolToLaunch ) {
            case CHUNK:
                scriptLocation = new Path( binDir, "illinois-chunker-server.sh" );
                port = 9092;
                break;
            case COREF:
                scriptLocation = new Path( binDir, "illinois-coref-server.sh" );
                port = 9094;
                break;
            case NER:
                scriptLocation = new Path( binDir, "illinois-ner-extended-server.pl" );
                port = 9096;
                break;
            case NOM_SRL:
                scriptLocation = new Path( binDir, "illinois-nom-srl-server.sh" );
                port = 14910;
                break;
            case PARSE:
                scriptLocation = new Path( binDir, "stanford-parser-server.sh" );
                port = 9095;
                break;
            case POS:
                scriptLocation = new Path( binDir, "illinois-pos-server.sh" );
                port = 9091;
                break;
            case TOKEN:
                // Dummy case for completeness. This is handled as a special case
                // at the top of this method.
                return;
            case VERB_SRL:
                scriptLocation = new Path( binDir, "illinois-verb-srl-server.sh" );
                port = 14810;
                break;
            case WIKI:
                scriptLocation = new Path( binDir, "illinois-wikifier-server.sh" );
                port = 15231;
                break;
            default:
                throw new IllegalArgumentException( "Tool"
                        + toolToLaunch.toString() + " is not known." );
        }

        StringBuilder command = new StringBuilder();
        if( toolToLaunch != AnnotationMode.NER ) {
            command.append( scriptLocation.toString() );
            command.append(" -p ");
            command.append( port );
            command.append(" >& ");
            command.append( getLogLocation( toolToLaunch ).toString() );
            command.append(" &");
            Runtime.getRuntime().exec( command.toString() );
        }
        else { // NER is a special case. Weird syntax.
            command.append( scriptLocation.toString() );
            command.append( " " );
            command.append( port );
            command.append( " " );
            command.append( new Path( configDir, "ner.conll.config" )
                    .toString() );
            command.append( " >& " );
            command.append( getLogLocation( toolToLaunch ).toString() );
            command.append( " &" );

            Runtime.getRuntime().exec( command.toString() );

            port++;
            StringBuilder secondCommand = new StringBuilder();
            secondCommand.append(scriptLocation);
            secondCommand.append( " " );
            secondCommand.append( port );
            secondCommand.append( " " );
            secondCommand.append( new Path( configDir, "ner.ontonotes.config" )
                    .toString() );
            secondCommand.append( " >& " );
            secondCommand.append(
                    new Path( logDir, "ner-ext-ontonotes.log").toString() );
            secondCommand.append( " &" );
            Runtime.getRuntime().exec( secondCommand.toString() );
        }
    }

    /**
     * Runs the shell script required to launch the indicated annotation tool.
     * If this script is not found in your Curator directory (i.e., at
     * `~/curator/dist/scripts/launch_curator_on_this_node.sh`), we'll simply
     * create it.
     */
    public void startCurator() throws IOException {
        Path scriptLoc = new Path( binDir, "curator.sh" );
        Path annotatorsConfigLoc = new Path( configDir, "annotators-local.xml" );
        // Ensure the config file exists; create it if not
        checkAnnotatorConfig(annotatorsConfigLoc);

        StringBuilder launchScript = new StringBuilder( scriptLoc.toString() );
        launchScript.append(" --annotators ");
        launchScript.append( annotatorsConfigLoc.toString() );
        launchScript.append(" --port 9010 --threads 10 >& ");
        launchScript.append( getCuratorLogLocation().toString() );
        launchScript.append(" &");

        Runtime.getRuntime().exec( launchScript.toString() );

    }

    /**
     * Checks the XML file used to point the Curator to the locally running
     * annotators. If the file doesn't exist, it creates it.
     * @param annotatorsConfigLoc The location at which the config file should be
     *                            created.
     */
    private void checkAnnotatorConfig( Path annotatorsConfigLoc ) {
        if( !FileSystemHandler.localFileExists( annotatorsConfigLoc ) ) {
            // Create the config file here
            // TODO: implement this
        }
    }

    /**
     * Returns the log location on the local node for the specified annotation
     * tool.
     * @param tool The tool whose log location we should get
     * @return A Path (on the local filesystem) where you can find the log for
     *         the indicated annotation tool.
     * @TODO: Fix this
     */
    public Path getLogLocation( AnnotationMode tool ) {
        switch( tool ) {
            case CHUNK:
                return new Path( logDir, "chunk.log" );
            case COREF:
                return new Path( logDir, "coref.log" );
            case NER:
                return new Path( logDir, "ner-ext-conll.log" );
            case NOM_SRL:
                return new Path( logDir, "nom-srl.log" );
            case PARSE:
                return new Path( logDir, "stanford.log" );
            case POS:
                return new Path( logDir, "pos.log" );
            case TOKEN:
                // TODO: Seriously? The Tokenizer doesn't keep a log file?
                return new Path("");
            case VERB_SRL:
                return new Path( logDir, "verb-srl.log" );
            case WIKI:
                return new Path( logDir, "wikifier.log" );
            default:
                throw new IllegalArgumentException( "Tool"
                        + tool.toString() + " is not known." );
        }
    }

    private final Path distDir;
    private final Path logDir;
    private final Path binDir;
    private final Path configDir;
    private FileSystem fs;
}
