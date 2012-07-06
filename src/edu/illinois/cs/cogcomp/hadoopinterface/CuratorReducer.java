package edu.illinois.cs.cogcomp.hadoopinterface;

import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.AnnotationMode;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.FileSystemHandler;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.HadoopRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.FileSystemHandler.fileWasModifiedInLastXMins;
import static edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.FileSystemHandler.writeFileToLocal;

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
        Path distDir = new Path( "~" + Path.SEPARATOR + "curator"
                                 + Path.SEPARATOR + "dist" );
        dir = new PathStruct( distDir );
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
            startCurator( toolToRun );
            Thread.sleep(1000); // Give it 1 second at minimum to start up

            while( !curatorIsRunning() ) {
                Thread.sleep(5000);
            }
        }

        // Create a new Curator client object
        HadoopCuratorClient client = new HadoopCuratorClient(
                fs, FileSystem.getLocal( new Configuration() ) );

        client.annotateSingleDoc( inValue, toolToRun );

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
            FileSystemHandler.mkdir( dir.log(), fs );
        } catch( IOException ignored ) { }

        // Figure out location of shell script based on tool in use
        Path scriptLocation;
        int port = -1;
        switch( toolToLaunch ) {
            case CHUNK:
                scriptLocation = new Path( dir.bin(), "illinois-chunker-server.sh" );
                port = 9092;
                break;
            case COREF:
                scriptLocation = new Path( dir.bin(), "illinois-coref-server.sh" );
                port = 9094;
                break;
            case NER:
                scriptLocation = new Path( dir.bin(), "illinois-ner-extended-server.pl" );
                port = 9096;
                break;
            case NOM_SRL:
                scriptLocation = new Path( dir.bin(), "illinois-nom-srl-server.sh" );
                port = 14910;
                break;
            case PARSE:
                scriptLocation = new Path( dir.bin(), "stanford-parser-server.sh" );
                port = 9095;
                break;
            case POS:
                scriptLocation = new Path( dir.bin(), "illinois-pos-server.sh" );
                port = 9091;
                break;
            case TOKEN:
                // Dummy case for completeness. This is handled as a special case
                // at the top of this method.
                return;
            case VERB_SRL:
                scriptLocation = new Path( dir.bin(), "illinois-verb-srl-server.sh" );
                port = 14810;
                break;
            case WIKI:
                scriptLocation = new Path( dir.bin(), "illinois-wikifier-server.sh" );
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
            command.append( new Path( dir.config(), "ner.conll.config" )
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
            secondCommand.append( new Path( dir.config(), "ner.ontonotes.config" )
                    .toString() );
            secondCommand.append( " >& " );
            secondCommand.append(
                    new Path( dir.log(), "ner-ext-ontonotes.log").toString() );
            secondCommand.append( " &" );
            Runtime.getRuntime().exec( secondCommand.toString() );
        }
    }

    /**
     * Runs the shell script required to launch the indicated annotation tool.
     * If this script is not found in your Curator directory (i.e., at
     * `~/curator/dist/scripts/launch_curator_on_this_node.sh`), we'll simply
     * create it.
     * @param runningTool The annotation tool that is already running on this
     *                    Hadoop node
     */
    public void startCurator( AnnotationMode runningTool ) throws IOException {
        Path scriptLoc = new Path( dir.bin(), "curator.sh" );
        Path annotatorsConfigLoc = getAnnotatorConfigLoc( runningTool );
        // Ensure the config file exists; create it if not


        StringBuilder launchScript = new StringBuilder( scriptLoc.toString() );
        launchScript.append(" --annotators ");
        launchScript.append( annotatorsConfigLoc.toString() );
        launchScript.append(" --port " );
        launchScript.append( Integer.toString( HadoopCuratorClient.PORT ) );
        launchScript.append(" --threads 10 >& ");
        launchScript.append( getCuratorLogLocation().toString() );
        launchScript.append(" &");

        Runtime.getRuntime().exec( launchScript.toString() );

    }

    /**
     * Returns the log location on the local node for the specified annotation
     * tool.
     * @param tool The tool whose log location we should get
     * @return A Path (on the local filesystem) where you can find the log for
     *         the indicated annotation tool.
     */
    private Path getLogLocation( AnnotationMode tool ) {
        switch( tool ) {
            case CHUNK:
                return new Path( dir.log(), "chunk.log" );
            case COREF:
                return new Path( dir.log(), "coref.log" );
            case NER:
                return new Path( dir.log(), "ner-ext-conll.log" );
            case NOM_SRL:
                return new Path( dir.log(), "nom-srl.log" );
            case PARSE:
                return new Path( dir.log(), "stanford.log" );
            case POS:
                return new Path( dir.log(), "pos.log" );
            case TOKEN:
                // TODO: Seriously? The Tokenizer doesn't keep a log file?
                return new Path("");
            case VERB_SRL:
                return new Path( dir.log(), "verb-srl.log" );
            case WIKI:
                return new Path( dir.log(), "wikifier.log" );
            default:
                throw new IllegalArgumentException( "Tool"
                        + tool.toString() + " is not known." );
        }
    }

    /**
     * @return The location, on the local file system, of the Curator log file
     */
    private Path getCuratorLogLocation() {
        return new Path( dir.log(), "curator.log" );
    }

    /**
     * Checks the XML file used to point the Curator to the locally running
     * annotators. If the file doesn't exist, it creates it.
     *
     * @param runningTool The annotator currently running on this node
     * @return The location at which the config file can be accessed.
     */
    private Path getAnnotatorConfigLoc(AnnotationMode runningTool) throws IOException {
        String fileName = "annotators-local-" + runningTool.toString() + ".xml";
        Path configLoc = new Path( dir.config(), fileName );

        // If the config file doesn't exist, go ahead and create it.
        if( !FileSystemHandler.localFileExists( configLoc ) ) {
            StringBuilder file = new StringBuilder();
            file.append( "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n" );
            file.append( "<curator-annotators>\n" );
            file.append( "<annotator>\n" );

            // TODO: Configs for missing annotators
            switch (runningTool) {
                case CHUNK:
                    file.append( "    <type>labeler</type>\n" );
                    file.append( "    <field>chunk</field>\n" );
                    file.append( "    <local>edu.illinois.cs.cogcomp.annotation.handler.IllinoisChunkerHandler</local>\n" );
                    file.append( "    <requirement>sentences</requirement>\n" );
                    file.append( "    <requirement>tokens</requirement>\n" );
                    file.append( "    <requirement>pos</requirement>\n" );
                    break;
                case COREF:
                    file.append( "    <type>clustergenerator</type>\n" );
                    file.append( "    <field>coref</field>\n" );
                    file.append( "    <local>edu.illinois.cs.cogcomp.annotation.handler.IllinoisCorefHandler</local>\n" );
                    file.append( "    <requirement>sentences</requirement>\n" );
                    file.append( "    <requirement>tokens</requirement>\n" );
                    file.append( "    <requirement>pos</requirement>\n" );
                    file.append( "    <requirement>ner</requirement>\n" );
                    break;
                case NER:
                    file.append( "    <type>labeler</type>\n" );
                    file.append( "    <field>ner</field>\n" );
                    file.append( "    <local>edu.illinois.cs.cogcomp.annotation.handler.IllinoisNERHandler</local>\n" );
                    break;
                case NOM_SRL:
                    file.append( "    \n" );
                    file.append( "    \n" );
                    file.append( "    \n" );
                    file.append( "    \n" );
                    break;
                case PARSE:
                    file.append( "    <type>multiparser</type>\n" );
                    file.append( "    <field>stanfordParse</field>\n" );
                    file.append( "    <field>stanfordDep</field>\n" );
                    file.append( "    <local>edu.illinois.cs.cogcomp.annotation.handler.StanfordParserHandler</local>\n" );
                    file.append( "    <requirement>tokens</requirement>\n" );
                    file.append( "    <requirement>sentences</requirement>\n" );
                    break;
                case POS:
                    file.append( "    <type>labeler</type>\n" );
                    file.append( "    <field>pos</field>\n" );
                    file.append( "    <local>edu.illinois.cs.cogcomp.annotation.handler.IllinoisPOSHandler</local>\n" );
                    file.append( "    <requirement>sentences</requirement>\n" );
                    file.append( "    <requirement>tokens</requirement>\n" );
                    break;
                case TOKEN:
                    file.append( "    <type>multilabeler</type>\n" );
                    file.append( "    <field>sentences</field>\n" );
                    file.append( "    <field>tokens</field>\n" );
                    file.append( "    <local>edu.illinois.cs.cogcomp.annotation.handler.IllinoisTokenizerHandler</local>\n");
                    break;
                case VERB_SRL:
                    file.append( "    \n" );
                    file.append( "    \n" );
                    file.append( "    \n" );
                    break;
                case WIKI:
                    file.append( "    \n" );
                    file.append( "    \n" );
                    file.append( "    \n" );
                    break;
            }

            file.append( "</annotator>\n" );
            file.append( "</curator-annotators>\n" );

            writeFileToLocal( file.toString(), configLoc );
        }
        return configLoc;
    }

    /**
     * Stores all the Path objects used by the Reducer. Simplifies usage of the
     * directories by providing a centralized, write-once data structure.
     */
    private class PathStruct {
        /**
         * Constructs all the Path objects used by the Reducer (which are located
         * relative to the `dist` directory)
         * @param distDir A Path to the local Curator's `dist` directory. Should
         *                be something like `~/curator/dist`.
         */
        public PathStruct( Path distDir ) {
            this.distDir = distDir;

            logDir = new Path( distDir, "logs" );
            binDir = new Path( distDir, "bin" );
            configDir = new Path( distDir, "configs" );
        }

        public Path dist() {
            return distDir;
        }

        public Path log() {
            return logDir;
        }

        public Path bin() {
            return binDir;
        }

        public Path config() {
            return configDir;
        }

        private final Path distDir;
        private final Path logDir;
        private final Path binDir;
        private final Path configDir;
    }

    private final PathStruct dir;
    private FileSystem fs;
}
