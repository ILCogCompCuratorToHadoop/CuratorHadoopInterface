package edu.illinois.cs.cogcomp.hadoopinterface;

import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.*;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.exceptions
        .CuratorNotFoundException;
import edu.illinois.cs.cogcomp.thrift.base.AnnotationFailedException;
import edu.illinois.cs.cogcomp.thrift.base.ServiceSecurityException;
import edu.illinois.cs.cogcomp.thrift.base.ServiceUnavailableException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.thrift.TException;

import java.io.File;
import java.io.IOException;
import java.util.*;

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
    public CuratorReducer() throws CuratorNotFoundException, IOException {
        Path curatorDir = new Path( System.getProperty( "user.home" ), "curator" );
        Path distDir = new Path( curatorDir, "dist" );
        distDir.makeQualified( FileSystem.get( new Configuration() ) );
        dir = new PathStruct( distDir );

        if( !FileSystemHandler.localFileExists( distDir ) ) {
            throw new CuratorNotFoundException("Curator directory does not exist "
                    + "at " + distDir.toString() + " on this Hadoop node. "
                    +"\nCannot continue...");

        }

        // These are the tools that must be started separately from the Curator
        toolsThatMustBeLaunched = new HashSet<AnnotationMode>();
        toolsThatMustBeLaunched.add( AnnotationMode.PARSE );
        toolsThatMustBeLaunched.add( AnnotationMode.WIKI );
        toolsThatMustBeLaunched.add( AnnotationMode.NER );
        toolsThatMustBeLaunched.add( AnnotationMode.VERB_SRL);
        toolsThatMustBeLaunched.add( AnnotationMode.NOM_SRL );

        spawnedCuratorProcesses = new LinkedList<Process>();
        spawnedAnnotatorProcesses = new LinkedList<Process>();
    }

    /**
     * Asks the Curator to get an annotation (the type of which is specified in the
     * context's configuration) for the document record in inValue.
     * @param inKey The document's hash
     * @param inValues The record(s) for the document(s), which include both the
     *                 original text file and the known annotations.
     * @param context The job context
     * @throws IOException Since reduce() is contractually obligated to throw
     *                     only IOExceptions and InterruptedExceptions, we're
     *                     forced to abuse the semantics here. IOExceptions can be
     *                     thrown if we cannot launch the Curator or an annotator,
     *                     if we fail to annotate the document correctly, and
     *                     so on.
     */
    @Override
    public void reduce( Text inKey,
                        Iterable<HadoopRecord> inValues,
                        Context context )
            throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get( context.getConfiguration() );
        this.fsHandler = new FileSystemHandler( fs );
        setEnvVars( context.getConfiguration() );

        AnnotationMode toolToRun = AnnotationMode
                .fromString( context.getConfiguration().get("annotationMode") );

        // Create a new Curator client object
        client = new HadoopCuratorClient( fs );

        // Launch the annotator and the Curator
        try {
            launchAnnotatorIfNecessary( toolToRun );
            context.progress();
            launchCuratorIfNecessary( toolToRun );
        } catch ( TException e ) {
            throw new IOException( e.getMessage() );
        }

        // Confirm the launch worked
        logger.logStatus( "Checking if tool can be run." );
        int numCyclesWaited = 0;
        while( !toolCanBeRun( toolToRun ) ) {
            // If the tool isn't ready, we'll wait a bit.
            logger.logStatus( "Annotator for " + toolToRun.toString()
                              + " isn't ready. Waiting...");

            Thread.sleep( getEstimatedTimeToStart( toolToRun ) );

            ++numCyclesWaited;
            if( numCyclesWaited > MAX_ATTEMPTS ) {
                try {
                    throw new IOException( toolToRun.toString()
                            + " cannot be used to " +
                            "annotate the document. Available annotators: "
                            + MessageLogger.getPrettifiedList(
                            client.listAvailableAnnotators() )
                            + client.describeAnnotations().toString() );
                } catch ( TException fromListAnnotators ) { }
            }
        }

        logger.logStatus( "Beginning document annotation." );
        // Annotate each document (There should only ever be one, but the contract
        // with reduce() says you have to accept an iterable of your values.)
        for( HadoopRecord inValue : inValues ) {
            String startingText = inValue.getRawText();

            // Warn the user if the input record already has this annotation.
            if( RecordTools.hasAnnotation( inValue, toolToRun ) ) {
                logger.logWarning( "Document "
                        + inValue.getIdentifier() + ", which begins '"
                        + RecordTools.getBeginningOfOriginalText( inValue )
                        + "', already has the requested "
                        + toolToRun.toString() + " annotation:\n\t"
                        + inValue.getLabelViews()
                                 .get( toolToRun.toCuratorString() ) );
            }

            // Annotate the document, and do a toooooooon of error handling.
            try {
                logger.logStatus( "Annotating the document." );
                client.annotateSingleDoc( inValue, toolToRun );
            } catch (ServiceUnavailableException e) {
                try {
                    String msg = toolToRun.toString()
                            + " annotations are not available.\nReason: "
                            + e.getReason() + "\nWe know of these annotations: "
                            + client.describeAnnotations().toString();
                    logger.logError( msg );
                    throw new IOException( msg );
                } catch ( TException ignored ) { }
            } catch (TException e) {
                String msg = "Transport exception when getting "
                        + toolToRun.toString() + " annotation.\nMessage: "
                        + e.getMessage() + "\nStack trace:\n"
                        + MessageLogger.getPrettifiedList(
                        Arrays.asList( e.getStackTrace() ) );
                logger.logError( msg );

                throw new IOException(msg);
            } catch (AnnotationFailedException e) {
                String msg = "Failed attempting annotation "
                        + toolToRun.toString() + ".\n" + e.getReason();
                logger.logError( msg );

                throw new IOException(msg);
            } catch ( ServiceSecurityException e ) {
                String msg = "Failed attempting database access for annotation "
                        + toolToRun.toString() + ".\n" + e.getReason();
                logger.logError( msg );

                throw new IOException(msg);
            }


            // Check that the raw text roughly matches what we started with.
            logger.logStatus( "Checking for catastrophic errors that may have "
                              + "occurred during annotation." );
            dieIfTextDoesntMatch( startingText,
                                  client.getLastAnnotatedRecord().getRawText() );


            // Serialize the updated record to the output directory
            logger.logStatus( "Writing the annotation's output." );
            Path outputDir = new Path( context.getConfiguration().get("outputDirectory") );
            try {
                client.writeOutputFromLastAnnotate( outputDir );
            } catch ( TException e ) {
                logger.logError( "Thrift error in HadoopCuratorClient writing " +
                                "output from annotation: " + e.getMessage() );
                e.printStackTrace();
            }

            // Pass Curator output back to Hadoop as Record
            logger.logStatus( "Finished serializing record "
                    + inValue.getDocumentHash() + " to " + outputDir.toString() );
            context.write(inKey, inValue);
        }
    }

    /**
     * Taking information from the job configuration, this sets up the
     * environment variables we'll use when launching the Curator and annotators.
     * @param config The job's Configuration object
     * @postcondition envVarsForRuntimeExec is safe to use
     */
    private void setEnvVars( Configuration config ) {
        if( config.get( "libPath" ) != null ) {
            envVarsForRuntimeExec = new String[] { "LD_LIBRARY_PATH="
                                                   + config.get( "libPath" ) };
        }
        else {
            envVarsForRuntimeExec = new String[0];
        }
    }

    /**
     * Throws an error if the two strings are not roughly the same.
     * @param original One string to be compared
     * @param other The other string
     * @throws IOException If the two strings are not roughly equal. However, if
     *                     the diff is just a few characters, we can attribute it
     *                     to differences in line endings and such.
     */
    private void dieIfTextDoesntMatch( String original, String other )
            throws IOException {
        if( other.equals( other ) ) {
            int diff = StringUtils.getLevenshteinDistance( other,
                    other );
            // Unless the diff is greater than a few characters, we can
            // probably attribute it to differences in line endings and
            // that sort of thing.
            if( diff > 10 ) {
                throw new IOException("Raw text for a record has changed. "
                        + "This is a big problem.\n\nIt used to be: "
                        + other + "\n\n...but it's now: "
                        + other );
            }
        }
    }

    /**
     * Checks to see that the indicated tool can be run (i.e., that the
     * annotation mode is provided by an annotator that the Curator can connect
     * to).
     * @param tool The annotation tool in question
     * @return True if the Curator lists that annotation tool as being among its
     *         avialable annotators, false otherwise
     */
    private boolean toolCanBeRun( AnnotationMode tool ) {
        try {
            String toolName = tool.toCuratorString();
            Map<String, String> annotators = client.describeAnnotations();

            // Tool can be run if we both know of the annotation and its
            // provider is not null.
            return annotators.containsKey( toolName )
                    && !annotators.get( toolName ).contains( "null" );
        } catch ( TException e ) {
            logger.logError( "Thrift error while checking if " + tool.toString()
                    + " tool can be run." );
            return false;
        }
    }

    /**
     * Checks to see if the Curator is running on the local node. If it is not,
     * it will launch it and wait to return until it confirms the Curator
     * has successfully started.
     * @param toolToRun The annotation tool that the Curator should be configured
     *                  to communicate with
     * @throws IOException If, after many attempts, we are unable to launch the
     *                     Curator.
     * @throws InterruptedException If sleeping the thread fails
     * @postcondition Curator is running and accessible through the client
     */
    public void launchCuratorIfNecessary( AnnotationMode toolToRun )
            throws IOException, InterruptedException, TException {
        int numCyclesWaited = 0;
        while( !client.curatorIsRunning() ) {
            // If not, start it and sleep until it's ready to go
            if( spawnedCuratorProcesses.isEmpty() ) {
                startCurator( toolToRun );

                // Give Curator itself time to start up
                Thread.sleep( getEstimatedTimeToStart( AnnotationMode.TOKEN ));
            }

            // If we've waited more than the max number of times, quit.
            ++numCyclesWaited;
            if( numCyclesWaited >= MAX_ATTEMPTS ) {
                destroyAllSpawnedProcesses();
                throw new IOException( "Unable to launch Curator. "
                        + "Waited " + numCyclesWaited
                        + " times longer than expected." );
            }
        }

        if( numCyclesWaited == 0 ) {
            logger.log( "Curator was already running on node." );
        }
        else {
            logger.log( "Successfully launched Curator on node." );
        }
    }

    /**
     * Since we keep track of all instances of the Curator that we launch, we can
     * use this method to kill them if necessary.
     */
    private void destroyAllSpawnedProcesses() throws IOException {
        for( Process p : spawnedCuratorProcesses ) {
            logger.logStatus( "Stopping a Curator process that " +
                    "was launched on this Reduce node." );
            p.destroy();
        }
        for( Process p : spawnedAnnotatorProcesses ) {
            logger.logStatus( "Stopping an annotator process " +
                    "that was launched on this Reduce node." );
            p.destroy();
        }
        spawnedCuratorProcesses.clear();
        spawnedAnnotatorProcesses.clear();
        setToolHasBeenLaunched( false );
    }

    /**
     * Checks to see if the indicated annotation tool is running already. If it
     * is not, it will launch it and wait to return until it confirms the tool
     * has successfully started.
     * @param toolToRun The annotation tool to launch
     * @throws IOException If, after many attempts, we are unable to launch the
     *                     annotation tool.
     * @throws InterruptedException If sleeping the thread fails
     * @postcondition The requested annotator is running and accessible through
     *                the client
     */
    public void launchAnnotatorIfNecessary( AnnotationMode toolToRun )
            throws IOException, InterruptedException, TException {

        // Launch the tool if it's both among the tools to launch separately
        // and no other threads on this machine have launched it
        if( toolsThatMustBeLaunched.contains(toolToRun) ) {
            // Check a file on the local machine (which just acts as a way of
            // communicating across instances of reduce() on a given machine)

            if( !toolHasBeenLaunched() ) {
                startTool( toolToRun );

                Thread.sleep( getEstimatedTimeToStart( toolToRun ) );

                setToolHasBeenLaunched( true );
            }
        }
    }

    private boolean toolHasBeenLaunched() {
        File flagInFileSystem = new File( dir.user().toString(),
                "_annotator_launched" );
        return flagInFileSystem.exists();
    }

    // TODO: Unset this after we shut down the tool!
    private void setToolHasBeenLaunched( boolean newValue ) throws IOException {
        File flagInFileSystem = new File( dir.user().toString(),
                "_annotator_launched" );
        if( newValue ) {
            flagInFileSystem.createNewFile();
        }
        else {
            flagInFileSystem.delete();
        }
    }

    /**
     * Gets the estimated number of milliseconds that it takes for an annotation
     * tool to launch. This is how long you should wait before attempting to start
     * that tool again.
     * @param toolToRun The annotation tool in question
     * @return The number of milliseconds you should wait before expecting the
     *         tool to be running
     */
    private long getEstimatedTimeToStart( AnnotationMode toolToRun ) {
        long timeForSmallModels = 3000; // 3 secs
        long timeForMidModels = 10000; // 10 secs
        long timeForLargeModels = 90000; // 90 secs
        switch ( toolToRun ) {
            case CHUNK:
                return timeForMidModels; // By my estimates, takes about 5 secs
            case COREF:
                return timeForSmallModels;
            case NER:
                return timeForMidModels*3; // Estimate: < 30 sec
            case NOM_SRL:
                return (int)(timeForMidModels * 1.5); // By my estimates, it takes 8 secs
            case PARSE:
                return timeForMidModels * 2; // Estimated: a bit over 10 secs
            case POS:
                return timeForSmallModels;
            case SENTENCE:
                return timeForSmallModels;
            case TOKEN:
                return timeForSmallModels;
            case VERB_SRL:
                return (int)(timeForLargeModels / 1.5); // Est <45 secs
            case WIKI:
                return timeForLargeModels;
            default:
                return timeForLargeModels; // better safe than sorry
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
    private void startCurator( AnnotationMode runningTool ) throws IOException {
        Path scriptLoc;
        if( runningTool.equals( AnnotationMode.NER ) ) {
            scriptLoc = new Path( dir.bin(), "curator.sh" );
        }
        else {
            scriptLoc = new Path( dir.bin(), "curator-local.sh" );
        }
        Path annotatorsConfigLoc = getAnnotatorConfigLoc( runningTool );
        // Ensure the config file exists; create it if not

        StringBuilder launchScript = new StringBuilder( scriptLoc.toString() );
        launchScript.append(" --annotators ");
        launchScript.append( annotatorsConfigLoc.toString() );
        launchScript.append(" --port " );
        launchScript.append( Integer.toString( HadoopCuratorClient.PORT ) );
        launchScript.append(" --threads 10");

        logger.logStatus( "Launching Curator on node with "
                + "command \n\t" + launchScript.toString() );
        Process p = Runtime.getRuntime().exec( launchScript.toString(),
                                               envVarsForRuntimeExec );
        spawnedCuratorProcesses.add( p );

        // Handle the output from the Curator (we don't want to print it, but
        // we can't just leave the output stream there, as it can cause deadlock
        // in some OS's implementations)
        StreamGobbler err = new StreamGobbler( p.getErrorStream(),
                                               "Curator ERR: ", false );
        StreamGobbler out = new StreamGobbler( p.getInputStream(),
                                               "Curator: ", false );

        err.start();
        out.start();
    }

    /**
     * Checks the XML file used to point the Curator to the locally running
     * annotators. If the file doesn't exist, creates it.
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


            // Make the tokenizer available to all configurations, as it is
            // memory-light enough not to slow anything down
            file.append( "<annotator>\n" );
            file.append( "    <type>multilabeler</type>\n" );
            file.append( "    <field>sentences</field>\n" );
            file.append( "    <field>tokens</field>\n" );
            file.append( "    <local>edu.illinois.cs.cogcomp.annotation.handler.IllinoisTokenizerHandler</local>\n");
            file.append( "</annotator>\n" );

            if( !runningTool.equals( AnnotationMode.TOKEN )
                    && !runningTool.equals( AnnotationMode.SENTENCE ) ) {

                file.append( "<annotator>\n" );
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
                        file.append( "    <field>ner-ext</field>\n" );   // added "-ext"
                        file.append( "    <host>localhost:9093</host>\n" ); // changed to non-local
                        break;
                    case NOM_SRL:
                        file.append( "    <type>parser</type>\n" );
                        file.append( "    <field>nom</field>\n" );
                        file.append( "    <host>localhost:14910</host>\n" );
                        file.append( "    <requirement>sentences</requirement>\n" );
                        file.append( "    <requirement>tokens</requirement>\n" );
                        file.append( "    <requirement>pos</requirement>\n" );
                        file.append( "    <requirement>chunk</requirement>\n" );
                        file.append( "    <requirement>charniak</requirement>\n" );
                        break;
                    case PARSE:
                        file.append( "    <type>parser</type>\n" );
                        file.append( "    <field>charniak</field>\n" );
                        file.append( "    <host>localhost:9987</host>\n" );
                        file.append( "    <requirement>tokens</requirement>\n" );
                        file.append( "    <requirement>sentences</requirement>\n" );
                        break;
                    case STANFORD_PARSE:
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
                        // Handled above; placed here for consistency
                        break;
                    case VERB_SRL:
                        file.append( "    <type>parser</type>\n" );
                        file.append( "    <field>srl</field>\n" );
                        file.append( "    <host>localhost:14810</host>\n" );
                        file.append( "    <requirement>sentences</requirement>\n" );
                        file.append( "    <requirement>tokens</requirement>\n" );
                        file.append( "    <requirement>pos</requirement>\n" );
                        file.append( "    <requirement>chunk</requirement>\n" );
                        file.append( "    <requirement>charniak</requirement>\n" );
                        break;
                    case WIKI:
                        file.append( "    <type>labeler</type>\n" );
                        file.append( "    <field>wikifier</field>\n" );
                        file.append( "    <host>localhost:15231</host>\n" );
                        file.append( "    <requirement>sentences</requirement>\n" );
                        file.append( "    <requirement>tokens</requirement>\n" );
                        file.append( "    <requirement>pos</requirement>\n" );
                        file.append( "    <requirement>chunk</requirement>\n" );
                        file.append( "    <requirement>ner</requirement>\n" );
                        break;
                }
                file.append( "</annotator>\n" );
            }

            file.append( "</curator-annotators>\n" );

            fsHandler.writeFileToLocal( file.toString(), configLoc );
        }
        return configLoc;
    }


    /**
     * Runs the shell script required to launch the indicated annotation tool.
     * If this script is not found in your Curator directory (i.e., at
     * `~/curator/dist/scripts/launch_annotator_on_this_node.sh`), we'll simply
     * create it.
     * @param toolToLaunch The annotation tool to launch (more accurately,
     *                     the type of annotation provided by the tool to be
     *                     launched). Only a few tools should be launched in this
     *                     way (like the Charniak parser).
     */
    private void startTool( AnnotationMode toolToLaunch )
            throws IOException {
        // Make sure log directory exists
        try {
            fsHandler.mkdir( dir.log() );
        } catch( IOException ignored ) { }

        // Figure out location of shell script based on tool in use
        Path scriptLocation = null;
        int port = -1;
        switch( toolToLaunch ) {
            case COREF:
                scriptLocation = new Path( dir.bin(), "illinois-coref-server.sh" );
                port = 9094;
                break;
            case NER:
                // NOTE: NER has to be launched from the directory above the
                // Curator. This is annoying.
                scriptLocation = new Path( "bin/illinois-ner-extended-server.pl" );
                port = 9093;
                break;
            case NOM_SRL:
                scriptLocation = new Path( "bin/illinois-nom-srl-server.sh" );
                port = 14910;
                break;
            case VERB_SRL:
                scriptLocation = new Path( "bin/illinois-verb-srl-server.sh" );
                port = 14810;
                break;
            case WIKI:
                scriptLocation = new Path( "bin/illinois-wikifier-server.sh" );
                port = 15231;
                break;
            case PARSE:
                // Charniak is started really weird. This is a dummy case so that
                // we don't throw an error. We'll handle the Charniak separately
                // below.
                break;
            default:
                throw new IllegalArgumentException( "Tool " +
                        toolToLaunch.toString() + " cannot be started manually." );
        }

        // Build up the command line command that will start the annotator
        File dirToLaunchAgainst = new File( dir.dist().toString() );
        StringBuilder cmd = new StringBuilder();
        if( toolToLaunch != AnnotationMode.NER &&
                toolToLaunch != AnnotationMode.PARSE ) {
            cmd.append( scriptLocation.toString() );
            cmd.append( " -p " );
            cmd.append( port );
            cmd.append( " >& " );
            cmd.append( getLogLocation( toolToLaunch ).toString() );
            cmd.append( " &" );

            logger.logStatus( "Launching " + toolToLaunch.toString()
                              + " annotator on node with command \n\t"
                              + cmd.toString() );

            // Launch the process from the user directory (e.g., /home/username/)
            spawnedAnnotatorProcesses.add( Runtime.getRuntime().exec(
                    cmd.toString(), envVarsForRuntimeExec, dirToLaunchAgainst ) );
        }
        // NER is launched in a weird way.
        else if( toolToLaunch.equals( AnnotationMode.NER ) ) {
            String configs = dir.config().toString();
            String logs = dir.log().toString();

            cmd.append( scriptLocation.toString() );
            cmd.append( " nerconll " ); // some ID
            cmd.append( port );
            cmd.append( " " );
            cmd.append( configs );
            cmd.append( "/ner.conll.config" );

            StringBuilder cmd2 = new StringBuilder( );
            cmd2.append( scriptLocation.toString() );
            cmd2.append( " nerontonotes " ); // some ID
            cmd2.append( ++port );
            cmd2.append( " " );
            cmd2.append( configs );
            cmd2.append( "/ner.ontonotes.config" );

            logger.logStatus( "Launching NER annotator on node with "
                    + "command \n\t" + cmd.toString()
                    + "\n\t" + cmd2.toString() );

            spawnedAnnotatorProcesses.add( Runtime.getRuntime().exec(
                    cmd.toString(), envVarsForRuntimeExec, dirToLaunchAgainst ) );
            spawnedAnnotatorProcesses.add( Runtime.getRuntime().exec(
                    cmd2.toString(), envVarsForRuntimeExec, dirToLaunchAgainst ) );

        }
        // Charniak parser is also launched differently
        else if( toolToLaunch.equals(AnnotationMode.PARSE) ) {
            dirToLaunchAgainst = new File(
                    new Path( dir.dist(), "CharniakServer" ).toString() );
            cmd.append( "parser05May26fixed/PARSE/charniakThriftServer " );
            cmd.append( "9987 config.txt" );

            logger.logStatus( "Launching Charniak parser on node with "
                    + "command \n\t" + cmd.toString()
                    + "\n\t from directory "
                    + dirToLaunchAgainst.toString() );

            // TODO: Make the Thrift library path a parameter
            spawnedAnnotatorProcesses.add( Runtime.getRuntime().exec(
                    cmd.toString(), envVarsForRuntimeExec, dirToLaunchAgainst ) );
        }

        // Use the StreamGobbler to output the messages from the annotator to the
        // standard output
        Process p = spawnedAnnotatorProcesses.get(0);
        StreamGobbler err = new StreamGobbler( p.getErrorStream(), "Annotator ERR: " );
        StreamGobbler out = new StreamGobbler( p.getInputStream(), "Annotator: " );

        err.start();
        out.start();
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
                return new Path( dir.log(), "charniak_parse.log" );
            case POS:
                return new Path( dir.log(), "pos.log" );
            case TOKEN:
                // Seriously, the Tokenizer doesn't keep a log file?
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
            userDir = distDir.getParent().getParent();
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

        public Path user() {
            return userDir;
        }

        private final Path userDir;
        private final Path distDir;
        private final Path logDir;
        private final Path binDir;
        private final Path configDir;
    }

    private final PathStruct dir;
    private FileSystemHandler fsHandler;
    private HadoopCuratorClient client;
    private String [] envVarsForRuntimeExec;
    private List<Process> spawnedCuratorProcesses;
    private List<Process> spawnedAnnotatorProcesses;
    private Set<AnnotationMode> toolsThatMustBeLaunched;
    private static final MessageLogger logger = HadoopInterface.logger;
    private static final int MAX_ATTEMPTS = 10;
}
