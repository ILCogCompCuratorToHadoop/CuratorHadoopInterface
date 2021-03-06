package edu.illinois.cs.cogcomp.hadoopinterface;

import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.*;
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
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.*;

/**
 * A Reducer that serves as a wrapper for the document annotation tool. It
 * handles creating a locally-running Curator, launching and calling the
 * annotation tool, and so on. After running the annotation tool, new annotation
 * will be written to HDFS.
 *
 * @precondition The Hadoop node running this reduce() operation has a complete,
 *               compiled Curator distribution located (on the local file system)
 *               at `~/curator/dist/` <em>or</em> you have specified where the
 *               Curator can be found using the "-curator" flag when calling
 *               the HadoopInterface (probably in your launch script).
 * @TODO: Extract a class to handle the launching of the NLP tools
 * @TODO: Don't check that tools are running via flags in the file system.
 *        Instead, inspect processes using pgrep and jps command-line tools.
 * @author Tyler A. Young
 * @author Lisa Y. Bao
 */
public class CuratorReducer
        extends Reducer<Text, HadoopRecord, Text, HadoopRecord> {
    public static final String userDir = System.getProperty( "user.home" );
    private static final String curatorLockName = "CURATOR_IS_IN_USE";
    private String thisNodesMacAddress;

    /**
     * Constructs a CuratorReducer
     */
    public CuratorReducer() {
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
        logger.log( "Beginning reduce() . . ." );
        FileSystem fs = FileSystem.get( context.getConfiguration() );
        this.fsHandler = new FileSystemHandler( fs );
        setEnvVars( context.getConfiguration() );
        setUpCuratorDirs( context.getConfiguration() );

        AnnotationMode toolToRun = AnnotationMode
                .fromString( context.getConfiguration().get("annotationMode") );

        // Create a new Curator client object
        client = new HadoopCuratorClient( fs );

        // Launch the annotator and the Curator
        try {
            shutDownCuratorFromPreviousJob( toolToRun );
            launchAnnotatorIfNecessary( toolToRun );
            context.progress();
            launchCuratorIfNecessary( toolToRun );
        } catch ( TException e ) {
            throw new IOException( e.getMessage() );
        }

        // Confirm the launch worked
        logger.logStatus( "Checking if tool can be run." );
        if( !toolCanBeRun( toolToRun ) ) {
            // Could have been launched and we haven't given it enough time.
            Thread.sleep( getEstimatedTimeToStart(toolToRun) );

            // Yep. Two tests before we die.
            if( !toolCanBeRun( toolToRun ) ) {
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
                logger.logStatus( "Annotating the document that begins \""
                        + RecordTools.getBeginningOfOriginalText( inValue )
                        + "\" (has ID " + inValue.getDocumentHash() + ").");
                client.annotateSingleDoc( inValue, toolToRun );
            } catch (ServiceUnavailableException e) {
                try {
                    String msg = toolToRun.toString() + " (a.k.a. '"
                            + toolToRun.toCuratorString() + "')"
                            + " annotations are not available.\nReason: "
                            + e.getReason() + "\nWe know of these annotations: "
                            + client.describeAnnotations().toString();
                    logger.logError( msg );

                    // Panic. This error probably indicates the NLP tool died a
                    // fiery death.
                    shutdownAllLocalNLPTools();
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
                        + toolToRun.toString() + ".\nReason: " + e.getReason();
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
        }
    }

    /**
     * Determines where the Curator is installed based on the job configuration.
     * @param config The job configuration for this MapReduce job.
     * @throws IOException
     */
    private void setUpCuratorDirs( Configuration config )
            throws IOException {
        String specifiedLoc = config.get("curatorLoc");
        Path curatorDir = null;

        // If the Curator directory is shared . . .
        if(  config.get("curatorLocIsShared") != null
                && !config.get("curatorLocIsShared").equals("") ) {
            // Curator resides on a shared (networked) disk. There will be many
            // Curator directories instead of just one (named [specifiedLoc]_1,
            // [specifiedLoc]_2, etc.). We need to "lock" one of those, or wait
            // for one to become unlocked.

            // Lots of nodes will be competing for access to the same Curator.
            // Let's wait a variable amount of time to give everyone a fighting
            // chance at safely getting a lock.
            Random rng = new Random();
            /*try {
                // Sleep somewhere between 0 and 30 seconds.
                Thread.sleep( rng.nextInt(1000*30) );
            } catch ( InterruptedException ignored ) { }       */


            // TODO: [long term] Change this code if more nodes may exist!
            // Note: we have 64 "nodes" in the cluster, but only 32 physical
            // machines, and thus a max of 32 copies (plus 1 for the local,
            // Master version) of the Curator/NLP tools running at a given time!
            final int maxCuratorInstallations = 33;

            // We use each node's mac address as an identifier
            // TODO: [long term] When MRv2 is ready for use, YARN can provide a node ID instead
            InetAddress ip = InetAddress.getLocalHost();
            NetworkInterface network = NetworkInterface.getByInetAddress(ip);
            String thisNodesMacAddress =
                    Arrays.toString( network.getHardwareAddress() );

            logger.log("My mac address is " + thisNodesMacAddress);

            // Confirm this machine can access the shared directories!
            File testFile1 = new File( specifiedLoc + "_1" );
            File testFile2 = new File( specifiedLoc + "_2" );
            File testFile3 = new File( specifiedLoc + "_3" );
            if( !testFile1.isDirectory() && !testFile2.isDirectory()
                    && !testFile3.isDirectory() ) {
                StringBuilder msg = new StringBuilder( );
                msg.append( "It looks like this machine is unable to access " );
                msg.append( "the shared Curator directories. \n" );
                msg.append( "Is /scratch/test accessible? " );
                File scratch = new File("/scratch/test");
                msg.append( scratch.isDirectory() ? "Yes" : "No" );
                msg.append( "?\nAborting..." );

                throw new IOException( msg.toString() );
            }

            // First we check all Curator directories for this node's
            // signature lock
            File curatorToTest;
            // TODO: [long term] If jobs can be expected to last more than 1 hour,
            // extend this!
            final long timeUntilLockIsStale = 1000 * 60 * 60; // 1 hour
            for( int i = 3; i < maxCuratorInstallations; i++ ) {
                curatorToTest = new File( specifiedLoc + "_" + i );
                File curatorLock = new File( curatorToTest, curatorLockName );

                if( curatorLock.exists() ) {
                    // If the lock is stale, overwrite it and claim this Curator
                    if( System.currentTimeMillis() - curatorLock.lastModified() >
                            timeUntilLockIsStale ) {
                        logger.log( "Found a stale lock in Curator directory "
                                    + curatorToTest.toString()
                                    + ". Removing it and using this Curator." );
                        LocalFileSystemHandler.writeStringToFile( curatorLock,
                                thisNodesMacAddress, true );
                        this.curatorLock = curatorLock;
                        curatorDir = new Path( curatorToTest.toString() );
                        break;
                    } else {
                        // If the lock contains our mac address, it means we
                        // locked it (thus, we are allowed to use this directory)
                        String lockContents =
                                LocalFileSystemHandler.readFileToString( curatorLock );
                        if( lockContents.contains( thisNodesMacAddress ) ) {
                            curatorDir = new Path( curatorToTest.toString() );
                            this.curatorLock = curatorLock;
                            curatorLock.setLastModified( System.currentTimeMillis() );
                            logger.log( "Found a currently-running Curator on this "
                                    + "node at " + curatorToTest.toString() );
                            break;
                        }
                    }
                } else { // No lock exists. Let's try claiming this directory.
                    if( lockCurator( curatorToTest ) ) {
                        logger.log( "Found a usable copy of Curator at "
                                    + curatorToTest.toString() );
                        curatorDir = new Path( curatorToTest.toString() );
                        break;
                    }
                }
            }

            // If we didn't find a Curator directory with our signature lock
            // (that is, if we still haven't found a curatorDir to use), try
            // them at random.
            int count = 0;
            while( curatorDir == null ) {
                curatorToTest = new File( specifiedLoc + "_"
                        + rng.nextInt(maxCuratorInstallations) );

                if( lockCurator( curatorToTest ) ) {
                    curatorDir = new Path( curatorToTest.toString() );
                    logger.logStatus( "Found that Curator at "
                            + curatorDir.toString() + " is unused." );
                } else {
                    logger.logStatus( "Found that Curator at "
                            + curatorToTest.toString() + " is in use." );
                    ++count;
                    if( count > 5*maxCuratorInstallations ) {
                        throw new IOException("Couldn't find a Curator to use...");
                    }
                }
            }
        } else { // Normal, node-local Curator
            if( specifiedLoc != null && !specifiedLoc.equals("") ) {
                curatorDir = new Path( specifiedLoc );
            }
            else {
                curatorDir = new Path( userDir, "curator" );
            }
        }

        logger.logStatus( "Using Curator at " + curatorDir.toString() + "." );
        Path distDir = new Path( curatorDir, "dist" );
        distDir.makeQualified( FileSystem.get( new Configuration() ) );
        dir = new PathStruct( distDir );

        if( !FileSystemHandler.localFileExists( distDir ) ) {
            throw new IOException("Curator directory does not exist "
                    + "at " + distDir.toString() + " on this Hadoop node. "
                    +"\nCannot continue...");

        }
    }

    private boolean lockCurator( File curatorDir ) throws IOException {
        // We use each node's mac address as an identifier
        // TODO: [long term] When MRv2 is ready for use, YARN can provide a node ID instead
        if( thisNodesMacAddress == null ) {
            InetAddress ip = InetAddress.getLocalHost();
            NetworkInterface network = NetworkInterface.getByInetAddress(ip);
            thisNodesMacAddress = Arrays.toString( network.getHardwareAddress() );
        }

        File curatorLock = new File( curatorDir, curatorLockName );
        // If this copy of Curator exists and is not locked . . .
        if( curatorDir.isDirectory() && !curatorLock.exists() ) {
            // Lock it!
            curatorLock.createNewFile();
            curatorLock.setWritable(true, false);
            LocalFileSystemHandler.writeStringToFile( curatorLock,
                    thisNodesMacAddress, true );

            this.curatorLock = curatorLock;
            return true;
        }
        logger.log( "Curator at " + curatorDir + " was locked. " +
                "Curator dir is a directory? " + Boolean.toString( curatorDir.isDirectory() )
                + " Lock exists? " + curatorLock.exists() );
        return false;
    }

    /**
     * If a Curator from a previous job is running (i.e., a Curator configured to
     * use an annotator other than the tool to be run now), shut it and any
     * annotators down. This performs the same function that CuratorKillerReducer
     * used to.
     * @param toolToRun The annotator being run by this Reducer. If a Curator is
     *                  running on this node and does not provide this annotator,
     *                  we assume it's from an older job and we shut it down.
     */
    private void shutDownCuratorFromPreviousJob( AnnotationMode toolToRun )
            throws IOException {
        try {
            if( !client.listAvailableAnnotators().contains( toolToRun ) ) {
                logger.log( "Found a Curator running on this machine, but it"
                            + "doesn't know of the annotator for " + toolToRun
                            + ". Shutting it down, so we can start a new "
                            + "instance." );
                shutdownAllLocalNLPTools();
            }
        } catch ( TException ignored ) {
            // Couldn't list available annotators (probably because the Curator
            // isn't running at all
        }
    }

    /**
     * Shuts down the Curator and any running annotation tools. This should
     * be used only when the reduce() job has failed catastrophically.
     * @throws IOException
     */
    private void shutdownAllLocalNLPTools() throws IOException {
        // Command to kill the Curator specifically:
        //      "jps -l | grep edu.illinois.cs.cogcomp.curator.CuratorServer | cut -d ' ' -f 1 | xargs -n1 kill"
        //      See here: http://stackoverflow.com/questions/2131874/shell-script-to-stop-a-java-program

        // We should never have more than MAX_RUNNING_TOOLS to kill.
        int MAX_RUNNING_TOOLS = 5;
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
                    System.out.println( "Successfully shut down "
                            + "a process." );
                }
                else {
                    // stop when grep fails to find a matching process
                    System.out.println( "Failed to shut down a process. "
                            + "Exiting . . ." );
                    break;
                }
            } catch( RuntimeException e ) {
                throw new IOException( "Runtime exception shutting down "
                        + "an older Curator process!\n" + e.getMessage() );
            } catch ( InterruptedException ignored ) { }
        }

        // Kill the Charniak parser in particular
        String killCharniakCmd = "pgrep charniak | xargs -n1 kill";
        String[] charniakCmd = {
                "/bin/sh",
                "-c",
                killCharniakCmd
        };
        try {
            Process p = Runtime.getRuntime().exec(charniakCmd);

            StreamGobbler err = new StreamGobbler( p.getErrorStream(),
                    "ERR: ", true );
            StreamGobbler out = new StreamGobbler( p.getInputStream(),
                    "", true );
            err.start();
            out.start();

            if( p.waitFor() == 0 ) {
                System.out.println( "Successfully shut down Charniak." );
            }
        } catch( Exception ignored ) { }

        // Make sure future Reducers don't think their tools are already running
        CuratorReducer.setToolHasBeenLaunched( false );
        if( curatorLock != null ) {
            curatorLock.delete();
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
            }

            // Give Curator itself time to start up
            // If we're running a tool that starts *with* the Curator
            // (instead of *before* the Curator), wait a bit longer
            if( !toolsThatMustBeLaunched.contains( toolToRun ) ) {
                Thread.sleep( getEstimatedTimeToStart( toolToRun ) );
            }
            else {
                // Minimum amount of time to wait is for the tokenizer
                Thread.sleep( getEstimatedTimeToStart(AnnotationMode.TOKEN) );
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
            if( !toolHasBeenLaunched( toolToRun ) ) {
                setToolHasBeenLaunched( true );

                startTool( toolToRun );

                Thread.sleep( getEstimatedTimeToStart( toolToRun ) );
            }
        }
    }

    /**
     * Checks whether a given annotation tool has been launched on this machine.
     * @param annotator The annotator to check for
     * @return True if this machine has a flag (simply a file in the file system)
     *         that indicates it has already launched the annotation tool, or if
     *         there is a Curator client running on this machine that claims to
     *         provide the annotator.
     */
    private boolean toolHasBeenLaunched( AnnotationMode annotator )
            throws IOException {
        // If a Curator running on this machine knows of the annotator, we are
        // done.
        StringBuilder msg = new StringBuilder( "Checking if " );
        msg.append( annotator );
        msg.append( " is running. Is it in the list of available annotators? " );
        try {
            if( client.listAvailableAnnotators().contains( annotator ) ) {
                setToolHasBeenLaunched( true );
                msg.append( "Yes." );
                logger.log( msg.toString() );
                return true;
            }
        } catch ( TException ignored ) { }

        msg.append( "No.\n" );

        // Otherwise (i.e., if no Curator is running or it doesn't know of
        // the tool), check the flag in the file system.
        File flagInFileSystem = new File( userDir, "_annotator_launched" );

        msg.append( "Does the flag in the file system say it's running? " );
        msg.append( flagInFileSystem.exists() );
        logger.log( msg.toString() );
        return flagInFileSystem.exists();
    }

    /**
     * Sets or unsets the flag in the file system which is used to indicate that
     * an annotation tool is running on this Reduce node.
     * @param newValue True if the flag should indicate that there is indeed an
     *                 annotator which needs to be shut down later, or false if
     *                 it should indicate there are no more running annotators.
     * @throws IOException If the flag in the file system cannot be set
     */
    public static void setToolHasBeenLaunched( boolean newValue ) throws IOException {
        File flagInFileSystem = new File( userDir, "_annotator_launched" );
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
        long timeForSmallModels = 3*1000; // 3 secs
        long timeForMidModels = 10*1000; // 10 secs
        long timeForLargeModels = 90*1000; // 90 secs
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
        spawnedCuratorProcesses.add( Runtime.getRuntime().exec(
                launchScript.toString(), envVarsForRuntimeExec ) );

        // Handle the output from the Curator (we don't want to print it, but
        // we can't just leave the output stream there, as it can cause deadlock
        // in some OS's implementations)
        File curatorLog = new File( getCuratorLogLocation().toString() );
        Process p = spawnedCuratorProcesses.get( 0 );
        StreamGobbler err = new StreamGobbler( p.getErrorStream(), "Curator ERR: " );
        StreamGobbler out = new StreamGobbler( p.getInputStream(), "Curator: ");

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

        // Remove old configuration files (probably not necessary
        /*if( FileSystemHandler.localFileExists( configLoc ) ) {
            logger.log("Deleting old configuration file " + fileName );
            FileSystemHandler.deleteLocal( configLoc );
        }*/

        // If the config file doesn't exist, go ahead and create it.
        if( !FileSystemHandler.localFileExists( configLoc ) ) {
            logger.log("Creating configuration file " + fileName );
            StringBuilder file = new StringBuilder();
            file.append( "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n" );
            file.append( "<curator-annotators>\n" );


            // Make the tokenizer available to all configurations, as it is
            // memory-light enough not to slow anything down
            Set<AnnotationMode> toolsToRunSimultaneously =
                    new HashSet<AnnotationMode>();
            toolsToRunSimultaneously.add( AnnotationMode.TOKEN );
            toolsToRunSimultaneously.add( AnnotationMode.SENTENCE );
            toolsToRunSimultaneously.add( AnnotationMode.POS );
            toolsToRunSimultaneously.add( AnnotationMode.CHUNK );
            if( toolsToRunSimultaneously.contains( runningTool ) ) {
                file.append( "<annotator>\n" );
                file.append( "    <type>multilabeler</type>\n" );
                file.append( "    <field>sentences</field>\n" );
                file.append( "    <field>tokens</field>\n" );
                file.append( "    <local>edu.illinois.cs.cogcomp.annotation.handler.IllinoisTokenizerHandler</local>\n");
                file.append( "</annotator>\n" );
                file.append( "<annotator>\n" );
                file.append( "    <type>labeler</type>\n" );
                file.append( "    <field>pos</field>\n" );
                file.append( "    <local>edu.illinois.cs.cogcomp.annotation.handler.IllinoisPOSHandler</local>\n" );
                file.append( "    <requirement>sentences</requirement>\n" );
                file.append( "    <requirement>tokens</requirement>\n" );
                file.append( "</annotator>\n" );
                file.append( "<annotator>\n" );
                file.append( "    <type>labeler</type>\n" );
                file.append( "    <field>chunk</field>\n" );
                file.append( "    <local>edu.illinois.cs.cogcomp.annotation.handler.IllinoisChunkerHandler</local>\n" );
                file.append( "    <requirement>sentences</requirement>\n" );
                file.append( "    <requirement>tokens</requirement>\n" );
                file.append( "    <requirement>pos</requirement>\n" );
                file.append( "</annotator>\n" );
            }
            else {
                file.append( "<annotator>\n" );
                switch (runningTool) {
                    case TOKEN:  // Handled above; placed here for completeness
                    case POS:
                    case CHUNK:
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
                        // Note that since our config names it without "-ext", we
                        // need to request the annotation as "ner", not "ner-ext"
                        file.append( "    <field>ner</field>\n" );
                        file.append( "    <host>localhost:9093</host>\n" );
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
     * If this script is not found in your Curator directory (e.g., at
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

            cmd.append( scriptLocation.toString() );
            cmd.append( " nerconll " ); // some ID
            cmd.append( port );
            cmd.append( " " );
            cmd.append( configs );
            cmd.append( "/ner.conll.config" );

            // Use one command or the other (either Ontonotes or Conll)
            /*StringBuilder cmd2 = new StringBuilder( );
            cmd2.append( scriptLocation.toString() );
            cmd2.append( " nerontonotes " ); // some ID
            cmd2.append( ++port );
            cmd2.append( " " );
            cmd2.append( configs );
            cmd2.append( "/ner.ontonotes.config" );*/

            logger.logStatus( "Launching NER annotator on node with "
                    + "command \n\t" + cmd.toString()
                    /*+ "\n\t" + cmd2.toString()*/ );

            spawnedAnnotatorProcesses.add( Runtime.getRuntime().exec(
                    cmd.toString(), envVarsForRuntimeExec, dirToLaunchAgainst ) );
            /*spawnedAnnotatorProcesses.add( Runtime.getRuntime().exec(
                    cmd2.toString(), envVarsForRuntimeExec, dirToLaunchAgainst ) );*/

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
            this.userDir = new Path( CuratorReducer.userDir );
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
    private PathStruct dir;

    private FileSystemHandler fsHandler;
    private HadoopCuratorClient client;
    private String [] envVarsForRuntimeExec;
    private List<Process> spawnedCuratorProcesses;
    private List<Process> spawnedAnnotatorProcesses;
    private Set<AnnotationMode> toolsThatMustBeLaunched;
    private static final MessageLogger logger = HadoopInterface.logger;
    private static final int MAX_ATTEMPTS = 10;
    private File curatorLock; // locks the installation of Curator we are using
}
