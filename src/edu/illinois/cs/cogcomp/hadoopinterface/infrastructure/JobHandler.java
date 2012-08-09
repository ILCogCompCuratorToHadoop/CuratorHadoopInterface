package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import com.sun.istack.internal.Nullable;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.exceptions
        .IllegalModeException;
import edu.illinois.cs.cogcomp.thrift.curator.Record;
import org.apache.thrift.TException;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * A class to handle annotation dependencies outside of the Curator.
 * Sits between the Curator-to-Hadoop batch script and the Hadoop-to-Curator
 * batch script.
 *
 * NOTE: This class requires that all files in the input directory have
 * the same "level" of existing annotations. Any document may be sampled to
 * determine which annotations have already been completed.
 *
 * This version of the Job Handler allows the user to optionally specify what
 * annotation level to begin at, so that a mixed input directory can be used by
 * overwriting existing higher-level annotations.
 *
 * @use java -jar JobHandler.jar < annotation to be run >
 *          < /absolute/path/to/raw/text> [ optional: first annotation to run ]
 * @example java -jar JobHandler.jar WIKI /home/jsmith/input_text_file_dir
 *          (If you want us to determine which annotation to run---preferred!)
 * @example java -jar JobHandler.jar WIKI /home/jsmith/input_text_file_dir POS
 *          (If you know for certain that your documents should be run through
 *          the POS tagger first.)
 * @example java -jar JobHandler.jar WIKI /home/jsmith/input_text_file_dir -test
 *          (If you want the locally-running Curator to verify that it gets all
 *          the same annotations. You almost assuredly should *not* use this
 *          with large document collections.)
 * @precondition The following files/directories are in place on this machine:
 *
 * <ul>
 *     <li>curator-0.6.9 (whose dist directory contains, among other things, the `client` directory</li>
 *     <li>JobHandler (contains a number of .jar dependencies and scripts)</li>
 * </ul>
 * @precondition You have opened the `.sh` files in the scripts directory and
 *               configured them as necessary--for instance, you may need to set
 *               the library directory to be used on each Hadoop node (set in the
 *               launch_hadoop_job.sh file), and you definitely need to define
 *               how we connect to Hadoop (in all scripts in the scripts
 *               directory).
 *
 * @TODO Add check to make sure that the input directory is real (else throw useful error)
 * @author Lisa Bao
 * @author Tyler Young
 */

public class JobHandler {
    private static final String locationOfInitialInputInHDFS =
            "first_serialized_input";

    /**
     * Set up a string of Hadoop jobs to run the requested annotation on the
     * documents in the (locally residing) input directory. Handles all
     * dependencies leading up to the requested annotation, and copies the output
     * of the final job back to the local machine.
     * @param argv String arguments from command line.
     *
     *             The first argument must be the targeted annotation type.
     *
     *             The second argument must be an absolute, local input
     *             directory path.
     *
     *             The third argument must be either a starting annotation
     *             (dependency), or empty.
     *
     *             If you want the locally-running Curator to
     */
    public static void main(String[] argv) throws Exception {
        // Parse arguments
        ArrayList<String> argList = new ArrayList<String>( Arrays.asList(argv) );
        boolean testing = false;
        if( argList.contains( "-test" ) || argList.contains( "-testing" ) ) {
            testing = true;
            argList.remove( "-test" );
        }

        AnnotationMode requestedAnnotation = null;
        AnnotationMode forcedFirstAnnotation = null;
        String inputDirAsString = null;
        for( String arg : argList ) {
            if( requestedAnnotation == null ) {
                try {
                    requestedAnnotation = AnnotationMode.fromString( arg );
                } catch( IllegalModeException ignored ) { }
            }
            else if( arg.length() < 10 ) { // short enough to be an annotation mode
                // already found the requested annotation.
                // If we can parse another annotation, it must be because the
                // user wants to force us to start at a different annotation
                try {
                    forcedFirstAnnotation = AnnotationMode.fromString( arg );
                } catch( IllegalModeException wasntAnAnnotationMode ) {
                    // Must be the input directory!
                    inputDirAsString = arg;
                }
            }
            else {
                // Must be the input directory!
                inputDirAsString = arg;
            }
        }

        // Inform the user of our interpretation of the arguments
        System.out.println( "It looks like you're using the following args:\n"
                + "\tInput directory: " + inputDirAsString + "\n"
                + "\tRequested annotation: " + requestedAnnotation + "\n"
                + "\tForced first annotation: " + forcedFirstAnnotation + "\n"
                + "\tTesting mode? " + Boolean.toString(testing) );

        // Check input
        File inputDir = new File( inputDirAsString );

        if( !inputDir.isDirectory() ) {
            throw new IOException( "You passed " + inputDirAsString
                    + " in as the job input directory, but it doesn't appear"
                    + "to be a directory at all. Aborting . . ." );
        }

        // If we find no files to sample, throw an error
        if( getSampleFileFromDir( inputDir ) == null ) {
            throw new IOException( "ERROR! The given directory "
                                   + inputDirAsString + " is not valid." );
        }


        boolean inputIsSerializedRecords = containsSerializedRecords( inputDir );

        if( inputIsSerializedRecords ) {
            System.out.println( "\nIt looks like the input you provided are "
                                + "serialized records (output from a previous"
                                + "Hadoop job)." );
        }
        else {
            System.out.println( "\nIt looks like the files you gave us are not "
                                + "the output of a previous annotation.\n"
                                + "We're going to assume they are new, raw "
                                + "text.\n" );
        }


        // Determine dependencies
        List<AnnotationMode> depsToRun =
                determineDependencies( requestedAnnotation, forcedFirstAnnotation,
                                       inputDir, inputIsSerializedRecords );



        // Call copy_input_to_hadoop
        // If necessary, this launches the Master Curator and serializes the raw
        // text into records before copying the files to HDFS
        copyInputToHadoop( inputDirAsString, inputIsSerializedRecords );


        // Annotate the documents for each new, intermediate dependency
        String inputToThisJob = locationOfInitialInputInHDFS;
        for( AnnotationMode dependencyToGet : depsToRun ) {
            String outputFromThisJob = dependencyToGet.toString();

            launchJob( dependencyToGet, inputToThisJob, outputFromThisJob );

            // Set up for the next job (next job's input is this job's output)
            inputToThisJob = outputFromThisJob;
        }
        // At this point, inputToThisJob is either the output from the last
        // dependency we needed, or it is "first_serialized_input" (in the case
        // where we didn't need any dependencies)




        // Launch final MapReduce job
        System.out.println("Launching final MapReduce job.");
        String finalOutputInHadoop = requestedAnnotation.toString();
        launchJob( requestedAnnotation, inputToThisJob, finalOutputInHadoop );
        System.out.println("Final MapReduce job is finished!\n\n");




        // Copy the output files (serialized records) from HDFS to the local disk,
        // and add them to a locally-running Curator's database
        // Uses the copy_output_from_hadoop.sh script
        copyOutputFromHadoop( inputDirAsString, finalOutputInHadoop, testing );

        System.out.println("\n\nJob completed successfully!\n\n");
    } // END OF MAIN


    /**
     * Copies the input directory (containing either serialized records or raw
     * text) from the local machine into the Hadoop Distributed File System
     * (HDFS).
     * @param localDirectoryToCopyFrom The location on the local machine from
     *                                 which we should copy files into Hadoop
     * @param inputIsSerializedRecords True if the files in the local directory
     *                                 are Thrift-serialized records, false if
     *                                 they are raw text instead.
     * @throws Exception
     */
    private static void copyInputToHadoop( String localDirectoryToCopyFrom,
                                           boolean inputIsSerializedRecords )
            throws Exception {
        System.out.println( "\nCopying your input files from \n\t"
                            + localDirectoryToCopyFrom
                            + "\nto the Hadoop cluster.");

        File fileVersion = new File( localDirectoryToCopyFrom );
        StringBuilder fileCopyCmd = new StringBuilder();
        fileCopyCmd.append( "scripts/copy_input_to_hadoop.sh " );
        fileCopyCmd.append( fileVersion.toString() );

        if( inputIsSerializedRecords ) {
            fileCopyCmd.append( " serial " );
        }
        else {
            fileCopyCmd.append( " raw " );
        }

        fileCopyCmd.append( locationOfInitialInputInHDFS );

        Process fileCopyProc =
                Runtime.getRuntime().exec( fileCopyCmd.toString() );

        // Capture the output and write it to a file
        gobbleOutputFromProcess( fileCopyProc, "copy_files_to_hadoop.log" );

        // Wait until the process finishes
        int processResult = fileCopyProc.waitFor();
        if( processResult != 0 ) {
            throw new Exception("Error while copying the input files to Hadoop!");
        }

        System.out.println( "\nFinished copying files.\n");
    }

    /**
     * Gets the list of annotations that need to be performed before running the
     * annotation requested by the user.
     * @param requestedAnnotation The annotation requested by the user
     * @param forcedFirstAnnotation The annotation the user wants us to run first
     *                              (forcing us to assume all dependencies prior
     *                              to that have been satisfied). If the user has
     *                              not requested such an annotation, pass in
     *                              null.
     * @param inputDirAsFile The input directory (on the local machine) in which
     *                       we will find the documents to be annotated (either
     *                       raw text or serialized records).
     * @param inputIsSerializedRecords True if the files in the input directory
     *                                 are Thrift-serialized records, false if
     *                                 they are raw text instead.
     * @return The list of annotations to get, in order, before getting the
     *         user's desired annotation.
     * @throws IOException
     * @throws TException
     */
    private static List<AnnotationMode> determineDependencies(
            AnnotationMode requestedAnnotation,
            AnnotationMode forcedFirstAnnotation,
            File inputDirAsFile, boolean inputIsSerializedRecords )
            throws IOException, TException {
        System.out.println( "\nGetting the list of dependencies to run.\n");
        List<AnnotationMode> dependencies = requestedAnnotation.getDependencies();
        List<AnnotationMode> depsToRun = new ArrayList<AnnotationMode>();

        if( forcedFirstAnnotation != null ) {
            // We assume that all records already meet the dependency
            // requirements to run the user-specified minAnnotation
            ArrayList<AnnotationMode> minDeps =
                    forcedFirstAnnotation.getDependencies();

            // remove the annotations that the user has guaranteed exist
            // from the list of annotations to be run
            depsToRun = new ArrayList<AnnotationMode>(dependencies);
            for (AnnotationMode a : minDeps) {
                depsToRun.remove(a);
            }
        }
        else { // Up to us to determine what to run first
            // We take a random sample of the files in the input and inspect
            // them for annotations
            Set<AnnotationMode> existingAnnos = new HashSet<AnnotationMode>();
            if( inputIsSerializedRecords ) {
                existingAnnos = getCommonAnnotations( inputDirAsFile );

                System.out.println( "It looks like the records you gave us "
                        + "have the following annotations already:\n\t"
                        + existingAnnos.toString()
                        + "\n" );
            }
            // Note: At this point, if we have only raw text, existingAnnos is empty

            // compare existing annotations to those in the dependencies list and
            // add annotations to our to-do list as necessary
            for( AnnotationMode annotation : dependencies ) {
                if( !existingAnnos.contains(annotation) ) {
                    depsToRun.add( annotation );
                }
            }
        } // END else

        // Tokenizer, POS, and chunker jobs are now able to run together.
        if( depsToRun.contains( AnnotationMode.CHUNK )
                || requestedAnnotation.equals( AnnotationMode.CHUNK ) ) {
            depsToRun.remove( AnnotationMode.POS );
            depsToRun.remove( AnnotationMode.TOKEN );
        }
        else if( depsToRun.contains( AnnotationMode.POS )
                || requestedAnnotation.equals( AnnotationMode.POS ) ) {
            depsToRun.remove( AnnotationMode.TOKEN );
        }

        // Inform the user what dependencies we need
        if( depsToRun.size() == 0 ) {
            System.out.println( "No dependencies need to be satisfied.\n\n" );
        }
        else {
            System.out.println( "We will get the following dependencies, in "
                                + "order, before moving on to "
                                + requestedAnnotation.toString()
                                + ":\n\t" + depsToRun.toString() + "\n\n" );
        }
        return depsToRun;
    }

    /**
     * Launches a Hadoop job on the cluster. Will annotate all documents in the
     * HDFS input directory with the indicated annotation mode, then write the
     * output of that job to the indicated HDFS output directory.
     * @param a The type of annotation to run on the document collection
     * @param inDir The location of the directory in the Hadoop Distributed File
     *              System (HDFS) where we can find the serialized records that
     *              should be annotated.
     * @param outDir The location in the Hadoop Distributed File
     *               System (HDFS) where we should write the serialized records
     *               after we finish annotating them
     * @throws Exception
     */
    private static void launchJob( AnnotationMode a,
                                   String inDir,
                                   String outDir ) throws Exception {
        // Launch MapReduce job on Hadoop cluster
        System.out.println( "Launching MapReduce job on the Hadoop cluster "
                            + "to get annotation " + a.toString()
                            + ", and writing the output of that job to directory "
                            + outDir + " in Hadoop." );

        // Command will be launched relative to the Hadoop directory
        StringBuilder cmd = new StringBuilder();
        cmd.append( "scripts/launch_hadoop_job.sh " );
        // We'll be really specific about which main class we want to run, just
        // in case the JAR's manifest gets damaged
        cmd.append( a.toString() );
        cmd.append( " " );
        cmd.append( inDir );
        cmd.append( " " );
        cmd.append( outDir ); // Output to a directory in HDFS named this

        Process proc = Runtime.getRuntime().exec(
                cmd.toString(), new String[0] );
        System.out.println( "Ran the command \n\t" + cmd.toString() + "\n" );

        gobbleOutputFromProcess( proc, a.toString() + "_job.log" );

        // Wait until the process finishes
        int processResult = proc.waitFor();
        if( processResult != 0 ) {
            throw new Exception("Error while attempting to run the Hadoop job!");
        }

        System.out.println("Job has finished...");
    }

    /**
     * After finishing all jobs in Hadoop, this method will copy the output
     * directory (containing the newly annotated serialized records) from the
     * Hadoop Distributed File System (HDFS) back to the local machine.
     * @param localDirectoryToCopyTo The location on the local machine to which
     *                               we should copy the output
     * @param directoryInHDFSToCopyFrom The location in HDFS from which we should
     *                                  copy the output
     * @param testing True if we should verify that the output is the same as what
     *                is available from a local (non-Hadoop) version of the
     *                Curator. Useful for ensuring we haven't done something
     *                significant wrong in the Hadoop implementation.
     * @throws Exception
     */
    private static void copyOutputFromHadoop( String localDirectoryToCopyTo,
                                              String directoryInHDFSToCopyFrom,
                                              boolean testing )
            throws Exception {
        String finalOutputInLocal = localDirectoryToCopyTo + "/output";
        System.out.println( "\nCopying your serialized records from \n\t"
                + directoryInHDFSToCopyFrom + "\non the Hadoop cluster back to "
                + "the local machine, into the directory\n\t"
                + finalOutputInLocal );

        String copyOutputCmd = "scripts/copy_output_from_hadoop.sh "
                + directoryInHDFSToCopyFrom + " "
                + finalOutputInLocal;

        if( testing ) {
            System.out.println( "\nAs you sent in the testing flag, we will "
                    + "have the locally-running Curator attempt the "
                    + "same annotations and verify that they match." );
            copyOutputCmd += " -test";
        }

        System.out.println( "Using the command: " + copyOutputCmd );
        Process copyOutputProc = Runtime.getRuntime().exec( copyOutputCmd );

        // Capture the output and write it to a file
        gobbleOutputFromProcess( copyOutputProc, "copy_output_from_hadoop.log" );

        // Wait until the process finishes
        int copyOutputResult = copyOutputProc.waitFor();
        if( copyOutputResult != 0 ) {
            throw new Exception("Error while copying the output from Hadoop!");
        }

        System.out.println( "\nFinished copying files.\n" );
    }


    /**
     * Logs the output from the indicated process to both the
     * standard output/standard error stream and to a log file with the indicated
     * name.
     * @param process The process whose output we should "gobble"
     * @param logFileName The name of the log file to which we should write.
     *                    This should be a filename only (like "mylog.txt")---we
     *                    will take care of placing it in the proper directory.
     * @throws IOException
     */
    private static void gobbleOutputFromProcess( Process process,
                                                 String logFileName )
            throws IOException {
        File logDir = new File( "logs" );
        logDir.mkdir(); // does nothing if it already exists

        File log = new File( logDir, logFileName );
        log.createNewFile(); // does nothing if it already exists

        System.out.println( "\nLogging output to " + log.toString() );
        StreamGobbler err = new StreamGobbler( process.getErrorStream(),
                "ERR: ", true, log );
        StreamGobbler out = new StreamGobbler( process.getInputStream(),
                "", true, log );
        err.start();
        out.start();
    }

    /**
     * Attempts to reconstruct text files in the directory into serialized
     * records. If it is successful, it returns true, indicating that we believe
     * the directory contains only serialized records.
     * @param directory The directory in question
     * @return True if we are able to reconstruct records from their serialized
     *         form (i.e., from text files in the directory). False otherwise.
     */
    private static boolean containsSerializedRecords( File directory ) {
        try {
            File sample = getSampleFileFromDir( directory );

            // Construct a (non-Hadoop) Record 'sampleRecord' from randomly
            // chosen File 'sample'
            Record sampleRecord =
                            ( new SerializationHandler() ).deserialize( sample );

            // If we were able to deserialize, it must be a serialized record!
            return true;
        } catch( Exception e ) {
            return false;
        }
    }

    /**
     * Looks at a relatively large subset of all the serialized records
     * present in the directory and returns a set of annotations which are common
     * to all examined records.
     * @param directory The directory containing serialized records
     * @return The set of common annotations shared between all records in the
     *         directory
     * @throws IOException
     * @throws TException
     */
    private static Set<AnnotationMode> getCommonAnnotations( File directory )
            throws IOException, TException {
        List<File> samples = getSampleFilesFromDir( directory, 25 );

        if( samples.size() == 0 ) {
            throw new IOException( "No records found in input directory "
                    + directory.toString() );
        }

        System.out.println( "Took a sample of " + samples.size()
                            + " records in determining the set of common "
                            + "annotations.");

        // Decide what annotations need to be run
        List<AnnotationMode> commonAnnotations = null;

        // Construct a Record 'sampleRecord' from randomly
        // chosen File 'sample'
        for( File sample : samples ) {
            Record sampleRecord =
                    ( new SerializationHandler() ).deserialize( sample );

            // Retrieve list of existing annotations for comparison
            List<AnnotationMode> thisRecordsAnnos =
                    RecordTools.getAnnotationsList( sampleRecord );

            if( commonAnnotations == null ) {
                // Set the baseline (the first record we have)
                commonAnnotations = new LinkedList<AnnotationMode>(
                        RecordTools.getAnnotationsList( sampleRecord ) );
            }
            else {
                System.out.println( "This record provides "
                        + thisRecordsAnnos.toString() + ".\nPrevious common: "
                        + commonAnnotations.toString() );
                for( int i = 0; i < commonAnnotations.size(); i++ ) {
                    // Check that we aren't out of bounds, since we're modifying
                    // the size of the array as we go
                    if( i < commonAnnotations.size() ) {
                       AnnotationMode commonAnno = commonAnnotations.get(i);

                        if( !thisRecordsAnnos.contains( commonAnno ) ) {
                            commonAnnotations.remove( commonAnno );
                        }
                    }
                }

                System.out.println( "New common: " + commonAnnotations.toString() );
            }
        }

        return new HashSet<AnnotationMode>( commonAnnotations );
    }

    /**
     * Returns a single file at random from the indicated directory.
     * @param directory The directory from which we should sample a file
     * @return A single non-directory, non-hidden file chosen at random from the
     *         directory. If there are no such files, returns null.
     */
    @Nullable
    private static File getSampleFileFromDir( File directory ) {
        List<File> sampleList = getSampleFilesFromDir( directory, 1 );
        if( sampleList.size() > 0 ) {
            return sampleList.get( 0 );
        }

        return null;
    }

    /**
     * Returns a collection of files from a directory. These files are chosen
     * at random from the set of all files in the directory, but there will never
     * be duplicates in the list, nor will the list contain hidden files or
     * subdirectories. The list returned will contain at most `numFiles` files,
     * but it may contain fewer files if the directory does not contain that
     * many (non-hidden, non-directory) files.
     * @param directory The directory from which we should sample files
     * @param numFiles The maximum number of files to sample (we will attempt to
     *                 get exactly this number, but may fail to do so if there
     *                 are not that many matching files in the directory)
     * @return A list containing files selected at random from the directory. May
     *         be empty if there were no files (or if what you passed in as the
     *         directory wasn't a directory at all!).
     */
    private static List<File> getSampleFilesFromDir( File directory,
                                                     int numFiles ) {
        if( directory.listFiles() == null ) {
            return new ArrayList<File>();
        }

        // We actually have to use the copy constructor here, because the List
        // returned from Arrays.asList is immutable!
        List<File> allFiles =
                new ArrayList<File>(Arrays.asList( directory.listFiles() ) );
        Random rng = new Random();
        List<File> samples = new ArrayList<File>();

        // Go until we have either collected the requested number of files
        // or we have no more files to consider
        while( samples.size() < numFiles && allFiles.size() > 0 ) {
            File potentialSample = allFiles.get( rng.nextInt( allFiles.size() ) );

            if( !potentialSample.isHidden() && !potentialSample.isDirectory() ) {
                samples.add( potentialSample );
                allFiles.remove( potentialSample );
            } else {
                allFiles.remove( potentialSample );
            }
        }

        return samples;
    }

}
