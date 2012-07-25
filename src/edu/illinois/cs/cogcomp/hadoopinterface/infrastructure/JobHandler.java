package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import edu.illinois.cs.cogcomp.thrift.curator.Record;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
 * @precondition The following files/directories are in place on this machine:
 * <ul>
 *     <li>curator-0.6.9</li>
 *     <li>JobHandler (contains a number of .jar dependencies and scripts)</li>
 * </ul>
 * @precondition You have opened the `.sh` files in the scripts directory and
 *               configured them as necessary--for instance, you may need to set
 *               the library directory to be used on each Hadoop node (set in the
 *               launch_hadoop_job.sh file), and you definitely need to define
 *               how we connect to Hadoop (in all scripts in the scripts
 *               directory).
 *
 * @author Lisa Bao
 * @author Tyler Young
 * @TODO Document methods!
 */

public class JobHandler {
    private static final String locationOfInitialInputInHDFS =
            "first_serialized_input";

    /**
     * @param argv String arguments from command line.
     *             The first argument must be the targeted annotation type.
     *             The second argument must be an absolute, local input directory path.
     *             The third argument must be either a starting annotation (dependency), or empty.
     */
    public static void main(String[] argv) throws Exception {
        AnnotationMode requestedAnnotation = AnnotationMode.fromString( argv[0] );
        String inputDirectory = argv[1];

        // Check input
        File inputDirAsFile = new File(inputDirectory);

        if (inputDirAsFile.listFiles() == null) {
            throw new IOException( "ERROR! The given directory "
                                   + inputDirectory + " is not valid." );
        }


        boolean inputIsSerializedRecords =
                containsSerializedRecords( inputDirectory );



        // Call copy_input_to_hadoop
        // If necessary, this launches the Master Curator and serializes the raw
        // text into records before copying the files to HDFS
        System.out.println( "\nCopying your input files from \n\t" + inputDirectory
                + "\nto the Hadoop cluster.");

        StringBuilder fileCopyCmd = new StringBuilder();
        fileCopyCmd.append( "scripts/copy_input_to_hadoop.sh " );
        fileCopyCmd.append( requestedAnnotation.toString() );
        fileCopyCmd.append( " " );
        fileCopyCmd.append( inputDirectory );

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



        // Determine dependencies
        System.out.println( "\nGetting the list of dependencies to run.\n");
        List<AnnotationMode> dependencies = requestedAnnotation.getDependencies();
        List<AnnotationMode> depsToRun = new ArrayList<AnnotationMode>();

        if( argv.length == 3 ) { // if we have 3 arguments
            // The user-specified first annotation tool to run
            // (i.e., the lowest-level tool to be run)
            AnnotationMode minAnnotation = AnnotationMode.fromString( argv[2] );

            // We assume that all records already meet the dependency
            // requirements to run the user-specified minAnnotation
            ArrayList<AnnotationMode> minDeps = minAnnotation.getDependencies();

            // remove the annotations that the user has guaranteed exist
            // from the list of annotations to be run
            depsToRun = new ArrayList<AnnotationMode>(dependencies);
            for (AnnotationMode a : minDeps) {
                depsToRun.remove(a);
            }
        }
        else { // only have 2 arguments from command line
            // We take a random sample of the files in the input and inspect
            // them for annotations
            List<File> files = Arrays.asList( inputDirAsFile.listFiles() );
            File sample = new File( files.get(0).toString() );

            // Decide what annotations need to be run
            List<AnnotationMode> existingAnnotations;
            if( inputIsSerializedRecords ) {
                // Construct a (non-Hadoop) Record 'sampleRecord' from randomly
                // chosen File 'sample'
                Record sampleRecord =
                        ( new SerializationHandler() ).deserialize( sample );

                // Retrieve list of existing annotations for comparison
                existingAnnotations =
                        RecordTools.getAnnotationsList( sampleRecord );

                System.out.println( "It looks like the records you gave us "
                        + "have the following annotations already:\n\t"
                        + RecordTools.getAnnotationsString( sampleRecord )
                        + "\n" );
            }
            else { // Input isn't serialized records
                inputIsSerializedRecords = false;

                System.out.println( "It looks like the files you gave us are not "
                                    + "the output of a previous annotation.\n"
                                    + "We're going to assume they are new, raw "
                                    + "text.\n" );
                existingAnnotations = new ArrayList<AnnotationMode>();
            }

            // compare existing annotations to those in the dependencies list and
            // add annotations to our to-do list as necessary
            for (AnnotationMode annotation : dependencies) {
                if (!existingAnnotations.contains(annotation)) {
                    depsToRun.add(annotation);
                }
            }
        } // END else


        // Inform the user what dependencies we need
        if( depsToRun.size() == 0 ) {
            System.out.println( "No dependencies need to be satisfied.\n\n" );
        }
        else {
            System.out.println( "We will get the following dependencies, in "
                                + "order:\n\t" + depsToRun.toString() + "\n\n" );
        }



        // Annotate the documents for each new, intermediate dependency
        String inputToThisJob = "first_serialized_input";
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
        String finalOutputInLocal = inputDirectory + "/output";
        System.out.println( "\nCopying your serialized records from \n\t"
                + finalOutputInHadoop + "\non the Hadoop cluster back to the local "
                + "machine, into the directory\n\t" + finalOutputInLocal );


        String copyOutputCmd = "scripts/copy_output_from_hadoop.sh "
                               + finalOutputInHadoop + " "
                               + finalOutputInLocal;
        Process copyOutputProc = Runtime.getRuntime().exec( copyOutputCmd );

        // Capture the output and write it to a file
        gobbleOutputFromProcess( copyOutputProc, "copy_output_from_hadoop.log" );

        // Wait until the process finishes
        int copyOutputResult = copyOutputProc.waitFor();
        if( copyOutputResult != 0 ) {
            throw new Exception("Error while copying the output from Hadoop!");
        }

        System.out.println( "\nFinished copying files.\n" );
    } // END OF MAIN


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

    private static boolean containsSerializedRecords( String directory ) {
        try {
            File dir = new File( directory );
            File sample = null;

            for( File f : dir.listFiles() ) {
                if( !f.isHidden() ) {
                    sample = f;
                    break;
                }
            }

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

}
