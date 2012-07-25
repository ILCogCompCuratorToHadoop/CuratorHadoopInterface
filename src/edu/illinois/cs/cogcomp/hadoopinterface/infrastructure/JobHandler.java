package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import edu.illinois.cs.cogcomp.thrift.curator.Record;
import org.apache.thrift.TException;

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
 *
 * @author Lisa Bao
 * @author Tyler Young
 */

public class JobHandler {
    /**
     * @param argv String arguments from command line.
     *             The first argument must be the targeted annotation type.
     *             The second argument must be an absolute, local input directory path.
     *             The third argument must be either a starting annotation (dependency), or empty.
     */
    public static void main(String[] argv) throws IOException, TException {
        AnnotationMode requestedAnnotation = AnnotationMode.fromString( argv[0] );
        String inputDirectory = argv[1];

        // Call batch_master_curator_to_hadoop
        // launches Master Curator, copies files to HDFS
        System.out.println( "\nCopying your text files from " + inputDirectory
                            + " to the Hadoop cluster.\n");
        Runtime.getRuntime().exec( "scripts/batch_master_curator_to_hadoop.sh "
                + requestedAnnotation.toString() + " "
                + inputDirectory );

        ArrayList<AnnotationMode> dependencies = requestedAnnotation.getDependencies();
        ArrayList<AnnotationMode> depsToRun = new ArrayList<AnnotationMode>();

        if( argv.length == 3 ) { // if we have 3 arguments
            String minDependency = argv[2];
            AnnotationMode minAnnotation = AnnotationMode.fromString(minDependency);
            ArrayList<AnnotationMode> minDeps = minAnnotation.getDependencies();

            // remove existing dependencies from run list
            depsToRun = new ArrayList<AnnotationMode>(dependencies);
            for (AnnotationMode a : minDeps) {
                depsToRun.remove(a);
            }
        }
        else { // only have 2 arguments from command line
            // We take a random sample of the files in the input and inspect them for annotations
            // TODO: differentiate between raw text and serialized?
            File dir = new File(inputDirectory);

            if (dir.listFiles() == null) {
                // TODO die gracefully
                throw new IOException("ERROR! The given directory " + inputDirectory + " is not valid.");
            }
            else {
                List<File> files = Arrays.asList( dir.listFiles() );
                File sample = new File( files.get(0).toString() );

                // Decide what annotations need to be run
                List<AnnotationMode> existingAnnotations;
                try {
                    // Construct a (non-Hadoop) Record 'sampleRecord' from randomly chosen File 'sample'
                    Record sampleRecord =
                            ( new SerializationHandler() ).deserialize( sample );

                    // Retrieve list of existing annotations for comparison
                    existingAnnotations = RecordTools.getAnnotationsList(sampleRecord);
                } catch( TException e ) { // Probably wasn't a serialized record
                    System.out.println( "It looks like the files you gave us are not "
                                        + "the output of a previous annotation.\n"
                                        + "We're going to assume they are new, raw"
                                        + "text.");
                    existingAnnotations = new ArrayList<AnnotationMode>();
                }

                // TODO: inform the user what we found regarding present annotations and dependencies

                // compare existing to dependencies list and add non-existing dependencies to new list
                for (AnnotationMode annotation : dependencies) {
                    if (!existingAnnotations.contains(annotation)) {
                        depsToRun.add(annotation);
                    }
                }
            }
        } // END else

        // Loop through new, intermediate dependencies
        boolean firstTime = true;
        AnnotationMode lastAnnotation;
        for (int i = 0; i < depsToRun.size(); i++) {
            if (firstTime) {
                launchJob(depsToRun.get(i), "first_serialized_input", depsToRun.get(i).toString());
                firstTime = false;
            }
            else {
                launchJob(depsToRun.get(i), depsToRun.get(i-1).toString(), depsToRun.get(i).toString());
            }
            // store last annotation mode for use as final input directory
            if (i == depsToRun.size()-1) {
                lastAnnotation = depsToRun.get(i);
            }
        }

        // Satisfy the compiler that this will not wind up as null
        lastAnnotation = depsToRun.get( depsToRun.size()-1 );

        // Launch final MapReduce job
        System.out.println("Launching final MapReduce job:");
        Runtime.getRuntime().exec("./bin/hadoop jar curator.jar -d " 
                + lastAnnotation.toString() + " -m " 
                + requestedAnnotation.toString() + " -out " 
                + requestedAnnotation.toString());
        System.out.println("Final MapReduce job is finished!");

        // Call batch_hadoop_to_master_curator
        // copies files from HDFS to local disk and database
        Runtime.getRuntime().exec("./batch_hadoop_to_master_curator " + requestedAnnotation.toString() + " " + inputDirectory);

    } // END OF MAIN

    private static void launchJob( AnnotationMode a, String inDir, String outDir ) throws IOException {
        // Launch MapReduce job on Hadoop cluster
        System.out.println("Launching intermediate MapReduce job on the Hadoop cluster:");
        Runtime.getRuntime().exec("./bin/hadoop jar curator.jar -d " + inDir + " -m " + a.toString() + " -out " + outDir);
        System.out.println("An intermediate job has been finished...");
    }

}
