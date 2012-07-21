package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import java.io.IOException;
import java.util.ArrayList;

/**
 * A class to handle annotation dependencies outside of the Curator.
 * Sits between the Curator-to-Hadoop batch script and
 * the Hadoop-to-Curator batch script.
 * @author Lisa Bao
 */

public class JobHandler {

    /**
     * @param argv String arguments from command line.
     *             The first argument must be the targeted annotation type.
     *             The second argument must be an ABSOLUTE input directory path.
     */
	public static void main(String[] argv) throws IOException {
        AnnotationMode requestedAnnotation = AnnotationMode.fromString( argv[0] );
        String inputDirectory = argv[1];

        // Call batch_master_curator_to_hadoop
        // launches Master Curator, copies files to HDFS
        Runtime.getRuntime().exec("./batch_master_curator_to_hadoop " + requestedAnnotation.toString() + " " + inputDirectory);

        // Retrieve list of dependencies for requested annotation
        ArrayList<AnnotationMode> dependencies = requestedAnnotation.getDependencies();
        ArrayList<AnnotationMode> alreadyRun = new ArrayList<AnnotationMode>();

        // TODO Currently this doesn't work because we need to process dependencies for each file individually
        // but the dependencies and alreadyRun arraylists are local to all files.
        // Quick solution: require consistency on user's part and simply sample the first file to determine needed dependencies.

        // TODO for each file in folder inputDirectory on HDFS:
            // TODO construct a HadoopRecord called temp
            HadoopRecord temp = new HadoopRecord();
            ArrayList<String> existingAnnotations = temp.getAnnotationsAsStringList();
            // convert String list to AnnotationMode list
            for (String a : existingAnnotations) {
                alreadyRun.add(AnnotationMode.fromString(a));
            }

        // Loop through all intermediate dependencies; really similar to the
        // dependency-checking logic in CuratorHandler's #provide() method
        // TODO: Tyler started this, but didn't finish  --- we really need to examine
        for (AnnotationMode a : dependencies) {
            if( !alreadyRun.contains( a ) ) {
                launchJob( a );
            }
            alreadyRun.add( a );
        }
        // Launch final MapReduce job
        Runtime.getRuntime().exec("echo -e \"Launching final MapReduce job:\"");
        Runtime.getRuntime().exec("./bin/hadoop jar curator.jar -d serialized -m " + requestedAnnotation + " -out serialized_output");
        Runtime.getRuntime().exec("echo -e \"\n\n\nFinal MapReduce job is finished!\n\n\"");

        // Call batch_hadoop_to_master_curator
        // copies files from HDFS to local disk and database
        Runtime.getRuntime().exec("./batch_hadoop_to_master_curator " + requestedAnnotation + " " + inputDirectory);

    } // END OF MAIN

    private static void launchJob( AnnotationMode a ) throws IOException {
        // Launch MapReduce job on Hadoop cluster
        Runtime.getRuntime().exec("echo -e \"\n\n\nLaunching intermediate MapReduce job on the Hadoop cluster:\"");
        Runtime.getRuntime().exec("./bin/hadoop jar curator.jar -d serialized -m " + a.toString() + " -out serialized_output");
        Runtime.getRuntime().exec("echo -e \"\n\n\nAn intermediate job has been finished...\n\n\"");
    }

}
