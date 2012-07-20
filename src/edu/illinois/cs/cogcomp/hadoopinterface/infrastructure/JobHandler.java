package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.AnnotationMode;

import java.io.IOException;
import java.util.Set;

public class JobHandler {

    /**
     * @param argv String arguments from command line.
     *             The first argument must be an annotation type
     *             and the second argument must be an ABSOLUTE input directory path.
     */
	public static void main(String[] argv) throws IOException {
        AnnotationMode requestedAnnotation = AnnotationMode.fromString( argv[0] );
        String inputDirectory = argv[1];

        // Call batch_master_curator_to_hadoop
        // launches Master Curator, copies files to HDFS
        Runtime.getRuntime().exec("./batch_master_curator_to_hadoop " + requestedAnnotation + " " + inputDirectory);

        // Retrieve list of dependencies for requested annotation
        Set<AnnotationMode> dependencies = requestedAnnotation.getDependencies();
        // Loop through all intermediate dependencies
        for (AnnotationMode a : dependencies) {
            // Launch MapReduce job on Hadoop cluster
            Runtime.getRuntime().exec("echo -e \"\n\n\nLaunching intermediate MapReduce job on the Hadoop cluster:\"");
            Runtime.getRuntime().exec("./bin/hadoop jar curator.jar -d serialized -m " + a.toString() + " -out serialized_output");
            Runtime.getRuntime().exec("echo -e \"\n\n\nAn intermediate job has been finished...\n\n\"");
        }
        // Launch final MapReduce job
        Runtime.getRuntime().exec("echo -e \"Launching final MapReduce job:\"");
        Runtime.getRuntime().exec("./bin/hadoop jar curator.jar -d serialized -m " + requestedAnnotation + " -out serialized_output");
        Runtime.getRuntime().exec("echo -e \"\n\n\nFinal MapReduce job is finished!\n\n\"");

        // Call batch_hadoop_to_master_curator
        // copies files from HDFS to local disk and database
        Runtime.getRuntime().exec("./batch_hadoop_to_master_curator " + requestedAnnotation + " " + inputDirectory);

    } // END OF MAIN

}
