package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.ArrayList;
//TODO import AnnotationMode
//TODO import FileSystemHandler
//TODO import SerializationHandler
//TODO import RecordTools

/**
 * A class to handle annotation dependencies outside of the Curator.
 * Sits between the Curator-to-Hadoop batch script and the Hadoop-to-Curator batch script.
 * 
 * NOTE: This class requires that all files in the input directory have
 * the same "level" of existing annotations. Any document may be sampled to determine
 * which annotations have already been completed.
 *
 * TODO Future upgrade: allow the user to optionally specify what annotation level to begin at,
 * so that a mixed input directory can be used by overwriting existing higher-level annotations.
 *
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
        
        Path dir = new Path(inputDirectory);
        ArrayList<Path> files = FileSystemHandler.getFilesOnlyInDirectory(dir);
        File sample = files[0].toFile();

        // Construct a (non-Hadoop) Record 'sampleRecord' from File 'sample'
        Record sampleRecord = SerializationHandler.deserialize(sample);
        
        // Retrieve list of existing annotations for comparison
        ArrayList<AnnotationMode> existingAnnotations = RecordTools.getAnnotationsList(sampleRecord);
        
        // compare existing to dependencies list and add non-existing dependencies to new list
        ArrayList<AnnotationMode> depsToRun = new ArrayList<AnnotationMode>();
        for (AnnotationMode annotation : dependencies) {
            if (!existingAnnotations.contains(annotation)) {
                depsToRun.add(annotation);
            }
        }
        
        // Loop through new, intermediate dependencies
        for (AnnotationMode a : depsToRun) {
            launchJob(a);
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
