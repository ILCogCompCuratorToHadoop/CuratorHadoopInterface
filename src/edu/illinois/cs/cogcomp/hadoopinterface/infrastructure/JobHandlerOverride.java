package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import edu.illinois.cs.cogcomp.thrift.curator.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A class to handle annotation dependencies outside of the Curator.
 * Sits between the Curator-to-Hadoop batch script and the Hadoop-to-Curator batch script.
 * 
 * NOTE: This class requires that all files in the input directory have
 * the same "level" of existing annotations. Any document may be sampled to determine
 * which annotations have already been completed.
 *
 * This version of the Job Handler allows the user to optionally specify what annotation level to begin at,
 * so that a mixed input directory can be used by overwriting existing higher-level annotations.
 *
 * @author Lisa Bao
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
        Runtime.getRuntime().exec("./batch_master_curator_to_hadoop " + requestedAnnotation.toString() + " " + inputDirectory);

        ArrayList<AnnotationMode> dependencies = requestedAnnotation.getDependencies();
        ArrayList<AnnotationMode> depsToRun = new ArrayList<AnnotationMode>();

        try { // if 3rd param is specified...
            String minDependency = argv[2];
            AnnotationMode minAnnotation = AnnotationMode.fromString(minDependency);
            ArrayList<AnnotationMode> minDeps = minAnnotation.getDependencies();
            
            // remove existing dependencies from run list
            depsToRun = new ArrayList<AnnotationMode>(dependencies);
            for (AnnotationMode a : minDeps) {
                depsToRun.remove(a);
            }
        } catch (ArrayIndexOutOfBoundsException e) { //otherwise, proceed as normal
            Path dir = new Path(inputDirectory);
            FileSystemHandler handler = new FileSystemHandler(
                    FileSystem.getLocal( new Configuration() ) );
            List<Path> files = handler.getFilesOnlyInDirectory( dir );
            File sample = new File( files.get(0).toString() );

            // Construct a (non-Hadoop) Record 'sampleRecord' from randomly chosen File 'sample'
            Record sampleRecord = ( new SerializationHandler() ).deserialize( sample );
            
            // Retrieve list of existing annotations for comparison
            List<AnnotationMode> existingAnnotations =
                    RecordTools.getAnnotationsList( sampleRecord );
            
            // compare existing to dependencies list and add non-existing dependencies to new list
            for (AnnotationMode annotation : dependencies) {
                if (!existingAnnotations.contains(annotation)) {
                    depsToRun.add(annotation);
                }
            }
        } // END CATCH

        // Loop through new, intermediate dependencies
        boolean firstTime = true;
        AnnotationMode lastAnnotation = new AnnotationMode();
        for (int i = 0; i < depsToRun.length(); i++) {
            if (firstTime) {
                launchJob(depsToRun[i], "first_serialized_input", depsToRun[i].toString());
                firstTime = false;
            }
            else {
                launchJob(depsToRun[i], depsToRun[i-1].toString(), depsToRun[i].toString());
            }
            // store last annotation mode for use as final input directory
            if (i == depsToRun.length()-1) {
                lastAnnotation = depsToRun[i];
            }
        }
		
        // Launch final MapReduce job
        System.out.println("Launching final MapReduce job:");
        Runtime.getRuntime().exec("./bin/hadoop jar curator.jar -d " + lastAnnotation.toString() + " -m " + requestedAnnotation.toString() + " -out " + requestedAnnotation.toString());
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
