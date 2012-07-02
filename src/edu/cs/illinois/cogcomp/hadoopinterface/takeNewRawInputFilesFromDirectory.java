package edu.cs.illinois.cogcomp.hadoopinterface;

import java.io.File;

/**
 * @author Tyler Young
 */
public class takeNewRawInputFilesFromDirectory {
    public void takeNewRawInputFilesFromDirectory( String directory ) {
        // Construct a path from the directory
        File inputDir = new File( directory );
        if( !inputDir.isDirectory() ) {
            throw new IllegalArgumentException("The location " + directory
                    + " does not refer to a directory.");
        }

        // For each file in the directory . . .
        for( File f : inputDir.listFiles() ) {
            // Add it to the CuratorClient's queue of documents to request from
            // the Curator (these will be deserialized and sent to Hadoop)
            addToInputList( f );
        }
    }

}
