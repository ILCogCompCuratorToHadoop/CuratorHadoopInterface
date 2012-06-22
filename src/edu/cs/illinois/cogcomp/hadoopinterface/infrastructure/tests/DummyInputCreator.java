package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.tests;

import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.AnnotationMode;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.FileSystemHandler;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Random;

/**
 * Generates text files to simulate input to the Hadoop interface.
 * @author Tyler Young
 */
public class DummyInputCreator {

    /**
     * Generates a document directory, complete with an original.txt file and
     * dummy annotation files for all required annotation types.
     * @param jobDirectory The directory which was given as input by the user
     *                     to be the root input directory
     */
    public static void generateDocumentDirectory( Path jobDirectory,
                                                  FileSystem fs )
            throws IOException {
        // Create doc directory
        fs.mkdirs( jobDirectory );

        // Create original.txt file
        String original = "Lorem ipsum dolor sit amet, consectetur adipiscing "
                          + "elit. Nullam eu mauris odio. Vivamus id fermentum"
                          + "elit. Quisque placerat arcu in nibh tincidunt "
                          + "consectetur.\n\n"
                          + getRandomString() + "\n\n" + getRandomString();
        Path originalPath = new Path( jobDirectory, "original.txt" );
        FileSystemHandler.writeFileToHDFS( original, originalPath, fs, false );

        // Create annotation files
        for( AnnotationMode mode : AnnotationMode.values() ) {
            String annotation = mode.toString()
                    + "\n\nLorem ipsum dolor sit amet, consectetur "
                    + "adipiscing elit. Nullam eu mauris odio. Vivamus id "
                    + "fermentum elit. Quisque placerat arcu in nibh tincidunt "
                    + "consectetur.";
            Path annotationPath = new Path( jobDirectory,
                    mode.toString() + ".txt" );
            FileSystemHandler.writeFileToHDFS( annotation, annotationPath,
                    fs, false );
        }
    }

    public static String getRandomString()
    {
        Random rng = new Random();
        int length = rng.nextInt( 1000 );
        String characters = "abcdefghijklmnopqrstuvwxyz \n"
                            + "ABCDEFGHIJKLMNOPQRSTUVWXYZ[]().!?!@#$%^&*()_+=-";

        char[] text = new char[length];
        for (int i = 0; i < length; i++)
        {
            text[i] = characters.charAt(rng.nextInt(characters.length()));
        }
        return new String(text);
    }

}
