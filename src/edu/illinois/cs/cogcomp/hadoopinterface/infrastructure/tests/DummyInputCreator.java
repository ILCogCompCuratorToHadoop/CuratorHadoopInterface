package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.tests;

import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.AnnotationMode;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.FileSystemHandler;
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
     * Generates a job directory and many document directions therein,
     * complete with an original.txt file and
     * dummy annotation files for all required annotation types.
     * @param jobDirectory The directory which was given as input by the user
     *                     to be the root input directory
     */
    public static void generateDocumentDirectories( Path jobDirectory,
                                                    FileSystem fs )
            throws IOException {
        // Create doc directory
        if( FileSystemHandler.isDir( jobDirectory, fs ) ) {
            FileSystemHandler.delete( jobDirectory, fs );
        }
        fs.mkdirs( jobDirectory );

        for( int i = 0; i < 10; i++ ) {
            createDocumentDirectory(
                    new Path( jobDirectory, "doc" + Integer.toString(i) ), fs );
        }
    }

    /**
     * Creates the text files for a single document.
     * @param docDir The path to the document directory
     * @param fs The file system against which to resolve the paths
     * @throws IOException
     */
    public static void createDocumentDirectory( Path docDir,
                                                FileSystem fs )
            throws IOException {
        // Create original.txt file
        String original = "Hello, I'm an original.txt file."
                + "\n\nLorem ipsum dolor sit amet, consectetur adipiscing "
                + "elit. Nullam eu mauris odio. Vivamus id fermentum"
                + "elit. Quisque placerat arcu in nibh tincidunt "
                + "consectetur.\n\n"
                + getRandomString() + "\n\n" + getRandomString();
        Path originalPath = new Path( docDir, "original.txt" );
        FileSystemHandler.writeFileToHDFS( original, originalPath, fs );

        // Create annotation files
        for( AnnotationMode mode : AnnotationMode.values() ) {
            String annotation = "Hello, I'm an annotation of type "
                    + mode.toString()
                    + "\n\nLorem ipsum dolor sit amet, consectetur "
                    + "adipiscing elit. Nullam eu mauris odio. Vivamus id "
                    + "fermentum elit. Quisque placerat arcu in nibh tincidunt "
                    + "consectetur.\n";
            Path annotationPath = new Path( docDir,
                    mode.toString() + ".txt" );
            FileSystemHandler.writeFileToHDFS( annotation, annotationPath, fs );
        }
    }

    /**
     * Writes a number of original (raw) text files to the input directory.
     * Useful for testing the ability to create a Record from raw text files.
     * @param docDir The directory to which we should write the text files
     * @param fs The file system against which we should resolve paths
     * @param numInputFiles The number of text files we should create
     */
    public static void createRawTextInputDirectory( Path docDir,
                                                    FileSystem fs,
                                                    int numInputFiles )
            throws IOException {
        // Create a bunch of text files as sample "raw text" files
        for( int i = 0; i < numInputFiles; i++ ) {
            String docName = "document" + Integer.toString(i) + ".txt";
            Path docPath = new Path( docDir, docName );
            String contents = "Hello, I'm an original.txt file."
                    + "\n\nLorem ipsum dolor sit amet, consectetur adipiscing "
                    + "elit. Nullam eu mauris odio. Vivamus id fermentum"
                    + "elit. Quisque placerat arcu in nibh tincidunt "
                    + "consectetur.\n\n"
                    + getRandomString() + "\n\n" + getRandomString();
            FileSystemHandler.writeFileToHDFS( contents, docPath, fs );
        }
    }

    public static String getRandomString()
    {
        Random rng = new Random();
        int length = rng.nextInt(1000);
        String characters = "abcdefghijklmnopqrstuvwxyz"
                            + "\"'abcdefghijklmnopqrstuvwxyz\n"
                            + "ABCDEFGHIJKLMNOPQRSTUVWXYZ \n"
                            + "ABCDEFGHIJKLMNOPQRSTUVWXYZ[]().!?!@#$%^&*()_+=-";

        char[] text = new char[length+1];
        for (int i = 0; i < length; i++)
        {
            text[i] = characters.charAt(rng.nextInt(characters.length()));
        }
        text[length] = '\n';
        return new String(text);
    }
}
