package edu.cs.illinois.cogcomp.hadoopinterface;

import edu.illinois.cs.cogcomp.thrift.base.Clustering;
import edu.illinois.cs.cogcomp.thrift.base.Forest;
import edu.illinois.cs.cogcomp.thrift.base.Labeling;
import edu.illinois.cs.cogcomp.thrift.base.View;
import edu.illinois.cs.cogcomp.thrift.curator.Record;
import org.apache.commons.io.FileExistsException;

import java.io.*;
import java.util.*;

/**
 * @author Tyler Young
 */
public class takeNewRawInputFilesFromDirectory {
    /**
     * Called by the main method when the Curator Client is flagged to accept
     * raw input files with no prexisting annotations. Useful when the documents
     * are known not to exist in the Curator's database cache.
     * @param inputDirectory The input directory from which we should read a batch of
     *                       input text files.
     */
    public void takeNewRawInputFilesFromDirectory( String inputDirectory ) {
        // Check that the input directory is valid
        File inputDir = new File( inputDirectory );
        if( !inputDir.isDirectory() ) {
            throw new IllegalArgumentException("The location " + inputDirectory
                    + " does not refer to a directory.");
        }

        // For each file in the directory . . .
        for( File f : inputDir.listFiles() ) {
            try {
                // Add it to the CuratorClient's queue of documents to serialize
                // in preparation for sending the input to Hadoop
                String fileContents = readFileToString( f );
                Record newRecord = generateNewRecord( fileContents );
                addToInputList( newRecord );    // TODO: Confirm method name
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * This should really be a constructor in Record. Whatever.
     *
     * Constructs a new Record object with no annotations at all to the original
     * document text.
     * @param originalText The document's raw text.
     * @return A record object for the document whose original text was passed in.
     */
    public Record generateNewRecord( String originalText ) {
        // TODO: How to generate ID?
        String id = "0xDEADBEEF";
        Map<String, Labeling> labels = new HashMap<String, Labeling>();
        Map<String, Clustering> cluster = new HashMap<String, Clustering>();
        Map<String, Forest> parse = new HashMap<String, Forest>();
        Map<String, View> views = new HashMap<String, View>();

        return new Record(id, originalText, labels, cluster, parse, views, false);
    }

    /**
     * Serializes all Records in the list of input Records and writes them to
     * the output directory. This will later be copied to the Hadoop file system.
     * @param outputDir The location to which we should write the serialized
     *                  records
     */
    public void writeSerializedInput( String outputDir ) throws IOException {
        // TODO: Replace with the real variable
        Iterable< Record > allInput = new LinkedList< Record >();

        for( Record r : allInput ) {
            String outputDirForRecord = outputDir + File.separator
                    + r.getIdentifier();

            Map< String, String > serializedForm = serializeRecord( r );
            for( String key : serializedForm.keySet() ) {
                writeFile( outputDirForRecord + File.separator + key,
                           serializedForm.get( key ) );
            }
        }
    }

    /**
     * Reads a file object from the disk and returns a string version of that
     * file.
     * @param f The file to be read
     * @return A string version of the input file
     */
    private String readFileToString( File f ) throws FileNotFoundException {
        // Read the file in to a String
        Scanner fileReader = new Scanner( f );
        StringBuilder fileContents = new StringBuilder();
        while( fileReader.hasNextLine() ) {
            fileContents.append(fileReader.nextLine());
        }

        fileReader.close();
        return fileContents.toString();
    }

    /**
     * Writes a file to the indicated path.
     * @param path The location to which the file should be written. Probably
     *             something like "/user/My_User/my_output_dir/a_text_file.txt".
     * @param text The text file to be written.
     * @throws FileExistsException If the file indicated by the path already
     *                             exists.
     */
    private void writeFile( String path, String text )
            throws IOException, FileExistsException {
        File theFile = new File( path );
        if( theFile.exists() ) {
            throw new FileExistsException( "File at path " + path
                    + " already exists; cannot overwrite it.");
        }

        BufferedWriter writer = new BufferedWriter( new FileWriter( theFile ) );
        Scanner stringScanner = new Scanner( text );
        while( stringScanner.hasNextLine() ) {
            String line = stringScanner.nextLine();
            writer.write( line );
        }

        stringScanner.close();
        writer.close();
    }
}
