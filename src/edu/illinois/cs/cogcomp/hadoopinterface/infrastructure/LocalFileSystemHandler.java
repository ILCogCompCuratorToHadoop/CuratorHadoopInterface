package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import java.io.*;
import java.util.Scanner;

public class LocalFileSystemHandler {
    /**
     * Reads a file object from the disk and returns a string version of that
     * file.
     *
     * @param f The file to be read
     * @return A string version of the input file
     */
    public static String readFileToString( File f )
            throws FileNotFoundException {
        // Read the file in to a String
        Scanner fileReader = new Scanner( f );
        StringBuilder fileContents = new StringBuilder();
        while ( fileReader.hasNextLine() ) {
            fileContents.append( fileReader.nextLine() );
            fileContents.append( NL );
        }

        fileReader.close();
        return fileContents.toString();
    }

    /**
     * Reads a file object from the disk and returns a byte array version of that
     * file.
     *
     * @param f The file to be read
     * @return A byte array version of the input file
     */
    public static byte[] readFileToBytes( File f ) throws IOException {
        byte[] buffer = null;

        BufferedReader bufferedReader = new BufferedReader( new FileReader( f ) );

        buffer = new byte[(int) f.length()];

        int i = 0;
        int c = bufferedReader.read();

        while ( c != -1 ) {
            buffer[i++] = (byte) c;
            c = bufferedReader.read();
        }

        return buffer;
    }

    /**
     * Writes a file to the indicated path.
     *
     * @param path The location to which the file should be written. Probably
     *             something like "/user/My_User/my_output_dir/a_text_file.txt".
     * @param text The text, as a byte array, to be written.
     * @throws java.io.IOException If the file indicated by the path already
     *                             exists.
     */
    public static void writeFile( File path, byte[] text )
            throws IOException {
        if ( path.exists() ) {
            throw new IOException( "File at path "
                                   + path
                                   + " already exists; cannot overwrite it." );
        }

        FileOutputStream writer = new FileOutputStream( path );
        writer.write( text );

        writer.close();
    }

    private static final String NL = System.getProperty("line.separator");
}
