package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import java.io.*;
import java.util.Scanner;

public class LocalFileSystemHandler {
    private static final String NL = System.getProperty("line.separator");

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
        BufferedReader bufferedReader = new BufferedReader( new FileReader( f ) );

        byte[] buffer = new byte[(int) f.length()];

        int i = 0;
        int c = bufferedReader.read();

        while ( c != -1 ) {
            buffer[i++] = (byte) c;
            c = bufferedReader.read();
        }

        return buffer;
    }

    /**
     * Writes a string to the indicated file.
     * @param path The file to be written to
     * @param data The string to write to the file
     * @param overwrite If true, we will overwrite the file if it exists.
     */
    public static void writeStringToFile( File path,
                                          String data,
                                          boolean overwrite )
            throws IOException {
        if ( path.exists() && !overwrite ) {
            throw new IOException( "File at path "+ path
                                   + " already exists; cannot overwrite it." );
        }

        BufferedWriter out = new BufferedWriter( new FileWriter( path ) );
        out.write( data );
        out.close();
    }

    /**
     * Writes a file to the indicated path.
     *
     * @param path The location to which the file should be written. Probably
     *             something like "/user/My_User/my_output_dir/a_text_file.txt".
     * @param text The text, as a byte array, to be written.
     * @param overwrite If set to true, will overwrite the file if it already
     *                  exists.
     * @throws java.io.IOException If the file indicated by the path already
     *                             exists.
     */
    public static void writeFile( File path, byte[] text, boolean overwrite )
            throws IOException {
        if ( path.exists() && !overwrite ) {
            throw new IOException( "File at path "
                                   + path
                                   + " already exists; cannot overwrite it." );
        }

        FileOutputStream writer = new FileOutputStream( path, !overwrite );
        writer.write( text );

        writer.close();
    }


    /**
     * Checks to see if the file contains some non-hidden files or directories.
     * Returns true if and only if the file in question is both a directory and
     * has files/folders inside it that are not hidden.
     * @param f The file (or directory) in question
     * @return True if the file contains non-hidden files, false otherwise
     */
    public static boolean containsNonHiddenFiles( File f ) {
        if( f.isDirectory() ) {
            for( File inner : f.listFiles() ) {
                if( !inner.isHidden() ) {
                    return true;
                }
            }
        }

        return false;
    }
}
