package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import java.io.*;
import java.util.Scanner;

/**
 * @author Tyler Young
 */
public class FSHandler extends AbstractFSHandler {

    /**
     * Reads a file to a string
     * @param f The file to read in
     * @return String version
     * @throws IOException You get the point
     */
    @Override
    public String readFileToString( File f ) throws IOException {
        Scanner s = new Scanner( f );

        StringBuilder text = new StringBuilder();
        while( s.hasNextLine() ) {
            text.append( s.nextLine() );
            text.append( NL );
        }

        return text.toString();
    }

    /**
     *
     * @param f
     * @return
     * @throws IOException
     */
    @Override
    public byte[] readFileToBytes( File f ) throws IOException {
        return readFileToString( f ).getBytes();
    }

    @Override
    public void writeStringToFile( File path, String data, boolean overwrite )
            throws IOException {
        if( path.exists() && !overwrite ) {
            throw new IOException("File exists!");
        }

        BufferedWriter writer = new BufferedWriter( new FileWriter( path ) );
        writer.write( data );

        writer.close();
    }

    @Override
    public boolean containsNonHiddenFiles( File f ) {
        if( f.isDirectory() ) {
            for( File inDir : f.listFiles() ) {
                if( !inDir.isHidden() ) {
                    return true;
                }
            }

        }
        return false;
    }
}
