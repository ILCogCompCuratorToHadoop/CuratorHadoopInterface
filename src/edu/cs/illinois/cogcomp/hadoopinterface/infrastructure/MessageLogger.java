package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * An object which standardizes the logging of errors and warnings across the
 * project.
 * @author Tyler Young
 */
public class MessageLogger
{
    /**
     * Constructs an ErrorLogger object. By default, the log is
     * not written to the standard output.
     */
    public MessageLogger() {
        printToStdOut = false;
        Path logDir = new Path( "logs" );
        logLocation = new Path( logDir, "0_curator_interface_log_" +
                getCurrentTimeNoSpaces() + ".txt" );
    }

    /**
     * Constructs an ErrorLogger object and specifies whether to
     * print the log to the standard output.
     * @param alsoLogToStdOut TRUE if the log should also be sent to the
     *                        standard output
     */
    public MessageLogger( boolean alsoLogToStdOut ) {
        printToStdOut = alsoLogToStdOut;
        Path logDir = new Path( "logs" );
        logLocation = new Path( logDir, "0_curator_interface_log_" +
                getCurrentTimeNoSpaces() + ".txt" );
    }

    /**
     * Logs a generic message
     * @param s A string containing the message
     */
    public void log( String s ) throws IOException {
        write( getCurrentTime() + "  " + s );
    }

    /**
     * Logs an error
     * @param s A string detailing the error
     */
    public void logError( String s ) throws IOException {
        write( getCurrentTime() + "  Error: " + s);
    }

    /**
     * Logs a warning
     * @param s A string detailing the warning
     */
    public void logWarning( String s ) throws IOException {
        write( getCurrentTime() + "  Warning: " + s );
    }

    /**
     * Logs a program status update
     * @param s A string detailing the warning
     */
    public void logStatus( String s ) throws IOException {
        write( "\n" + getCurrentTime() + "  Status: " + s );
    }

    /**
     * Gets a prettified string version of a list. Useful for outputting the
     * contents of the list to the log.
     * @param l The list to prettify
     * @return A string version of the list, with newlines and tabs inserted
     *         as appropriate.
     */
    public String getPrettifiedList( List l ) {
        String pretty = "\n\t";
        for( Object item : l ) {
            pretty = pretty + item.toString() + "\n\t";
        }

        return pretty;
    }

    private void write( String message ) throws IOException {
        if( printToStdOut ) {
            System.out.println( message );
        }

        FileSystemHandler.writeFileToLocal( message, logLocation, true );
    }

    private String getCurrentTime() {
        return ( new SimpleDateFormat("hh:mm:ss a") ).format( new Date() );
    }

    private String getCurrentTimeNoSpaces() {
        return ( new SimpleDateFormat("hh.mm.ss.a") ).format( new Date() );
    }

    private boolean printToStdOut;
    private Path logLocation;
}
