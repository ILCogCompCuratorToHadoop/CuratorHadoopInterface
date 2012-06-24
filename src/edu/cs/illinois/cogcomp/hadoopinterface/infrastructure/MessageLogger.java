package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
        logLocation = new Path( "MessageLog.txt" );
    }

    /**
     * Constructs an ErrorLogger object and specifies whether to
     * print the log to the standard output.
     * @param alsoLogToStdOut TRUE if the log should also be sent to the
     *                        standard output
     */
    public MessageLogger(boolean alsoLogToStdOut) {
        printToStdOut = alsoLogToStdOut;
        logLocation = new Path( "MessageLog.txt" );
    }

    /**
     * Logs a generic message
     * @param s A string containing the message
     */
    public void log( String s ) throws IOException {
        write( getCurrentTime() + s );
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

    private void write( String message ) throws IOException {
        if( printToStdOut ) {
            System.out.println( message );
        }

        FileSystemHandler.writeFileToLocal( message, logLocation, true );
    }

    private String getCurrentTime() {
        return ( new SimpleDateFormat("hh:mm:ss a") ).format( new Date() );
    }

    private boolean printToStdOut;
    private Path logLocation;
}
