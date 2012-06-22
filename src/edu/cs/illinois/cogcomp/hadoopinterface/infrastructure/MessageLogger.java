package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

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
        write( s );
    }

    /**
     * Logs an error
     * @param s A string detailing the error
     */
    public void logError( String s ) throws IOException {
        write("Error: " + s);
    }

    /**
     * Logs a warning
     * @param s A string detailing the warning
     */
    public void logWarning( String s ) throws IOException {
        write( "Warning: " + s );
    }

    /**
     * Logs a program status update
     * @param s A string detailing the warning
     */
    public void logStatus( String s ) throws IOException {
        write( "\nStatus: " + s );
    }

    private void write( String message ) throws IOException {
        if( printToStdOut ) {
            System.out.println( message );
        }

        FileSystemHandler.writeFileToLocal( message, logLocation, true );
    }

    private boolean printToStdOut;
    private Path logLocation;
}
