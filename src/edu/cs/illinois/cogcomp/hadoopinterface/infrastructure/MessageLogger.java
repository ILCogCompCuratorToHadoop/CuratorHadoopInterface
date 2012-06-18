package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure;

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
    public ErrorLogger() {
        printToStdOut = false;
    }

    /**
     * Constructs an ErrorLogger object and specifies whether to
     * print the log to the standard output.
     * @param alsoLogToStdOut TRUE if the log should also be sent to the
     *                        standard output
     */
    public ErrorLogger( boolean alsoLogToStdOut ) {
        printToStdOut = alsoLogToStdOut;
    }

    /**
     * Logs a generic message
     * @param s A string containing the message
     */
    public void log( String s ) {
        if( printToStdOut ) {
            System.out.println( s );
        }
    }

    /**
     * Logs an error
     * @param s A string detailing the error
     */
    public void logError( String s ) {
        if( printToStdOut ) {
            System.out.println( "Error: " + s );
        }
    }

    /**
     * Logs a warning
     * @param s A string detailing the warning
     */
    public void logWarning( String s ) {
        if( printToStdOut ) {
            System.out.println( "Warning: " + s );
        }
    }

    private boolean printToStdOut;
}
