package edu.cs.illinois.cogcomp.hadoopinterface;

/**
 * @author Tyler Young
 */
public class ErrorLogger
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
     */
    public ErrorLogger( boolean alsoLogToStdOut ) {
        printToStdOut = alsoLogToStdOut;
    }
    
    public void log( String s ) {
        if( printToStdOut ) {
            System.out.println( s );
        }
    }
    
    public void logError( String s ) {
        if( printToStdOut ) {
            System.out.println( "Error: " + s );
        }
    }

    public void logWarning( String s ) {
        if( printToStdOut ) {
            System.out.println( "Warning: " + s );
        }
    }

    private boolean printToStdOut;
}
