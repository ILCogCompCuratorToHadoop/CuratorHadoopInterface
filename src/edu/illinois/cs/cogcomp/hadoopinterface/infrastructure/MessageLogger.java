package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * An object which standardizes the logging of errors and warnings across the
 * project. Note that each logger's disk writing is disabled until you call its
 * beginWritingToDisk() method.
 *
 * @bug The underlying file writer does *not* work when called from the
 *      InputSplit (DirectorySplit), the InputFormat (DirectoryInputFormat),
 *      the RecordReader (CuratorRecordReader), and maybe more. Get around
 *      this by delaying the disk write until all those tasks have finished.
 *
 * @author Tyler Young
 */
public class MessageLogger
{
    /**
     * Constructs an ErrorLogger object. By default, the log is
     * not written to the standard output and the log is not written to disk.
     */
    public MessageLogger()  {
        initializeCommonVars( false );
    }

    /**
     * Constructs an ErrorLogger object and specifies whether to
     * print the log to the standard output.
     * @param alsoLogToStdOut TRUE if the log should also be sent to the
     *                        standard output
     */
    public MessageLogger( boolean alsoLogToStdOut ) {
        initializeCommonVars( alsoLogToStdOut );
    }

    private void initializeCommonVars( boolean printToStdOut ) {
        this.printToStdOut = printToStdOut;
        Path logDir = new Path( "logs" );
        logLocation = new Path( logDir, "0_curator_interface_log_" +
                getCurrentTimeNoSpaces() + ".txt" );
        backlog = "";
        delayWritingToDisk = true;
    }

    /**
     * Logs a generic message
     * @param s A string containing the message
     */
    public void log( String s )  {
        write( getCurrentTime() + "  " + s );
    }

    /**
     * Logs an error
     * @param s A string detailing the error
     */
    public void logError( String s )  {
        write( getCurrentTime() + "  Error: " + s);
    }

    /**
     * Logs a warning
     * @param s A string detailing the warning
     */
    public void logWarning( String s )  {
        write( getCurrentTime() + "  Warning: " + s );
    }

    /**
     * Logs a program status update
     * @param s A string detailing the warning
     */
    public void logStatus( String s ) {
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

    public void beginWritingToDisk() {
        delayWritingToDisk = false;
    }

    /**
     * Turns off disk writes until you call continueWritingToDisk(). Useful for
     * getting around bugs in the underlying file system writer.
     */
    public void delayWritingToDisk() {
        logStatus( "Delaying further disk writes." );
        delayWritingToDisk = true;
    }

    /**
     * Enables disk writing once again, and writes whatever backlog has been
     * generated to the disk.
     */
    public void continueWritingToDisk() {
        beginWritingToDisk();

        logStatus( "Re-enabled disk writes. Backlog follows." );

        if( backlogIsTruncated ) {
            backlogIsTruncated = false;
            logWarning( "This backlog was truncated due to excessive size." );
        }

        // Write the backlog to the file
        boolean prevStdOut = printToStdOut;
        printToStdOut = false;
        write( backlog );
        printToStdOut = prevStdOut;

        // Reset the backlog
        backlog = "";

        logStatus( "End of backlog." );
    }

    private void write( String message ) {
        if( printToStdOut ) {
            System.out.println( message );
        }

        if( !delayWritingToDisk ) {
            try {
                FileSystemHandler.writeFileToLocal( message, logLocation, true );
            } catch( IOException e ) {
                System.out.println("I/O error in logging to file!");
            }
        }
        else {
            if( backlogMemUsageInMegabytes() > 20.0 && !backlogIsTruncated ) {
                backlogIsTruncated = true;
                backlog = backlog
                        + "\n Backlog truncated here due to excessive length.";
            }
            if( !backlogIsTruncated ) {
                backlog = backlog + message + "\n";
            }
        }
    }

    private String getCurrentTime() {
        return ( new SimpleDateFormat("hh:mm:ss a") ).format( new Date() );
    }

    private String getCurrentTimeNoSpaces() {
        return ( new SimpleDateFormat("hh.mm.ss.a") ).format( new Date() );
    }

    /**
     * Gets a rough estimate of the size of the backlog.
     * @return A decimal representing the estimated size, in megabytes, of the
     *         message backlog.
     */
    private double backlogMemUsageInMegabytes() {
        long estUsageInBytes =  8 * (int) (((( backlog.length() ) * 2) + 45) / 8);
        return ((double)estUsageInBytes / 1024.0 / 1024.0);
    }

    private boolean printToStdOut;
    private boolean delayWritingToDisk;
    private Path logLocation;
    private String backlog;
    private boolean backlogIsTruncated;
}
