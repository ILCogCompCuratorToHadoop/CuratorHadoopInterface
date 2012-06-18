package edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.exceptions;

/**
 * @author Tyler Young
 */
public class BadCommandLineUsageException extends IllegalArgumentException
{
    public BadCommandLineUsageException( String s ) {
        super(s);
    }
}
