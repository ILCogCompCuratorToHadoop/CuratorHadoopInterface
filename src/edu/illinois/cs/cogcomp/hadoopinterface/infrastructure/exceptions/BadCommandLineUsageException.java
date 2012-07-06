package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.exceptions;

/**
 * @author Tyler Young
 */
public class BadCommandLineUsageException extends IllegalArgumentException
{
    public BadCommandLineUsageException( String s ) {
        super(s);
    }
}
