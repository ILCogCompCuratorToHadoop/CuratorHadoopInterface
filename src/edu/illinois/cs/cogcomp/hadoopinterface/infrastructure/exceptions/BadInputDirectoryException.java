package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.exceptions;

import java.io.IOException;

/**
 * This functions basically as an enumerated type to say that we got a bad
 * input directory from the command line.
 * @author Tyler Young
 */
public class BadInputDirectoryException extends IOException
{
    public BadInputDirectoryException(String s)
    {
        super(s);
    }
}
