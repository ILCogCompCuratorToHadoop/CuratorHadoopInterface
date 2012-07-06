package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.exceptions;

import java.io.IOException;

/**
 * This functions basically as an enumerated type to say that we found no input
 * files in the (valid) input directory.
 * @author Tyler Young
 */
public class EmptyInputException extends IOException
{
    public EmptyInputException(String s)
    {
        super(s);
    }
}
