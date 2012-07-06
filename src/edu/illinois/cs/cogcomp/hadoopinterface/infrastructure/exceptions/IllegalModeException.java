package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.exceptions;

/**
 * This functions basically as an enumerated type to say that we got a bad
 * mode from the command line.
 * @author Tyler Young
 */
public class IllegalModeException extends IllegalArgumentException {
    public IllegalModeException(String s)
    {
        super(s);
    }
}
