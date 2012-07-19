package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.exceptions;

import edu.illinois.cs.cogcomp.thrift.base.AnnotationFailedException;

/**
 * @author Tyler Young
 */
public class CuratorNotFoundException extends AnnotationFailedException {
    public CuratorNotFoundException( String s ) {
        super(s);
    }
}
