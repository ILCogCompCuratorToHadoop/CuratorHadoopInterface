package org.apache.thrift.transport;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * May become a standard Thrift class at some point. For now, we've adapted it
 * from the Thrift issue here: https://issues.apache.org/jira/browse/THRIFT-1012
 * @author Jake Farrell
 */
public class TDataTransport extends TTransport {

    /**
     * Underlying DataInput
     */
    protected DataInput din_ = null;

    /**
     * Underlying DataOutput
     */
    protected DataOutput dout_ = null;

    /**
     * Subclasses can invoke the default constructor and
     * then assign the input streams in the open method.
     */
    protected TDataTransport() {
    }

    /**
     * Input stream constructor.
     *
     * @param din Input stream to read from
     */
    public TDataTransport( DataInput din ) {
        din_ = din;
    }

    /**
     * Output stream constructor.
     *
     * @param dout Output stream to read from
     */
    public TDataTransport( DataOutput dout ) {
        dout_ = dout;
    }

    /**
     * Two-way stream constructor.
     *
     * @param din Input stream to read from
     * @param dout Output stream to read from
     */
    public TDataTransport( DataInput din, DataOutput dout ) {
        din_ = din;
        dout_ = dout;
    }

    /**
     * The streams must already be open at construction
     * time, so this should always return true.
     *
     * @return true
     */
    public boolean isOpen() {
        return true;
    }

    /**
     * The streams must already be open. This method does
     * nothing.
     */
    public void open() throws TTransportException {
    }

    /**
     * No ability to close IO. This method does nothing
     */
    public void close() {

    }

    /**
     * Reads from the underlying input stream if not
     * null.
     */
    public int read( byte[] buf, int off, int len ) throws TTransportException {
        if ( din_ == null ) {
            throw new TTransportException( TTransportException.NOT_OPEN,
                                           "Cannot read from null input" );
        }

        try {
            din_.readFully( buf, off, len );
        } catch ( IOException iox ) {
            throw new TTransportException( TTransportException.UNKNOWN, iox );
        }

        return len;
    }

    /**
     * Writes to the underlying output stream if not
     * null.
     */
    public void write( byte[] buf, int off, int len ) throws TTransportException {
        if ( dout_ == null ) {
            throw new TTransportException
                    ( TTransportException.NOT_OPEN,
                      "Cannot write to null output" );
        }
        try {
            dout_.write( buf, off, len );
        } catch ( IOException iox ) {
            throw new TTransportException( TTransportException.UNKNOWN, iox );
        }
    }

    /**
     * Can't flush IO, this method does nothing
     */
    public void flush() throws TTransportException {
    }
}

