package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

import java.io.*;

/**
 * Used for writing Thrift objects to the file system
 * @author Tyler Young
 */
public class SerializationHandler {
        // Output stream for writing to the file system
        private BufferedOutputStream out;
        // Used by Thrift for binary serialization of an object
        private TBinaryProtocol binaryOut;

        /**
         * Constructs a serialization handler.
         */
        public SerializationHandler() {
        }

    /**
     * Write the object to disk.
     * @param objectToSerialize A Thrift data structure object to serialize
     * @param destination The file that will be written to
     */
    public void writeObjectToFile( TBase objectToSerialize, File destination )
            throws IOException, TException {
        open( destination );
        objectToSerialize.write( binaryOut );
        out.flush();
        close();
    }

    public byte[] getSerializedForm( TBase objectToSerialize )
            throws TException, IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        out = new BufferedOutputStream( byteStream );
        binaryOut = new TBinaryProtocol( new TIOStreamTransport( out ) );
        objectToSerialize.write( binaryOut );
        out.flush();
        close();

        return byteStream.toByteArray();
    }

    /**
     * Opens the file for writing and prepares the Thrift binary output stream
     * @param destination The file that will be written to
     */
    private void open( File destination ) throws FileNotFoundException {
        out = new BufferedOutputStream(new FileOutputStream(destination), 2048);
        binaryOut = new TBinaryProtocol( new TIOStreamTransport( out ) );
    }

    /**
     * Closes the file stream.
     */
    public void close() throws IOException {
        out.close();
    }
}
