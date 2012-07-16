package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.exceptions.EmptyInputException;
import edu.illinois.cs.cogcomp.thrift.curator.Record;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

import java.io.*;

/**
 * Used for writing Thrift objects to the file system. This is a thin wrapper
 * for the Thrift TSerializer and TDeserializer classes. Because it uses Thrift,
 * it relies on having a Thrift server (in our case, a Curator) that it can
 * connect to. You can either specify manually which such Curator to connect to
 * (by passing in a Transport), or let us just connect to localhost on port
 * 9010 (the default configuration for the Curator).
 * @author Tyler Young
 */
public class SerializationHandler {
    /**
     * Constructs a serialization handler.
     */
    public SerializationHandler( ) {
    }

    /**
     * Writes a serialized version of the Record object to the indicated file.
     * Will overwrite that file if it already exists.
     *
     * This is intended for writing to the local file system.
     *
     * This method is the inverse of #deserialize().
     *
     * @param data A Record object to serialize
     * @param destination The file that the Record will be written to
     * @throws org.apache.thrift.TException
     */
    public void serialize( Record data, File destination )
            throws TException, IOException {
        try {
            destination.createNewFile(); // does nothing if it already exists
        } catch( IOException e ) {
            System.out.println("Failed to create file " + destination.toString() );
            throw e;
        }

        // We use Thrift's built-in methods to serialize to an output stream
        // (which happens to be a *file* output stream)
        FileOutputStream fileWriter =
                new FileOutputStream( destination, false );
        serializeToOutputStream( data, fileWriter );
    }

    /**
     * Implements the serialization on a generic (i.e., non-file system dependent)
     * output stream. You should probably <em>not</em> use this unless you have
     * no other choice. Instead, stick to the #serialize() methods.
     *
     * This method is the inverse of #deserializeFromInputStream().
     * @param data The Record to serialize
     * @param oStream The output stream to ask Thrift to write to
     */
    public void serializeToOutputStream( Record data, OutputStream oStream )
            throws TException, IOException {
        TBinaryProtocol thriftWriter =
                new TBinaryProtocol( new TIOStreamTransport( oStream ) );

        data.write( thriftWriter ); // write to the output stream
        oStream.flush();
        oStream.close();
    }

    /**
     * Rebuilds (i.e., deserializes) a Record data structure from a serialized
     * (file) version of that data structure.
     *
     * This method is the inverse of #serialize().
     * @param serializedData A location at which we can find a text file
     *                       containing the serialized form of the Record.
     * @return The Record object constructed from the serialized data
     */
    public Record deserialize( File serializedData )
            throws TException, IOException {
        if( serializedData.length() == 0 ) {
            throw new EmptyInputException( "Can't deserialize an empty file." );
        }

        // Set up the input stream from which we will deserialize
        FileInputStream fileReader = new FileInputStream( serializedData );
        Record readVersion = deserializeFromInputStream( fileReader );
        fileReader.close();
        return readVersion;
    }


    /**
     * Implements the deserialization on an input stream. You should probably
     * <em>not</em> use this unless you have no other choice. Instead, stick
     * to the deserialize() methods.
     *
     * This method is the inverse of #serializeFromOutputStream().
     * @param in The input stream to read from the serialized file
     * @return The reconstructed Record
     */
    public Record deserializeFromInputStream( InputStream in )
            throws TException, IOException {
        TBinaryProtocol thriftReader =
                new TBinaryProtocol( new TIOStreamTransport( in ) );

        Record readVersion = new Record();
        readVersion.read( thriftReader ); // read from the input stream

        in.close();

        return readVersion;
    }
}
