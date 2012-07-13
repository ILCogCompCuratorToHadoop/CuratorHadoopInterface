package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.exceptions.EmptyInputException;
import edu.illinois.cs.cogcomp.thrift.curator.Record;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.File;
import java.io.IOException;

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
     * Constructs a serialization handler. Creates a Thrift transport assuming
     * a Curator exists at localhost on port 9010.
     */
    public SerializationHandler() {
        this( new TFramedTransport( new TSocket("localhost", 9010) ) );
    }

    /**
     * Constructs a serialization handler that relies on a specified Transport
     * to connect to the Curator.
     * @param transport The Thrift transport with which we can connect to a
     *                  Curator
     */
    public SerializationHandler( TTransport transport ) {
        this.transport = transport;

        serializer = new TSerializer();
        deserializer = new TDeserializer();
    }

    /**
     * Returns a byte array version of a Record.
     *
     * This method is the inverse of #deserialize().
     *
     * @param data A Record object to serialize
     * @return A string version of the input data structure
     * @throws org.apache.thrift.TException
     */
    public byte[] serialize(Record data) throws TException {
        return serializer.serialize(data);
    }

    /**
     * Rebuilds (i.e., deserializes) a Record data structure from a serialized
     * (byte array) version of that data structure.
     *
     * This method is the inverse of #serialize().
     * @param serializedData A byte array representation of a Thrift
     *                       data structure. Created using Thrift's common
     *                       implementation of the java.io.Serializable interface.
     */
    public Record deserialize( byte[] serializedData )
            throws TException, EmptyInputException {
        if( serializedData.length == 0 ) {
            throw new EmptyInputException( "Can't deserialize an empty string." );
        }

        if( !transport.isOpen() ) {
            transport.open();
        }
        Record r = new Record();
        deserializer.deserialize( r, serializedData );
        transport.close();
        return r;
    }

    /**
     * Rebuilds (i.e., deserializes) a Record data structure from a serialized
     * (byte array) version of that data structure.
     *
     * This method is the inverse of #serialize().
     * @param serializedData A file that stores a serialized Thrift
     *                       data structure.
     */
    public Record deserializeFromFile( File serializedData )
            throws TException, IOException {
        byte[] byteForm = LocalFileSystemHandler.readFileToBytes(serializedData);

        Record r = new Record();
        deserializer.deserialize( r, byteForm );
        return r;
    }

    private TSerializer serializer;
    private TDeserializer deserializer;
    private TTransport transport;
}
