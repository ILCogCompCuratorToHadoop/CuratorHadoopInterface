package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import edu.illinois.cs.cogcomp.thrift.curator.Record;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.io.File;
import java.io.IOException;

/**
 * Used for writing Thrift objects to the file system. This is a thin wrapper
 * for the Thrift TSerializer and TDeserializer classes.
 * @author Tyler Young
 */
public class SerializationHandler {
    /**
     * Constructs a serialization handler.
     */
    public SerializationHandler() {
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
    public Record deserialize( byte[] serializedData ) throws TException {
        Record r = new Record();
        deserializer.deserialize( r, serializedData );
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
}
