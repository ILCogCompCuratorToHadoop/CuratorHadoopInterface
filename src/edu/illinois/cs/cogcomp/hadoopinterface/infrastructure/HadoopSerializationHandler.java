package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.exceptions
        .EmptyInputException;
import edu.illinois.cs.cogcomp.thrift.curator.Record;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TDataTransport;

import java.io.DataInput;
import java.io.DataOutput;
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
public class HadoopSerializationHandler extends SerializationHandler {
    /**
     * Constructs a serialization handler.
     */
    public HadoopSerializationHandler() {
    }

    /**
     * Writes a serialized version of the Record object to the indicated file.
     * Will overwrite that file if it already exists.
     *
     * This is intended for writing to the Hadoop Distributed File System (HDFS).
     *
     * This method is the inverse of #deserialize().
     *
     * @param data A Record object to serialize
     * @param destination The file that the Record will be written to
     * @param fs The file system object against which we should resolve the path
     * @throws org.apache.thrift.TException
     */
    public void serialize( Record data, Path destination, FileSystem fs )
            throws IOException, TException {
        FSDataOutputStream fileWriter = fs.create( destination );
        serializeToOutputStream( data, fileWriter );
    }

    /**
     * Rebuilds a Record data structure from the serialized form in HDFS.
     *
     * This method is the inverse of #serialize().
     * @param serializedData A location in HDFS at which we can find a text file
     *                       containing the serialized form of the Record.
     * @param fs The Hadoop FileSystem object against which we will resolve
     *           the path
     * @return The Record object constructed from the serialized data
     * @throws java.io.IOException
     */
    public Record deserialize( Path serializedData, FileSystem fs )
            throws IOException, TException {
        FileSystemHandler fsHandler = new FileSystemHandler( fs );
        if( !fsHandler.HDFSFileExists( serializedData ) ||
                fsHandler.getFileSizeInBytes( serializedData ) == 0 ) {
            throw new EmptyInputException( "Can't deserialize an empty file." );
        }

        FSDataInputStream fileReader = fs.open( serializedData );
        return deserializeFromInputStream( fileReader );
    }


    /**
     * Implements the serialization on a generic DataOutput object
     * @param data The Record to serialize
     * @param out The data output stream to ask Thrift to write to
     * @throws org.apache.thrift.TException
     */
    public void serializeToDataOutput( Record data, DataOutput out )
            throws TException {
        TBinaryProtocol thriftWriter =
                new TBinaryProtocol( new TDataTransport( out ) );

        data.write( thriftWriter ); // write to the output stream
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
    public Record deserializeFromDataInput( DataInput in ) throws TException {
        TBinaryProtocol thriftReader =
                new TBinaryProtocol( new TDataTransport( in ) );

        Record readVersion = new Record();
        readVersion.read( thriftReader ); // read from the input stream

        return readVersion;

    }
}
