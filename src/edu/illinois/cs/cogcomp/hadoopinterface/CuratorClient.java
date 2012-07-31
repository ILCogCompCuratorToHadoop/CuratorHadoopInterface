package edu.illinois.cs.cogcomp.hadoopinterface;

import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.*;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.exceptions.EmptyInputException;
import edu.illinois.cs.cogcomp.thrift.base.*;
import edu.illinois.cs.cogcomp.thrift.curator.Curator;
import edu.illinois.cs.cogcomp.thrift.curator.Record;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * A class to handle interactions with the Curator. Used in the Curator-Hadoop
 * interface as a means of serializing and deserializing records on the "master"
 * machine (i.e., a user's machine, outside of Hadoop).
 *
 * If you want to use this to work with your own, pre-existing Records (i.e.,
 * not new plain text documents), you'll need to first programmatically create
 * Records for each document. Then, construct a CuratorClient object, call its
 * addToInputList() method for each Record that you have, and call its
 * writeSerializedRecords() method to get serialized Records which you can later
 * transfer to Hadoop for processing.
 *
 * @author Lisa Bao
 * @author Tyler Young
 */
public class CuratorClient {
    private static final String NL = System.getProperty("line.separator");

    // Two Thrift objects for interfacing with the Curator
    private Curator.Client client;
    private final TTransport transport;

    // The list of all the input records that we will write to disk (to later
    // be transferred to Hadoop by another program)
    private ArrayList<Record> newInputRecords;

    // Provides serialize() and deserialize() methods for Record objects
    private final SerializationHandler serializer;
    private static boolean testing;

    public enum CuratorClientMode {
        PRE_HADOOP, POST_HADOOP;

        public static CuratorClientMode fromString( String s ) {
            try {
                return CuratorClientMode.valueOf( s );
            } catch ( IllegalArgumentException e ) {
                // This map will contain a bunch of strings which we will turn into
                // case insensitive regular expressions. If we match one of them,
                // we will return the AnnotationMode that the string is mapped to.
                Map<String, CuratorClientMode> regexes =
                        new HashMap<String, CuratorClientMode>();
                regexes.put( "pre", PRE_HADOOP );
                regexes.put( "post", POST_HADOOP);

                for( String key : regexes.keySet() ) {
                    Pattern pattern = Pattern.compile( Pattern.quote( key ),
                                                       Pattern.CASE_INSENSITIVE );
                    Matcher matcher = pattern.matcher(s);
                    if( matcher.find() ) {
                        return regexes.get(key);
                    }
                }

                throw new IllegalArgumentException( "Unknown CuratorClient mode '"
                                                    + s );
            }
        }
    }

    /**
     * Constructs a CuratorClient object with the default Curator host and port
     * (i.e., "localhost" and port 9010)
     */
    public CuratorClient() {
        this("localhost", 9010);
    }

    /**
     * Constructs a CuratorClient object
     * @param host The host name for the Curator we will connect to (e.g., in
     *             local mode, this is "localhost").
     * @param port The port on which we should connect to the Curator (e.g., we
     *             commonly use port 9010).
     */
    public CuratorClient( String host, int port ) {
        newInputRecords = new ArrayList<Record>();

        // Set up Thrift Curator Client
        transport = new TFramedTransport( new TSocket(host, port ) );
        TProtocol protocol = new TBinaryProtocol(transport);
        client = new Curator.Client(protocol);

        serializer = new SerializationHandler();
    }

    /**
     * @return A list of AnnotationModes which can be provided by annotators
     *         (essentially, the list of annotators to which the Curator can
     *         connect). This list may be empty.
     * @throws TException If we were unable to connect to the Curator
     */
    public List<AnnotationMode> listAvailableAnnotators() throws TException {
        Map<String, String> curatorAnnotations;
        try {
            if( !transport.isOpen() ) {
                transport.open();
            }
            curatorAnnotations = client.describeAnnotations();
        } finally {
            if( transport.isOpen() ) {
                transport.close();
            }
        }

        List<AnnotationMode> available = new LinkedList<AnnotationMode>();
        for( String key : curatorAnnotations.keySet() ) {
            available.add( AnnotationMode.fromString( key ) );
        }

        return available;
    }

    /**
     * A pure wrapper for the Thrift client's describeAnnotations.
     * @return A Map associating keys (which are Curator annotation types,
     *         such as "ner" or "tokens") with values (which is a string
     *         describing the class that provide the annotation, such as
     *         "Illinois Tokenizer identifies as illinoistokenizer-0.4")
     * @throws TException
     */
    public Map<String,String> describeAnnotations( ) throws TException {
        try {
            if( !transport.isOpen() ) {
                transport.open();
            }
            return client.describeAnnotations();
        } finally {
            if( transport.isOpen() ) {
                transport.close();
            }
        }


    }

    /**
     * Attempts to connect to the Curator. If it does so successfully,
     * it will return true.
     * @return True if we were able to connect to the Curator, false otherwise
     */
    public boolean curatorIsRunning() {
        try {
            listAvailableAnnotators();
        } catch ( TException e ) {
            System.out.println("\nCouldn't list available annotators. "
                    + "\nThis may be perfectly normal (if, for instance, the "
                    + "Curator hasn't finished starting yet).\n\t"
                    + "TException reason: " + e.getMessage());
            return false;
        }

        return true;
    }

    /**
     * Run the indicated annotator on the record to be annotated. Returns the
     * updated version of that record. Note, however, that there is no guarantee
     * that the toBeAnnotated record will not be modified---treat it as a
     * potentially in/out parameter, but rely on the returned record for the
     * canonical updated version.
     * @param toBeAnnotated The record that should have an annotation performed
     *                      on it
     * @param annotator The annotator to run on the record
     * @return A version of the input record updated to include the new
     *         annotation type.
     * @throws AnnotationFailedException If the record being returned was for
     *                                   some reason not actually updated with
     *                                   the requested annotation type.
     */
    public Record annotate( Record toBeAnnotated, AnnotationMode annotator )
            throws ServiceUnavailableException, TException,
            AnnotationFailedException, ServiceSecurityException {
        MessageLogger logger = HadoopInterface.logger;
        if( !RecordTools.meetsDependencyReqs( toBeAnnotated, annotator ) ) {
            StringBuilder msg = new StringBuilder();
            msg.append( "Cannot annotate document with the provided annotations. " );
            msg.append( "It is missing dependencies. We require " );
            msg.append( MessageLogger.getPrettifiedList(
                    new ArrayList<AnnotationMode>(
                            (Collection<AnnotationMode>)
                                    annotator.getDependencies() ) ) );
            msg.append( " but you provided " );
            msg.append( RecordTools.getAnnotationsString( toBeAnnotated ) );
            throw new AnnotationFailedException( msg.toString() );
        }
        else {
            logger.logStatus( "Record provides annotations: "
                    + RecordTools.getAnnotationsString( toBeAnnotated ) );
        }

        try {
            if( !transport.isOpen() ) {
                transport.open();
            }

            // performAnnotation() doesn't work. The following (asking the
            // Curator to store the record, then using provide()) is a
            // cludgy workaround.
            // TODO: [Long-term] Fix the performAnnotation() function!!
            if( !annotator.equals( AnnotationMode.TOKEN )
                    && !annotator.equals( AnnotationMode.SENTENCE ) ) {
                HadoopInterface.logger.logStatus( "Storing record..." );
                client.storeRecord( toBeAnnotated );
            }

            HadoopInterface.logger.logStatus( "Calling provide for "
                                              + annotator.toString() + "..." );
            // NOTE: forceUpdate must be false or else we will also try to
            // update the dependencies, leading to a fiery death.
            toBeAnnotated = client.provide( annotator.toCuratorString(),
                                            toBeAnnotated.getRawText(),
                                            false );
        } finally {
            if( transport.isOpen() ) {
                transport.close();
            }
        }

        HadoopInterface.logger.logStatus( "Ensuring we got the annotation..." );
        if( !RecordTools.hasAnnotation( toBeAnnotated, annotator ) ) {
            throw new AnnotationFailedException(
                    "The Curator job ran without error, but for some reason, we "
                    + "failed to annotate document whose hash is "
                    + toBeAnnotated.getIdentifier()
                    + " with annotation type " + annotator.toString() + ".\n"
                    + "Is the Curator providing " + annotator.toString() + "? "
                    + ( listAvailableAnnotators().contains( annotator )
                        ? "Yes." : "No." )
                    + "\nRecord's annotations: "
                    + RecordTools.getAnnotationsString( toBeAnnotated ) );
        }

        return toBeAnnotated;
    }

    /**
     * Takes a path to documents in a mirror of the HDFS directory structure
     * and creates new Curator Records. Calls #addToInputList() to add each new
     * Record to the class's list of input records.
     *
     * Checks for an existing record in the database, if requested; note that
     * this flag will trust the database to have the most up-to-date Record for
     * a given document.
     *
     * @param jobDir Path pointing to, e.g. `/user/home/job123/` directory
     * @param checkdb If true, checks for an existing Record for this document in
     *                the Curator database
     */
    public void addRecordsFromJobDirectory( File jobDir, boolean checkdb )
            throws TException, FileNotFoundException, ServiceUnavailableException,
            AnnotationFailedException {
        // Check that the path is valid
        if (!jobDir.isDirectory()) {
            throw new IllegalArgumentException( "The job path " + jobDir.toString()
                                                + " is not a directory.");
        }
        if( !LocalFileSystemHandler.containsNonHiddenFiles( jobDir ) ) {
            throw new IllegalArgumentException(
                    "The job path " + jobDir.toString()
                    + " contains no (non-hidden) files.");
        }

        // LOOP: for each file in the job directory...
        for (File doc : jobDir.listFiles() ) {
            if( !doc.isDirectory() ) {
                Record currentRecord = null;

                // Check the database for the record, if necessary
                if (checkdb) {
                    File originalTxtFile = new File(doc, "original.txt");
                    currentRecord = getRecFromDatabase( originalTxtFile );
                }

                // If we got a record from the database, we won't try to construct
                // it from the directory
                if( currentRecord == null ) {
                    try {
                        currentRecord = serializer.deserialize( doc );
                    } catch ( IOException e ) {
                        System.out.println("Exception attempting to serialize "
                                + "record from job directory.");
                        e.printStackTrace();
                    }
                }

                addToInputList( currentRecord );
            }
        } // END for each document directory
    } // END function

    /**
     * Checks the database archive for a record corresponding to a certain
     * original (raw) text file
     * @param originalFile The raw text file for this document
     * @return A filled-in record if it exists in the database, but NULL if no
     *         record was found.
     */
    private Record getRecFromDatabase( File originalFile )
            throws FileNotFoundException, ServiceUnavailableException,
            AnnotationFailedException, TException {
        if (!originalFile.isFile()) {
            System.out.println("ERROR: Attempt to check database " +
                                       "for nonexistent original file");
        }

        String text = LocalFileSystemHandler.readFileToString( originalFile );

        Record dbRecord;
        try {
            if( !transport.isOpen() ) {
                transport.open();
            }
            dbRecord = client.getRecord( text );
        }
        finally {
            if( transport.isOpen() ) {
                transport.close();
            }
        }
        return dbRecord;
    }

    /**
     * Takes the name of an annotation text file and extracts out the annotation
     * type. E.g., if you pass in "original.txt", this will return "original".
     * If you pass in "chunk.txt", it will return "chunk". It basically just
     * returns what you pass in minus the ".txt" extension.
     * @param fileName The filename of the annotation file in question.
     * @return The type of annotation that the filename represents
     */
    private static String getAnnotationTypeFromFileName( String fileName ) {
        int lastCharOfType = fileName.length() - 4;
        // remove .txt from file name
        return fileName.substring(0, lastCharOfType);
    }

    /**
     * Creates a new Record from a document's original (raw) text and adds it to
     * the class's list of input Records. The input directory should have some
     * number of plain text files directly inside it. For instance, if your
     * input directory was `job123`, your directory structure might look like
     * this:
     *
     * <h4>job123</h4>
     * <ul>
     *     <li>document0.txt</ul>
     *     <li>document1.txt</ul>
     *     <li>document2.txt</ul>
     *     <li>document3.txt</ul>
     * </ul>
     *
     * Any subdirectories of your input directory will be ignored.
     *
     * @param inputDir The directory to draw original text files from
     */
    public void createRecordsFromRawInputFiles( File inputDir ) {
        // Check that the input directory is valid
        if( !inputDir.isDirectory() ) {
            throw new IllegalArgumentException("The location "
                    + inputDir.toString() + " does not refer to a directory.");
        }

        // For each file in the directory . . .
        for( File f : inputDir.listFiles() ) {
            try {
                if( !f.isDirectory() && !f.isHidden() ) {
                    // Add it to the CuratorClient's queue of documents to serialize
                    // in preparation for sending the input to Hadoop
                    String fileContents = LocalFileSystemHandler
                            .readFileToString( f );
                    Record newRecord = RecordTools.generateNew( fileContents );
                    addToInputList( newRecord );
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * Takes a File of the single document to be added and creates a new
     * Curator Record for it. Also calls addToInputList().
     * The document should be named "original.txt" and be located in
     * a directory named its hash code.
     * Note that this method does not accommodate existing annotations.
     *
     * Presently for testing purposes, to easily input one test document.
     *
     * @param originalTxt a File object pointing to the specified document's
     *                    `original.txt` file
     * @return a copy of the new Curator Record for the specified document
     */
    public Record addOneRecord(File originalTxt)
            throws IllegalArgumentException, FileNotFoundException {
        if (!originalTxt.isFile()) {
            throw new IllegalArgumentException( "The file " + originalTxt.toString()
                    + " is not a valid normal file.");
        }

        String type = getAnnotationTypeFromFileName( originalTxt.getName() );
        if (type.equals("original")) {
            String id = originalTxt.getParent();
            String original = LocalFileSystemHandler.readFileToString( originalTxt );
            Record newRecord = RecordTools.generateNew( id, original );

            addToInputList(newRecord);

            return newRecord;
        }
        else {
            throw new IllegalArgumentException( "ERROR: " + type + " is not the "
                    + "required original.txt (i.e., raw text) document.");
        }
    }

    /**
     * Serializes all Records in the list of input Records and writes them to
     * the output directory. This will later be copied to the Hadoop file system.
     * @param outputDir The location to which we should write the serialized
     *                  records
     */
    public void writeSerializedRecords( File outputDir )
            throws IOException, TException {
        // Create the output directory if necessary
        if( !outputDir.isDirectory() ) {
            if( !outputDir.mkdir() ) {
                throw new IOException("Failed to create output directory "
                                              + outputDir.toString() );
            }
        }

        for( Record r : newInputRecords ) {
            File txtFileLoc = getLocForSerializedForm( r, outputDir );

            // Write the serialized form to the file
            serializer.serialize( r, txtFileLoc );

            // Ensure we can read back the same thing we just wrote
            if( testing ) {
                Record copy = serializer.deserialize( txtFileLoc );
                System.out.println( "Copy matches written? "
                                    + (copy.equals(r) ? "Yes" : "No") );
            }
        }
    }

    /**
     * Returns the location of the serialized form of the indicated record, which
     * depends on the location that it should be written to. At present, this will
     * always be a text file, named with the record's hash, within the containing
     * directory. However, this is subject to change. As an example, the current
     * structure looks like this:
     *
     * <ul>
     *     <li>[containing_directory_name] <ul>
     *         <li>[document_hash].txt</li>
     *         <li>[another_documents_hash].txt</li>
     *     </ul></li>
     * </ul>
     *
     * @param containingDir The directory containing this serialized record
     * @param r The record in question
     * @return The location at which the serialized form of the record should
     *         be found
     */
    private File getLocForSerializedForm( Record r, File containingDir ) {
        return new File( containingDir, r.getIdentifier() + ".txt" );
    }

    /**
     * Takes a Curator Record object and adds it to a list of newly
     * added records for future serialization.
     *
     * @param record A Curator Record object
     */
    public void addToInputList(Record record) {
        if( record.getRawText().equals("") ) {
            System.out.println( "Tried to add a record with no original text." );
            System.out.println( "Claims to have ID " + record.getIdentifier() );
            System.out.println( "We are skipping that record.\n" );
        }
        else {
            newInputRecords.add( record );
        }
    }

    /**
     * Gets the number of Records to be serialized.
     * @return The number of input Records ready to be processed
     */
    public int getNumInputRecords() {
        return newInputRecords.size();
    }

    /**
     * Lists the available annotators to the standard output.
     * @throws TException
     */
    public void printInfoOnKnownAnnotators() throws TException {
        try {
            if( !transport.isOpen() ) {
                transport.open();
            }
            Map<String, String> avail = client.describeAnnotations();
            System.out.println("Available annotations:");
            for (String key : avail.keySet()) {
                System.out.println("\t" + key + " provided by " + avail.get(key) );
            }
        }
        finally {
            if( transport.isOpen() ) {
                transport.close();
            }
        }
    }

    /**
     * Returns the Thrift transport used to connect to the Curator
     * @return the transport
     */
    protected TTransport getTransport() {
        return transport;
    }

    /**
     * In post-Hadoop mode, this is used to send the Curator the Records from
     * the input list (i.e., the records we got from Hadoop, then reconstructed
     * using the #addRecordsFromJobDirectory() method).
     */
    private void informDatabaseOfUpdatedRecords()
            throws ServiceUnavailableException, TException,
            AnnotationFailedException, ServiceSecurityException {
        for( Record r : newInputRecords ) {
            Record old;
            try {
                if( !transport.isOpen() ) {
                    transport.open();
                }
                old = client.getRecord( r.getRawText() );
            } finally {
                if( transport.isOpen() ) {
                    transport.close();
                }
            }

            int oldNumViews = RecordTools.getNumViews( old );
            int newNumViews = RecordTools.getNumViews( r );

            StringBuilder msg = new StringBuilder();
            if( oldNumViews < newNumViews ) {
                msg.append( "\n\nThe Curator database knew of " );
                msg.append( oldNumViews );
                msg.append( " annotations for the document that begins '" );
                msg.append( RecordTools.getBeginningOfOriginalText( r ) );
                msg.append( "', whose hash is " );
                msg.append( r.getIdentifier() ) ;
                msg.append( ".\n\nHowever, we know of " );
                msg.append( newNumViews );
                msg.append( " views. Updating the database accordingly." );
                System.out.println( msg.toString() );

                try {
                    if( !transport.isOpen() ) {
                        transport.open();
                    }
                    client.storeRecord( r );
                } catch( ServiceSecurityException e ) {
                    System.out.println( "Security exception. It looks like you" +
                            "don't have write access to your Curator database" +
                            "(indicated by a 'Curator does not support " +
                            "storeRecord' error). Check your dist/configs/" +
                            "curator.properties file for write access.\n" +
                            e.getReason() );
                    throw e;
                } finally {
                    if( transport.isOpen() ) {
                        transport.close();
                    }
                }
            }
            else {
                msg.append( "\n\nWe have no new data on the document that begins '" );
                msg.append( RecordTools.getBeginningOfOriginalText( r ) );
                msg.append( "', whose hash is " );
                msg.append( r.getIdentifier() ) ;
                msg.append( ". We have " );
                msg.append( newNumViews );
                msg.append( " views for it.\nNo database update is necessary, " );
                msg.append( "but this is troubling, UNLESS you have been running " );
                msg.append( "your jobs on this machine (in which case it's expected).\n" );
                msg.append( "Views we now know of: ");
                msg.append( RecordTools.getAnnotationsString( r ) );
                msg.append( "\nOld views we knew of: ");
                msg.append( RecordTools.getAnnotationsString( old ) );
                System.out.println( msg.toString() );
            }
        }
    }

    /**
     * The main method for the external, "master" Curator client.
     * @param commandLineArgs  String arguments from the command line. Should
     *                         contain the host name for the (already-running)
     *                         Curator, the port number for connecting to the
     *                         Curator, the job input directory, and the mode at
     *                         minimum.
     */
    public static void main( String[] commandLineArgs )
            throws ServiceUnavailableException, TException,
            AnnotationFailedException, IOException, ServiceSecurityException {
        // Parse input
        CuratorClientArgParser args = new CuratorClientArgParser(commandLineArgs);
        args.printArgsInterpretation();

        // Set up local vars
        CuratorClient theClient = new CuratorClient( args.getHost(),
                                                     args.getPort() );
        testing = args.isTesting();
        String msg = "Curator is running on localhost, port 9010? " +
                (theClient.curatorIsRunning() ? "Yes." : "No.");
        System.out.println(msg);

        if( args.getMode() == CuratorClientMode.PRE_HADOOP ) {
            // Create records from the input text files
            System.out.println( "Ready to create records from the plain text in " +
                                "the input directory." );
            theClient.createRecordsFromRawInputFiles( args.getInputDir() );

            System.out.println( "Turned " + theClient.getNumInputRecords()
                                + " text files in the directory into records.");

            if( testing ) {
                // Check available annotations
                theClient.printInfoOnKnownAnnotators();

                // Run a tool (for testing purposes)
                theClient.testPOSAndTokenizer( args.getOutputDir() );
            }

            // Serialize output
            System.out.println( "Serializing those records to: "
                                        + args.getOutputDir().toString() );

            theClient.writeSerializedRecords( args.getOutputDir() );
        }
        else { // Post-Hadoop. Time to add the records to the database.
            theClient.addRecordsFromJobDirectory( args.getInputDir(), false );

            if( theClient.getNumInputRecords() > 0 ) {
                theClient.informDatabaseOfUpdatedRecords();
            }
            else {
                throw new EmptyInputException( "Found no serialized Records in "
                                               + "the directory. Exiting...");
            }
        }
    }

    /**
     * Makes the Thrift calls necessary to run the known records through both
     * the Tokenizer and the POS tagger. Writes debugging information to the
     * standard output.
     */
    private void testPOSAndTokenizer( File outputDir )
            throws ServiceUnavailableException, AnnotationFailedException,
            TException, IOException {
        System.out.println( "Running the tokenizer on those new files. "
                                    + "(For testing only)" );

        ArrayList<Record> replaceTheRecords = new ArrayList<Record>();
        for( Record r : newInputRecords ) {
            transport.open();

            String tokens = AnnotationMode.TOKEN.toCuratorString(); // "tokens"
            r = client.provide(tokens, r.getRawText(), true);

            // This fails, for whatever reason.
            // client.performAnnotation(r, tokens, true);

            // Confirm it worked
            if( !RecordTools.hasAnnotation( r, AnnotationMode.TOKEN ) ) {
                System.out.println( "Couldn't find " + tokens + " annotation!" );
            }

            client.provide("pos", r.getRawText(), false);
            transport.close();

            // Confirm it worked
            if( !RecordTools.hasAnnotation( r, AnnotationMode.POS ) ) {
                System.out.println( "Couldn't find " + AnnotationMode.POS.toCuratorString() + " annotation!" );
            }

            int numViews = RecordTools.getNumViews( r );
            System.out.println("Record now has " + numViews + " views.");

            System.out.println("Testing serialization.");

            File writtenVersion = getLocForSerializedForm(r, outputDir );
            serializer.serialize( r, writtenVersion );
            Record reconstructed = serializer.deserialize( writtenVersion );
            if( !r.equals(reconstructed) ) {
                System.out.println("\tSerialization didn't work.");
                System.out.println("\tHere's the original:");
                System.out.println( "\t\tOrig identifier: " + r.getIdentifier() );
                System.out.println( "\t\tOriginal raw text: " + r.getRawText() );
                System.out.println("\n\n\tHere's the reconstructed:");
                System.out.println( "\t\tReconstructed identifier: "
                                    + reconstructed.getIdentifier() );
                System.out.println( "\t\tReconstructed text: "
                                    + reconstructed.getRawText() );
            }
            else {
                System.out.println("\tSerialization worked!!");
            }

            replaceTheRecords.add(r);
        }
        newInputRecords = replaceTheRecords;
    }

    public void runNER() throws TException, ServiceUnavailableException,
            AnnotationFailedException, ServiceSecurityException {
        transport.open();

        String ner = AnnotationMode.NER.toCuratorString();
        for( Record r : newInputRecords ) {
            client.storeRecord( r );
            r = client.provide(ner, r.getRawText(), false);

            // Confirm it worked
            if( !RecordTools.hasAnnotation( r, AnnotationMode.NER ) ) {
                System.out.println( "Couldn't find " + ner + " annotation!" );
            }
        }

        transport.close();
    }

    private void callABunchOfAnnotationsFromDemo(TTransport transport)
            throws ServiceUnavailableException, AnnotationFailedException, TException {
        System.out.println("\n\nWe are going to inspect the Curator for the available annotations:\n");

        Map<String, String> avail = null;
        try {
            transport.open();
            avail = client.describeAnnotations();
            transport.close();
        } catch (TException e1) {
            e1.printStackTrace();
        }

        for (String key : avail.keySet()) {
            System.out.println("\t"+key + " provided by " + avail.get( key ));
        }

        System.out.println();

        boolean forceUpdate = true;

        System.out.println("Next we'll call the extended NER (more entity types)...");
        System.out.print("Calling curator.provide(\"ner-ext\", text, false)... ");

        String text = "Lorem ipsum.";
        Record record = new Record();
        try {
            transport.open();
            //call Curator
            record = client.provide("ner-ext", text, forceUpdate);
            transport.close();
        } catch (ServiceUnavailableException e) {
            if (transport.isOpen()) {
                transport.close();
            }
            System.out.println("ner-ext annotations are not available");
            System.out.println(e.getReason());

        } catch (TException e) {
            if (transport.isOpen()) {
                transport.close();
            }
            e.printStackTrace();
        }
        System.out.println("done.\n");
        System.out.println();
        if (avail.containsKey("ner-ext")) {
            System.out.println( RecordTools.getContents( record ) );
            System.out.println();

            System.out.println("Extended Named Entities\n---------\n");
            for (Span span : record.getLabelViews().get("ner-ext").getLabels()) {
                System.out.println(span.getLabel() + " : "
                + record.getRawText().substring(span.getStart(), span.getEnding()));
            }
            System.out.println();
            System.out.println();
            System.out.println("The raw data structure containing the NEs looks like this:");
            System.out.println(record.getLabelViews().get("ner"));
        }
        System.out.println();


        System.out.println("Next we will get a chunking (shallow parse) of the text.");
        System.out.print("Calling curator.provide(\"chunk\", text, forceUpdate = '" + ( forceUpdate ? "TRUE" : "FALSE" ) + "')... ");
        try {
            transport.open();
            //call Curator
            record = client.provide("chunk", text, forceUpdate);
            transport.close();
        } catch (ServiceUnavailableException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
        System.out.println("done.");
        System.out.println();
        System.out.println( RecordTools.getContents( record ) );
        System.out.println();
        System.out.println("Notice that the record now contains chunk and sentences, tokens and pos fields.\n" +
				"This is because pos tags are required for chunking.  And tokenization is required by the pos tagger");
        System.out.println("\nSentences\n--------\n");
        for (Span span : record.getLabelViews().get("sentences").getLabels()) {
            System.out.println("# " +record.getRawText().substring(span.getStart(), span.getEnding()));
        }
        System.out.println("\nPOS Tags\n------\n");
        StringBuffer result = new StringBuffer();
        for (Span span : record.getLabelViews().get("pos").getLabels()) {
            result.append(record.getRawText().substring(span.getStart(), span.getEnding()) + "/" + span.getLabel());
            result.append(" ");
        }
        System.out.println(result.toString());
        System.out.println();
        System.out.println("Chunking\n---------\n");
        result = new StringBuffer();
        for (Span span : record.getLabelViews().get("chunk").getLabels()) {
            result.append("["+span.getLabel()+ " ");
            result.append(record.getRawText().substring(span.getStart(), span.getEnding()));
            result.append("] ");
        }
        System.out.println(result.toString());

        System.out.println("\n");
        System.out.println("Next we will get the stanford dependency annotations of the text.\n");
        System.out.print("Calling curator.provide(\"stanfordDep\", text, forceUpdate = '" + ( forceUpdate ? "TRUE" : "FALSE" ) + "')... ");

        try {
            transport.open();
            //call Curator
            record = client.provide("stanfordDep", text, forceUpdate);
            transport.close();
        } catch (ServiceUnavailableException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }

        System.out.println();
        System.out.println("Stanford Dependencies\n------------------\n\n");
        for (Tree depTree : record.getParseViews().get("stanfordDep").getTrees()) {
            int top = depTree.getTop(); //this tells us where in nodes the head node is
            Stack<Integer> stack = new Stack<Integer>();
            stack.push(top);
            result = new StringBuffer();
            while (!stack.isEmpty()) {
                int headIndex = stack.pop();
                Node head = depTree.getNodes().get(headIndex);
                if (!head.isSetChildren()) {
                    continue;
                }
                for (Integer childIndex : head.getChildren().keySet()) {
                    stack.push(childIndex);
                    Node child = depTree.getNodes().get(childIndex);
                    String relation = head.getChildren().get(childIndex);
                    result.append(relation);
                    result.append("(");
                    result.append(record.getRawText().substring(head.getSpan().getStart(), head.getSpan().getEnding()));
                    result.append(", ");
                    result.append(record.getRawText().substring(child.getSpan().getStart(), child.getSpan().getEnding()));
                    result.append(")\n");
                }
            }
            System.out.println("Dependency tree");
            System.out.println(result.toString());
        }

        System.out.println();

        System.out.println();
        System.out.println("Next we will get the Wikifier's view of the text.");
        System.out.print("Calling curator.provide(\"wikifier\", text, forceUpdate = '" + ( forceUpdate ? "TRUE" : "FALSE" ) + "')... ");
        try {
            transport.open();
            //call Curator
            record = client.provide("wikifier", text, forceUpdate);
            transport.close();
        } catch (ServiceUnavailableException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
        System.out.println("done.");
        System.out.println();
        System.out.println( RecordTools.getContents( record ) );
        System.out.println();

        result = new StringBuffer();
        for (Span span : record.getLabelViews().get("wikifier").getLabels()) {
            result.append("Term from text: '");
            result.append(record.getRawText().substring(span.getStart(), span.getEnding()));
            result.append( "'\nLabel: " + span.getLabel()+ "\nProperties: \n" );

            for ( Entry< String, String > e : span.getAttributes().entrySet() )
			    result.append( e.getKey() + ", " + e.getValue() + "; " + "\n" );
            result.append("----------------------\n");
        }
        System.out.println(result.toString());

        System.out.println("\n");


        System.out.println();
        System.out.println("Next we will get the verb Semantic Role structures...");
        System.out.print("Calling curator.provide(\"srl\", text, forceUpdate)... ");
        try {
            transport.open();
            //call Curator
            record = client.provide("srl", text, forceUpdate);
            transport.close();
        } catch (ServiceUnavailableException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
        System.out.println("done.");


        System.out.println();
        System.out.println("Semantic role labels (verbs):\n------------------\n\n");
        for (Tree depTree : record.getParseViews().get("srl").getTrees()) {
            int top = depTree.getTop(); //this tells us where in nodes the head node is
            Stack<Integer> stack = new Stack<Integer>();
            stack.push(top);
            result = new StringBuffer();
            while (!stack.isEmpty()) {
                int headIndex = stack.pop();
                Node head = depTree.getNodes().get(headIndex);
                if (!head.isSetChildren()) {
                    continue;
                }
                for (Integer childIndex : head.getChildren().keySet()) {
                    stack.push(childIndex);
                    Node child = depTree.getNodes().get(childIndex);
                    String relation = head.getChildren().get(childIndex);
                    result.append(relation);
                    result.append("(");
                    result.append(record.getRawText().substring(head.getSpan().getStart(), head.getSpan().getEnding()));
                    result.append(", ");
                    result.append(record.getRawText().substring(child.getSpan().getStart(), child.getSpan().getEnding()));
                    result.append(")\n");
                }
            }
            System.out.println("Verb SRL predicate-argument structure:");
            System.out.println(result.toString());
        }

        System.out.println();

        System.out.println();

        System.out.println();
        System.out.println("Next we will get the noun Semantic Role structures...");
        System.out.print("Calling curator.provide(\"nom\", text, forceUpdate)... ");
        try {
            transport.open();
            //call Curator
            record = client.provide("nom", text, forceUpdate);
            transport.close();
        } catch (ServiceUnavailableException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
        System.out.println("done.");


        System.out.println("Semantic role labels (de-verbal nouns):\n------------------\n\n");
        for (Tree depTree : record.getParseViews().get("nom").getTrees()) {
            int top = depTree.getTop(); //this tells us where in nodes the head node is
            Stack<Integer> stack = new Stack<Integer>();
            stack.push(top);
            result = new StringBuffer();
            while (!stack.isEmpty()) {
                int headIndex = stack.pop();
                Node head = depTree.getNodes().get(headIndex);
                if (!head.isSetChildren()) {
                    continue;
                }
                for (Integer childIndex : head.getChildren().keySet()) {
                    stack.push(childIndex);
                    Node child = depTree.getNodes().get(childIndex);
                    String relation = head.getChildren().get(childIndex);
                    result.append(relation);
                    result.append("(");
                    result.append(record.getRawText().substring(head.getSpan().getStart(), head.getSpan().getEnding()));
                    result.append(", ");
                    result.append(record.getRawText().substring(child.getSpan().getStart(), child.getSpan().getEnding()));
                    result.append(")\n");
                }
            }
            System.out.println("Noun SRL predicate-argument structure:");
            System.out.println(result.toString());
        }

        System.out.println();


        // 		System.out.println("We could continue calling the Curator for other annotations but we'll stop here.");


        System.out.println();
        System.out.println("Next we call the MentionDetector..." );
        System.out.print("Calling curator.provide(\"mention\", text, forceUpdate = '" + ( forceUpdate ? "TRUE" : "FALSE" ) + "')... ");
        try {
            transport.open();
            //call Curator
            record = client.provide("mention", text, forceUpdate);
            transport.close();
        } catch (ServiceUnavailableException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
        System.out.println("done.");
        System.out.println();
        System.out.println( RecordTools.getContents( record ) );
        System.out.println();

        result = new StringBuffer();
        for (Span span : record.getLabelViews().get("mention").getLabels()) {
            result.append("Term from text: '");
            result.append(record.getRawText().substring(span.getStart(), span.getEnding()));
            result.append( "'\nLabel: " + span.getLabel()+ "\nProperties: \n" );

            for ( Entry< String, String > e : span.getAttributes().entrySet() )
                result.append( e.getKey() + ", " + e.getValue() + "; " + "\n" );
            result.append("----------------------\n");
        }
        System.out.println(result.toString());

        System.out.println("\n");
    }



}
