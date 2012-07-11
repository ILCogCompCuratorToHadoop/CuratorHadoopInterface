package edu.illinois.cs.cogcomp.hadoopinterface;

import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.*;
import edu.illinois.cs.cogcomp.thrift.base.*;
import edu.illinois.cs.cogcomp.thrift.curator.Curator;
import edu.illinois.cs.cogcomp.thrift.curator.Record;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;


/**
 * A class to handle interactions with the Curator. Used in the Curator-Hadoop
 * interface as a means of serializing and deserializing records on the "master"
 * machine (i.e., a user's machine, outside of Hadoop).
 * @author Lisa Bao
 * @author Tyler Young
 */
public class CuratorClient {
    public static final String serializedRecFileName = "record.txt";
    private static final String NL = System.getProperty("line.separator");

    private Curator.Client client;

    // The list of all the input records that we will write to disk (to later
    // be transferred to Hadoop by another program)
    private ArrayList<Record> newInputRecords;
    private TTransport transport;
    private SerializationHandler serializationHandler;

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

        serializationHandler = new SerializationHandler();
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
    public void addRecordsFromJobDirectory(File jobDir, boolean checkdb)
            throws TException, FileNotFoundException, ServiceUnavailableException,
            AnnotationFailedException {
        // check that the path is valid
        if (!jobDir.isDirectory()) {
            throw new IllegalArgumentException( "The job path " + jobDir.toString()
                                                + " is not a directory.");
        }

        // TODO: Ensure the job directory and the document directories are not empty
        // LOOP: for each file in the job directory...
        for (File docDir : jobDir.listFiles()) {
            // get the hash ID string from the subdirectory name
            String id = docDir.getName();

            Record currentRecord = null;

            // TODO: Extract method here?
            // Check the database for the record, if necessary
            if (checkdb) {
                File originalFile = new File(jobDir, id + File.separator + "original.txt");
                if (!originalFile.isFile()) {
                    System.out.println("ERROR: Attempt to check database for nonexistent original file");
                }
                String originalText = LocalFileSystemHandler
                        .readFileToString( originalFile );

                transport.open();
                Record dbRecord = client.getRecord(originalText);
                transport.close();

                if ( RecordTools.recordHasAnnotations( dbRecord ) ) {
                    currentRecord = dbRecord;
                }
            }
            // If we got a record from the database, we won't try to construct
            // it from the directory
            if( currentRecord == null ) {
                File serializedRecFile = new File( docDir, serializedRecFileName );

                try {
                    byte[] serializedRec = LocalFileSystemHandler
                            .readFileToBytes( serializedRecFile );
                    currentRecord = serializationHandler
                            .deserialize( serializedRec );
                } catch ( IOException e ) {
                    e.printStackTrace();
                }
            } // END if we don't have a record yet

            addToInputList( currentRecord );
        } // END for each document directory
    } // END function

    /**
     * Takes the name of an annotation text file and extracts out the annotation
     * type. E.g., if you pass in "original.txt", this will return "original".
     * If you pass in "chunk.txt", it will return "chunk". It basically just
     * returns what you pass in minus the ".txt" extension.
     * @param fileName The filename of the annotation file in question.
     * @return The type of annotation that the filename represents
     * @TODO: Remove since it's unused (?)
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
     * - job123
     *      - document0.txt
     *      - document1.txt
     *      - document2.txt
     *      - document3.txt
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
                    Record newRecord = RecordTools
                            .generateNewRecord( fileContents );
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
     * @param file a File object pointing to the specified document
     * @return a copy of the new Curator Record for the specified document
     */
    public Record addOneRecord(File file)
            throws IllegalArgumentException, FileNotFoundException {
        if (!file.isFile()) {
            throw new IllegalArgumentException( "The file " + file.toString()
                    + " is not a valid normal file.");
        }

        String type = getAnnotationTypeFromFileName( file.getName() );
        if (type.equals("original")) {
            String id = file.getParent();
            String original = LocalFileSystemHandler.readFileToString( file );
            Record newRecord = RecordTools.generateNewRecord( id, original );

            addToInputList(newRecord);

            return newRecord;
        }
        else {
            throw new IllegalArgumentException(
                    "ERROR: " + type + " is not the required original document.");
        }
    }

    /**
     * Takes a Curator Record object and adds it to a list of newly
     * added records for future serialization.
     *
     * @param record A Curator Record object
     */
    private void addToInputList(Record record) {
        if( newInputRecords == null ) {
            newInputRecords = new ArrayList<Record>();
        }
        newInputRecords.add(record);
    }

    /**
     * Deletes all the newly added records from our memory. Probably only useful
     * for testing purposes.
     */
    private void clearInputList() {
        newInputRecords.clear();
    }

    /**
     * Gets the list of Records to be serialized. This is for testing purposes
     * only.
     * @return the input Records ready to be processed
     */
    private List<Record> getInputList() {
        return newInputRecords;
    }

    /**
     * Gets the number of Records to be serialized.
     * @return The number of input Records ready to be processed
     */
    public int getNumberOfInputRecords() {
        return newInputRecords.size();
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

        System.out.println( "Writing output for "
                                    + Integer.toString(newInputRecords.size())
                                    + " records." );

        for( Record r : newInputRecords ) {
            File recordOutputDir = new File( outputDir, r.getIdentifier() );

            if( !recordOutputDir.mkdir() && !recordOutputDir.isDirectory() ) {
                throw new IOException( "Failed to create output directory "
                                               + recordOutputDir.toString() );
            }

            byte[] serializedForm =  serializationHandler.serialize( r );
            File txtFile = new File( recordOutputDir, serializedRecFileName );
            LocalFileSystemHandler.writeFile( txtFile, serializedForm );
        }
    }

    /**
     * The main method for the external, "master" Curator client.
     * @param args String arguments from the command line. Should contain, in
     *             order, the host name for the (already-running) Curator, the
     *             port number for connecting to the Curator, and the job
     *             input directory.
     * @TODO: Give the option of using the HDFS-style directories
     */
    public static void main(String[] args) throws ServiceUnavailableException,
            TException, AnnotationFailedException, IOException {
        String host = args[0];
        int port  = Integer.parseInt( args[1] );
        File inputDir = new File( args[2] );
        CuratorClient theClient = new CuratorClient( host, port );

        // Parse input
	    if ( args.length != 3 )
		{
		    System.err.println( "Usage: CuratorClient curatorHost curatorPort inputDir" );
		    System.exit( -1 );
		}

        System.out.println( "You gave us " + inputDir.toString()
                            + " as the input directory." );


        // Create records from the input text files
        System.out.println( "Ready to create records from the text in " +
                            "the input directory." );

        theClient.createRecordsFromRawInputFiles(inputDir);

        System.out.println( "Turned "
                            + Integer.toString( theClient.getInputList().size() )
                            + " text files in the directory into records.");

        // Check available annotations
        theClient.printInfoOnKnownAnnotators();

        // Run a tool (for testing purposes)
        theClient.testPOSAndTokenizer();

        // Serialize output
        File outputDir = new File( inputDir, "output" );
        System.out.println( "Serializing those records, along with the new "
                            + "tokenization, to: " + outputDir.toString() );

        theClient.writeSerializedRecords( outputDir );


        // Begin old stuff. Let's not actually run this.
        /*try {
            callABunchOfAnnotationsFromDemo(transport);
        } catch (Exception e) {
            e.printStackTrace();
        }*/
    }

    /**
     * Makes the Thrift calls necessary to run the known records through both
     * the Tokenizer and the POS tagger. Writes debugging information to the
     * standard output.
     */
    private void testPOSAndTokenizer()
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

            client.performAnnotation(r, "pos", true);
            transport.close();

            // Confirm it worked
            if( !RecordTools.hasAnnotation( r, AnnotationMode.POS ) ) {
                System.out.println( "Couldn't find " + AnnotationMode.POS.toCuratorString() + " annotation!" );
            }

            int numViews = RecordTools.getNumViews( r );
            System.out.println("Record now has " + numViews + " views.");

            System.out.println("Testing serialization.");

            Record reconstructed = new Record();
            reconstructed = serializationHandler
                    .deserialize( serializationHandler.serialize( r ) );
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
                System.out.println("\tSerialization worked!!.");
            }

            replaceTheRecords.add(r);
        }
        newInputRecords = replaceTheRecords;
    }

    /**
     * Lists the available annotators to the standard output.
     * @throws TException
     */
    private void printInfoOnKnownAnnotators() throws TException {
        transport.open();
        Map<String, String> avail = client.describeAnnotations();
        System.out.println("Available annotations:");
        for (String key : avail.keySet()) {
            System.out.println("\t" + key + " provided by " + avail.get(key) );
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
            System.out.println( RecordTools.getRecordContents( record ) );
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
        System.out.println( RecordTools.getRecordContents( record ) );
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
        System.out.println( RecordTools.getRecordContents( record ) );
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
        System.out.println( RecordTools.getRecordContents( record ) );
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
