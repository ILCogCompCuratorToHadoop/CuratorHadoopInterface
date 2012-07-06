package edu.illinois.cs.cogcomp.hadoopinterface;

import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.AnnotationMode;
import edu.illinois.cs.cogcomp.hadoopinterface.infrastructure.ViewType;
import edu.illinois.cs.cogcomp.archive.Identifier;
import edu.illinois.cs.cogcomp.thrift.base.*;
import edu.illinois.cs.cogcomp.thrift.curator.Curator;
import edu.illinois.cs.cogcomp.thrift.curator.Record;
import org.apache.commons.io.FileExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
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

    private static Curator.Client client;
    private static final String CHAR_ENCODING = "UTF-8";
    // The list of all the input records that we will write to disk (to later
    // be transferred to Hadoop by another program)
    private static ArrayList<Record> newInputRecords;

    /**
     * Returns a string version of a Thrift data structure. Works for Clustering,
     * Forest, and Labeling views, among many others.
     *
     * This method is the inverse of #getThrifStructure().
     * @param data A Thrift data structure object
     * @return A string version of the input data structure
     * @throws TException
     */
    private static String getSerializedThriftStructure( TBase data )
            throws TException {
        TSerializer serializer = new TSerializer();
        return serializer.toString( data, CHAR_ENCODING );
    }

    /**
     * Rebuilds (i.e., deserializes) a Thrift data structure from a serialized
     * (String) version of that data structure. Works for Strings created by
     * Clustering, Forest, and Labeling views, among others. Modifies the input
     * TBase object to become the object represented by the serialized data.
     *
     * This method is the inverse of #getSerializedThriftStructure().
     * @param inOutBaseObject The Thrift data structure object to read into. This
     *                        object will become a copy of the object that
     *                        serializedData represents (it will be written to).
     * @param serializedData A String representation of a serialized Thrift
     *                       data structure. Created using Thrift's common
     *                       implementation of the java.io.Serializable interface.
     */
    private static void getThriftStructureFromString( TBase inOutBaseObject,
                                                      String serializedData )
            throws TException {
        TDeserializer deserializer = new TDeserializer();
        deserializer.deserialize(inOutBaseObject, serializedData, CHAR_ENCODING);
    }

	/**
     * Converts a Record data structure object into strings of
     * the original raw text, the hash/identifier, and each existing annotation,
     * stored and returned in a Map.
     * @param record The record to be serialized
	 */
	public static Map<String, String> serializeRecord(Record record)
            throws TException {
        Map<String, String> serializedForm = new HashMap<String, String>();
        String raw = record.getRawText();
        serializedForm.put("original", raw);
        serializedForm.put("hash", record.getIdentifier());

        // Join all the annotation views into a single data structure (we'll
        // iterate over this momentarily)
        Map< String, TBase > allViews = new HashMap<String, TBase>();
        allViews.putAll( record.getLabelViews() );
        allViews.putAll( record.getClusterViews() );
        allViews.putAll( record.getParseViews() );
        allViews.putAll( record.getViews() );

        // For each view type, get the serialized form and add it to the
        // collection of serialized version of the record
        TBase view;
        String serializedLabeling;
        String hadoopFriendlyKey;
        for (String key : allViews.keySet()) {
            view = allViews.get( key );
            serializedLabeling = getSerializedThriftStructure( view );
            hadoopFriendlyKey = AnnotationMode.fromString(key).toString();
            serializedForm.put(hadoopFriendlyKey, serializedLabeling );

            // Note: this is unnecessary; in the unlikely event that two maps
            // contained the same key, that key was overwritten in the preceding
            // putAll() operations
            /*if (key.equals("coref") && !coref) {
                coref = true;
            }
            else {
                System.out.println("ERROR: multiple instances of COREF");
            }*/
        }

        return serializedForm;
	}

    /**
     * Converts a Map of strings (original text and annotations)
     * into a Curator Record object.
     *
     * @param map containing raw text and annotations, as generated by
     *            #serializeRecord()
     */
	public static Record deserializeRecord( Map<String, String> map ) throws TException {
        Map<String, Labeling> labels = new HashMap<String, Labeling>();
        Map<String, Clustering> cluster = new HashMap<String, Clustering>();
        Map<String, Forest> parse = new HashMap<String, Forest>();
        Map<String, View> views = new HashMap<String, View>();

        String raw = map.get("original");
        String hash = map.get("hash");

        // For each key, add its value to the appropriate view collection
        for( String key : map.keySet() ) {
            if(  !key.equals("original") && !key.equals("hash") ) {
                // Safe to assume that the key is an annotation mode
                AnnotationMode annotation = AnnotationMode.fromString( key );
                ViewType view = AnnotationMode.getViewType( annotation );

                // Add the deserialized form of this annotation to the
                // appropriate map
                if( view == ViewType.LABEL ) {
                    Labeling deserialized = new Labeling();
                    getThriftStructureFromString( deserialized, map.get(key) );
                    labels.put( annotation.toCuratorString(), deserialized );
                }
                else if( view == ViewType.CLUSTER ) {
                    Clustering deserialized = new Clustering();
                    getThriftStructureFromString( deserialized, map.get(key) );
                    cluster.put( annotation.toCuratorString(), deserialized );
                }
                else if( view == ViewType.PARSE ) {
                    Forest deserialized = new Forest();
                    getThriftStructureFromString( deserialized, map.get(key) );
                    parse.put(annotation.toCuratorString(), deserialized);
                }
                else { // Generic view
                    View deserialized = new View();
                    getThriftStructureFromString( deserialized, map.get(key) );
                    views.put( annotation.toCuratorString(), deserialized );
                }
            }
        }

        // TODO: Should "whitespaced" be true? (Not clear what this means.)
        return new Record( hash, raw, labels, cluster, parse, views, false);
	}

    /**
     * Takes a Path to documents in a mirror of the HDFS directory structure
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
    public static void addRecordsFromJobDirectory(Path jobDir, boolean checkdb)
            throws TException, FileNotFoundException, ServiceUnavailableException,
            AnnotationFailedException {
        String filepath = jobDir.toString();
        File dir = new File(filepath);
        // check that the path is valid
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("The job path " + filepath + " is not a directory.");
        }

        // LOOP: for each file in the job directory...
        for (File i : dir.listFiles()) {
            // get the hash ID string from the subdirectory name
            String id = i.getName();

            if (checkdb) {
                File originalFile = new File(filepath + Path.SEPARATOR + id + Path.SEPARATOR + "original.txt");
                if (!originalFile.isFile()) {
                    System.out.println("ERROR: Attempt to check database for nonexistent original file");
                }
                String originalText = readFileToString( originalFile );

                Record dbRecord = client.getRecord(originalText);
                if ( recordHasNoAnnotations(dbRecord) ) {
                    addToInputList(dbRecord); // trust the database
                }

                else {
                    // LOOP: for each file in a hash id directory...
                    for (File j : i.listFiles()) {
                        try {
                            String annotation = readFileToString(j);
                            String fileName = j.getName();
                            int index = fileName.length() - 4; // remove .txt from file name
                            String type = fileName.substring(0, index);

                            Map<String, Labeling> labels = new HashMap<String, Labeling>();
                            Map<String, Clustering> cluster = new HashMap<String, Clustering>();
                            Map<String, Forest> parse = new HashMap<String, Forest>();
                            Map<String, View> views = new HashMap<String, View>();

                            boolean valid = true;
                            String original = "ERROR: Original text not populated.";
                            if (type.equals("original")) {
                                original = annotation;
                            }
                            else if (type.equals("PARSE")) {
                                Forest anno = new Forest();
                                getThriftStructureFromString(anno, annotation);
                                parse.put("stanfordParse", anno); // same as stanfordDep
                            }
                            else if (type.equals("VERB_SRL")) {
                                Forest anno = new Forest();
                                getThriftStructureFromString(anno, annotation);
                                parse.put("srl", anno);
                            }
                            else if (type.equals("NOM_SRL")) {
                                Forest anno = new Forest();
                                getThriftStructureFromString(anno, annotation);
                                parse.put("nom", anno);
                            }
                            else if (type.equals("TOKEN")) {
                                Labeling anno = new Labeling();
                                getThriftStructureFromString(anno, annotation);
                                labels.put("token", anno);
                            }
                            else if (type.equals("NER")) {
                                Labeling anno = new Labeling();
                                getThriftStructureFromString(anno, annotation);
                                labels.put("ner", anno);
                            }
                            else if (type.equals("POS")) {
                                Labeling anno = new Labeling();
                                getThriftStructureFromString(anno, annotation);
                                labels.put("pos", anno);
                            }
                            else if (type.equals("CHUNK")) {
                                Labeling anno = new Labeling();
                                getThriftStructureFromString(anno, annotation);
                                labels.put("chunk", anno);
                            }
                            else if (type.equals("WIKI")) {
                                Labeling anno = new Labeling();
                                getThriftStructureFromString(anno, annotation);
                                labels.put("wikifier", anno);
                            }
                            else if (type.equals("COREF")) {
                                Clustering anno = new Clustering();
                                getThriftStructureFromString(anno, annotation);
                                cluster.put("coref", anno);
                            }
                            else {
                                System.out.println("ERROR: " + type + " is not a valid annotation type, skipped");
                                valid = false;
                            }

                            if (valid) {
                                Record newRecord = new Record( id, original,
                                        labels, cluster, parse, views, false);
                                addToInputList(newRecord);
                            }
                        }
                        catch (FileNotFoundException e) {
                            e.printStackTrace();
                        } // END try/catch
                    } // END j loop
                } // END checkdb else
            } // END checkdb if
        } // END i loop
    } // END function

    public static boolean recordHasNoAnnotations(Record dbRecord) {
        return ( dbRecord.getClusterViewsSize() != 0
                 || dbRecord.getLabelViewsSize() != 0
                 || dbRecord.getParseViewsSize() != 0
                 || dbRecord.getViewsSize() != 0 );
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
     * @param inputDirectory The directory to draw original text files from
     */
    public static void takeNewRawInputFilesFromDirectory( String inputDirectory ) {
        // Check that the input directory is valid
        File inputDir = new File( inputDirectory );
        if( !inputDir.isDirectory() ) {
            throw new IllegalArgumentException("The location " + inputDirectory
                    + " does not refer to a directory.");
        }

        // For each file in the directory . . .
        for( File f : inputDir.listFiles() ) {
            try {
                // Add it to the CuratorClient's queue of documents to serialize
                // in preparation for sending the input to Hadoop
                String fileContents = readFileToString( f );
                Record newRecord = generateNewRecord( fileContents );
                addToInputList( newRecord );
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * This should really be a constructor in Record. Whatever.
     *
     * Constructs a new Record object with no annotations at all to the original
     * document text.
     * @param originalText The document's raw text.
     * @return A record object for the document whose original text was passed in.
     */
    public static Record generateNewRecord( String originalText ) {
        String id = Identifier.getId(originalText, false);
        Map<String, Labeling> labels = new HashMap<String, Labeling>();
        Map<String, Clustering> cluster = new HashMap<String, Clustering>();
        Map<String, Forest> parse = new HashMap<String, Forest>();
        Map<String, View> views = new HashMap<String, View>();

        return new Record(id, originalText, labels, cluster, parse, views, false);
    }

    /**
     * Takes a Curator Record object and adds it to a list of newly
     * added records for future serialization.
     *
     * @param record A Curator Record object
     */
    public static void addToInputList(Record record) {
        newInputRecords.add(record);
    }

    /**
     * Gets the list of Records to be serialized. This is for testing purposes
     * only.
     * @return the input Records ready to be processed
     */
    public static List<Record> getInputList() {
        return newInputRecords;
    }

    /**
     * Serializes all Records in the list of input Records and writes them to
     * the output directory. This will later be copied to the Hadoop file system.
     * @param outputDir The location to which we should write the serialized
     *                  records
     */
    public static void writeSerializedInput( String outputDir )
            throws IOException, TException {
        for( Record r : newInputRecords ) {
            String outputDirForRecord = outputDir + File.separator
                    + r.getIdentifier();

            Map< String, String > serializedForm = serializeRecord( r );
            for( String key : serializedForm.keySet() ) {
                writeFile( outputDirForRecord + File.separator + key,
                           serializedForm.get( key ) );
            }
        }
    }

    /**
     * Reads a file object from the disk and returns a string version of that
     * file.
     * @param f The file to be read
     * @return A string version of the input file
     */
    private static String readFileToString( File f ) throws FileNotFoundException {
        // Read the file in to a String
        Scanner fileReader = new Scanner( f );
        StringBuilder fileContents = new StringBuilder();
        while( fileReader.hasNextLine() ) {
            fileContents.append(fileReader.nextLine());
        }

        fileReader.close();
        return fileContents.toString();
    }

    /**
     * Writes a file to the indicated path.
     * @param path The location to which the file should be written. Probably
     *             something like "/user/My_User/my_output_dir/a_text_file.txt".
     * @param text The text file to be written.
     * @throws FileExistsException If the file indicated by the path already
     *                             exists.
     */
    private static void writeFile( String path, String text )
            throws IOException, FileExistsException {
        File theFile = new File( path );
        if( theFile.exists() ) {
            throw new FileExistsException( "File at path " + path
                    + " already exists; cannot overwrite it.");
        }

        BufferedWriter writer = new BufferedWriter( new FileWriter( theFile ) );
        Scanner stringScanner = new Scanner( text );
        while( stringScanner.hasNextLine() ) {
            String line = stringScanner.nextLine();
            writer.write( line );
        }

        stringScanner.close();
        writer.close();
    }

    private static String recordContents(Record record) {
        StringBuilder result = new StringBuilder();
        result.append("Annotations present in the record:\n");
        result.append("- rawText: ");
        result.append(record.isSetRawText() ? "Yes" : "No");
        result.append("\nThe following Label Views: ");
        for (String key : record.getLabelViews().keySet()) {
            result.append(key);
            result.append(" ");
        }
        result.append("\n");
        result.append("The following Cluster Views: ");
        for (String key : record.getClusterViews().keySet()) {
            result.append(key);
            result.append(" ");
        }
        result.append("\n");
        result.append("The following Parse Views: ");
        for (String key : record.getParseViews().keySet()) {
            result.append(key);
            result.append(" ");
        }
        result.append("\n");
        result.append("The following general Views: ");
        for (String key : record.getViews().keySet()) {
            result.append(key);
            result.append(" ");
        }
        result.append("\n");
        return result.toString();
    }

    // START OF MAIN

    public static void main(String[] args) throws AnnotationFailedException, FileNotFoundException, ServiceUnavailableException, TException {

        newInputRecords = new ArrayList<Record>(); // initialize list

	    if ( args.length != 3 ) 
		{
		    System.err.println( "Usage: CuratorClient curatorHost curatorPort textFile" );
		    System.exit( -1 );
		}

	    String host = args[0];
	    int port  = Integer.parseInt( args[1] );
	    String fileName = args[2];

	    StringBuilder textBldr = new StringBuilder();
	    String NL = System.getProperty("line.separator");
    
	    
	    Scanner scanner = new Scanner(new FileInputStream(fileName) );
	    try {
            while (scanner.hasNextLine()){
                textBldr.append(scanner.nextLine());
                textBldr.append(NL);
            }
	    }
	    finally{
		    scanner.close();
	    }

	    String text = textBldr.toString();

	    System.err.println( "## read in text: " + text );

		//First we need a transport
		TTransport transport = new TSocket(host, port );
		//we are going to use a non-blocking server so need framed transport
		transport = new TFramedTransport(transport);
		//Now define a protocol which will use the transport
		TProtocol protocol = new TBinaryProtocol(transport);
		//make the client
		client = new Curator.Client(protocol);

        System.out.println("We are going to be calling the Curator with the following text:\n");
        System.out.println(text);

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
            System.out.println("\t"+key + " provided by " + avail.get(key));
        }

        // TODO print out map keys
        // (Probably) returns a record will all known annotations for the string
        // "This text string is so meta"
        Record record = client.getRecord( "This text string is so meta" );
        Map<String, Forest> map = record.getParseViews();
        for (String key : map.keySet()) {
            System.out.println( "Key " + key + " is associated with value " + map.get(key).toString() );
        }

        System.out.println();

        System.out.println("First we'll get the named entities in the text.");
        System.out.print("Calling curator.provide(\"ner\", text, false)... ");

        boolean forceUpdate = true;
        //         try {
        //             transport.open();
        //             //call Curator
        //             record = client.provide("ner", text, forceUpdate);
        //             transport.close();
        //         } catch (ServiceUnavailableException e) {
        //         	if (transport.isOpen())
        //         		transport.close();
        //             System.out.println("ner annotations are not available");
        //             System.out.println(e.getReason());

        //         } catch (TException e) {
        //         	if (transport.isOpen())
        //         		transport.close();
        //             e.printStackTrace();
        //         }
        // 		System.out.println("done.\n");
        // 		System.out.println();
        //         if (avail.containsKey("ner")) {
        //         	System.out.println(recordContents(record));
        //         	System.out.println();

        //             System.out.println("Named Entities\n---------\n");
        //             for (Span span : record.getLabelViews().get("ner").getLabels()) {
        //                 System.out.println(span.getLabel() + " : "
        //                 + record.getRawText().substring(span.getStart(), span.getEnding()));
        //             }
        //             System.out.println();
        //             System.out.println();
        //             System.out.println("The raw data structure containing the NEs looks like this:");
        //             System.out.println(record.getLabelViews().get("ner"));
        //         }
        //         System.out.println();


        System.out.println("Next we'll call the extended NER (more entity types)...");
        System.out.print("Calling curator.provide(\"ner-ext\", text, false)... ");
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
            System.out.println(recordContents(record));
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
        System.out.println(recordContents(record));
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
        System.out.println(recordContents(record));
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

        // 	      System.out.print("Calling curator.provide(\"charniak_k_best\", text, forceUpdate = '" + ( forceUpdate ? "TRUE" : "FALSE" ) + "')... ");
        // 	        try {
        // 	            transport.open();
        // 	            //call Curator
        // 	            record = client.provide("charniak_k_best", text, forceUpdate);
        // 	            transport.close();
        // 	        } catch (ServiceUnavailableException e) {
        // 	            e.printStackTrace();
        // 	        } catch (TException e) {
        // 	            e.printStackTrace();
        // 	        }

        // 	        System.out.println();



        // 		System.out.println();


        /*
          System.out.println("Charniak k-best parses:\n------------------\n\n");

      for (Tree depTree : record.getParseViews().get("charniak_k_best").getTrees()) {
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
              System.out.println("Parse tree");
              System.out.println(result.toString());
          }

          System.out.println();

          */


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
		System.out.println(recordContents(record));
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
