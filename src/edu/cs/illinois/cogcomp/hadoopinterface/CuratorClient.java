package edu.illinois.cs.cogcomp.hadoopinterface;

import java.util.Map;
import java.util.Stack;
import java.util.Map.Entry;
import java.util.Scanner;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import edu.illinois.cs.cogcomp.thrift.base.AnnotationFailedException;
import edu.illinois.cs.cogcomp.thrift.base.Node;
import edu.illinois.cs.cogcomp.thrift.base.ServiceUnavailableException;
import edu.illinois.cs.cogcomp.thrift.base.Span;
import edu.illinois.cs.cogcomp.thrift.base.Tree;
import edu.illinois.cs.cogcomp.thrift.curator.Curator;
import edu.illinois.cs.cogcomp.thrift.curator.Record;

import edu.illinois.cs.cogcomp.thrift.base.*;
import edu.cs.illinois.cogcomp.hadoopinterface.infrastructure.FileSystemHandler;


public class CuratorClient {
	
    private ArrayList<Record> newInputRecords;

	private static String recordContents(Record record) {
		StringBuffer result = new StringBuffer();
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

	/**
     * Converts a Record data structure object into strings of
     * the original raw text and each existing annotation,
     * stored and returned in a Map.
	 */
	public static Map<String, String> serializeRecord(Record record) {
        Map<String, String> map = new HashMap<String, String>();
        String raw = record.getRawText();
        map.put("original", raw);

        Map<String, Labeling> labels = record.getLabelViews();
        Map<String, Clustering> cluster = record.getClusterViews();
        Map<String, Forest> parse = record.getParseViews();
        Map<String, View> views = record.getViews();

        String value = "";
        boolean coref = false;

        // if these logic statements are erroring, try checking for all known keys in each map        
        for (String key : labels.keys()) {
            if (key.equals("pos")) {
                value = labels.get(key); // TODO fix types, class object to String
                map.put("POS", value);
            }
            else if (key.equals("chunk")) {
                value = labels.get(key);
                map.put("CHUNK", value);
            }
            else if (key.equals("ner-ext")) {
                value = labels.get(key);
                map.put("NER", value);
            }
            else if (key.equals("tokens")) {
                vlaue = labels.get(key);
                map.put("TOKEN", value);
            }
            else if (key.equals("wikifier")) {
                value = labels.get(key);
                map.put("WIKI", value);
            }
            else if (key.equals("coref")) {
                if (coref == false) {
                    coref = true;
                    value = labels.get(key);
                    map.put("COREF", value);
                }
                else {
                    System.out.println("ERROR: multiple instances of COREF");
                }
            }
        }

        for (String key : cluster.keys()) {
            if (key.equals("stanfordDep") || key.equals("stanfordParse")) {
                value = cluster.get(key);
                map.put("PARSE", value);
            }
            else if (key.equals("srl")) {
                value = cluster.get(key);
                map.put("VERB_SRL", value);
            }
            else if (key.equals("nom")) {
                value = cluster.get(key);
                map.put("NOM_SRL", value);
            }
            else if (key.equals("coref")) {
                if (coref == false) {
                    coref = true;
                    value = labels.get(key);
                    map.put("COREF", value);
                }
                else {
                    System.out.println("ERROR: multiple instances of COREF");
                }
            }
        }

        for (String key : parse.keys()) { 
            if (key.equals("coref")) {
                if (coref == false) {
                    coref = true;
                    value = labels.get(key);
                    map.put("COREF", value);
                }
                else {
                    System.out.println("ERROR: multiple instances of COREF");
                }
            }
        }

        for (String key : views.keys()) { 
            if (key.equals("coref")) {
                if (coref == false) {
                    coref = true;
                    value = labels.get(key);
                    map.put("COREF", value);
                }
                else {
                    System.out.println("ERROR: multiple instances of COREF");
                }
            }
        }

        return map;
        
	}

    /**
     * Converts a Map of strings (original text and annotations)
     * into a Curator Record object.
     *  
     * @param map containing raw text and annotations
     * @param id String identifier for the Record
     *
     */
	public static Record deserializeRecord(Map<String, String> map, String id) {
        Map<String, Labeling> labels = new HashMap<String, Labeling>();
        Map<String, Clustering> cluster = new HashMap<String, Clustering>();
        Map<String, Forest> parse = new HashMap<String, Forest>();
        Map<String, View> views = new HashMap<String, Views>();
        
        String raw = "";
        String value = "";

        for(String key : map.keys()) {
            if (key.equals("original") {
                raw = map.get(key);
            }
            
            // Forest map
            if (key.equals("PARSE")) {
                value = map.get(key);
                parse.put("stanfordParse", value); // arbitrary choice over stanfordDep
            }
            else if (key.equals("VERB_SRL")) {
                value = map.get(key);
                parse.put("srl", value);
            }
            else if (key.equals("NOM_SRL")) {
                value = map.get(key);
                parse.put("nom", value);
            }

            // Labeling map
            else if (key.equals("TOKEN")) {
                value = map.get(key);
                labels.put("tokens", value);
            }
            else if (key.equals("NER")) {
                value = map.get(key);
                labels.put("ner-ext", value);
            }
            else if (key.equals("POS")) {
                value = map.get(key);
                labels.put("pos", value);
            }
            else if (key.equals("CHUNK")) {
                value = map.get(key);
                labels.put("chunk", value);
            }
            else if (key.equals("WIKI")) {
                value = map.get(key);
                labels.put("wikifier", value);
            }

            // Clustering map is currently EMPTY
            
            // (general) View map - temporary location for coref
            else if (key.equals("COREF")) {
                value = map.get(key);
                views.put("COREF", value);
            }

            // throw an error
            else {
                System.out.println("ERROR: unrecognized key of " + key + " corresponding to value of " + map.get(key));
            }                
        }

        Record record = new Record(id, raw, labels, cluster, parse, views, false);
        return record;
	}

    /**
     * Takes a Path to documents in a mirror of the HDFS directory structure
     * and creates new Curator Records. Calls addRecordToList to add each new record to a class variable.
     * Checks for an existing record in the database, if requested.
     *
     * @param jobDir Path pointing to, e.g. /user/home/job123 directory
     * @param checkdb If true, checks for an existing duplicate Record in the Curator database
     */
    public void addRecordsFromJobDirectory(Path jobDir, boolean checkdb=true) {
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
                //TODO check database
            }
            
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
                        Forest anno = new Forest(annotation); //TODO how to convert string to Forest obj?
                        parse.put("stanfordParse", anno); // same as stanfordDep
                    }
                    else if (type.equals("VERB_SRL")) {
                        Forest anno = new Forest(annotation); //TODO
                        parse.put("srl", anno);
                    }
                    else if (type.equals("NOM_SRL")) {
                        Forest anno = new Forest(annotation); //TODO
                        parse.put("nom", anno);
                    }
                    else if (type.equals("TOKEN")) {
                        Labeling anno = new Labeling(annotation); //TODO convert String to Labeling obj
                        labels.put("token", anno);
                    }
                    else if (type.equals("NER")) {
                        Labeling anno = new Labeling(annotation); //TODO
                        labels.put("ner", anno);
                    }
                    else if (type.equals("POS")) {
                        Labeling anno = new Labeling(annotation); //TODO
                        labels.put("pos", anno);
                    }
                    else if (type.equals("CHUNK")) {
                        Labeling anno = new Labeling(annotation); //TODO
                        labels.put("chunk", anno);
                    }
                    else if (type.equals("WIKI")) {
                        Labeling anno = new Labeling(annotation);
                        labels.put("wikifier", anno);
                    }
                    else if (type.equals("COREF")) {
                        Labeling temp = new Labeling();
                        temp.fromString(annotation); //TODO convert String annotation to Labeling obj
                        List<Labeling> clusters = new List<Labeling>();
                        clusters.add(temp);
                        Clustering anno = new Clustering();
                        anno.setClusters(clusters);
                        anno.setSource(id);
                        cluster.put("coref", anno);
                    }
                    else {
                        System.out.println("ERROR: " + type + " is not a valid annotation type, skipped");
                        valid = false;
                    }

                    if (valid) {                    
                        newRecord = new Record(id, original, labels, cluster, parse, views, false);
                        addToInputList(newRecord);
                    }
                }
                catch (FileNotFoundException e) {
                    e.printStackTrace();
                } 
            }
        }
    }

    public void takeNewRawInputFilesFromDirectory( String inputDirectory ) {
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
    public Record generateNewRecord( String originalText ) {
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
    public void addToInputList(Record record) {
        newInputRecords.add(record);
    }

    /**
     * Serializes all Records in the list of input Records and writes them to
     * the output directory. This will later be copied to the Hadoop file system.
     * @param outputDir The location to which we should write the serialized
     *                  records
     */
    public void writeSerializedInput( String outputDir ) throws IOException {
        // TODO: Replace with the real variable
        Iterable< Record > allInput = new LinkedList< Record >();

        for( Record r : allInput ) {
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
    private String readFileToString( File f ) throws FileNotFoundException {
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
    private void writeFile( String path, String text )
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

    // START OF MAIN

    public static void main(String[] args) throws AnnotationFailedException, FileNotFoundException {

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
		    textBldr.append(scanner.nextLine() + NL);
		}
	    }
	    finally{
		scanner.close();
	    }


// 		String text = "With less than 11 weeks to go to the final round of climate talks in "
// 				+ "Copenhagen, the UN chief, Ban Ki-Moon did not bother to hide his frustration "
// 				+ "in his opening remarks. \"The world's glaciers are now melting faster than "
// 				+ "human progress to protect them -- or us,\" he said. Others shared his gloom. "
// 				+ "\"Today we are on a path to failure,\" said France's Nicolas Sarkozy.";


	    String text = textBldr.toString();

	    System.err.println( "## read in text: " + text );

		//First we need a transport
		TTransport transport = new TSocket(host, port );
		//we are going to use a non-blocking server so need framed transport
		transport = new TFramedTransport(transport);
		//Now define a protocol which will use the transport
		TProtocol protocol = new TBinaryProtocol(transport);
		//make the client
		Curator.Client client = new Curator.Client(protocol);

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
        Map map = record.getParseViews();
        for (String key : map.keys()) {
            System.out.println( "Key " + key " is associated with value " map.get(key).toString() );
        }

        System.out.println();

        System.out.println("First we'll get the named entities in the text.");
        System.out.print("Calling curator.provide(\"ner\", text, false)... ");
        Record record = null;

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
