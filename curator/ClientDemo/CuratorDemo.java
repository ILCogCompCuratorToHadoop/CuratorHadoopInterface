// You can download [CuratorDemo.java][java].
// [java]: CuratorDemo.java
//
// The Curator is a service which provides the ability to retrieve and perform
// annotations on a text.
//
// Visit the [status page][status] to see if the Curator is currently running
// and what annotations are available.  You can also query the Curator using the
// `describeAnnotations()` method.
// 
// The [Curator web demo][demo] acts as a reference Curator client.  You can use
// the demo to explore the annotations and experiment with the Curator. The web
// demo is also useful for testing text that you are having problems with in
// your own code, verifying that the same problem occurs on the web demo can
// help us find bugs quicker.
//
// Before you start you will need the following jars in your classpath:
//
// * `libthrift.jar`
// * `curator-interfaces.jar`
// * sl4j api
// * sl4j framework wrapper, e.g. sl4j-simple
// * commons-lang 2.5
//
// [status]: demo/status.php
// [demo]: demo/
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
  
import edu.illinois.cs.cogcomp.thrift.curator.Curator;
import edu.illinois.cs.cogcomp.thrift.curator.Record;
import edu.illinois.cs.cogcomp.thrift.base.ServiceUnavailableException;
import edu.illinois.cs.cogcomp.thrift.base.AnnotationFailedException;
import edu.illinois.cs.cogcomp.thrift.base.Span;
import edu.illinois.cs.cogcomp.thrift.base.Labeling;
import edu.illinois.cs.cogcomp.thrift.base.Clustering;
import edu.illinois.cs.cogcomp.thrift.base.Forest;
import edu.illinois.cs.cogcomp.thrift.base.Tree;
import edu.illinois.cs.cogcomp.thrift.base.Node;

// **Client Code Walkthrough**  
// This example will walk you through creating a client to the Curator and
// performing various annotations.
public class CuratorDemo {
    public static void main(String[] args) {
        // Lets first define where the Curator is running.
        String hostname = "myhost.cs.uic.edu";
        int port = 9090;
        
        //This part is boiler plate code for Thrift which you will need to
        //create a client.  We will first get a transport and then wrap it in a
        //framed transport because we are using a non-blocking server. Finally
        //define a protocol that uses the transport.
        TTransport transport = new TSocket(hostname, port);
        transport = new TFramedTransport(transport);
        TProtocol protocol = new TBinaryProtocol(transport);

        //Create the client from the protocol. Every Thrift service has an inner Client class.
        Curator.Client client = new Curator.Client(protocol);
       
        //Here is the text will will use for this demo. Notice how it is untokenized.
        String text = "With less than 11 weeks to go to the final round of climate "
            + "talks in Copenhagen, the UN chief, Ban Ki-Moon did not bother to hide "
            + "his frustration in his opening remarks. \"The world's glaciers are "
            + "now melting faster than human progress to protect them -- or us,\" he "
            + "said. Others shared his gloom. \"Today we are on a path to failure,"
            + "\" said France's Nicolas Sarkozy.";

        //Now we are going to make the call to the Curator.  We must first open
        //the transport and then make the call to the Curator.
        Record record = null;
        try {
            transport.open();
            //The Curator has multiple methods you can view all the methods on
            //the [Curator API page][capi]. We will focus on the `provide`
            //method which takes three arguments:
            //
            // 1. `view_name` represented as a String. (e.g., `pos`, `ner`,
            // `srl` etc). For a list of the available view names call
            // `describeAnnotations()` or check the [status page][status].
            // 2. `text` the text you want to process.  This can contain
            // multiple sentences and new lines.
            // 3. `boolean` representing whether the annotation should be
            // reprocessed regardless of the cache.
            //
            //[capi]: interfaces/curator.html#Svc_Curator
            //[status]: demo/status.php
            record = client.provide("ner", text, false);
            // Don't forget to close the transport once you have finished using
            //the Curator.
            transport.close();
        } catch (ServiceUnavailableException e) {
        	if (transport.isOpen())
        		transport.close();
            e.printStackTrace();
        } catch (AnnotationFailedException e) {
        	if (transport.isOpen())
        		transport.close();
            e.printStackTrace();
        } catch (TException e) {
        	if (transport.isOpen())
        		transport.close();
            e.printStackTrace();
        }

        //The `record` object now contains the annotations request and
        //cached. [Record][r]s contain the raw text that was processed, an
        //identifier and multiple maps which hold the annotations.  Each
        //annotation is access by a specific key.
        //
        // * [Labeling][l] annotations are stored in the `labelViews` field,
        //accessed with `record.getLabelViews()` which returns a `Map<String,
        //Labeling>` where the key is the name of the annotation. (e.g., `pos`,
        //`ner`, `quantities`).
        //
        // * [Clustering][c] annotations are stored in
        //`clusterViews`. `record.getClusterViews()` returns a `Map<String,
        //Clustering>`.
        //
        // * [Forest][f] annotations are stored in
        // `parseViews`. `record.getParseViews()` returns a `Map<String,
        // Forest>`.
        //
        //The semantics of each data structure depends on the producing
        //annotator. The [README files][readme] files for each server should
        //contain a description of the semantics.
        //
        // [l]: interfaces/base.html#Struct_Labeling
        // [c]: interfaces/base.html#Struct_Clustering
        // [f]: interfaces/base.html#Struct_Forest
        // [r]: interfaces/curator.html#Struct_Record
        // [readme]: servers/
        //
        // 
        String rawText = record.getRawText();
        String identifier = record.getIdentifier();
        Map<String, Labeling> labelViews = record.getLabelViews();
        Map<String, Clustering> clusterViews = record.getClusterViews();
        Map<String, Forest> parseViews = record.getParseViews();
            
        // We can now iterate over the named entity annotations we requested
        // printing out the detected entities and their label. [Span][span]s
        // contain multiple fields but the most important fields are the `start`
        // and `ending` fields which refer to the position the span covers in
        // the record's `rawText` field. See the [Span definition][span] for more
        // information.
        //
        // The output of this loop should be:
        // > LOC : Copenhagen  
        // > ORG : UN  
        // > PER : Ban Ki-Moon  
        // > LOC : France  
        // > PER : Nicolas Sarkozy
        // [span]: interfaces/base.html#Struct_Span
        for (Span span : labelViews.get("ner").getLabels()) {
            System.out.println(span.getLabel() + " : "
                    + text.substring(span.getStart(), span.getEnding()));
        }

        //**Using pretokenized text**  
        // Often corpora come pre-tokenized. Running a tokenizer over
        // pre-tokenized text can lead to really bad tokenization. The Curator
        // has additional methods that are designed for pretokenized text.
        // These methods are prefixed with `ws` to signify whitespace
        // methods. When a `ws` method is called the Curator will run a
        // whitespace tokenizer over the text to obtain the tokens.  
        
        // `wsprovde` works liked `provide` except rather than taking a String
        // representing the text it takes a `List<String>` where each String is
        // a whitespace tokenized sentence.
        
        List<String> sentences = new ArrayList<String>();
        sentences.add("BHP Billiton , based in Australia , is offering $ 130 for each "
                      + "share of Potash , a 16 percent premium to its closing price "
                      + "Monday .");
        sentences.add("But Potash 's shares , which trade mainly on the New York Stock "
                      + "Exchange , surged 28 percent to $ 143.17 .");
        
        // Now we have the data structure to pass to the Curator we will open a
        // transport and the make the call like before.  This time we will
        // request the chunker (shallow parse) annotation.
        try {
            transport.open();
            record = client.wsprovide("chunk", sentences, false);
            transport.close();
        } catch (ServiceUnavailableException e) {
        	if (transport.isOpen())
        		transport.close();
            e.printStackTrace();
        } catch (AnnotationFailedException e) {
        	if (transport.isOpen())
        		transport.close();
            e.printStackTrace();
        } catch (TException e) {
        	if (transport.isOpen())
        		transport.close();
            e.printStackTrace();
        }

        //Note that the record returned has exactly the same structure as
        //before, however the field `whitespaced` will now be `true`.
        //Iterating over the annotations should produce:
        //
        // > NP : BHP Billiton   
        // > VP : based   
        // > PP : in  
        // > NP : Australia  
        // > VP : is offering   
        // > NP : $ 130   
        // > PP : for   
        // > NP : each share   
        // > PP : of   
        // > NP : Potash   
        // > NP : a 16 percent premium   
        // > PP : to   
        // > NP : its closing price   
        // > NP : Monday   
        // > NP : Potash   
        // > NP : 's shares   
        // > NP : which   
        // > VP : trade   
        // > AD :VP mainly   
        // > PP : on   
        // > NP : the New York Stock Exchange   
        // > VP : surged   
        // > NP : 28 percent   
        // > PP : to   
        // > NP : $ 143.17  
        boolean whitespaced = record.isWhitespaced();

        for (Span span : record.getLabelViews().get("chunk").getLabels()) {
            System.out.println(span.getLabel() + " : "
                    + text.substring(span.getStart(), span.getEnding()));
        }

    }
}