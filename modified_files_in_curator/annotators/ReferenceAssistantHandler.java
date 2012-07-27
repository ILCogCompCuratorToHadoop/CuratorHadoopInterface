package edu.illinois.cs.cogcomp.annotation.handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

import net.didion.jwnl.data.POS;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import org.apache.commons.cli.ParseException;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.illinois.cs.cogcomp.edison.sentences.Constituent;
import edu.illinois.cs.cogcomp.edison.sentences.TextAnnotation;
import edu.illinois.cs.cogcomp.edison.sentences.ViewNames;
import edu.illinois.cs.cogcomp.edison.data.curator.CuratorClient;
import edu.illinois.cs.cogcomp.edison.data.curator.CuratorDataStructureInterface;
import edu.illinois.cs.cogcomp.thrift.base.AnnotationFailedException;
import edu.illinois.cs.cogcomp.thrift.base.Labeling;
import edu.illinois.cs.cogcomp.thrift.labeler.Labeler;
import edu.illinois.cs.cogcomp.thrift.base.Span;
import edu.illinois.cs.cogcomp.thrift.curator.Record;
import CommonSenseWikifier.Caching.CachingCurator;
import CommonSenseWikifier.ProblemRepresentationDatastructures.DisambiguationProblem;
import CommonSenseWikifier.ProblemRepresentationDatastructures.ParametersAndGlobalVariables;
import CommonSenseWikifier.ProblemRepresentationDatastructures.ReferenceInstance;
import CommonSenseWikifier.ProblemRepresentationDatastructures.WikifiableEntity;
import CommonSenseWikifier.TrainingAndInference.InferenceEngine;
import IO.InFile;

public class ReferenceAssistantHandler implements Labeler.Iface {

    private static final String WIKI_KEY = "wikifier";

    private static Logger logger = LoggerFactory.getLogger(ReferenceAssistantHandler.class);

    private InferenceEngine inference =null;
    private HashMap<String,Vector<String>> titleToCategory = null; // what the the category ids assigned to this title? Remember that titleId is a string!
    private HashMap<String, Boolean> categories = null; // what the the category ids assigned to this title? Remember that titleId is a string!
    private int curatorPort;
    private String curatorHost;
    private boolean isCuratorComponent = false;

    private long lastAnnotationTime;
    
    public ReferenceAssistantHandler(String configFile, String curatorServerMachine, int curatorServerPort) throws Exception{
        // Set the starting time for our activity monitor
        lastAnnotationTime = System.currentTimeMillis();

        ParametersAndGlobalVariables.loadConfig(configFile);

        curatorHost = curatorServerMachine;
        curatorPort = curatorServerPort;
        initCategoryAttributesData(ParametersAndGlobalVariables.pathToTitleCategoryKeywordsInfo);
        inference=new InferenceEngine(false);
    }

    public ReferenceAssistantHandler( String configFile ) throws Exception
    {
        // Set the starting time for our activity monitor
        lastAnnotationTime = System.currentTimeMillis();

	System.err.println( "## ReferenceAssistantHandler( configfile ) -- constructor for wikifier as curator component..." );
        ParametersAndGlobalVariables.loadConfig(configFile);
        initCategoryAttributesData(ParametersAndGlobalVariables.pathToTitleCategoryKeywordsInfo);
        inference=new InferenceEngine(false);
        this.isCuratorComponent = true;
    }

    /**
     * @return The time of the last annotation performed (may be either the
     *         beginning or end of the last annotation operation)
     */
    public long getTimeOfLastAnnotation() {
        return lastAnnotationTime;
    }
    
    public void initCategoryAttributesData(String titlesToAttributesFile) {
        titleToCategory = new HashMap<String, Vector<String>>();
        categories = new HashMap<String, Boolean>();
        InFile in = new InFile(titlesToAttributesFile);
        String line =in.readLine();
        while(line!=null) {
            StringTokenizer st = new StringTokenizer(line, "\t");
            String tid = st.nextToken(); // not used!
            String titleName = st.nextToken();
            Vector<String> cats = new Vector<String>();
            while(st.hasMoreTokens()) {
                String cat = st.nextToken();
                st.nextToken(); // skipping the weight...
                if(!categories.containsKey(cat)) 
                    categories.put(cat, true);
                cats.addElement(cat);
            }
            titleToCategory.put(titleName, cats);
            line =in.readLine();
        }
        in.close();
    }

    /**
     * for use ONLY when Wikifier is a Curator Component; 
     *    sidesteps ALL CACHING; assumes Curator will NOT be
     *    called to annotate text as all annotations should be
     *    in the input record
     *    
     * @param record
     * @return
     * @throws AnnotationFailedException
     * @throws TException
     */
    public String annotateRecord(Record record)
    throws AnnotationFailedException, TException 
    {
        // Update the time of the last annotation for purposes of monitoring
        // inactivity
        lastAnnotationTime = System.currentTimeMillis();

	System.err.println( "## ReferenceAssistantHandler.annotateRecord()..." );
    String taggedText = "";
        try{

            TextAnnotation ta = CuratorDataStructureInterface
                .getTextAnnotationViewsFromRecord("", "", record);

            if ( !checkViews( ta ) )
                throw new Exception( "Required views not found." );

            ParametersAndGlobalVariables.curator.curatorComponentTa = ta;
          
            record.putToLabelViews( WIKI_KEY, this.tagText( ta.getText() ) );
            
            logger.info(  "Wikifier: annotated text... returning to Curator..." );
//            DisambiguationProblem prob= new DisambiguationProblem("serverinput",ta.getText(),new Vector<ReferenceInstance>());
//            inference.annotate(prob, null, false, false, 0);
//            taggedText = prob.wikificationString(false);

        }catch (Exception e){
            throw new AnnotationFailedException("Failed to annotate the text :\n" +record.rawText+"\nThe exception was: \n"+e.toString());
        }

        // Update the time of the last annotation for purposes of monitoring
        // inactivity
        lastAnnotationTime = System.currentTimeMillis();
        return taggedText;
    }

    public String annotateText(String input) throws AnnotationFailedException,
    TException {
        // Update the time of the last annotation for purposes of monitoring
        // inactivity
        lastAnnotationTime = System.currentTimeMillis();

	System.err.println( "##ReferenceAssistantHandler.annotateText()..." );

        System.out.println("Input text: "+ input);
        String output = "";
        TextAnnotation ta = getTextAnnotation( input, isCuratorComponent ) ;
            
        DisambiguationProblem prob;
        try
        {
            prob = new DisambiguationProblem("serverinput",ta.getText(),new Vector<ReferenceInstance>());
            inference.annotate(prob,  null, false, false, 0);
            output = prob.wikificationString(false);
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw new AnnotationFailedException("Failed to annotate the text :\n" +input+"\nThe exception was: \n"+e.toString());
        }

        // Update the time of the last annotation for purposes of monitoring
        // inactivity
        lastAnnotationTime = System.currentTimeMillis();
        return output;
    }

    private TextAnnotation getTextAnnotation( String input, boolean isCuratorComponent ) throws AnnotationFailedException
    {
	System.err.println( "##ReferenceAssistantHandler.getTextAnnotation()..." );
        String corpId = "wikifier_input";
        String spanId = "wikifier_span";
        TextAnnotation ta = new TextAnnotation( corpId, spanId, input );
        boolean forceUpdate = false;

        if ( !isCuratorComponent )
            try{
                CuratorClient client = new CuratorClient( curatorHost, curatorPort );
            
                client.addChunkView( ta, forceUpdate );
                client.addNamedEntityView( ta, forceUpdate );
                client.addPOSView( ta, forceUpdate );
            }catch (Exception e){
                throw new AnnotationFailedException("ReferenceAssistantHandler.getTextAnnotation(): " + 
						    "Failed to annotate the text :\n" +input+"\nThe exception was: \n"+e.toString());
            }
            
	else
	    {
		throw new AnnotationFailedException( "ReferenceAssistantHandler.getTextAnnotation(): " + 
						     " this shouldn't be called when RAH is a Curator component!!!" );
	    }

        return ta;
    }


    /**
     * assumes that TextAnnotation has already been created one way or another. 
     */

    public Labeling tagText(String input) throws AnnotationFailedException,
    TException {
        // Update the time of the last annotation for purposes of monitoring
        // inactivity
        lastAnnotationTime = System.currentTimeMillis();

        String text=input;
        System.out.println("RAH.tagText()...\n----------Input text: "+ text);

        try{
            Labeling labeling = new Labeling();
            List<Span> labels = new ArrayList<Span>();
            HashMap<String, Span> knownSpans = new HashMap<String, Span>();

	    System.err.println( "## RAH.tagText(): instantiating DisambiguationProblem... ");
            DisambiguationProblem prob= new DisambiguationProblem("serverinput",text,new Vector<ReferenceInstance>());
	    System.err.println( "## RAH.tagText(): calling inference engine... ");
            inference.annotate(prob,   null, false, false, 0);
	    System.err.println( "## RAH.tagText(): done with calling inference engine... ");
            
            
            /*
             * This takes care of the Wikifier output. However, notice that I'll be assigning some Wikipedia categories to non-Wikified expressions as well...
             */
            for(int i=0;i<prob.components.size();i++){
                WikifiableEntity e=prob.components.elementAt(i);
                String disambiguation=null;
                String title=e.topDisambiguation.normalizedTitleName;
                disambiguation="http://en.wikipedia.org/wiki/"+title.replace(' ', '_');
                Span entity = new Span(e.startOffsetCharsInText,e.startOffsetCharsInText+e.entityLengthChars);
                knownSpans.put(entity.getStart()+"-"+entity.getEnding(), entity);
                HashMap<String, String> attributes = new HashMap<String, String>();
                attributes.put("IsLinked", String.valueOf(e.finalSolutionDisambiguation!=null));
                attributes.put("LinkerScore", String.valueOf(e.linkerScore));
                attributes.put("RankerScore", String.valueOf(e.topDisambiguation.rankerScore));
                String titleWikiCats = "";
                if(titleToCategory.containsKey(title.replace(' ', '_'))) {
                    Vector<String> v = titleToCategory.get(title.replace(' ', '_'));
                    for(int j=0;j<v.size();j++)
                        titleWikiCats+="\t"+v.elementAt(j);                 
                } else {
                    attributes.put("TitleMismatchError", "Title:"+title.replace(' ', '_')+"does not appear in the titles to categories index. The error is probably due to mismatch in Wikipedia versions");
                }
                attributes.put("TitleWikiCatAttribs", titleWikiCats);
                Vector<String> tokens = InFile.aggressiveTokenize(text.substring(e.startOffsetCharsInText,e.startOffsetCharsInText+e.entityLengthChars).toLowerCase());
                String surfaceFormsAttribs = "";
                for (int j=0;j<tokens.size();j++) { 
                    String lemma = ParametersAndGlobalVariables.wordnet.getLemma(tokens.elementAt(j).toLowerCase(), POS.NOUN);
                    if(categories.containsKey(lemma))
                        surfaceFormsAttribs+="\t"+lemma;
                }
                attributes.put("SurfaceFormWikiCatAttribs", surfaceFormsAttribs);
                entity.setAttributes(attributes);
                entity.setAttributesIsSet(true);
                entity.setLabel(disambiguation);
                entity.setScore(e.linkerScore);
                labels.add(entity);
                System.out.println("*) Entity:"+text.substring(e.startOffsetCharsInText,e.startOffsetCharsInText+e.entityLengthChars)+"; disambiguation: "+disambiguation);
            }

	    TextAnnotation ta = ParametersAndGlobalVariables.curator.getTextAnnotation(text);
            int N = ta.getTokens().length;
            for (int tid = 0; tid< N ;tid++) {
                String pos = ta.getView(ViewNames.POS).getConstituentsCoveringToken(tid).get(0).getLabel();
                //  if the word is part of an NER, don't generate attributes. For example, "NY Giants" is not a real giant.
                // also, use only the noun sences - not the adjectives or the verbs...
                if(ta.getView(ViewNames.NER).getConstituentsCoveringToken(tid).size()==0&&
                        (pos.startsWith("NN")||pos.startsWith("JJ"))) {
                    Constituent c = new Constituent("", "", ta, tid, tid+1);
                    Span entity = new Span(c.getStartCharOffset(), c.getEndCharOffset());
                    String key = entity.getStart()+"-"+entity.getEnding();
                    if(!knownSpans.containsKey(key)) {
                        Vector<String> tokens = InFile.aggressiveTokenize(ta.getToken(tid).toLowerCase()); // my tokenization is more aggressive than Eddisson's
                        String surfaceFormsAttribs = "";
                        for (int j=0;j<tokens.size();j++) { 
                            String lemma = ParametersAndGlobalVariables.wordnet.getLemma(tokens.elementAt(j).toLowerCase(), POS.NOUN);
                            if(categories.containsKey(lemma))
                                surfaceFormsAttribs+="\t"+lemma;
                        }
                        if(surfaceFormsAttribs.length()>0) {
                            System.out.println("Token id="+tid+"; token="+ta.getToken(tid)+"; substring="+
                                    text.substring(entity.start,entity.ending)+
                                    "; character span = "+key+"; attribs: "+surfaceFormsAttribs);
                            knownSpans.put(key,entity);
                            HashMap<String, String> attributes = new HashMap<String, String>();
                            attributes.put("IsLinked", "false");
                            attributes.put("LinkerScore", "-999.0");
                            attributes.put("RankerScore", "-999.0");
                            attributes.put("TitleWikiCatAttribs", "");
                            attributes.put("SurfaceFormWikiCatAttribs", surfaceFormsAttribs);
                            entity.setAttributes(attributes);
                            entity.setAttributesIsSet(true);
                            entity.setLabel("UNMAPPED");
                            entity.setScore(-999.0);
                            labels.add(entity);
                        }
                    }
                }
            }
            labeling.setLabels(labels);
            List<Span> labelsDebug = labeling.getLabels();
            for(int i=0;i<labelsDebug.size();i++){
                Span e = labelsDebug.get(i);
                System.out.println(text.substring(e.getStart(),e.getEnding())+  ": Label="+e.getLabel()+"\tAttribs:"+ e.getAttributes());
            }
            labeling.setSource(getSourceIdentifier());

            // Update the time of the last annotation for purposes of monitoring
            // inactivity
            lastAnnotationTime = System.currentTimeMillis();

            return labeling;
        }catch (Exception e){
            throw new AnnotationFailedException("Failed to annotate the text :\n" +input+"\nThe exception was: \n"+e.toString());
        }
    }

    public String getName() throws TException {
        return "illinoiswikifier";
    }

    public String getSourceIdentifier() throws TException {
        // TODO Auto-generated method stub
        return getName() + "-" + getVersion();
    }

    public String getVersion() throws TException {
        // TODO Auto-generated method stub
        return "1.6";   
    }

    public boolean ping() throws TException {
        // TODO Auto-generated method stub
        return true;
    }



    public Labeling labelRecord(Record record)
    throws AnnotationFailedException, TException 
    {
        // Update the time of the last annotation for purposes of monitoring
        // inactivity
        lastAnnotationTime = System.currentTimeMillis();
        String text=record.rawText;
	System.out.println( "## RAH.labelRecord(): text is: " + text );
        try{

	    if ( isCuratorComponent )
		{

		    TextAnnotation ta = CuratorDataStructureInterface
			.getTextAnnotationViewsFromRecord("", "", record);

		    if ( !checkViews( ta ) )
			throw new Exception( "Required views not found." );

		    ParametersAndGlobalVariables.curator.curatorComponentTa = ta;

		}

	    Labeling labeling =tagText(text);

            // Update the time of the last annotation for purposes of monitoring
            // inactivity
            lastAnnotationTime = System.currentTimeMillis();
	    return labeling;
	}catch (Exception e){
	    throw new AnnotationFailedException("Failed to annotate the text :\n" +record.rawText+"\nThe exception was: \n"+e.toString());
	}
    }




    private boolean checkViews( TextAnnotation ta_ ) 
    {
	boolean isOk = false;
	
	if ( ta_.hasView( ViewNames.POS ) &&
	     ta_.hasView( ViewNames.SHALLOW_PARSE ) &&
	     ta_.hasView( ViewNames.NER )
	     )
	    isOk = true;

	return isOk;
    }
}
