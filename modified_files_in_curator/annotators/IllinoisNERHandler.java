package edu.illinois.cs.cogcomp.annotation.handler;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.illinois.cs.cogcomp.LbjNer.ExpressiveFeatures.*;
import edu.illinois.cs.cogcomp.LbjNer.InferenceMethods.*;
import edu.illinois.cs.cogcomp.LbjNer.LbjFeatures.*;
import edu.illinois.cs.cogcomp.LbjNer.LbjTagger.*;
import edu.illinois.cs.cogcomp.LbjNer.ParsingProcessingData.*;
import edu.illinois.cs.cogcomp.edison.sentences.TextAnnotation;
import edu.illinois.cs.cogcomp.thrift.base.Labeling;
import edu.illinois.cs.cogcomp.thrift.base.Span;
import edu.illinois.cs.cogcomp.thrift.curator.Record;
import edu.illinois.cs.cogcomp.thrift.labeler.Labeler;


import LBJ2.classify.Classifier;
import LBJ2.parse.LinkedVector;


/**
 * @author James Clarke
 * 
 */
public class IllinoisNERHandler implements Labeler.Iface {
	private final Logger logger = LoggerFactory.getLogger(IllinoisNERHandler.class);
	private NETaggerLevel1 t1;
	private NETaggerLevel2 t2;

    private long lastAnnotationTime;

	public IllinoisNERHandler()  throws Exception {
		this("configs/ner.config");
	}
	
	public IllinoisNERHandler(String params) throws Exception {
        // Set the starting time for our activity monitor
        lastAnnotationTime = System.currentTimeMillis();

		Parameters.readConfigAndLoadExternalData(params);
		ParametersForLbjCode.currentParameters.forceNewSentenceOnLineBreaks = false;
		System.out.println("Reading model file : " + ParametersForLbjCode.currentParameters.pathToModelFile+".level1");
		NETaggerLevel1 tagger1=new NETaggerLevel1(ParametersForLbjCode.currentParameters.pathToModelFile+".level1",ParametersForLbjCode.currentParameters.pathToModelFile+".level1.lex");
		System.out.println("Reading model file : " + ParametersForLbjCode.currentParameters.pathToModelFile+".level2");
		NETaggerLevel2 tagger2=new NETaggerLevel2(ParametersForLbjCode.currentParameters.pathToModelFile+".level2", ParametersForLbjCode.currentParameters.pathToModelFile+".level2.lex");
		setTaggers(tagger1, tagger2);
	}

	public IllinoisNERHandler(NETaggerLevel1 tag1, NETaggerLevel2 tag2) throws TException {
        // Set the starting time for our activity monitor
        lastAnnotationTime = System.currentTimeMillis();

		setTaggers(tag1, tag2);
	}

    /**
     * @return The time of the last annotation performed (may be either the
     *         beginning or end of the last annotation operation)
     */
    public long getTimeOfLastAnnotation() {
        return lastAnnotationTime;
    }

	public void setTaggers(NETaggerLevel1 tag1, NETaggerLevel2 tag2) throws TException {
		t1 = tag1;
		t2 = tag2;
	}

	/**
	 * Performs that call to the LBJ NER library.
	 * 
	 * @param input
	 * @param nl2sent
	 *            convert new lines to sentences?
	 * @return
	 */
	private synchronized Data performNER(String input,
			boolean nl2sent)  throws TException{
        // Update the time of the last annotation for purposes of monitoring
        // inactivity
        lastAnnotationTime = System.currentTimeMillis();

		long startTime = lastAnnotationTime;
		logger.debug("Performing NER (nl2sent: {}) on:", nl2sent);
		logger.debug(input);
		ParametersForLbjCode.currentParameters.forceNewSentenceOnLineBreaks = nl2sent;
		if (input.trim().equals("")) {
			Vector<LinkedVector> v = new Vector<LinkedVector>();
			v.addElement(new LinkedVector());
			return new Data(new NERDocument(v, "empty"));
		}
		Vector<LinkedVector> sentences = PlainTextReader.parseText(input);
		Data data = new Data(new NERDocument(sentences, "input"));
		try {
			ExpressiveFeaturesAnnotator.annotate(data);
			Decoder.annotateDataBIO(data,t1,t2);
		} catch (Exception e) {
			System.out.println("Cannot annotate the test, the exception was: ");
			e.printStackTrace();
			throw new TException("Cannot annotate the test, the exception was "+e.toString());
		}

		long endTime = System.currentTimeMillis();
		long time = endTime - startTime;
		logger.info("Performed NER in {}ms", time);

        // Update the time of the last annotation for purposes of monitoring
        // inactivity
        lastAnnotationTime = endTime;
		return data;
	}


	public Labeling performNer(String input, boolean nl2sent) throws  TException {
		Data data = performNER(input, nl2sent);
		List<Integer> quoteLocs = findQuotationLocations(input);
		List<Span> labels = new ArrayList<Span>();

		// track the location we have reached in the input
		int location = 0;

		// the data always has a single document
		// each LinkedVector in data corresponds to a sentence.
		for (int i = 0; i < data.documents.elementAt(0).sentences.size(); i++) {
			LinkedVector vector = data.documents.elementAt(0).sentences.get(i);
			boolean open = false;

			// the span for this entity
			Span span = null;

			// lets cache the predictions and words
			String[] predictions = new String[vector.size()];
			String[] words = new String[vector.size()];
			for (int j = 0; j < vector.size(); j++) {
				predictions[j] = ((NEWord) vector.get(j)).neTypeLevel2;
				words[j] = ((NEWord) vector.get(j)).form;
				System.out.print(words[j]+" ");
			}

			System.out.println("");
			for (int j = 0; j < vector.size(); j++) {

				// the current word (NER's interpretation)
				String word = words[j];
				// this int[] will store start loc of word in 0th index and end
				// in 1st index.
				int[] startend = findStartEndForSpan(input, location, word,
						quoteLocs);
				location = startend[1];

				if (predictions[j].startsWith("B-")
						|| (j > 0 && predictions[j].startsWith("I-") && (!predictions[j - 1]
								.endsWith(predictions[j].substring(2))))) {
					span = new Span();
					span.setStart(startend[0]);
					span.setLabel(predictions[j].substring(2));
					open = true;
				}

				if (open) {
					boolean close = false;
					if (j == vector.size() - 1) {
						close = true;
					} else {
						if (predictions[j + 1].startsWith("B-"))
							close = true;
						if (predictions[j + 1].equals("O"))
							close = true;
						if (predictions[j + 1].indexOf('-') > -1
								&& (!predictions[j].endsWith(predictions[j + 1]
										.substring(2))))
							close = true;
					}
					if (close) {
						span.setEnding(startend[1]);
						labels.add(span);
						open = false;
					}
				}
			}

		}
		Labeling labeling = new Labeling();
		labeling.setSource(getSourceIdentifier());
		labeling.setLabels(labels);
		return labeling;
	}

	/**
	 * Finds that start and end indices in the input of the span corresponding
	 * to the word. The location is a pointer to how far we have processed the
	 * input.
	 * 
	 * @param input
	 * @param location
	 * @param word
	 * @param quoteLocs
	 * @return
	 */
	private int[] findStartEndForSpan(String input, int location, String word,
			List<Integer> quoteLocs) {
		int[] startend = null;

		if (word.equals("\"")) {
			// double quote is a special case because it could have been a
			// double tick before
			// inputAsNer is how NER viewed the input (we replicate the
			// important transforms
			// ner makes, this is very fragile!)
			StringBuffer inputAsNer = new StringBuffer();
			inputAsNer.append(input.substring(0, location));
			// translate double ticks to double quote in the original input
			inputAsNer.append(input.substring(location).replace("``", "\"")
					.replace("''", "\""));
			// find start end for the word in the input as ner
			startend = findSpan(location, inputAsNer.toString(),
					word);
			if (quoteLocs.contains(startend[0])) {
				// if the double quote was original translated we should move
				// the end pointer one
				startend[1]++;
			}
		} else {
			startend = findSpan(location, input, word);
		}
		return startend;
	}

	/**
	 * Finds where double tick quotation marks are and returns their start
	 * locations in the string NER will use.
	 * 
	 * NER performs preprocessing internally on the input string to turn double
	 * ticks to the double quote character, we need to be able to recover the
	 * double tick locations in the original to make sure our spans are
	 * consistent.
	 * 
	 * If input is: He said, ``This is great'', but he's "hip". NER will modify
	 * it to: He said, "This is great", but he's "hip". so we need to know that
	 * locations <9, 23> are locations in the NER string that should be double
	 * ticks.
	 * 
	 * @param input
	 *            rawText input not modified by NER
	 * @return list of integer locations that double quote should be translated
	 *         to double tick
	 */
	private List<Integer> findQuotationLocations(String input) {
		List<Integer> quoteLocs = new ArrayList<Integer>();
		if (input.contains("``") || input.contains("''")) {
			int from = 0;
			int index;
			int counter = 0;
			while ((index = input.indexOf("``", from)) != -1) {
				quoteLocs.add(index - counter);
				counter++;
				from = index + 2;
			}
			while ((index = input.indexOf("''", from)) != -1) {
				quoteLocs.add(index - counter);
				counter++;
				from = index + 2;
			}
			Collections.sort(quoteLocs);
		}
		return quoteLocs;
	}

	public boolean ping() {
		logger.info("PONG!");
		return true;
	}

	public Labeling labelRecord(Record record) throws TException {
		Labeling ner = performNer(record.getRawText(), false);
		return ner;
	}

	public String getName() throws TException {
		return "Illinois Named Entity Recognizer";
	}

	public String getVersion() throws TException {
		return "2.1";
	}

	public String getSourceIdentifier() throws TException {
		return "illinoisner-" + getVersion();
	}

	/**
	 * Finds the span (as start and end indices) where the word occurs in the
	 * rawText starting at from.
	 * 
	 * @param from
	 * @param rawText
	 * @param word
	 * @return
	 */
	private  int[] findSpan(int from, String rawText, String word) {
		int start = rawText.indexOf(word, from);
		if(start>0) {
			int end = start + word.length();
			return new int[] { start, end };
		} else {
			// find the span brutally (very brute force...)			
			for(start = from; start<rawText.length(); start++) {
				String sub = rawText.substring(start, Math.min(start+5+word.length()*2, rawText.length()));
				if(PlainTextReader.normalizeText(sub).startsWith(word)) {
					for(int end = start+word.length(); end<start+Math.min(start+5+word.length()*2, rawText.length()); end++) 
						if(PlainTextReader.normalizeText(rawText.substring(start,end)).equals(word)) 
							return new int[]{start, end};
				}
			}			
			System.out.println("Critical warning: word "+word+" is not found in the text "+ rawText.substring(start,rawText.length()));
			logger.debug("Critical warning: word "+word+" is not found in the text "+ rawText.substring(start,rawText.length()));
			logger.info("Critical warning: word "+word+" is not found in the text "+ rawText.substring(start,rawText.length()));
			return new int[]{0, 0};
		}
	}
	
	
	
	public static void main(String[] args) throws Exception {
		String text  = "WASHINGTON (AP) _ All else being equal, Duane Roelands would prefer to dash off short instant text messages to co-workers and friends with the service offered by Microsoft _ the one he finds easiest to use. But for Roelands, all else is not equal: His office, clients and nearly everyone else he knows use America Online's messaging system. Now, he does too. ``There are features that I want and I like,'' said Roelands, a Web developer, who likens it to the battle between VHS and Beta video recorders in the 1980s. ``But the reality is if I use the better product, I get less functionality.'' For this reason, instant messaging rivals like Microsoft, AT&AMP;T and ExciteAtHome maintain their users ought to be able to send messages to anyone else, regardless of what service they happen to have. That's not currently possible. The companies are lobbying the Federal Communications Commission to require AOL to make its product compatible with those offered by competitors as a condition of its merger with Time Warner. So far, the agency appears to favor a more tailored approach. The commission's staff has recommended that AOL be required to make its system work with at least one other provider, but the requirement would apply only to advanced instant messaging services offered over Time Warner's cable lines. How the agency defines advanced services is unclear. They could refer to features beyond text messaging, such as video teleconferencing, the sharing of files or messaging over interactive television. Today, consumers more commonly take advantage of the garden variety functions. They type short real-time phrases to others, allowing them to ``chat'' back-and-forth using text. Unlike e-mail, it's instantaneous and gets the recipient's attention right away. People can communicate with international friends without the hefty phone bills. And the service has taken hold with those who have hearing or speech disabilities. Unlike the telephone, people can discreetly interact with others _ or decide not to. ``It's communications that can be ignored,'' said Jonathan Sacks, a vice president at AOL, which runs the two leading messaging services _ ICQ and AIM _ with 140 million users. ``On the telephone, you can't see when somebody is near the phone. You can't see when it's convenient for them to communicate with you.'' AOL rivals say that if instant messaging is to be as ubiquitous as the phone network, it has to work the same way: People who use different providers must still be able to contact one another. They continue to lobby the FCC, hoping to see the conditions broadened before the agency issues its final decision. ``It's really important to get this right before innovation is squashed because one company has a monopoly,'' said Jon Englund, vice president of government affairs for ExciteAtHome. ``It's absolutely critical that Internet uses have real choice among competing platforms.'' AOL has said it wants to work toward interoperability, but first needs to protect consumer privacy and security to prevent the kinds of problems that have emerged in the e-mail world, like spamming _ unwanted junk messages. Company officials disagreed that AOL's market share was keeping out competitors. AOL executives cited a recent study by Media Metrix indicating that the messaging services offered by Yahoo! and Microsoft are the fastest growing in the United States. Why all the fuss over a free product that anyone, even those who don't subscribe to AOL, can use? Some pointed to the recent demise of two instant messaging competitors _ iCAST and Tribal Voice _ as evidence that AOL's dominance could prevent choices in the market. Another concern is that AOL could use its substantial customer base to tack on new advanced services and then charge for them. Rivals said the ability of various services to work together will become increasingly important in the future. For example, as instant messaging migrates to cell phones or hand-held computer organizers, consumers won't want to have to install multiple services on these devices, said Brian Park, senior product for Yahoo! Communications Services. ``You can have the best service and the coolest features, but nobody is going to use it if it doesn't communicate with other services,'' Park said. ___ On the Net: America Online corporate site: http://corp.aol.com IMUnified, coalition formed by AT&AMP;T, ExciteAtHome, Microsoft: http://www.imunified.org/ ";
		TextAnnotation ta = new TextAnnotation("blahCorpus", "blahId", text);
		IllinoisNERHandler handler = new IllinoisNERHandler("DemoConfig/ontonotes.config");
		Labeling labels = handler.performNer(ta.getText(), true);
		for(Iterator<Span> label= labels.getLabelsIterator(); label.hasNext() ; ) {
			Span span = label.next();
			System.out.println("["+span.start+"-"+span.ending+"]");
			System.out.println(ta.getText().substring(span.start, span.ending)+"\t:\t"+span.getLabel());	
		}
	}
}
