package edu.illinois.cs.cogcomp.annotation.handler;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.illinois.cs.cogcomp.lbj.coref.decoders.BIODecoder;
import edu.illinois.cs.cogcomp.lbj.coref.decoders.BestLinkDecoder;
import edu.illinois.cs.cogcomp.lbj.coref.decoders.ExtendHeadsDecoder;
import edu.illinois.cs.cogcomp.lbj.coref.decoders.MentionDecoder;
import edu.illinois.cs.cogcomp.lbj.coref.io.loaders.DocFromTextLoader;
import edu.illinois.cs.cogcomp.lbj.coref.io.loaders.DocLoader;
import edu.illinois.cs.cogcomp.lbj.coref.ir.Chunk;
import edu.illinois.cs.cogcomp.lbj.coref.ir.Mention;
import edu.illinois.cs.cogcomp.lbj.coref.ir.docs.Doc;
import edu.illinois.cs.cogcomp.lbj.coref.ir.solutions.ChainSolution;
import edu.illinois.cs.cogcomp.lbj.coref.learned.Emnlp8;
import edu.illinois.cs.cogcomp.lbj.coref.learned.MDExtendHeads;
import edu.illinois.cs.cogcomp.lbj.coref.learned.MTypePredictor;
import edu.illinois.cs.cogcomp.lbj.coref.learned.MentionDetectorMyBIOHead;
import edu.illinois.cs.cogcomp.thrift.curator.Record;
import edu.illinois.cs.cogcomp.thrift.base.AnnotationFailedException;
import edu.illinois.cs.cogcomp.thrift.base.Clustering;
import edu.illinois.cs.cogcomp.thrift.base.Labeling;
import edu.illinois.cs.cogcomp.thrift.base.Span;
import edu.illinois.cs.cogcomp.thrift.cluster.ClusterGenerator;

/**
 * @author James Clarke
 * 
 */
public class IllinoisCorefHandler implements ClusterGenerator.Iface {
	private Logger logger = LoggerFactory.getLogger(IllinoisCorefHandler.class);
	private DocLoader loader;
	private MentionDecoder mDec;
	private MTypePredictor mTyper;
	private BestLinkDecoder decoder;
	private Emnlp8 corefClassifier;

	private String nerfield = "ner";
	private String tokensfield = "tokens";
	private String sentencesfield = "sentences";
	private String posfield = "pos";

    private long lastAnnotationTime;

	public IllinoisCorefHandler() throws TException {
		this("");
	}

	public IllinoisCorefHandler(String configFilename) throws TException {
        // Set the starting time for our activity monitor
        lastAnnotationTime = System.currentTimeMillis();

		if (configFilename.trim().equals("")) {
			configFilename = "configs/coref.properties";
		}
		Properties config = new Properties();
		try {
			FileInputStream in = new FileInputStream(configFilename);
			config.load(new BufferedInputStream(in));
			in.close();
		} catch (IOException e) {
			logger.warn("Error reading configuration file. {}", configFilename);
		}
		tokensfield = config.getProperty("tokens.field", "tokens");
		sentencesfield = config.getProperty("sentences.field", "sentences");
		posfield = config.getProperty("pos.field", "sentences");
		nerfield = config.getProperty("ner.field", "ner");
		loadCorefSystem();

		logger.info( this.getName() + ", version " + this.getVersion() + " is now instantiated." );
	}

    /**
     * @return The time of the last annotation performed (may be either the
     *         beginning or end of the last annotation operation)
     */
    public long getTimeOfLastAnnotation() {
        return lastAnnotationTime;
    }

	private void loadCorefSystem() {
		logger.info("Loading classifier");
		corefClassifier = new Emnlp8();
		corefClassifier.setThreshold(-8.0);
		logger.info("Loading decoder");
		decoder = new BestLinkDecoder(corefClassifier);
		logger.info("Loading mention decoder");
		mDec = new ExtendHeadsDecoder(new MDExtendHeads(), new BIODecoder(
				new MentionDetectorMyBIOHead()));
		logger.info("Loading mention typer");
		mTyper = new MTypePredictor();
		logger.info("Loading document loader");
		loader = new DocFromTextLoader(mDec, mTyper);
		logger.info("Components loaded.");
	}

	public boolean ping() throws TException {
		return true;
	}

	public String getName() throws TException {
		return "Illinois Coreference Resolver";
	}

	public String getVersion() throws TException {
		return "0.2";
	}

	public Clustering clusterRecord(Record record) throws TException {
        // Update the time of the last annotation for purposes of monitoring
        // inactivity
        lastAnnotationTime = System.currentTimeMillis();

		String rawText = record.getRawText();
		Doc doc = loader.loadDoc(rawText);
		if (record.getLabelViews().containsKey(nerfield)) {
			List<Mention> mentions = new ArrayList<Mention>();
			Labeling nes = record.getLabelViews().get(nerfield);
			for (Span span : nes.getLabels()) {
				Chunk c = new Chunk(doc, span.getStart(), span.getEnding() - 1,
						rawText.substring(span.getStart(), span.getEnding()));
				Mention m = new Mention(doc, c);
				m.setType("NAM");
				m.setEntityType(span.getLabel());
				mentions.add(m);
			}
			mentions.addAll(doc.getPredMentions());
			doc.setPredictedMentions(mentions);
		}

        // Update the time of the last annotation for purposes of monitoring
        // inactivity
        lastAnnotationTime = System.currentTimeMillis();

		return corefDoc(doc);
	}

	/**
	 * @param doc
	 * @return
	 * @throws TException
	 */
	private synchronized Clustering corefDoc(Doc doc) throws TException {
        // Update the time of the last annotation for purposes of monitoring
        // inactivity
        lastAnnotationTime = System.currentTimeMillis();

		long startTime = lastAnnotationTime;
		ChainSolution<Mention> sol = decoder.decode(doc);
		List<Labeling> clusters = new ArrayList<Labeling>();
		for (Set<Mention> chain : sol.getChains()) {
			List<Span> labels = new ArrayList<Span>();
			for (Mention m : chain) {
				Chunk c = m.getExtent();
				Span span = new Span();
				span.setStart(c.getStart());
				span.setEnding(c.getEnd() + 1);
				if (!m.getEntityID().equals("NONE"))
					span.setLabel(m.getEntityID());
				

				TreeMap attMap = new TreeMap< String, String > ();

				if ( !( "NONE".equalsIgnoreCase( m.getEntityType() ) ) ) {
				    attMap.put( "ENTITY_TYPE", m.getEntityType() );
				    attMap.put( "COARSE_ENTITY_TYPE", m.getEntityType() );
				}
				else if ( "PRO".equalsIgnoreCase( m.getType() ) )  {
				    attMap.put( "ENTITY_TYPE", "COREF_PRONOUN" );
				}
				else {
				    attMap.put( "ENTITY_TYPE", "COREF_NON_NE_MENTION" );
				}

				span.setAttributes( attMap );

				labels.add(span);

			}
			Labeling cluster = new Labeling();
			cluster.setLabels(labels);
			clusters.add(cluster);
		}
		Clustering result = new Clustering();
		result.setSource(getSourceIdentifier());
		result.setClusters(clusters);
		long endTime = System.currentTimeMillis();
		long time = endTime - startTime;
		logger.info("Performed Coref in {}ms", time);

        // Update the time of the last annotation for purposes of monitoring
        // inactivity
        lastAnnotationTime = endTime;

		return result;
	}

	public String getSourceIdentifier() throws TException {
		return "illinoiscoref-" + getVersion();
	}

	public Clustering clusterRecords(List<Record> records)
			throws AnnotationFailedException, TException {
        // Update the time of the last annotation for purposes of monitoring
        // inactivity
        lastAnnotationTime = System.currentTimeMillis();

		StringBuffer rawText = new StringBuffer();
		List<Mention> mentions = new ArrayList<Mention>();

		for (Record record : records) {
			rawText.append(record.getRawText());
			rawText.append(" ");
		}
		Doc doc = loader.loadDoc(rawText.toString());
		int offset = 0;
		List<Integer> offsets = new ArrayList<Integer>();
		for (int i = 0; i < records.size(); i++) {
			offsets.add(offset);
			Record record = records.get(i);
			if (record.getLabelViews().containsKey(nerfield)) {
				Labeling nes = record.getLabelViews().get(nerfield);
				for (Span span : nes.getLabels()) {
					Chunk c = new Chunk(doc, offset + span.getStart(), offset
							+ span.getEnding() - 1, record.getRawText().substring(
							span.getStart(), span.getEnding()));
					Mention m = new Mention(doc, c);
					m.setType("NAM");
					m.setEntityType(span.getLabel());
					mentions.add(m);
				}
			}
			offset += record.getRawText().length() + 1;
		}
		mentions.addAll(doc.getPredMentions());
		doc.setPredictedMentions(mentions);
		Clustering coref = corefDoc(doc);
		for (Labeling labeling : coref.getClusters()) {
			for (Span span : labeling.getLabels()) {
				adjustSpan(span, offsets);
			}
		}
		return coref;
	}

	private void adjustSpan(Span span, List<Integer> offsets) {
		int previous = 0;
		boolean adjusted = false;
		for (int i = 0; i < offsets.size(); i++) {
			int offset = offsets.get(i);
			if (span.getStart() < offset) {
				span.setStart(span.getStart() - previous);
				span.setEnding(span.getEnding() - previous);
				span.setMultiIndex(i);
				adjusted = true;
				break;
			}
			previous = offset;
		}
		if (!adjusted) {
			logger.warn("Did not perform any adjustment on span.");
		}
	}

}
