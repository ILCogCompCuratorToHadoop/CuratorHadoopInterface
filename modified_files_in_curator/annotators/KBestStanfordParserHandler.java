package edu.illinois.cs.cogcomp.annotation.handler;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.illinois.cs.cogcomp.thrift.base.AnnotationFailedException;
import edu.illinois.cs.cogcomp.thrift.base.Forest;
import edu.illinois.cs.cogcomp.thrift.base.Node;
import edu.illinois.cs.cogcomp.thrift.base.Span;
import edu.illinois.cs.cogcomp.thrift.base.Tree;
import edu.illinois.cs.cogcomp.thrift.curator.Record;
import edu.illinois.cs.cogcomp.thrift.parser.MultiParser;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.Word;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.process.DocumentPreprocessor;
import edu.stanford.nlp.trees.LabeledScoredTreeFactory;
import edu.stanford.nlp.trees.TreeFactory;
import edu.stanford.nlp.util.ScoredObject;

/**
 * @author James Clarke
 * 
 */
public class KBestStanfordParserHandler implements MultiParser.Iface {
	private final Logger logger = LoggerFactory
			.getLogger(KBestStanfordParserHandler.class);
	private final LexicalizedParser parser;
	private static final String VERSION = "0.3";
	private String sentencesfield = "sentences";
	private String tokensfield = "tokens";
	private boolean useTokens = true;
	private int k = 50; //number of trees to return

    private long lastAnnotationTime;

	public KBestStanfordParserHandler() {
		this("configs/stanford.properties");
	}

	public KBestStanfordParserHandler(String configFilename) {
        // Set the starting time for our activity monitor
        lastAnnotationTime = System.currentTimeMillis();

		if (configFilename.trim().equals("")) {
			configFilename = "configs/stanford.properties";
		}
		Properties config = new Properties();

		try {
			FileInputStream in = new FileInputStream(configFilename);
			config.load(new BufferedInputStream(in));
			in.close();
		} catch (IOException e) {
			logger.warn("Error reading configuration file. {}", configFilename);
		}

		String data = config.getProperty("stanford.data",
				"data/englishPCFG.ser.gz");

		tokensfield = config.getProperty("tokens.field", "tokens");
		sentencesfield = config.getProperty("sentences.field", "sentences");

		if (config.getProperty("usetokens", "true").equals("false")) {
			useTokens = false;
		} else {
			useTokens = true;
		}
		
		String kstr = config.getProperty("stanford.k", "50");
		try {
			k = Integer.parseInt(kstr);
		} catch (NumberFormatException e) {
			logger.warn("Couldn't recognize {} as an integer. Defaulting to 50.", kstr);
			k = 50;
		}
		
		parser = new LexicalizedParser(data);
		parser.setOptionFlags(new String[] { "-retainTmpSubcategories" });
	}

    /**
     * @return The time of the last annotation performed (may be either the
     *         beginning or end of the last annotation operation)
     */
    public long getTimeOfLastAnnotation() {
        return lastAnnotationTime;
    }

	private Node generateNode(edu.stanford.nlp.trees.Tree parse, Tree tree,
			int offset) throws TException {
		if (!tree.isSetNodes()) {
			tree.setNodes(new ArrayList<Node>());
		}
		List<Node> nodes = tree.getNodes();
		Node node = new Node();

		node.setLabel(parse.value());
		for (edu.stanford.nlp.trees.Tree pt : parse.getChildrenAsList()) {
			if (!node.isSetChildren()) {
				node.setChildren(new TreeMap<Integer, String>());
			}
			if (pt.isLeaf()) {
				continue;
			} else {
				Node child = generateNode(pt, tree, offset);
				nodes.add(child);
				node.getChildren().put(nodes.size() - 1, "");
			}
		}
		Span span = new Span();
		List<Word> words = parse.yield();
		span.setStart(words.get(0).beginPosition() + offset);
		span.setEnding(words.get(words.size() - 1).endPosition() + offset);
		node.setSpan(span);
		return node;
	}

	public List<Forest> parseRecord(Record record)
			throws AnnotationFailedException, TException {
        // Update the time of the last annotation for purposes of monitoring
        // inactivity
        lastAnnotationTime = System.currentTimeMillis();

		String rawText = record.getRawText();
		List<Forest> forests = new ArrayList<Forest>();
		for (Span sentence : record.getLabelViews().get(sentencesfield)
				.getLabels()) {

			// int offset = sentence.getStart();
			int offset = 0;
			Object input = null;
			String rawsent = rawText.substring(sentence.getStart(),
					sentence.getEnding());
			if (useTokens) {
				List<Word> s = new ArrayList<Word>();
				for (Span t : record.getLabelViews().get(tokensfield)
						.getLabels()) {
					if (t.getStart() >= sentence.getStart()
							&& t.getEnding() <= sentence.getEnding()) {
						s.add(new Word(rawText.substring(t.getStart(),
								t.getEnding()), t.getStart(), t.getEnding()));
					}
				}
				input = s;
			} else {
				input = rawsent;
				offset = sentence.getStart();
			}
			List<ScoredObject<edu.stanford.nlp.trees.Tree>> kParses = parseK(
					input, k);
			int kcounter = 0;
			for (ScoredObject<edu.stanford.nlp.trees.Tree> so : kParses) {
				if (so.object().numChildren() > 1) {
					logger.warn("More than one child in the top Tree.\n{}",
							rawText);
				}
				edu.stanford.nlp.trees.Tree pt = so.object().firstChild();
				Tree tree = new Tree();
				tree.setScore(so.score());
				Node top = generateNode(pt, tree, offset);
				tree.getNodes().add(top);
				tree.setTop(tree.getNodes().size() - 1);
				if (forests.size() < kcounter+1) {
					Forest forest = new Forest();
					forest.setSource(getSourceIdentifier());
					forest.setTrees(new ArrayList<Tree>());
					forests.add(forest);
				}
				forests.get(kcounter).getTrees().add(tree);
				kcounter++;
			}
		}

        // Update the time of the last annotation for purposes of monitoring
        // inactivity
        lastAnnotationTime = System.currentTimeMillis();

		return forests;
	}

	private synchronized List<ScoredObject<edu.stanford.nlp.trees.Tree>> parseK(
			Object input, int k) {
		long startTime = System.currentTimeMillis();
		boolean parsed = false;
		if (input instanceof List) {
			List<? extends HasWord> words = (List<? extends HasWord>) input;
			parsed = parser.parse(words);
		} else if (input instanceof String) {
			parsed = parser.parse((String) input);
		}
		if (parsed) {
			long endTime = System.currentTimeMillis();
			logger.info("Parsed input in {}ms", endTime - startTime);
			return parser.getKBestPCFGParses(k);
		}
		// if can't parse or exception, fall through
		// this was taken from the LexicalizedParser source
		List<? extends HasWord> lst = null;
		if (input instanceof List) {
			lst = (List<? extends HasWord>) input;
		} else if (input instanceof String) {
			DocumentPreprocessor dp = new DocumentPreprocessor(
					parser.getOp().tlpParams.treebankLanguagePack()
							.getTokenizerFactory());
			lst = dp.getWordsFromString((String) input);
		}
		TreeFactory lstf = new LabeledScoredTreeFactory();
		List<edu.stanford.nlp.trees.Tree> lst2 = new ArrayList<edu.stanford.nlp.trees.Tree>();
		for (Object obj : lst) {
			String s = obj.toString();
			edu.stanford.nlp.trees.Tree t = lstf.newLeaf(s);
			edu.stanford.nlp.trees.Tree t2 = lstf.newTreeNode("X",
					Collections.singletonList(t));
			lst2.add(t2);
		}
		List<ScoredObject<edu.stanford.nlp.trees.Tree>> result = new ArrayList<ScoredObject<edu.stanford.nlp.trees.Tree>>();
		result.add(new ScoredObject<edu.stanford.nlp.trees.Tree>(lstf
				.newTreeNode("X", lst2), 0.0));
		long endTime = System.currentTimeMillis();
		logger.debug("Parsed input in {}ms", endTime - startTime);
		return result;
	}

	public String getName() throws TException {
		return "Stanford K-Best Parser (k="+k+")";
	}

	public String getSourceIdentifier() throws TException {
		return "stanford-"+k+"best-" + getVersion();
	}

	public String getVersion() throws TException {
		return VERSION;
	}

	public boolean ping() throws TException {
		return true;
	}

}
