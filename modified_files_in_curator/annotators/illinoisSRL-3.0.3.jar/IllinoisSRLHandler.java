package edu.illinois.cs.cogcomp.srl.curator;

import java.util.List;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.illinois.cs.cogcomp.edison.data.curator.CuratorDataStructureInterface;
import edu.illinois.cs.cogcomp.edison.sentences.Constituent;
import edu.illinois.cs.cogcomp.edison.sentences.PredicateArgumentView;
import edu.illinois.cs.cogcomp.edison.sentences.TextAnnotation;
import edu.illinois.cs.cogcomp.srl.main.SRLSystem;
import edu.illinois.cs.cogcomp.thrift.base.AnnotationFailedException;
import edu.illinois.cs.cogcomp.thrift.base.Forest;
import edu.illinois.cs.cogcomp.thrift.curator.Record;
import edu.illinois.cs.cogcomp.thrift.parser.Parser;

public class IllinoisSRLHandler implements Parser.Iface {
    private long lastAnnotationTime;

	private static Logger logger = LoggerFactory
			.getLogger(IllinoisSRLHandler.class);

	private final SRLSystem srlSystem;
	private final boolean beamSearch;

	public IllinoisSRLHandler(SRLSystem srlSystem, boolean beamSearch) {
        // Set the starting time for our activity monitor
        lastAnnotationTime = System.currentTimeMillis();

		this.beamSearch = beamSearch;
		this.srlSystem = srlSystem;
	}

    /**
     * @return The time of the last annotation performed (may be either the
     *         beginning or end of the last annotation operation)
     */
    public long getTimeOfLastAnnotation() {
        return lastAnnotationTime;
    }

	@Override
	public boolean ping() throws TException {
		logger.info("PONG!");
		return true;
	}

	@Override
	public String getName() throws TException {
		return srlSystem.getSRLSystemName();
	}

	@Override
	public String getVersion() throws TException {
		return srlSystem.getSRLSystemVersion();
	}

	@Override
	public String getSourceIdentifier() throws TException {
		return srlSystem.getSRLSystemIdentifier();
	}

	public Forest parseRecord(Record record, List<Constituent> predicates)
			throws AnnotationFailedException, TException {
		checkAvailableViews(record);
		return performSRL(record, predicates);
	}

	@Override
	public Forest parseRecord(Record record) throws AnnotationFailedException,
			TException {
		checkAvailableViews(record);
		return performSRL(record);
	}

	protected void checkAvailableViews(Record record)
			throws AnnotationFailedException {
		if (!record.isSetLabelViews()
				|| !record.getLabelViews().containsKey("pos")) {
			throw new AnnotationFailedException(
					"Unable to find POS view in the input record");
		}

		if (!record.isSetLabelViews()
				|| !record.getLabelViews().containsKey("chunk")) {
			throw new AnnotationFailedException(
					"Unable to find chunk view in the input record");
		}

		if (!record.isSetParseViews())
			throw new AnnotationFailedException(
					"Unable to find parse view in the input record");

		if (!record.getParseViews().containsKey("charniak")
				&& !record.getParseViews().containsKey("stanfordParse")) {
			throw new AnnotationFailedException(
					"Unable to find parse view in the input record"
							+ ". Expecting charniak or stanfordParse.");
		}

	}

	private synchronized Forest performSRL(Record record,
			List<Constituent> predicates) {
        // Update the time of the last annotation for purposes of monitoring
        // inactivity
        lastAnnotationTime = System.currentTimeMillis();

		TextAnnotation ta = CuratorDataStructureInterface
				.getTextAnnotationViewsFromRecord("", "", record);

		PredicateArgumentView srl = srlSystem.getSRL(ta, predicates,
				this.beamSearch);

		Forest srlForest = CuratorDataStructureInterface
				.convertPredicateArgumentViewToForest(srl);

        // Update the time of the last annotation for purposes of monitoring
        // inactivity
        lastAnnotationTime = System.currentTimeMillis();

		return srlForest;

	}

	private synchronized Forest performSRL(Record record) {
        // Update the time of the last annotation for purposes of monitoring
        // inactivity
        lastAnnotationTime = System.currentTimeMillis();

		TextAnnotation ta = CuratorDataStructureInterface
				.getTextAnnotationViewsFromRecord("", "", record);

		PredicateArgumentView srl = srlSystem.getSRL(ta, this.beamSearch);

		Forest srlForest = CuratorDataStructureInterface
				.convertPredicateArgumentViewToForest(srl);

        // Update the time of the last annotation for purposes of monitoring
        // inactivity
        lastAnnotationTime = System.currentTimeMillis();

		return srlForest;

	}

}
