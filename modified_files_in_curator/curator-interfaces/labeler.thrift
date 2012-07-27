/**
 * Thrift interface for a labeler.
 *
 * thrift -r --gen <lang> labeler.thrift 
 * James Clarke <clarkeje@gmail.com>
 **/

include "base.thrift"
include "curator.thrift"

namespace java edu.illinois.cs.cogcomp.thrift.labeler
namespace cpp  cogcomp.thrift.labeler
namespace py cogcomp.labeler
namespace perl cogcomp.Labeler
namespace php tagger

/**
 * Labeling service.
 **/
service Labeler extends base.BaseService {
  /**
   * Labels a given record.
   **/
  base.Labeling labelRecord(1:curator.Record record) throws (1:base.AnnotationFailedException ex),

  /** Returns the (standard Unix milliseconds) time of last annotation 
   activity (which may be the beginning or end of the last annotation performed).*/
  i64 getTimeOfLastAnnotation(),
}

/** 
Multi-Labeler Labeling service. Useful for things that tag the text
twice. i.e., tokenizers (tokens and sentences).  
*/
service MultiLabeler extends base.BaseService {
  /**
   * MultiLabels a given record.
   **/
  list<base.Labeling> labelRecord(1:curator.Record record) throws (1:base.AnnotationFailedException ex),

  /** Returns the (standard Unix milliseconds) time of last annotation 
   activity (which may be the beginning or end of the last annotation performed).*/
  i64 getTimeOfLastAnnotation(),

}