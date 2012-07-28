/**
 * Thrift interface for the Curator and Record.
 *
 *  thrift --gen <lang> curator.thrift
 *
 * James Clarke <clarkeje@gmail.com>
 **/

include "base.thrift"

namespace java edu.illinois.cs.cogcomp.thrift.curator
namespace cpp cogcomp.thrift.curator
namespace py cogcomp.curator
namespace perl Cogcomp.curator
namespace php curator


/**
 * Record's are the objects that hold all annotations on a text.
 *
 * <code>identifier</code> - a unique identifier for this record.<br/>
 * <code>rawText</code> - the raw text associated with this record.<br/>
 * <code>labelViews</code> - Label views. Contains all the Labelings for this record.<br/>
 * <code>clusterViews</code> - Cluster views. Contains all the Clusterings for this record.<br/>
 * <code>parseViews</code> - Parse views. Contains all the Forests for this record.<br/>
 * <code>views</code> - Label views. Contains all the Views for this record.<br/>
 * <code>whitespaced</code> - Was this Record created using a ws* method?<br/>
 **/
struct Record {
   /** how to identify this record. */
   1: required string identifier,
   /** The raw text string. */
   2: required string rawText,
   /** Label views.  Contains all the Labelings. */
   3: required map<string, base.Labeling> labelViews,
   /** Cluster views.  Contains all the Clusterings. */
   4: required map<string, base.Clustering> clusterViews,
   /** Parse views.  Contains all the Forests. */
   5: required map<string, base.Forest> parseViews,
   /** General views.  Contains all the Views. */
   6: required map<string, base.View> views,
   /** Was this Record created using a ws* method. */
   7: required bool whitespaced,
}

struct MultiRecord {
  1: required string identifier,
  2: required list<string> records,
  3: required map<string, base.Labeling> labelViews,
  4: required map<string, base.Clustering> clusterViews,
  5: required map<string, base.Forest> parseViews,
  6: required map<string, base.View> views,
}

service Curator extends base.BaseService { 

  /** Returns a map of view_names to annotation server names. Useful for
  inspecting the annotations available via the curator.  */
  map<string,string> describeAnnotations(),

  /** Is caching enabled on this server? */
  bool isCacheAvailable(),

  /** Provide annotation of view_name for a given text. forceUpdate forces the
  annotation to be reprocessed even if it is available in the cache.*/
  Record provide(1:string view_name, 2:string text, 3:bool forceUpdate)
  throws (1:base.ServiceUnavailableException suex, 2:base.AnnotationFailedException afex),

  /** Provide annotation of view_name for a given list of strings.  Each string
  will be tokenized on whitespace and each string should represent one
  sentence. forceUpdate forces the annotation to be reprocessed even if it is
  available in the cache.*/
  Record wsprovide(1:string view_name, 2:list<string> sentences, 3:bool forceUpdate)
  throws (1:base.ServiceUnavailableException suex, 2:base.AnnotationFailedException afex),

  /** Returns the (standard Unix milliseconds) time of last annotation 
   activity (which may be the beginning or end of the last annotation performed).*/
  i64 getTimeOfLastAnnotation(),

  /** Returns the Record for a given text. */
  Record getRecord(1:string text) throws (1:base.ServiceUnavailableException ex, 2:base.AnnotationFailedException ex2),

  /** Returns the Record for a given list of strings. Each string will be
  tokenized on whitespace and each string should represent one sentence. */
  Record wsgetRecord(1:list<string> sentences) throws
  (1:base.ServiceUnavailableException ex, 2:base.AnnotationFailedException ex2),

  /** Returns the Record associated with a given identifier. */
  Record getRecordById(1:string identifier) throws (1:base.ServiceUnavailableException ex, 2:base.AnnotationFailedException ex2),

  /** Store the given Record.  Usually disabled. */
  void storeRecord(1:Record record) throws (1:base.ServiceSecurityException ssex), 

  /** Returns a MultiRecord for a given list of texts. */
  MultiRecord getMultiRecord(1:list<string> texts) throws (1:base.ServiceUnavailableException ex, 2:base.AnnotationFailedException ex2),

  /** Provides the view_name for multiple texts in a MultiRecord. */
  MultiRecord provideMulti(1:string view_name, 2:list<string> texts, 3:bool forceUpdate)
  throws (1:base.ServiceUnavailableException suex, 2:base.AnnotationFailedException afex),

  /** Store the given MultiRecord.  Usually disabled. */
  void storeMultiRecord(1:MultiRecord record) throws (1:base.ServiceSecurityException ssex), 


}
