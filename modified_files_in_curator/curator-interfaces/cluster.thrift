/**
 * Thrift interface for a clusterer.
 *
 * thrift -r --gen <lang> cluster.thrift 
 * James Clarke <clarkeje@gmail.com>
 **/

include "base.thrift"
include "curator.thrift"

namespace java edu.illinois.cs.cogcomp.thrift.cluster
namespace cpp  cogcomp.thrift.cluster
namespace py cogcomp.cluster
namespace perl cogcomp.Cluster
namespace php cluster

/**
 * Clusterer service.
 **/
service ClusterGenerator extends base.BaseService {
     
  /**
   * Cluster objects in the Record.
   **/
   base.Clustering clusterRecord(1:curator.Record record) throws (1:base.AnnotationFailedException ex),

  /**
   * Cluster objects in the list of Records.
   **/
   base.Clustering clusterRecords(1:list<curator.Record> records) throws (1:base.AnnotationFailedException ex),

  /** Returns the (standard Unix milliseconds) time of last annotation 
   activity (which may be the beginning or end of the last annotation performed).*/
  i64 getTimeOfLastAnnotation(),
}
