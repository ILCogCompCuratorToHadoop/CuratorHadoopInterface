/**
 * Thrift interface for a parser.
 *
 * thrift -r --gen <lang> parser.thrift 
 * James Clarke <clarkeje@gmail.com>
 **/

include "base.thrift"
include "curator.thrift"

namespace java edu.illinois.cs.cogcomp.thrift.parser
namespace cpp  cogcomp.thrift.parser
namespace py cogcomp.parser
namespace perl cogcomp.Parser
namespace php parser

/**
 * Parser service.
 **/
service Parser extends base.BaseService {

  /**
   * Parses the Record.
   **/
  base.Forest parseRecord(1:curator.Record record) throws (1:base.AnnotationFailedException ex),
}

/**
 * MultiParser service (useful for parsers that return 
 * phrase-structure trees and dependency trees)
 **/
service MultiParser extends base.BaseService {
  /**
   * Parses the Record.
   **/
  list<base.Forest> parseRecord(1:curator.Record record) throws (1:base.AnnotationFailedException ex),

  /** Returns the (standard Unix milliseconds) time of last annotation 
   activity (which may be the beginning or end of the last annotation performed).*/
  i64 getTimeOfLastAnnotation(),
}
