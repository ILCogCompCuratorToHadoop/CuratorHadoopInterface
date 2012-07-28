/**
 * Thrift Server wrapper for Charniak's Syntactic Parser
 *
 *  NOTE: assumes config is in exec dir
 *  NOTE: only ever returns top parse
 *  NOTE: never times anything
 *  NOTE: this version is extended to include Head information
 *       (courtesy of Vasin Punyakanok)
 *  NOTE: there are some suspicious hard-coded constants in InputTree; 
 *        not sure if these reflect WORD length (in which case they are fine)
 *        or SENTENCE length (in which case things may not be fine)
 *        -- looks like hard limit of 800 in SentRep.h (data member 'words_')
 *           and "assert( length_ < 400)" in SentRep.C
 *
 *  August 2010: updated to use new Curator architecture
 *
 *  CHANGES:
 *    -- no parseText() method
 *
 *
 *
 *  TODO:
 *    -- specify labels for views needed by parser (i.e. tokens, sentences) 
 *       in config file
 *    -- change ECArgs/loadConfig to use map of label to value for readability/
 *       maintainability
 *
 */


//#define DEBUG_CTS


#include <fstream>
#include <iostream>
#include <sstream>
#include <unistd.h>
#include <math.h>
#include <cerrno>
#include <iconv.h>
#include <stdio.h> // ugly, but needed to use iconv
#include <time.h>

#include "GotIter.h"
#include "Wrd.h"
#include "InputTree.h"
#include "Bchart.h"
#include "ECArgs.h"
#include "MeChart.h"
#include "extraMain.h"
#include "AnsHeap.h"
#include "UnitRules.h"
#include "Params.h"
#include "ewDciTokStrm.h"
#include "headFinder.h"

// THRIFT headers

#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

// from /shared/grandpa/servers/curator/deploy/components/gen-cpp/

#include "Curator.h"
#include "base_types.h"
#include "curator_types.h"
#include "Parser.h" 
#include "CharniakException.h"



using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace cogcomp::thrift;
using namespace cogcomp::thrift::base;

using boost::shared_ptr;

using namespace cogcomp::thrift::parser;


void showNode( const Node & node_ );
void showTree( const Tree & tree_ );
void showForest( const Forest & forest_ );


static const double log600 = log2(600.0);
static const char * CONFIG_FILE = "config.txt";
static const string VERSION = "0.7";
static const string NAME = "Charniak Syntactic Parser";
static const string SHORT_NAME = "charniak";

static string TOK_VIEW = "tokens";
static string SENT_VIEW = "sentences";

MeChart* curChart;
Params 	 params;


class ParserHandler : virtual public ParserIf {
 private:
  time_t lastAnnotationTime;
 public:
  ParserHandler() {
    // Set the starting time for our activity monitor
    lastAnnotationTime = time( NULL );

    char * argv[6];
    int argc = loadConfig( CONFIG_FILE, argv );
    
    ECArgs args( argc, argv );
    /* l = length of sentence to be proceeds 0-100 is default
       n = work on each #'th line.
       d = print out debugging info at level #
       t = report timings */
    

    params.init( args );


    ECString  path( args.arg( 0 ) );
    generalInit( path, params.numParses() );    
  }


  ParserHandler( string configFile_ ) {

    char * argv[6];
    int argc = loadConfig( configFile_.c_str(), argv );
    
    ECArgs args( argc, argv );
    /* l = length of sentence to be proceeds 0-100 is default
       n = work on each #'th line.
       d = print out debugging info at level #
       t = report timings */
    

    params.init( args );


    ECString  path( args.arg( 0 ) );
    generalInit( path, params.numParses() );    
  }



  virtual ~ParserHandler()
  { }
  
  /**
   * @return The time of the last annotation performed (may be either the
   *         beginning or end of the last annotation operation)
   */
  public long getTimeOfLastAnnotation() {
    return static_cast<long> lastAnnotationTime;
  }

  void getName( string & name_ ) {
    name_ = NAME;
  }

  void getShortName( string & shortName_ ) {
    shortName_ = SHORT_NAME;
  }

  void getVersion( string & version_ ) {
    version_ = VERSION;
  }

  bool ping() {
    return true;
  }

  void getSourceIdentifier( string & sourceId_ ) {
    stringstream nameStrm;
    nameStrm << SHORT_NAME << "-" << VERSION;
    sourceId_ = nameStrm.str();
  }


      

      
  /**
   * Assumes only a single sentence is sent in 'input'. 
   * If more than one sentence is sent, they will be treated as a 
   *   single sentence. 
   * @param startCharOffset_: base index to be used as starting 
   *   point for annotation char offsets
   */

  void parseSentence(cogcomp::thrift::base::Tree& parseTree_, 
		     const cogcomp::thrift::base::Text& input_,
		     const int startCharOffset_ ) 
  {
    // Update the time of the last annotation for purposes of monitoring
    // inactivity
    lastAnnotationTime = time( NULL );

    bool isInputOk = false;

    try {
      isInputOk = checkEncodedStringIsCompatible( input_, 
						  "UTF-8",
						  "ASCII"
						  );
    }
    catch ( AnnotationFailedException & e ) {
      e.reason += "parseSentence().\n";
      throw e;
    }

    if ( !isInputOk ) {
      stringstream errStrm;
      errStrm << "ERROR: charniakThriftServer::parseSentence(): "
	      << "detected non-ascii input in UTF-8 string '"
	      << input_ << "'. Charniak can't deal with it. " << endl
	      << "Try cleaning non-ascii characters from your input first."
	      << endl;
      AnnotationFailedException e;
      e.reason = errStrm.str();
      throw e;
    }

    

    stringstream inStrm;

    
    inStrm << "<s> " << input_ << " </s>" << endl;

#ifdef DEBUG_CTS
	cerr << "## processing input sentence '" 
	     << inStrm.str() << endl;
#endif

    ewDciTokStrm* inStream = new ewDciTokStrm( inStrm, Bchart::tokenize);
    
    SentRep srp( *inStream, SentRep::SGML );

#ifdef DEBUG_CTS
      cerr << "## instantiated sentRep..." << endl;
#endif

    parseTokenizedSentence( parseTree_,
			    srp,
			    startCharOffset_ 
			    );
    delete inStream;

    // Update the time of the last annotation for purposes of monitoring
    // inactivity
    lastAnnotationTime = time( NULL );

    return;
  }



  void parseTokenizedSentence( cogcomp::thrift::base::Tree & parseTree_, 
			       SentRep & srp_,
			       const int startCharOffset_
			       ) 
  {
    // Update the time of the last annotation for purposes of monitoring
    // inactivity
    lastAnnotationTime = time( NULL );

    int len = srp_.length();

    if(len > params.maxSentLen) {
      AnnotationFailedException e;
      e.reason = "input too long.";
      throw e;
    }

    if(len == 0) {
      AnnotationFailedException e;
      e.reason = "input had zero length.";
      throw e;
    }

    try {
        
      MeChart*	chart = new MeChart( srp_, params.numParses() );
      curChart = chart;
      
      
      chart->parse( );
      
      Item* topS = chart->topS();
      
      if(!topS) {
	delete chart;
	
	AnnotationFailedException e;
	e.reason = "parse failed.";
	throw e;
      }
      
    // compute the outside probabilities on the items so that we can
    // skip doing detailed computations on the really bad ones 
      
      chart->set_Alphas();
      
#ifdef DEBUG_CTS
      cerr << "## finding map parse for input: '"
	   << srp_ << "'..." << endl;
#endif    
      
      AnsTreeStr& at = chart->findMapParse();
      
      if( at.probs[0] <= 0 ) {
	AnnotationFailedException e;
	e.reason = "mapProbs did not return answer";
	throw e;
      }
      
      short pos = 0;
      InputTree*  mapparse = inputTreeFromAnsTree(&at.trees[0], 
						  pos,
						  srp_ );
      
      double logP =log(at.probs[0]);
      logP -= (srp_.length()*log600);
      
      
      getSourceIdentifier( parseTree_.source );
      parseTree_.score = logP;
      
      parseTree_.__isset.source = true;
      parseTree_.__isset.score = true;
      
      // recursive method
      addRootNodeAndTraverse( mapparse, 
			      parseTree_.nodes, 
			      startCharOffset_  
			      );
      
      if ( !parseTree_.nodes.empty() ) {
	parseTree_.top = parseTree_.nodes.size() - 1;  // index of node in node list, from zero
      }
      
#ifdef DEBUG_CTS
      cerr << "## chk parse tree is: " << endl << *mapparse << endl;
      cerr << "## displaying nodes in returned parse tree: " << endl;
      
      vector<Node>::const_iterator
	it_node = parseTree_.nodes.begin(),
	it_node_end = parseTree_.nodes.end();
      
      int num = 0;
      
      for ( ; it_node != it_node_end; ++it_node ) {
	cerr << "## node " << num++ << ": " << endl;
	showNode( *it_node );
      }
      
      cerr << "## source identifier is: " << parseTree_.source 
	   << "; isset.source is: " << parseTree_.__isset.source << endl;
#endif
      

      delete mapparse;
      delete chart;


    }
    catch ( CharniakException & e ) 
    {

      stringstream errStrm;
      errStrm << "ERROR: charniakThriftServer::parseTokenizedSetnence(): " 
	      << "caught CharniakException: " << e.what()  << endl;

      cerr << errStrm.str();

      AnnotationFailedException e;
      e.reason = errStrm.str();
      throw e;
    }

    // Update the time of the last annotation for purposes of monitoring
    // inactivity
    lastAnnotationTime = time( NULL );
  }



  void parseRecord(cogcomp::thrift::base::Forest& _parses, 
		   const cogcomp::thrift::curator::Record& record_) {
    // Update the time of the last annotation for purposes of monitoring
    // inactivity
    lastAnnotationTime = time( NULL );

#ifdef DEBUG_CTS
    cerr << "## parseRecord()..." << endl;
#endif

    // identify sentences using text member and sentence spans
    // pass start offset of each sentence when generating parse tree
    // parse each one, add to parse Forest


    Text sentenceText = record_.rawText;
    cogcomp::thrift::base::Labeling sentences = getLabelView( record_, SENT_VIEW );
    cogcomp::thrift::base::Labeling tokens = getLabelView( record_, TOK_VIEW );

    if ( 0 == sentences.labels.size() ) 
    {
      stringstream errStrm;
      errStrm << "ERROR: charniakThriftServer::parseRecord(): "
	      << "no sentences in record (sentences.labels.size() is zero); "
	      << "raw text is: '" << endl << sentenceText << "'. " 
	      << endl;

      cerr << errStrm.str();

      AnnotationFailedException e;
      e.reason = errStrm.str();
      throw e;
    }


    for ( int i = 0; i < sentences.labels.size(); ++i ) {

      int start = sentences.labels[i].start;
      int end = sentences.labels[i].ending;
      
      vector< StringWithOffsets > tokenVec;

      if ( !Bchart::tokenize ) {

#ifdef DEBUG_CTS
	cerr << "## Bchart::tokenize is set to false..." << endl;
#endif

	vector< Span > labels = tokens.labels;  


        for ( int i = 0; i < labels.size(); i++ ) {  

            Span span = labels[ i ];  
	    int length = span.ending - span.start;

#ifdef DEBUG_CTS
	    cerr << "## start: " << start << "; end: " << end << endl;
	    cerr << "## span start: " << span.start << "; span end: " << span.ending << endl;
#endif
	    if ( ( span.start < start ) ||
		 ( span.start > end ) ||
		 ( span.ending < start ) ||
		 ( span.ending > end ) 
		 )
	      
	      continue;


            string tokenStr = sentenceText.substr(span.start, length);  
	    //            printf("%s : %s\n", label.c_str(), words.c_str());  
#ifdef DEBUG_CTS
	    cerr << "## read token '" << tokenStr << "'..." << endl;
#endif	    
	    tokenVec.push_back( StringWithOffsets( tokenStr, span.start, span.ending ) );
        }  
      }


      Tree parseTree;
      
      if ( Bchart::tokenize ) {

#ifdef DEBUG_CTS
	cerr << "## calling parseSentence with string '" 
	     << sentenceText << "'..." << endl;
#endif
	parseSentence( parseTree, sentenceText, start );
      
      }
      else {

#ifdef DEBUG_CTS
	cerr << "## calling parseSentence with " << tokenVec.size() 
	     << " tokens..." << endl;
#endif
	SentRep srp( tokenVec );

	parseTokenizedSentence( parseTree, srp, start );


      }

      _parses.trees.push_back( parseTree );
    }

    getSourceIdentifier( _parses.source );
    _parses.__isset.source = true;

    _parses.rawText = record_.rawText;
    _parses.__isset.rawText = true;

    showForest( _parses );

    // Update the time of the last annotation for purposes of monitoring
    // inactivity
    lastAnnotationTime = time( NULL );

    return;
  }


  /**
   * generate a node for the tree, recursively visit/generate children,
   *   then add this node to the tree
   * @param chkParse_: charniak output tree
   * @param nodes_: master list of nodes in the tree
   * @param startCharOffset_: index of starting char of this sentence in 
   *   the original string (i.e. not necessarily zero)
   */

  void addRootNodeAndTraverse( InputTree * chkParse_, 
			      vector< Node > & nodes_,
			      const int startCharOffset_ )
  {
    Node myNode; 
    myNode.label = chkParse_->term() + chkParse_->ntInfo();   
    myNode.span.start = chkParse_->startOffset(); // + startCharOffset_;
    myNode.span.ending = chkParse_->endOffset(); // + startCharOffset_;

    myNode.__isset.span = true;

    InputTrees children = chkParse_->subTrees();

    ConstInputTreesIter  subTreeIter= children.begin();
    InputTree  *subTree;

    // traverse children, add to nodeList
    // add edges to children; when head child reached, add
    //   label 'HEAD'

    const int childOffset( headPosFromTree( chkParse_ ) );

#ifdef DEBUG_CTS
    cerr << "## child offset of head is " << childOffset << endl;
#endif

    int childIndex = 0;

    for ( ; subTreeIter != children.end() ; subTreeIter++ ) {

#ifdef DEBUG_CTS
      cerr << "## processing child " << childIndex << endl;
#endif

      myNode.__isset.children = true;

      // add child id and, if head, label to node children

      subTree = *subTreeIter;

      // make recursive call
      
      addRootNodeAndTraverse( subTree,
			      nodes_,
			      startCharOffset_
			      );


      int childId = nodes_.size() - 1;

      if ( childOffset == childIndex) 
	myNode.children.insert( make_pair( childId, "HEAD" ) );
      else
	myNode.children.insert( make_pair( childId, "" ) );

      childIndex++;
    }

    nodes_.push_back( myNode );
    
#ifdef DEBUG_CTS

    cerr << "## created node with id " << ( nodes_.size() - 1 ) 
	 << ": " << endl;
    showNode( myNode );

    cerr << "## displaying last element of nodes_: " << endl;
    int size = nodes_.size();
    showNode( nodes_[ size - 1 ] );

#endif

 
    return;
  }


  /**
   * read the desired options from a file: format is same as command line,
   *   without 'parseIt' command (i.e., just the arguments)
   */

  int loadConfig( const char * fileName_, char * argv_[6] )
  {
    ifstream in( fileName_ );
    
    if ( !in ) {
      stringstream errStrm;
      errStrm << "ERROR: CharniakThriftServer: couldn't open file '"
	      << fileName_ << "' to read configuration.  Error was: " 
	      << strerror( errno ) << "." << endl;
      
      cerr << errStrm.str();
      exit( -1 );
    }
    
    string arg;
    int numArgs = 0;
    
    /**
     * MS: changed to read view names from config, without affecting 
     *     number of arguments recognized by original code
     */


    while ( in >> arg ) {
      cerr << "read arg '" << arg << "'." << endl;

      if ( arg == "SENTENCE_VIEW" ) 
	in >> SENT_VIEW;
      else if ( arg == "TOK_VIEW" )
	in >> TOK_VIEW;
      else {
	
	char * buf = new char[ arg.size() + 1];
	argv_[ ++numArgs ] = strcpy( buf, arg.c_str() );
      
	cerr << "## argv_[" << numArgs << "] is '" << argv_[ numArgs ] 
	     << "'." << endl;
      }
    }
    
    return numArgs;
  }
  

  bool checkEncodedStringIsCompatible( const string & str_,
				       const string & inputEncoding_,
				       const string & outputEncoding_ ) const
  {
    bool isCompatible = true;
    
    
    int outBufLen = 2 * str_.size() + 1;    
    char inbuf[ str_.size() + 1 ];

    strcpy( inbuf, str_.c_str() );

    char outbuf[ outBufLen ];
    
    iconv_t          cd;     /* conversion descriptor          */
    size_t           inleft; /* number of bytes left in inbuf  */
    size_t           outleft;/* number of bytes left in outbuf */
    int              rc;     /* return code of iconv()         */
    
    
    if ( ( cd = iconv_open(inputEncoding_.c_str(), outputEncoding_.c_str() ) ) 
	 == (iconv_t)(-1)) {
      
      stringstream errStrm;
      errStrm << "ERROR: charniakThriftServer::checkEncodedStringIsCompatible():"
	      << " no conversion available from " << inputEncoding_ 
	      << " to " << outputEncoding_ << "." << endl;
      
      AnnotationFailedException e;
      e.reason = errStrm.str();
      throw e;
    }
    
    inleft = str_.size();
    outleft = outBufLen;
    char * inptr = (char*) inbuf;
    char * outptr = (char*) outbuf;
    
    rc = iconv(cd, &inptr, &inleft, &outptr, &outleft);
    
    if (rc == -1) {
      stringstream errStrm;
      errStrm << "ERROR: charniakThriftServer::checkEncodedStringIsCompatible():"
	      << " could not convert characters from " << inputEncoding_
	      << " to " << outputEncoding_ << "." << endl;
      
      AnnotationFailedException e;
      e.reason = errStrm.str();
      throw e;
    }
    else if ( rc > 0 ) {
      isCompatible = false;
    }
    
    iconv_close(cd);
    
    return isCompatible;
  }


  cogcomp::thrift::base::Labeling getLabelView( cogcomp::thrift::curator::Record record_, 
						const string & viewName_ 
						)
  {
    cogcomp::thrift::base::Labeling view;
    
    map< string, cogcomp::thrift::base::Labeling >::const_iterator it_labeling
      = record_.labelViews.find( viewName_ );
    
    if ( it_labeling != record_.labelViews.end() )
      view = it_labeling->second;
    
    return view;
  }

  
  
};









int loadConfig( const char * fileName_, char * argv_[6] )
{
  ifstream in( fileName_ );

  if ( !in ) {
    stringstream errStrm;
    errStrm << "ERROR: CharniakThriftServer: couldn't open file '"
	    << fileName_ << "' to read configuration.  Error was: " 
	    << strerror( errno ) << "." << endl;

    cerr << errStrm.str();
    AnnotationFailedException e;
    e.reason = errStrm.str();
    throw e;
  }
  
  string arg;
  int numArgs = 0;

  while ( in >> arg ) {
    char * buf = new char[ arg.size() + 1];
    argv_[ numArgs++ ] = strcpy( buf, arg.c_str() );
  }

  return numArgs;
}
  
void showNode( const Node & node_ )
{

  cerr << "## Node: \nlabel: " << node_.label 
       << endl << "Span: start: " << node_.span.start
       << "; end: " << node_.span.ending << endl
       << "Children: ";

  map< int32_t, string >::const_iterator
    it_c = node_.children.begin(),
    it_c_end = node_.children.end();

  for ( ; it_c != it_c_end; ++it_c ) 
    cerr << "(" << it_c->first << ": " << it_c->second << ") ";

  cerr << endl << "isSet.span: " 
       << ( node_.__isset.span ? "TRUE" : "FALSE" ) 
       << endl << "isSet.children: " 
       << ( node_.__isset.children ? "TRUE" : "FALSE" ) 
       << endl << endl;


  return;
}


void showForest( const Forest & forest_ )
{
  cerr << "## forest:" << endl 
       << "identifier: " << forest_.source 
       << "; isset.source is: " << forest_.__isset.source << endl
       << endl << "__isset.rawText: "
       << ( forest_.__isset.rawText ? "TRUE" : "FALSE" )
       << endl << "Trees: " << endl;

  for ( int i = 0; i < forest_.trees.size(); ++i ) {
    showTree( forest_.trees[i] );
  }

  return;
}

void showTree( const Tree & tree_ )
{
  cerr << "## tree: " << endl
       << ( tree_.__isset.source ? "TRUE" : "FALSE" ) << endl
       << "__isset.score: " 
       << ( tree_.__isset.score ? "TRUE" : "FALSE" ) << endl
       << endl 
       << "top: " << tree_.top << endl
       << "Nodes: " << endl;

  for ( int i = 0; i < tree_.nodes.size(); ++i ) {
    cerr << "Node index: " << i << endl;
    showNode( tree_.nodes[i] );
  }

  cerr << endl;

  return;
}






/**
 * main()
 */

int main(int argc, char **argv) {

  if( argc != 3 ) {
    cerr << "Usage: " << argv[0] << " port configFile" << endl;
    exit( -1 );
  }

  int port = atoi( argv[1] );
  string config( argv[2] );

  shared_ptr<ParserHandler> handler(new ParserHandler( config ));
  shared_ptr<TProcessor> processor(new ParserProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  //  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

   //  shared_ptr<TTransport> bufTransport( new TBufferedTransport( serverTransport ) );
  shared_ptr<TTransportFactory> transportFactory( new TFramedTransportFactory() );
//   shared_ptr<TProtocol> protocol( new TBinaryProtocol( transport ) );


  TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
  server.serve();
  return 0;
}


