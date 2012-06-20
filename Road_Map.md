<!-- -*- Markdown -*- -->

Road Map for the Curator-to-Hadoop Interface
============================================

1. In progress: HadoopInterface.java

	* Sets up the annotation jobs in a way usable by Hadoop.
	
	* Accepts command-line arguments in a flexible way, requiring only
      the specification of the location of the document collection in
      distributed storage and the annotation mode to be used, but
      allowing for much finer control as desired.
	
	* Distributes inputs across the cluster at the file level.
	
	* Uses the locally-running Curator on each node to interface with
      the annotation tool.

2. Changes to Curator

	* TODO: Create Master mode with the following responsibilities:
	
		* Sets up document collection in Hadoop Distributed File
          System (HDFS).
		  
		* Launches local-mode Curators and associated annotation tools
          on all Hadoop nodes.
		  
		* Sends batch job to Hadoop cluster (i.e., starts
          HadoopInterface.java with the proper parameters).
		
		* Waits for error messages from the annotation tools, and logs
          them in a user-actionable way.
		
	* TODO: Create local mode with the following responsibilities:
	
		* Interface with exactly one annotation tool, as specified by
          the Master Curator.
		  
		* Assume all dependencies for all documents are present in
          HDFS, and skip those documents which do not meet the
          requirements.
		  
		* Logs errors from the annotation tools in a user-actionable
          way.

3. Scripts and miscellaneous

	* TODO: Script to launch local-mode Curators and associated
      annotation tools on all Hadoop nodes. (?)
	  

