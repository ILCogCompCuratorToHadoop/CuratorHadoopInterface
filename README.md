<!-- -*-Markdown-*- -->

The Curator-to-Hadoop Interface
========================================

Project Overview
----------------------------------------

This is a tool for interfacing the [Curator](http://cogcomp.cs.illinois.edu/trac/wiki/Curator) with a [Hadoop](http://hadoop.apache.org/) cluster. The overall goal of the project is to provide an architecture for efficiently processing large text corpora using Hadoop/MapReduce. To contribute to the project, contact [Tyler Young][].

[Tyler Young]: https://github.com/s3cur3

### Architecture ###

* New classes, inheriting from the original Curator Client, which include:  
    1. a local client that runs on each node in the Hadoop cluster and waits for input from the Hadoop process manager.
    2. a master client that sets up the inputs for the Hadoop cluster and sends batch jobs to the cluster.
	
* An interface to Hadoop, sent as a MapReduce job by our JobHandler.

* A set of scripts to start the Curator in local mode on all nodes in the Hadoop cluster. The actual running of these scripts is handled by the JobHandler.

### Advantages ###

* Data parallelism which scales linearly with the size of the Hadoop cluster, in a tool set written to operate in a strictly linear fashion. We can greatly increase the amount of data processed per second *without* re-writing the natural language processing tools themselves.

* Pre-processing of large corpora (for instance, after one annotation tool has been upgraded) with little user intervention, freeing up more time to solve research problems.

Getting Started
----------------------------------------

First make sure you have all the files present in the **Manifest** section, below. The following guide is written for a GNU/Linux system and will not provide modifications for installing on Mac OSX or Windows. The Curator itself is only tested extensively on CentOS, so porting to Mac OSX or Windows systems is not supported.

### Installation ###

If you are running this interface on an existing Hadoop cluster, you can probably skip steps 1-3 below unless you want to install your own copy of Hadoop for local (pseudo-distributed) testing. **Note**: If you don't have access to local file storage on each of the nodes in your Hadoop cluster, you may need to create duplicate versions of the Curator build on some shared network location (accessible from each Hadoop node) so that each node can run a separate instance. This is the case for the version tested at UIUC, and it requires additional configuration on your end. See the section **Configuring Curator without Installing Software on Hadoop Nodes** below for more information.

1. Make sure that Java 1.6.x, ssh, and sshd are installed on your system.

2. Download [Hadoop](http://hadoop.apache.org/common/releases.html). This interface has been tested on Hadoop 1.0.3 and 0.20.203, but should work with future releases as well.

3. Unpack the Hadoop distribution and configure it according to the [official documentation](http://hadoop.apache.org/common/docs/r1.0.3/single_node_setup.html#Prepare+to+Start+the+Hadoop+Cluster). Initially, you probably will want to set up Hadoop as a pseudo-distributed operation.

4. Download the [Curator](http://cogcomp.cs.illinois.edu/trac/wiki/Curator) (scroll down to the **Download** section). The testing version is 0.6.9. **Note**: A few minor changes are required to each ofs the annotators you plan to use, as well as to the Curator itself (on the Hadoop cluster, all tools must shut themselves down if they are idle for too long). You can either build the Curator using the files stored in `modified_files_in_curator` or contact [Tyler Young][] for the compiled custom Curator build.

5. Compile and/or configure your Curator installation. Don't forget to set your JAVA_HOME environment variable!

6. Download the [CuratorHadoopInterface package](https://github.com/ILCogCompCuratorToHadoop/CuratorHadoopInterface) files from GitHub, including the Java source code and shell scripts. You can also get a pre-compiled package by contacting [Tyler Young][].

7. Compile and/or configure your CuratorHadoopInterface installation. You can compile by simply typing `ant` from the `CuratorHadoopInterface` root directory. 

8. In the `scripts` folder, edit the shell scripting variables in the designated section of each file with your directory paths.

9. Compile `CuratorHadoopInterface/src/edu/illinois/cs/cogcomp/hadoopinterface/infrastructure/JobHandler.java` into a JAR file. You will probably want to store this JAR in a separate `JobHandler` folder on the same level as `HadoopInterface` and `curator`. For reference, our directory structure looks like this:

* `HadoopInterface`

    * `HadoopInterface.jar`
	
* `JobHandler`

    * `JobHandler.jar`
	* `lib`
	* `scripts`

* `curator-0.6.9/dist`

    * `client` (contains our `CuratorClient.class`)

10. Congratulations, you've installed Hadoop, Curator, and the Curator-Hadoop interface!

### Running a Job ###

The Curator-Hadoop interface takes input and produces output in a Thrift-serialized file format. By default, it will launch a locally-running Curator that will read in those Thrift-serialized records and add them to the Curator's database. One day, you may be able to visit the [Curator](http://cogcomp.cs.illinois.edu/trac/wiki/Curator) webpage to acquire a file reader for this format.

1. Make sure that your input files are in Thrift-serialized format, consistent, and organized in a common directory for each job. *Consistent* means that you should be prepared to (re-)run all dependent annotators up to your requested annotation and starting with the lowest common existing annotation in a *random* sample of 25 files in the input directory. It is preferred that all of your documents in a given job have the same existing annotations.

2. From your `JobHandler` directory, execute `java -jar JobHandler.jar REQUESTED_ANNOTATION /absolute/path/to/input_dir` with the optional third parameter of `STARTING_ANNOTATION`. For example, to run the tokenizer and the part of speech parser, you would execute `java -jar JobHandler.jar POS /home/jdoe/job123`. The program will automatically detect existing annotations in the input files via random sampling, so if your files are inconsistent (or if you're running into dependency errors), you may want to specify the minimum starting annotation as `java -jar JobHandler.jar POS /home/jdoe/job123 TOKEN`.

3. Locate your newly annotated files, in Thrift-serialized format (and automatically cached in the your locally-running Curator's database), in a folder copied to your original input directory. The output folder will be named in the form `ANNOTATION_output`, e.g. `.../job123/POS_output/`.

### Examining the Log Files ###

Although we have attempted to log as much as possible to standard text files, your best source of log information on a Hadoop/MapReduce job will come from Hadoop's logs. (This is due to an oddity in the way Hadoop handles both standard output and shared loggers.) 

To access the Hadoop logs:

1. Navigate to your Hadoop JobTracker's web interface. The address may look something like this: http://somejobtracker.cs.illinois.edu:50030/jobtracker.jsp

2. Your jobs will show up there, under the list of Running, Completed, or Failed jobs. Click on the job ID of the job you're interested in.

3. Click on the **reduce** link. (All the interesting work our program does occurs in the Reduce phase, so that's the only place to check for logs.)

4. Click on a **Task** link. There should be one task per document that you passed in.

5. On the far right of the screen are Task Log links. You probably will want to click the **All** link.

6. Scroll through the log you've found!

Manifest
----------------------------------------

Ensure that, in addition to your standard Hadoop and custom Curator installations on a GNU/Linux system, the following files are present in your directories:

* `HadoopInterface.jar` (When you run the Ant build, this should automatically package all the program's dependencies inside the JAR.)

* `curator-0.6.9/dist/`

    * `bin/`

         * `curator-local.sh` modified to add stuff to the class path. Full class path line: `COMPONENT_CLASSPATH=$CURATOR_BASE:$COMPONENTDIR/curator-server.jar:$COMPONENTDIR/illinois-tokenizer-server.jar:$COMPONENTDIR/illinois-pos-server.jar:$COMPONENTDIR/illinois-chunker-server.jar:$COMPONENTDIR/illinois-coref-server.jar:$COMPONENTDIR/stanford-parser-server.jar:$COMPONENTDIR/curator-interfaces.jar:$COMPONENTDIR/illinois-ner-extended-server.jar`

Modifications to the Curator
----------------------------------------

This package currently requires a custom-built version of the Curator which makes the following modifications:

* In `[Curator home]/curator-interfaces/`, `curator.thrift` and all `[annotator].thrift` files: added `getTimeOfLastAnnotation()` method.
* In `[Curator home]/curator-server/CuratorHandler.java`: added `getTimeOfLastAnnotation()` and modified `performAnnotation()` to update a new private field `lastAnnotationTime`.
* In `[Curator home]/curator-server/CuratorHandler.java`: added the private `InactiveCuratorKiller` class, which periodically queries the `CuratorHandler`'s last annotation time and calls `Runtime.exit()` if the last annotation took place too long ago. Also modified the `runServer()` method's signature to take a
  Curator.Iface (the CuratorHandler), and had it spawn a thread of the `InactiveCuratorKiller` right before calling `server.serve()`.
* In `[Curator home]/curator-annotators/`: each Server/Handler pair modified to monitor and kill the handler after a period of inactivity, almost identical to the modifications made in `CuratorServer/CuratorHandler`.

Troubleshooting
----------------------------------------

* Make sure that you can launch the annotators on the Hadoop node using the same commands you find in the Hadoop logs. For instance, for tools that do not run in local mode (all tools except the tokenizer, POS, Stanford parser, and chunker), each time a Hadoop node annotates its first document using a given annotation, it will print the command it used to launch the annotator. If you cannot use that same command to launch the annotator manually on the Hadoop node, there will be problems.

* If you have issues running the annotation tools in Hadoop (especially the Charniak parser), try passing an additional argument when you launch the job on Hadoop. (To do so, you may have to modify the script that gives the job to the Hadoop Job Handler.) Add the argument `-lib some\_library\_path` to the call to Hadoop. That library path should be an absolute path (i.e., one beginning with `/`), and inside that directory should be the Thrift library files (e.g., `libthrift.so.0`).

* If you are using the JobHandler wrapper class and having problems with the automatic dependency handling, you can force a particular starting annotation by specifying it as a third parameter: `java -jar JobHandler.jar REQUESTED_ANNOTATION /absolute/path/to/input_dir STARTING_ANNOTATION`.

Configuring Curator without Installing Software on Hadoop Nodes
----------------------------------------
If you are unable to install the Curator to a common location on each node of your Hadoop cluster, you will have to run it from a networked disk. This is sub-optimal due to the network transfer overhead, but it may be unavoidable. Ideally, each node would be connected to its own networked disk, but again, this may not be possible.

Thus, it may be the case that all nodes of the Hadoop cluster must access the same disk, launching the Curator from there. In this case, you will need to create Curator directories like this:

* /path/to/shared\_location

    * curator\_1
	* curator\_2
	* curator\_3
	* . . .
	* curator\_*n*

where *n* is the number of nodes in your Hadoop cluster. The actual name of those Curator directories is unimportant, so long as they have "\_*n*" as a suffix. In our case, we chose to name them `curator-0.6.9_1` through `curator-0.6.9_32`.

Having made many copies of your Curator installation, you will need to modify the `launch_hadoop_job.sh` script to point to the Curator installations *without* any suffixes. So, in our case, as we had Curator directories `curator-0.6.9_1` through `curator-0.6.9_32`, we modified the line in the script that assigns `CURATOR_DIR_ON_HADOOP_NODES` as follows:

    CURATOR_DIR_ON_HADOOP_NODES=/project/cogcomp/curator-0.6.9

Then, you must also modify the line of `launch_hadoop_job.sh` that assigns `LAUNCH_HADOOP_COMMAND` to use the "-shared" flag. In our case, that line looked like this:

	LAUNCH_HADOOP_COMMAND="bin/hadoop jar /project/cogcomp/HadoopInterface/HadoopInterface.jar edu.illinois.cs.cogcomp.hadoopinterface.HadoopInterface -d $INPUT_DIR_IN_HDFS -m $ANNOTATION_TOOL_TO_RUN -out $OUTPUT_DIR_IN_HDFS -reduces 3 -curator $CURATOR_DIR_ON_HADOOP_NODES -shared"

With that done, when you launch a job using the JobHandler, each node involved in the MapReduce job will automatically figure out which copy of the Curator is available for its use and "lock" that copy so that only that node can use it (for the time being, of course). Those locks will be ignored once they are about an hour old--i.e., we will assume the machine locking that copy of the Curator is no longer using it--but if you run into trouble with locked copies, you may have to manually delete the lock files between jobs using the `cleanup.sh` script.
