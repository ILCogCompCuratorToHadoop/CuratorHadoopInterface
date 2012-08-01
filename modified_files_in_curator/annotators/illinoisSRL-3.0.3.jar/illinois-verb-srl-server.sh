#!/bin/sh
START=$PWD
DIRNAME=`dirname "$0"`
SCRIPTS_HOME=`cd "$DIRNAME" > /dev/null && pwd`
CURATOR_BASE=$SCRIPTS_HOME/..
CURATOR_BASE=`cd "$CURATOR_BASE" > /dev/null && pwd`
LIBDIR=$CURATOR_BASE/lib
COMPONENTDIR=$CURATOR_BASE/components
CONFIGDIR=$CURATOR_BASE/configs
MODEL_JAR=illinoisSRL-verb-models-3.0.3.jar
BIN_JAR=illinoisSRL-3.0.3.1.jar

COMPONENT_CLASSPATH=$COMPONENTDIR/illinois-verb-srl-server.jar

MODEL_CLASSPATH=$LIBDIR/$MODEL_JAR

LIB_CLASSPATH=$CURATOR_BASE:$LIBDIR/$BIN_JAR:$LIBDIR/commons-cli-1.2.jar:$LIBDIR/commons-collections-3.2.1.jar:$LIBDIR/commons-configuration-1.6.jar:$LIBDIR/commons-lang-2.5.jar:$LIBDIR/commons-logging-1.1.1.jar:$LIBDIR/coreUtilities-0.1.2.jar:$LIBDIR/curator-client-0.6.jar:$LIBDIR/edison-0.2.9.jar:$LIBDIR/jwnl-1.4_rc3.jar:$LIBDIR/LBJ2Library-2.8.2.jar:$LIBDIR/libthrift.jar:$LIBDIR/logback-classic-0.9.17.jar:$LIBDIR/logback-core-0.9.17.jar:$LIBDIR/slf4j-api-1.5.8.jar:$LIBDIR/snowball-1.0.jar

CLASSPATH=$COMPONENT_CLASSPATH:$LIB_CLASSPATH:$MODEL_CLASSPATH:$CONFIGDIR

cd $CURATOR_BASE

echo "LIBDIR: $LIBDIR"

CMD="java -cp $CLASSPATH -Dhome=$CURATOR_BASE -Xmx10G edu.illinois.cs.cogcomp.annotation.server.IllinoisVerbSRLServer -c $CONFIGDIR/srl-config.properties  $@"
#CMD="java -cp $CLASSPATH -Dhome=$CURATOR_BASE -Xmx10G edu.illinois.cs.cogcomp.annotation.server.IllinoisVerbSRLServer  $@"

echo $CMD
exec $CMD
cd $START
