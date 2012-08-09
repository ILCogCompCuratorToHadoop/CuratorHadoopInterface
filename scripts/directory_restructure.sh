#!/bin/bash

# A script to rearrange output from the Ant build of the CuratorHadoopInterface into the recommended directory structure specified in the README.
# You must run this script from its original location in the scripts directory!

echo -e "Copying Ant output into recommended directory structure..."

mkdir ../../CuratorHadoopInterface
mkdir ../../JobHandler
cp ../out/artifacts/Jar/CuratorHadoopInterface.jar ../../CuratorHadoopInterface
cp ../out/artifacts/JobHandler/JobHandler.jar ../../JobHandler
cp -R ../lib ../../JobHandler
cp -R ../scripts ../../JobHandler

echo -e "Directory restructuring is complete!"
