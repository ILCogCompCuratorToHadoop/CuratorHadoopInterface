#!/bin/bash

VERSION=3.0.3
echo "Deploying Illinois SRL version $VERSION"

# First copy all the models
BIN=target/classes
LBJBIN=target/classes
SRC=src/main/java/
GSP=$SRC

MODELSDIR=models


# now deploy
echo "Deploying code jar"
mvn clean package deploy

