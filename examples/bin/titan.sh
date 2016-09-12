#!/bin/bash

# Application name
APP_NAME=cross-sell

# Cross-sell Application Configurations
X_INPUT_FILE=data/Amazon0302.small.txt
X_LIBEXT=libext

# Spark configurations
NUM_THREADS=2
EXEC_MEM=2g
NUM_PARTS=4
SHUFFLE=0.1
STORAGE=0.4

#sbt -Dhadoop.home.dir=$WIN_HADOOP_HOME \
sbt \
"project examples" \
"run $APP_NAME \
-i $X_INPUT_FILE \
-l $X_LIBEXT \
-h $HADOOP_HOME/etc/hadoop \
-t $NUM_THREADS \
-m $EXEC_MEM \
-p $NUM_PARTS \
-x $SHUFFLE \
-s $STORAGE"