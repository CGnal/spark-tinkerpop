#!/bin/bash

INPUT_FILE=data/Amazon0302.txt
LIBEXT=libext

sbt -Dhadoop.home.dir=$WIN_HADOOP_HOME "project examples" "run cross-sell -i $INPUT_FILE -l $LIBEXT -h $HADOOP_HOME/etc/hadoop"