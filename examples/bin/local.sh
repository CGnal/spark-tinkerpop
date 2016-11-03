#!/bin/bash

# Read input arguments

while [[ $# -gt 1 ]]
do
key="$1"

case $key in
    -n|--name)
    APP_NAME="$2"
    shift
    ;;
    -d|--lib-dir)
    X_LIBEXT="$2"
    shift
    ;;
    -i|--input)
    X_INPUT_FILE="$2"
    shift
    ;;
    *)
    ;;
esac
shift
done

# Application name
APP_NAME=${APP_NAME:-graphson}

# Cross-sell Application Configurations
X_INPUT_FILE=${X_INPUT_FILE:-data/Amazon0302.small.txt}
X_LIBEXT=${X_LIBEXT:-libext}

# Spark configurations
NUM_THREADS=2
EXEC_MEM=2g
NUM_PARTS=4
SHUFFLE=0.1
STORAGE=0.4

### Start ###

echo "[app-info]   Starting ..."
echo "[app-conf]   --name $APP_NAME"
echo "[app-conf]   --lib-dir $X_LIBEXT"
echo "[app-conf]   --input $X_INPUT_FILE"
echo "[app-conf]   --threads $NUM_THREADS"
echo "[app-conf]   --memory $EXEC_MEM"
echo "[app-conf]   --partitions $NUM_PARTS"
echo "[app-conf]   --shuffle $SHUFFLE"
echo "[app-conf]   --storage $STORAGE"

#sbt -Dhadoop.home.dir=$HADOOP_HOME \
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