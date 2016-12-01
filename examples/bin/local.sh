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
    --debug)
    X_DEBUG=true
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

if [ "$X_DEBUG" = true ];
then
    X_DEBUG_OPT="-Dbuild.debug"
else
    X_DEBUG_OPT=""
fi

# Spark configurations
NUM_THREADS=4
EXEC_MEM=2g
NUM_PARTS=8
SHUFFLE=0.1
STORAGE=0.7

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
echo "[app-conf]   --debug $X_DEBUG_OPT"

sbt $X_DEBUG_OPT \
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