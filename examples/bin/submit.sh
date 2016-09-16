#!/usr/bin/env bash

# Read input arguments

while [[ $# -gt 1 ]]
do
key="$1"

case $key in
    -n|--name)
    APP_NAME_VALUE="$2"
    shift 
    ;;
    -d|--lib-dir)
    APP_LIB_VALUE="$2"
    shift 
    ;;
    -i|--input)
    APP_DATA_VALUE="$2"
    shift
    ;;
    -p|--principal)
    APP_PRINCIPAL_VALUE="$2"
    shift
    ;;
    -k|--keytab)
    APP_KEYTAB_VALUE="$2"
    shift
    ;;
    *)
    ;;
esac
shift 
done

### Configuration ###
APP_NAME_VALUE=${APP_NAME_VALUE:-titan}
APP_LIB_VALUE=${APP_LIB_VALUE:-libext}
APP_DATA_VALUE=${APP_DATA_VALUE:-data/Amazon.0302.small.txt}

# Application Config
APP_MASTER_VALUE=yarn-client

# HOME directory
CONF_HOME_DIR=$HOME/spark-tinkerpop
HADOOP_HOME_DIR=/etc/hbase/
HADOOP_CONF_DIR=$HADOOP_HOME_DIR/conf

# CONF directory
# APP directory
# LOG directory
CONF_BASE_DIR=$CONF_HOME_DIR/conf
CONF_APP_DIR=$CONF_HOME_DIR/lib
CONF_APP_LOG_DIR=$CONF_HOME_DIR/log
CONF_TITAN=titan-config.yml

# Spark Driver memory
# Spark Driver max result set size
CONF_DRIV_MEM=1g
CONF_DRIV_RES=400m

# Spark Executor memory
# Spark Executor off-heap memory
# Spark Number of executors
# Spark Number of executor-cores
# Spark Default number of partitions
CONF_EXEC_MEM=2g
CONF_EXEC_MEM_OFF=512m
CONF_EXEC_NUM=2
CONF_EXEC_COR=2
CONF_EXEC_PAR=4

# Spark SQL default number of partitions
CONF_SQL_PARTS=16

# Spark Kryo Serialization Buffer Size
CONF_KRYO_BUFFER=512m

# Spark Akka Frame size
CONF_AKKA_FRAME=512

# Spark ClassPaths
CONF_DRIVER_CLASSPATH="$CONF_BASE_DIR:$HADOOP_CONF_DIR:$CONF_BASE_DIR/$CONF_TITAN"
CONF_EXEC_CLASSPATH="$HADOOP_CONF_DIR:$CONF_BASE_DIR/$CONF_TITAN"

# Spark Java Opts
CONF_DRIVER_OPTS="-DAPP_LOG_DIR=$CONF_APP_LOG_DIR -Dcgnal.graph.config=$CONF_TITAN -Djava.net.preferIPv4Stack=true"
CONF_EXEC_OPTS="-DAPP_LOG_DIR=$CONF_APP_LOG_DIR -Dcgnal.graph.config=$CONF_TITAN -Djava.net.preferIPv4Stack=true"

# Spark extra files
CONF_EXEC_FILES="$CONF_BASE_DIR/$CONF_TITAN"

### Start ###

echo "[app-info]   Starting ..."
echo "[app-conf]   --name $APP_NAME_VALUE"
echo "[app-conf]   --lib-dir $APP_LIB_VALUE"
echo "[app-conf]   --input $APP_DATA_VALUE"
echo "[app-conf]   --driver-memory $CONF_DRIV_MEM"
echo "[app-conf]   --executor-memory $CONF_EXEC_MEM"
echo "[app-conf]   --num-executors $CONF_EXEC_NUM"
echo "[app-conf]   --executor-cores $CONF_EXEC_COR"
echo "[app-conf]   --files $CONF_EXEC_FILES"
echo "[app-conf]   --principal $APP_PRINCIPAL_VALUE"
echo "[app-conf]   --keytab $APP_KEYTAB_VALUE"
echo "[app-conf]   --driver-class-path $CONF_DRIVER_CLASSPATH"
echo "[app-conf]   --conf spark.driver.maxResultSize=$CONF_DRIV_RES"
echo "[app-conf]   --conf spark.driver.extraJavaOptions=\"$CONF_DRIVER_OPTS\""
echo "[app-conf]   --conf spark.executor.extraJavaOptions=\"$CONF_EXEC_OPTS\""
echo "[app-conf]   --conf spark.executor.extraClassPath=$CONF_EXEC_CLASSPATH"
echo "[app-conf]   --conf spark.default.parallelism=$CONF_EXEC_PAR"
echo "[app-conf]   --conf spark.akka.frameSize=$CONF_AKKA_FRAME"
echo "[app-conf]   --conf spark.sql.parquet.binaryAsString=true"
echo "[app-conf]   --conf spark.sql.shuffle.partitions=$CONF_SQL_PARTS"
echo "[app-conf]   --conf spark.serializer=org.apache.spark.serializer.KryoSerializer"
echo "[app-conf]   --conf spark.kryoserializer.buffer.max=$CONF_KRYO_BUFFER"
echo "[app-conf]   --conf spark.sql.parquet.useDataSourceApi=false"
echo "[app]        Starting application using Spark-Submit"

spark-submit --class org.cgnal.graphe.application.ApplicationRunner --master yarn-client \
--driver-memory $CONF_DRIV_MEM \
--executor-memory $CONF_EXEC_MEM \
--num-executors $CONF_EXEC_NUM \
--executor-cores $CONF_EXEC_COR \
--driver-class-path $CONF_DRIVER_CLASSPATH \
--files $CONF_EXEC_FILES \
--principal $APP_PRINCIPAL_VALUE \
--keytab $APP_KEYTAB_VALUE \
--conf spark.driver.maxResultSize=$CONF_DRIV_RES \
--conf spark.driver.extraJavaOptions="$CONF_DRIVER_OPTS" \
--conf spark.executor.extraJavaOptions="$CONF_EXEC_OPTS" \
--conf spark.executor.extraClassPath="$CONF_EXEC_CLASSPATH" \
--conf spark.default.parallelism=$CONF_EXEC_PAR \
--conf spark.akka.frameSize=$CONF_AKKA_FRAME \
--conf spark.sql.parquet.binaryAsString=true \
--conf spark.sql.shuffle.partitions=$CONF_SQL_PARTS \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.kryoserializer.buffer.max=$CONF_KRYO_BUFFER \
--conf spark.sql.parquet.useDataSourceApi=false \
$CONF_APP_DIR/spark-tinkerpop-examples-1.0-SNAPSHOT.jar \
$APP_NAME_VALUE \
--lib-dir $APP_LIB_VALUE \
--input $APP_DATA_VALUE \
--hadoop $HADOOP_CONF_DIR \
--kerberos "$APP_PRINCIPAL_VALUE:$APP_KEYTAB_VALUE" \
--threads 0