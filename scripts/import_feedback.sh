#This script imports the whole bucket from ES into the HFS
#COUCHBASE_IP=192.168.56.105
#COUCHBASE_PORT=8091
source env.sh

hadoop fs -rm -r DUMP
sqoop import --username feedback --verbose \
    --connect http://$COUCHBASE_IP:$COUCHBASE_PORT/pools --table DUMP
hadoop fs -getmerge  DUMP feedback
hadoop fs -rm -r DUMP*
sqoop import --username meta-feedback --verbose \
    --connect http://$COUCHBASE_IP:$COUCHBASE_PORT/pools --table DUMP
hadoop fs -mv feedback DUMP/feedback
hadoop fs -rm DUMP.java
hadoop fs -rm DUMP/*.crc
hadoop fs -rm DUMP/_SUCCESS*
hadoop fs -getmerge  DUMP/ input.txt
