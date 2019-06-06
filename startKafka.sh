#!/usr/bin/bash

KAFKA_HOME=/opt/kafka

$KAFKA_HOME/bin/kafka-server-stop.sh
$KAFKA_HOME/bin/zookeeper-server-stop.sh
pkill -9 java
pkill -9 python
pkill -9 python3

KAFKA_HEAP_OPTS="-Xmx64M" $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties > /tmp/zookeeper.log &
KAFKA_HEAP_OPTS="-Xmx400M" $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties > /tmp/kafka.log 2>&1 &
