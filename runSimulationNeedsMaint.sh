#!/usr/bin/bash
SIM_DIR=$HOME/UCSCExt-IotBigDataAnalytics
KAFKA_HEAP_OPTS="-Xmx20M" $SIM_DIR/ride_simulator-3.0.py -c 10 -m | /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic iotmsgs
