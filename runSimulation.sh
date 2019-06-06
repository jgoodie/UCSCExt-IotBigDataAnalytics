#!/usr/bin/bash

KAFKA_HEAP_OPTS="-Xmx20M" $HOME/ride_simulator-3.0.py -c 10  | /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic iotmsgs
