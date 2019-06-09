#!/usr/bin/bash
KAFKA_DIRECT=$HOME/UCSCExt-IotBigDataAnalytics
# KAFKA_HEAP_OPTS="-Xmx1M" spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.0.0-preview.jar $KAFKA_DIRECT/kafka-direct-iotmsg.py -b localhost:9092 -t iotmsgs
KAFKA_HEAP_OPTS="-Xmx32M" spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.0.0-preview.jar $KAFKA_DIRECT/kafka-direct-iot-sql3.py localhost:9092 iotmsgs
