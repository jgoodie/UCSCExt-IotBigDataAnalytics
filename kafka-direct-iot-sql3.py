#!/usr/bin/env python3
"""
 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      kafka-direct-iot-sql3.py \
      localhost:9092 test`
"""
from __future__ import print_function

import sys
import re
import json

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import OffsetRange
from pyspark.sql.functions import countDistinct, avg, mean, stddev, format_number
from operator import add

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)
    sqlContext = SQLContext(sc)
    sc.setLogLevel("WARN")

    ##############
    # Globals
    ##############
    globals()['maxTemp'] = sc.accumulator(0.0)

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    # jsonDStream = kvs.map(lambda (key, value): value)
    jsonDStream = kvs.map(lambda key_value: key_value[1])


    # Define function to process RDDs of the json DStream to convert them
    #   to DataFrame and run SQL queries
    def process(time, rdd):
        # Match local function variables to global variables
        maxTemp = globals()['maxTemp']

        print("========= %s =========" % str(time))

        try:
            # Parse the one line JSON string from the DStream RDD
            jsonString = rdd.map(lambda x: re.sub(r"\s+", "", x, flags=re.UNICODE)).reduce(add)
            # print("jsonString = %s" % str(jsonString))

            # Convert the JSON string to an RDD
            jsonRDDString = sc.parallelize([str(jsonString)])

            # Convert the JSON RDD to Spark SQL Context
            jsonRDD = sqlContext.read.json(jsonRDDString)

            # Register the JSON SQL Context as a temporary SQL Table
            # print("JSON Schema\n=====")
            # jsonRDD.printSchema()
            jsonRDD.registerTempTable("iotmsgsTable")

            #############
            # Processing and Analytics go here
            #############
            # sqlContext.sql("select payload.data.* from iotmsgsTable order by totalTrainMass desc").show(n=100)
            # sqlContext.sql("select payload.data.totalTrainMass from iotmsgsTable order by totalTrainMass desc").show(n=100)
            # sqlContext.sql("select payload.data.* from iotmsgsTable").describe().show()
            # sqlContext.sql("select payload.data.* from iotmsgsTable").groupBy("blockLocation").mean().show()

            print("Train Mass with Passengers Stats:\n=====")
            sqlContext.sql("select payload.data.totalTrainMass from iotmsgsTable").describe().show()

            print("Speed by Track Location:\n=====")
            speed = sqlContext.sql("select payload.data.blockLocation, \
                    payload.data.speed from iotmsgsTable").groupBy("blockLocation").mean()
            speed.select(speed['blockLocation'], format_number(speed['avg(speed)'], 4).alias('Average Speed')).show()

            print("Vibration by Track Location:\n=====")
            vibration = sqlContext.sql("select payload.data.blockLocation, payload.data.vibration1, \
                                        payload.data.vibration2 from iotmsgsTable").groupBy("blockLocation").mean()
            vibration.select(vibration['blockLocation'],
                             format_number(vibration['avg(vibration1)'], 6).alias('Average Vibration1'),
                             format_number(vibration['avg(vibration2)'], 6).alias('Average Vibration2')).show()

            # Search
            print("Vibration Greater than 0.99 may indicate maintenance is needed:\n=====")
            sqlContext.sql("select eventTime, payload.data.vibration1, payload.data.vibration2 \
                                        from iotmsgsTable where payload.data.vibration1 > 0.99").show(n=100)

            print("Safety Harness metrics less than 1 indicates maintenance needed:\n=====")
            harness = sqlContext.sql("select payload.data.blockLocation, payload.data.safetyHarness \
                                     from iotmsgsTable").groupBy("blockLocation").mean()
            harness.select(harness['blockLocation'],
                           format_number(harness['avg(safetyHarness)'], 2).alias('Safety Harness')).show()

            print("Environmental Conditions by Track Location:\n=====")
            env = sqlContext.sql("select payload.data.blockLocation, payload.data.ambientTemp, \
                                        payload.data.ambientHumidity, \
                                        payload.data.moisture from iotmsgsTable").groupBy("blockLocation").mean()
            env.select(env['blockLocation'],
                       format_number(env['avg(ambientTemp)'], 6).alias('Ambient Temp'),
                       format_number(env['avg(ambientHumidity)'], 6).alias('Humidity'),
                       format_number(env['avg(moisture)'], 6).alias('Moisture')).show()

            # Sort
            # sqlContext.sql("select payload.data.temperature from iotmsgsTable order by temperature desc").show(n=100)

            # currentTemp = sqlContext.sql("select payload.data.temperature from iotmsgsTable order by temperature desc").collect()[0].temperature
            # print("Current temp = " + str(currentTemp))
            # if (currentTemp > maxTemp.value):
            #  maxTemp.value = currentTemp
            # print("Max temp = " + str(maxTemp.value))

            # # Search
            # print("Vibration Greater than 0.99:\n=====")
            # sqlContext.sql("select eventTime, payload.data.vibration1, payload.data.vibration2 \
            #                 from iotmsgsTable where payload.data.vibration1 > 0.99").show(n=100)

            # Filter
            #sqlContext.sql("select payload.data.temperature from iotmsgsTable where payload.data.temperature > 85").show(n=100)

            # Clean-up
            sqlContext.dropTempTable("iotmsgsTable")
        # Catch any exceptions
        except:
            pass


    # Process each RDD of the DStream coming in from Kafka
    jsonDStream.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()
