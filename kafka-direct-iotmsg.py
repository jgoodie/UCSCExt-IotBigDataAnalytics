#!/usr/bin/env python3

"""
 Processes direct stream from kafka, '\n' delimited text directly received
   every 2 seconds.
 Usage: kafka-direct-iotmsg.py <broker_list> <topic>

 To run this on your local machine, you need to setup Kafka and create a
   producer first, see:
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      kafka-direct-iotmsg.py \
      localhost:9092 iotmsgs`
"""
from __future__ import print_function
import argparse
import sys
import json
import re

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from operator import add

###############
# Globals
###############
tempTotal = 0.0
tempCount = 0
tempAvg = 0.0
humidityTotal = 0.0
humidityCount = 0
humidityAvg = 0
passengerTotal = 0
passengerCount = 0
passengerAvg = 0.0
trainMassTotal = 0.0
trainMassCount = 0.0
trainMassAvg = 0.0
trainSpeedTotal = 0.0
trainSpeedCount = 0.0
trainSpeedAvg = 0.0
trainAccelerationTotal = 0.0
trainAccelerationCount = 0.0
trainAccelerationAvg = 0.0
trainVibrationTotal = 0.0
trainVibrationCount = 0.0
trainVibrationAvg = 0.0
chainDogTotal = 0.0
chainDogCount = 0.0
chainDogAvg = 0.0
brakeEngagedTotal = 0.0
brakeEngagedCount = 0.0
brakeEngagedAvg = 0.0
moistureTotal = 0.0
moistureCount = 0.0
moistureAvg = 0.0


############
# Processing
############
# foreach function to iterate over each RDD of a DStream
def processTemperatureRDD(time, rdd):
  # Declare Global Variables
  global tempTotal
  global tempCount
  global tempAvg

  tempList = rdd.collect()
  for tempFloat in tempList:
    tempTotal += float(tempFloat)
    tempCount += 1
    tempAvg = tempTotal / tempCount
  print("Ambient Temp Total = " + str(tempTotal))
  print("Ambient Temp  Count = " + str(tempCount))
  print("Avg Ambient Temp = " + str(tempAvg))
  print("-------------------------------------------")


def processHumidityRDD(time, rdd):
  # Declare Global Variables
  global humidityTotal
  global humidityCount
  global humidityAvg

  humidityList = rdd.collect()
  for humidityFloat in humidityList:
    humidityTotal += float(humidityFloat)
    humidityCount += 1
    humidityAvg= tempTotal / tempCount
  print("Ambient Humidity Total = " + str(humidityTotal))
  print("Ambient Humidity Count = " + str(humidityCount))
  print("Avg Ambient Humidity = " + str(humidityAvg))
  print("-------------------------------------------")


def processPassengerRDD(time, rdd):
  # Declare Global Variables
  global passengerTotal
  global passengerCount
  global passengerAvg

  passengerList = rdd.collect()
  for passenger in passengerList:
    passengerTotal += int(passenger)
    passengerCount += 1
    passengerAvg = float(passengerTotal) / float(passengerCount)
  print("Passenger Total = " + str(passengerTotal))
  print("Passenger Count = " + str(passengerCount))
  print("Avg Passenger = " + str(passengerAvg))
  print("-------------------------------------------")


def processTrainMassRDD(time, rdd):
  # Declare Global Variables
  global trainMassTotal
  global trainMassCount
  global trainMassAvg

  trainMassList = rdd.collect()
  for mass in trainMassList:
    trainMassTotal += float(mass)
    trainMassCount += 1
    trainMassAvg = float(trainMassTotal) / float(trainMassCount)
  print("Total Mass = " + str(trainMassTotal))
  print("Train Count = " + str(trainMassCount))
  print("Avg Train Mass = " + str(trainMassAvg))
  print("-------------------------------------------")


def processTrainSpeedRDD(time, rdd):
  # Declare Global Variables
  global trainSpeedTotal
  global trainSpeedCount
  global trainSpeedAvg

  trainSpeedList = rdd.collect()
  for speed in trainSpeedList:
    trainSpeedTotal += float(speed)
    trainSpeedCount += 1
    trainSpeedAvg = float(trainSpeedTotal) / float(trainSpeedCount)
  #print("Total Mass = " + str(trainSpeedTotal))
  #print("Train Count = " + str(trainSpeedCount))
  print("Avg Train Speed = " + str(trainSpeedAvg))
  print("-------------------------------------------")


def processTrainAccelerationRDD(time, rdd):
  # Declare Global Variables
  global trainAccelerationTotal
  global trainAccelerationCount
  global trainAccelerationAvg

  trainAccelerationList = rdd.collect()
  for acceleration in trainAccelerationList:
    trainAccelerationTotal += float(acceleration)
    trainAccelerationCount += 1
    trainAccelerationAvg = float(trainSpeedTotal) / float(trainSpeedCount)
  #print("Total Mass = " + str(trainSpeedTotal))
  #print("Train Count = " + str(trainSpeedCount))
  print("Avg Train Acceleration = " + str(trainAccelerationAvg))
  print("-------------------------------------------")


def processTrainVibrationRDD(time, rdd):
  # Declare Global Variables
  global trainVibrationTotal
  global trainVibrationCount
  global trainVibrationAvg

  trainVibrationList = rdd.collect()
  for vibration in trainVibrationList:
    trainVibrationTotal += float(vibration)
    trainVibrationCount += 1
    trainVibrationAvg = float(trainVibrationTotal) / float(trainVibrationCount)
  #print("Total Mass = " + str(trainSpeedTotal))
  print("Train Vibration Count = " + str(trainVibrationCount))
  print("Avg Train Vibration = " + str(trainVibrationAvg))
  print("-------------------------------------------")


def processTrainChainDogRDD(time, rdd):
  # Declare Global Variables
  global chainDogTotal
  global chainDogCount
  global chainDogAvg

  chainDogList = rdd.collect()
  for chainDog in chainDogList:
    chainDogTotal += float(chainDog)
    chainDogCount += 1
    chainDogAvg = float(chainDogTotal) / float(chainDogCount)
  #print("Total Mass = " + str(trainSpeedTotal))
  #print("Train Vibration Count = " + str(chainDogCount))
  print("Avg Time on the Chain Dog = " + str(chainDogAvg))
  print("-------------------------------------------")


def processTrainBrakeEngagedDogRDD(time, rdd):
  # Declare Global Variables
  global brakeEngagedTotal
  global brakeEngagedCount
  global brakeEngagedAvg

  brakeEngagedList = rdd.collect()
  for brake in brakeEngagedList:
    brakeEngagedTotal += float(brake)
    brakeEngagedCount += 1
    brakeEngagedAvg = float(chainDogTotal) / float(chainDogCount)
  #print("Total Mass = " + str(trainSpeedTotal))
  #print("Train Vibration Count = " + str(chainDogCount))
  print("Avg Time Braking = " + str(brakeEngagedAvg))
  print("-------------------------------------------")


def processMoistureRDD(time, rdd):
  # Declare Global Variables
  global moistureTotal
  global moistureCount
  global moistureAvg

  moistureList = rdd.collect()
  for moisture in moistureList:
    moistureTotal += float(moisture)
    moistureCount += 1
    moistureAvg = float(moistureTotal) / float(moistureCount)
  #print("Total Mass = " + str(trainSpeedTotal))
  #print("Train Vibration Count = " + str(chainDogCount))
  print("Avg Moisture Reading = " + str(moistureAvg))
  print("-------------------------------------------")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-b", "--brokers", help="The kafka broker i.e: localhost:9092")
    ap.add_argument("-t", "--topics", nargs='+', default=[], help="the topic, i.e: iotmsgs")
    args = vars(ap.parse_args())

    sc = SparkContext(appName="PythonStreamingRollerCoaster")
    ssc = StreamingContext(sc, 2)
    sc.setLogLevel("WARN")

    brokers, topic = args['brokers'], args['topics']
    kvs = KafkaUtils.createDirectStream(ssc, topic, {"metadata.broker.list": brokers})

    # Read in the Kafka Direct Stream into a TransformedDStream
    lines = kvs.map(lambda x: x[1])
    jsonLines = lines.map(lambda x: re.sub(r"\s+", "", x, flags=re.UNICODE))

    # Search for Temperature IoT data values (assumes jsonLines are split(','))
    tempValues = jsonLines.filter(lambda x: re.findall(r"ambientTemp.*", x, 0))
    # tempValues.pprint(num=10000)
    # Parse out just the value without the JSON key
    parsedTempValues = tempValues.map(lambda x: re.sub(r"\"ambientTemp\":", "", x).split(',')[0])


    # Search for Humidity IoT data values (assumes jsonLines are split(','))
    humidityValues = jsonLines.filter(lambda x: re.findall(r"ambientHumidity.*", x, 0))
    # humidityValues.pprint(num=10000)
    # Parse out just the value without the JSON key
    parsedHumidityValues = humidityValues.map(lambda x: re.sub(r"\"ambientHumidity\":", "", x).split(',')[0])

    # Search for Track Moisture IoT data values (assumes jsonLines are split(','))
    trainMoistureValues = jsonLines.filter(lambda x: re.findall(r"moisture.*", x, 0))
    parsedTrainMoistureValues = trainMoistureValues.map(lambda x: re.sub(r"\"moisture\":", "", x).split(',')[0])

    # Search for Passenger IoT data values (assumes jsonLines are split(','))
    passengerValues = jsonLines.filter(lambda x: re.findall(r"passengerCount.*", x, 0))
    # humidityValues.pprint(num=10000)
    # Parse out just the value without the JSON key
    parsedPassengerValues = passengerValues.map(lambda x: re.sub(r"\"passengerCount\":", "", x).split(',')[0])

    # Search for Train Mass IoT data values (assumes jsonLines are split(','))
    trainMassValues = jsonLines.filter(lambda x: re.findall(r"totalTrainMass.*", x, 0))
    parsedTrainMassValues = trainMassValues.map(lambda x: re.sub(r"\"totalTrainMass\":", "", x).split(',')[0])

    # Search for Train speed IoT data values (assumes jsonLines are split(','))
    trainSpeedValues = jsonLines.filter(lambda x: re.findall(r"speed.*", x, 0))
    parsedTrainSpeedValues = trainSpeedValues.map(lambda x: re.sub(r"\"speed\":", "", x).split(',')[0])

    # Search for Train acceleration IoT data values (assumes jsonLines are split(','))
    trainAccelerationValues = jsonLines.filter(lambda x: re.findall(r"acceleration.*", x, 0))
    parsedTrainAccelerationValues = trainAccelerationValues.map(lambda x: re.sub(r"\"acceleration\":", "", x).split(',')[0])

    # Search for Train vibration data values (assumes jsonLines are split(','))
    trainVibrationValues = jsonLines.filter(lambda x: re.findall(r"vibration.*", x, 0))
    #trainVibrationValues.pprint(num=1000)
    parsedTrainVibrationValues = trainVibrationValues.map(lambda x: re.sub(r"\"vibration.*\":", "", x).split(',')[0])
    #parsedTrainVibrationValues.pprint(num=1000)

    # Search for Train chainDog IoT data values (assumes jsonLines are split(','))
    trainChainDogValues = jsonLines.filter(lambda x: re.findall(r"chainDog.*", x, 0))
    parsedTrainChainDogValues = trainChainDogValues.map(lambda x: re.sub(r"\"chainDog\":", "", x).split(',')[0])
    #parsedTrainChainDogValues.pprint(num=1000)

    # Search for Train Brake IoT data values (assumes jsonLines are split(','))
    trainBrakeEngagedValues = jsonLines.filter(lambda x: re.findall(r"brakeEngaged.*", x, 0))
    parsedTrainBrakeEngagedValues = trainBrakeEngagedValues.map(lambda x: re.sub(r"\"brakeEngaged\":", "", x).split(',')[0])

    # Count how many values were parsed
    # countMap = parsedTrainVibrationValues.map(lambda x: 1).reduce(add)
    # valueCount = countMap.map(lambda x: "Total Count of Vibration Msgs: " + str(x))
    # valueCount.pprint()

    # Sort all the IoT values
    # sortedValues = parsedTempValues.transform(lambda x: x.sortBy(lambda y: y))
    # sortedValues.pprint(num=10000)
    # sortedValues = parsedHumidityValues.transform(lambda x: x.sortBy(lambda y: y))
    # sortedValues.pprint(num=10000)

    # Iterate on each RDD in parsedTempValues DStream to use w/global variables
    parsedTempValues.foreachRDD(processTemperatureRDD)
    parsedHumidityValues.foreachRDD(processHumidityRDD)
    parsedPassengerValues.foreachRDD(processPassengerRDD)
    parsedTrainMassValues.foreachRDD(processTrainMassRDD)
    parsedTrainSpeedValues.foreachRDD(processTrainSpeedRDD)
    parsedTrainAccelerationValues.foreachRDD(processTrainAccelerationRDD)
    parsedTrainVibrationValues.foreachRDD(processTrainVibrationRDD)
    parsedTrainChainDogValues.foreachRDD(processTrainChainDogRDD)
    parsedTrainBrakeEngagedValues.foreachRDD(processTrainBrakeEngagedDogRDD)
    parsedTrainMoistureValues.foreachRDD(processMoistureRDD)

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()

