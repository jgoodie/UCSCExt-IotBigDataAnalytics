#!/usr/bin/env python3

import uuid
import json
import datetime
import argparse
import numpy as np
from time import sleep
# from kafka import KafkaProducer
# from kafka.errors import KafkaError

#####################################################################
#
# NOTES:
#
# Create a function or way to pump in "interesting" data. 
# Maybe have a different CLI option to send 
# Fix the data to be more "boring" within a specific range
# so that when the "interesting" data is sent, it will be more
# "interesting"
# 
# Add in sensor readings for lap bars for riders 0 = lap bar up
# 1 = lap bar down/engaged.
#
# Change Rain sensor to moisture sensor
#
# Add in CLI option do completely random UUID
#
# Use the python-kafka library to send iotmsgs instead of the kafka
# console producer
#
######################################################################



def gen_iotmsg(location, numPassengers, passengerWeight):
    """
    Function to generate/simulate theme park roller coaster IoT Data
    :return:
    """
    # Time Stamp
    ts = str(datetime.datetime.today().isoformat())

    # Destination UUID
    destination = "b2aebc43-2318-4e4e-884a-0834b276d43d"

    # Assume 8 roller coaster sections with 4 sensors
    # Create 32 random UUIDS (4x8=32 UUIDS)
    uuids = []
    for x in range(32):
        uuids.append(str(uuid.uuid4()))
    randGUID = np.random.choice(uuids)

    # Train mass in Kg
    # Assume passenger weight between 36-136 Kg
    # Assume train car is 550 Kg * 6 cars
    totalTrainMass = passengerWeight * numPassengers + (550 * 6)

    # Weather affecting coaster operations
    randTemp = np.random.uniform(0.0, 110.0)
    randHumidity = np.random.uniform(0.0, 100.0)
    randMoisture = int(np.random.choice([0,0,0,0,0,0,0,0,0,0,0,0,0,0,1]))

    # Assume speed is in 0.0 - 45.0 m/s
    randSpeed = np.random.uniform(0.0, 45.0)

    # Assume acceleration is m/s^2
    randAcceleration = np.random.uniform(0.0, 20.0)

    # Create a sin wave data and use it for the accelerometer data
    sinewave = np.sin(np.linspace(0, 2 * np.pi, 1024))
    randVibration1 = np.random.choice(sinewave)
    cosine = np.cos(np.linspace(0, 2 * np.pi, 1024))
    randVibration2 = np.random.choice(cosine)

    # Probably don't need to initializat this.... but just in case...
    randBrake = np.random.randint(0, 2)

    # Safety Harness engaged?
    harness = np.random.randint(0, 2)

    # Is the train on the chain dog?
    # Assume one hill/chain dog out of 8 ride sections
    randChainDog = int(np.random.choice([0,0,0,0,0,0,0,1]))

    # Try to make this more to be more like a roller coaster than complete random chaos
    if randChainDog or location == "liftHill":
        randSpeed = 5.0
        randAcceleration = 0
        randBrake = 0
        harness = 1
    if location == "endBrake":
        randBrake = 1
        harness = 1
    if location == "station":
        randBrake = 1
        harness = 0
    if location == "midCourseBrake":
        randBrake = 1
        harness = 1
    if "section" in location:
        randBrake = 0
        harness = 1
    iotmsg = {
                    "guid": randGUID,
                    "destination": destination,
                    "eventTime": ts,
                    "payload": {
                        "format": "urn:themepark:sensors:rollercoaster",
                        "data": {
                            "passengerCount": numPassengers,
                            "totalTrainMass": round(totalTrainMass, 1),
                            "safetyHarness": harness,
                            "ambientTemp": round(randTemp, 1),
                            "ambientHumidity": round(randHumidity, 1),
                            "speed": round(randSpeed, 1),
                            "acceleration": round(randAcceleration, 1),
                            "vibration1": randVibration1,
                            "vibration2": randVibration2,
                            "blockLocation": location,
                            "chainDog": randChainDog,
                            "brakeEngaged": randBrake,
                            "moisture": randMoisture

                        }
                    }
                }
    return iotmsg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-c", "--count", type=int, default=1, help="The number of messages to send, default 1")
    ap.add_argument("-r", "--rate", type=int, default=0, help="The rate at which to stream the data")
    args = vars(ap.parse_args())

    # Get number of messages from argparse, default 1
    num_msgs = args['count']

    # Block location
    # https://www.coaster101.com/2011/11/23/coasters-101-brakes-blocks-and-sensors/
    blockLocations = ["station", "liftHill", "section1", "section2", "section3",
                      "midCourseBrake", "section4", "section5", "section6", "endBrake"]
    print("[")
    for msg in range(num_msgs):
        # Random Passenger Count between 0 and 12 passengers
        # Assume 6 cars at 2 passengers per car
        randPassenger = np.random.randint(0, 13)
        passengerWeight = np.random.uniform(36.0, 136)

        for location in blockLocations:
            iotmsg = gen_iotmsg(location, randPassenger, passengerWeight)
            if (location is "endBrake") and (msg == num_msgs-1):
                print(json.dumps(iotmsg, indent=4))
            else:
                print(json.dumps(iotmsg, indent=4) + ",")
            sleep(args['rate'])
    print("]")

if __name__ == '__main__':
    main()
