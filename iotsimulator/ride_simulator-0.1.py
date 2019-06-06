#!/usr/bin/env python3

import uuid
import json
import random
import datetime
import argparse


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-c", "--count", type=int, default=1, help="The number of messages to send, default 1")
    args = vars(ap.parse_args())

    # Get number of messages from argparse, default 1
    nummsgs = args['count']

    # Fixed values
    rand_guids = []
    for x in range(32):
        rand_guids.append(str(uuid.uuid4()))
    format_str = "urn:themepark:sensors:rollercoaster"
    iotmsg = {
                "guid": "0-ZZZ123456786F",
                "destination": "4ecae5bb-9fb5-44e9-937f-8a44135cbde0",
                "eventTime": "2019-05-15T03:51:55.730530Z",
                "payload": {
                    "format": format_str,
                    "data": {
                        "passengerCount": 1,
                        "totalTrainMass": 95.7,
                        "temperature": 52.0,
                        "humidity": 46.7
                    }
                }
            }

    msglist = []
    for counter in range(nummsgs):
        print(counter)
        today = datetime.datetime.today()
        datestr = today.isoformat()
        iotmsg['eventTime'] = datestr
        iotmsg['guid'] = random.choice(rand_guids)
        iotmsg['payload']['data']['passengerCount'] = random.randrange(0, 12)
        iotmsg['payload']['data']['totalTrainMass'] = random.uniform(36.0, 100) \
                                                      * iotmsg['payload']['data']['passengerCount'] \
                                                      + (550 * 6)
        iotmsg['payload']['data']['temperature'] = random.uniform(0.0, 110.0)
        iotmsg['payload']['data']['humidity'] = random.uniform(0.0, 100.0)
        msglist.append(iotmsg)
    print(json.dumps(msglist, indent=4))


if __name__ == '__main__':
    main()
