import paho.mqtt.client as mqtt
from random import randrange, uniform
from datetime import datetime
import time
import json
import random
import os


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,"Vehicle_Speed_Publisher")
client.connect("127.0.0.1")
truck = input("Enter Truck Name : ")

dir = str(os.getcwd())
path = dir +"\\" +  truck 
input_file = open(path)
json_array = json.load(input_file)
coordinates = json_array['coordinates']

Driver_ID = input("Enter Driver ID : ")
Vehicle_ID = "IND" + str(random.randint(1000, 9999))

for coordinate in coordinates:
    data = {
        "Driver_ID":  Driver_ID,
        "Vehicle_ID":  Vehicle_ID,
        "Latitude": coordinate[1],
        "Longitude": coordinate[0],
        "Current Speed": random.randint(10, 120),
        "Event_Time": str(datetime.now())
    }
    jsonData = json.dumps(data)
    client.publish("SPA_Vehicle_Speed", jsonData)
    print(data)
    time.sleep(2)