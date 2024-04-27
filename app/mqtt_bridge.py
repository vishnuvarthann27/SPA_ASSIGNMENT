from itsdangerous import Serializer
import paho.mqtt.client as mqtt
from confluent_kafka import Producer
import json


KAFKA_CONFIG = ''

def read_config():
  config = {}
  with open("client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

def on_message(client, userdata, message):
    msg = str(message.payload.decode("utf-8", "ignore"))
    jsonMessage = json.loads(msg)  
    print("Received message: ", jsonMessage)
    

    producer = Producer(KAFKA_CONFIG)
    producer.produce("SPA_Vehicle_Data", json.dumps(jsonMessage),jsonMessage['Driver_ID'])
    producer.flush()

if __name__ == '__main__':
    KAFKA_CONFIG = read_config()

    #mqttBroker = "mqtt.eclipseprojects.io"
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,"Vehicle_Speed_MQTT_Bridge")
    client.connect("127.0.0.1")

    client.subscribe("SPA_Vehicle_Speed")
    client.on_message = on_message
    client.loop_forever()