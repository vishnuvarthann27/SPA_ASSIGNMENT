from confluent_kafka import Producer, Consumer
import json
import collections.abc
collections.Iterable = collections.abc.Iterable
collections.Mapping = collections.abc.Mapping
collections.MutableSet = collections.abc.MutableSet
collections.MutableMapping = collections.abc.MutableMapping
from ksql.client import KSQLAPI


def read_config():
  config = {}
  with open("client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

def checkForSpeedLimit(speed_data, KAFKA_CONFIG):
  if(speed_data['Current Speed'] > 80):
    data = {
        "Driver_ID":  speed_data['Driver_ID'],
        "Vehicle_ID":  speed_data['Vehicle_ID'],
        "speed": speed_data['Current Speed']
    }
    producer = Producer(KAFKA_CONFIG)
    producer.produce("SPA_Driver_Speed_Filtered", json.dumps(data),data['Driver_ID'])
    producer.flush()
    # query = "INSERT INTO VEHICLE_SPEED VALUES (" + "'" + speed_data['Driver_ID'] + "','" + speed_data['Vehicle_ID'] + "'," + "{0} );".format(speed_data['Current Speed'])
    # client = KSQLAPI('https://pksqlc-12mvg3.eastus2.azure.confluent.cloud:443', api_key="YC3JRXRZDJZEGYH6", secret="ASKKPZz8mXtdayfm3+M6W+K5diAaECAi2n0Ik+oiV6xFnK1oL375tG009GhLN64Y")
    # response = client.query(query_string=query)
    # print(response)


def consume(topic, config):
  config["group.id"] = "python-group-1"
  config["auto.offset.reset"] = "earliest"

  consumer = Consumer(config)

  consumer.subscribe([topic])

  try:
    while True:
      
      msg = consumer.poll(1.0)
      if msg is not None and msg.error() is None:
        key = msg.key().decode("utf-8")
        value = msg.value().decode("utf-8")
        checkForSpeedLimit(json.loads(value), config)
        print(f"Consumed message from topic {topic}: key = {key:12} value = {value:12}")
  except KeyboardInterrupt:
    pass
  finally:
    consumer.close()

def main():
  config = read_config()
  topic = "SPA_Vehicle_Data"
  consume(topic, config)

if __name__ == '__main__':
    main()