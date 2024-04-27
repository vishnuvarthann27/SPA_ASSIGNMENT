import collections.abc
collections.Iterable = collections.abc.Iterable
collections.Mapping = collections.abc.Mapping
collections.MutableSet = collections.abc.MutableSet
collections.MutableMapping = collections.abc.MutableMapping
from ksql.client import KSQLAPI
client = KSQLAPI('https://pksqlc-12mvg3.eastus2.azure.confluent.cloud:443', api_key="YC3JRXRZDJZEGYH6", secret="ASKKPZz8mXtdayfm3+M6W+K5diAaECAi2n0Ik+oiV6xFnK1oL375tG009GhLN64Y")
query = client.query('select * from VEHICLE_SPEED')
for item in query: print(item)

#, api_key="DCJRCXLIUSMUPDRB", secret="29tKoEYBGygoy1tg8nFl9U6+5QnQIlJB5N1Ohjz3rTnPNxakvnYFzJ48k4/z2GcT"