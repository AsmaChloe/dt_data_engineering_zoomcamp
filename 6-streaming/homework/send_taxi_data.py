import pandas as pd
import json
import time 
from kafka import KafkaProducer


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


# Read file
url = "./green_tripdata_2019-10.csv.gz"
usecols = ['lpep_pickup_datetime','lpep_dropoff_datetime','PULocationID','DOLocationID','passenger_count','trip_distance','tip_amount']
df_green = pd.read_csv(url, usecols=usecols)

# Connect to server
topic_name = 'green-trips'
server = 'localhost:9092'
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)
producer.bootstrap_connected()

# Send data
print(f'Sending...')
t0 = time.time()
for row in df_green.itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}
    
    producer.send(topic_name, value=row_dict)
t1 = time.time()    
print(f'Sending messages took {(t1 - t0):.5f} seconds')