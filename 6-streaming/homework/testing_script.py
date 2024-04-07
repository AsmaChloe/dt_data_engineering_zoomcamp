import json
import time 

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

producer.bootstrap_connected()

t0 = time.time()

topic_name = 'test-topic'

t1 = time.time()
for i in range(10):
    message = {'number': i}
    producer.send(topic_name, value=message)
    print(f"Sent: {message}")
    time.sleep(0.05)
t2 = time.time()
print(f'Sending messages took {(t2 - t1):.5f} seconds')

t3 = time.time()
producer.flush()
t4 = time.time()
print(f'Flusing took {(t4 - t3):.5f} seconds')

t5 = time.time()
print(f'Total process took {(t5 - t0):.5f} seconds')