import time
from kafka import KafkaProducer
import random

bootstrap_server = ["localhost:9092"]
topic = "naturalnumbers"
producer = KafkaProducer(bootstrap_servers = bootstrap_server)
producer =KafkaProducer()

def senddata():
    data = random.randint(0,1000)
    message = producer.send(topic,bytes(str(data),"utf-8"))
    metadata =message.get()
    print(metadata.topic)
    print(metadata.partition)
    time.sleep(3)
while True:
    senddata()