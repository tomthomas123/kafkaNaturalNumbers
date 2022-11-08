from kafka import KafkaProducer
import time 
import random


bootstrap_server = ["localhost:9092"]

topic = "naturalnumbers"

producer = KafkaProducer(bootstrap_servers = bootstrap_server)

producer = KafkaProducer()

def senddata():
    random_number = random.randint(0,1000)
    message = producer.send(topic, bytes(str(random_number),"utf-8"))
    metadata = message.get()
    print(metadata.topic)
    print(metadata.partition)
    time.sleep(5)

while True:
    senddata()