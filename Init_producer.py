from confluent_kafka import Producer
import socket

conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)
topic='Mytopic'
producer.produce(topic, key="Maryam", value="This is Maryam")

prodocer.flush()

#producer.poll(1)
