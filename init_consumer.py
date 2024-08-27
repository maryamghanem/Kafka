from confluent_kafka import Consumer,KafkaException, KafkaError

conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'group1',
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

topic_name = 'Mytopic'
consumer.subscribe([topic_name])

try:
    while True:
        # Poll for new messages
        msg = consumer.poll(timeout=1.0)  # Timeout in seconds

        if msg is None:
            continue

        if msg.error():
            # Check for any errors
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print(f"Reached end of partition {msg.partition()}")
            else:
                # Error occurred
                raise KafkaException(msg.error())
        else:
            # Message received successfully
            print(f"Received message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    # Handle manual interruption
    print("Consumer interrupted")

finally:
    
    # Close down consumer cleanly
    consumer.close()
    