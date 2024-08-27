from confluent_kafka import Consumer, KafkaException, KafkaError
import sys

# Get the consumer group ID from the command line argument
group_id = sys.argv[1] if len(sys.argv) > 1 else 'default_group'

# Configuration for the Kafka consumer
conf = {
    'bootstrap.servers': 'localhost:9092',   # Kafka broker(s)
    'group.id': group_id,                    # Consumer group ID
    'auto.offset.reset': 'earliest'          # Start reading at the earliest message
}

# Create a Consumer instance
consumer = Consumer(conf)

# Subscribe to the topic
topic_name = 'Mytopic'
consumer.subscribe([topic_name])

try:
    while True:
        # Poll for new messages
        msg = consumer.poll(timeout=1.0)  # Timeout in seconds

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Reached end of partition {msg.partition()}")
            else:
                raise KafkaException(msg.error())
        else:
            # Customize the output based on the consumer group
            message = msg.value().decode('utf-8')
            if group_id == 'group1':
                print(f"Group 1: Received message: {message}")
            elif group_id == 'group2':
                print(f"Group 2: Processed message: {message}")
            elif group_id == 'group3':
                print(f"Group 3: Logging message: {message}")
            else:
                print(f"Group {group_id}: {message}")

except KeyboardInterrupt:
    print("Consumer interrupted")

finally:
    consumer.close()
