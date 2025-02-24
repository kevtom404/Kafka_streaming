from confluent_kafka import Consumer, Producer
import json

KAFKA_BROKER = "localhost:29092"
SOURCE_TOPIC = "user-login"
DEST_TOPIC = "processed-user-login"

# Kafka Consumer Configuration
consumer_config = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": "fetch-consumer-group",
    "auto.offset.reset": "earliest",
}

# Kafka Producer Configuration
producer_config = {
    "bootstrap.servers": KAFKA_BROKER
}

def process_message(message):
    """Process and transform Kafka messages."""
    try:
        data = json.loads(message)
        data["masked_ip"] = ".".join(data["ip"].split(".")[:2]) + ".XXX.XXX"
        return json.dumps(data)
    except Exception as e:
        print(f"Error processing message: {e}")
        return None

def main():
    consumer = Consumer(consumer_config)
    producer = Producer(producer_config)

    consumer.subscribe([SOURCE_TOPIC])
    print("Kafka Consumer started...")

    while True:
        msg = consumer.poll(1.0)  
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        processed_data = process_message(msg.value().decode("utf-8"))
        if processed_data:
            producer.produce(DEST_TOPIC, processed_data)
            producer.flush()

    consumer.close()

if __name__ == "__main__":
    main()
