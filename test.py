from confluent_kafka import Producer
import json
import time
import socket
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Automatically detect whether running inside Docker
def get_kafka_broker():
    try:
        socket.gethostbyname("kafka")  # Try resolving "kafka"
        return "kafka:9092"  # Running inside Docker network
    except socket.gaierror:
        return "localhost:9092"  # Running outside Docker

KAFKA_BROKER = get_kafka_broker()
TOPIC_NAME = "clickstream"

# Kafka producer configuration
conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": socket.gethostname(),
    "acks": "all",  # Ensure message durability
}

# Initialize Kafka producer
producer = Producer(conf)
logger.info(f"Initialized Kafka producer: {KAFKA_BROKER}, topic: {TOPIC_NAME}")

# Callback for delivery reports
def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message sent to {msg.topic()} [{msg.partition()}]")

# Send messages in a loop
while True:
    message = {"event": "page_view", "user_id": 123, "timestamp": time.time()}
    producer.produce(TOPIC_NAME, key="user_123", value=json.dumps(message), callback=delivery_report)
    producer.flush()  # Ensure messages are sent
    logger.info(f"Sent: {message}")
    time.sleep(1)  # Send 1 event per second
