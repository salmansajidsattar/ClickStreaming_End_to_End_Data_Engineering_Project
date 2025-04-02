# kafka_producer/producer.py
import json
import logging
import time
from confluent_kafka import Producer
import sys
import os

# Add parent directory to path to import from other modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_generator.clickstream_generator import ClickstreamGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ClickstreamProducer:
    """Produces clickstream events to a Kafka topic."""
    
    def __init__(self, bootstrap_servers, topic_name):
        """Initialize the Kafka producer.
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka brokers
            topic_name: Name of the Kafka topic to produce to
        """
        self.topic_name = topic_name
        self.generator = ClickstreamGenerator()
        
        # Configure Kafka producer
        self.producer_config = {
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'python-producer',
            'api.version.request': True,
            'security.protocol':"PLAINTEXT",
        }
        
        self.producer = Producer(self.producer_config)
        logger.info(f"Initialized Kafka producer: {bootstrap_servers}, topic: {topic_name}")
    
    def delivery_callback(self, err, msg):
        """Callback function for message delivery reports."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")
    
    def produce_event(self, event):
        """Produce a single event to Kafka topic."""
        try:
            # Convert event to JSON string
            event_json = json.dumps(event)
            self.producer.poll(0)
            # Produce message
            self.producer.produce(
                topic=self.topic_name,
                value=event_json,
                callback=self.delivery_callback
            )
            
        except Exception as e:
            logger.error(f"Error producing event: {e}")
    
    def produce_batch(self, batch_size=100):
        """Produce a batch of events to Kafka topic."""
        events = self.generator.generate_batch(batch_size)
        for event in events:
            self.produce_event(event)
        
        # Wait for all messages to be delivered
        logger.info(f"Flushing {batch_size} messages...")
        self.producer.flush()
        logger.info(f"Successfully produced batch of {batch_size} messages")
    
    def start_continuous_production(self, events_per_second=10, duration_seconds=None):
        """Continuously produce events at the specified rate.
        
        Args:
            events_per_second: Average number of events to produce per second
            duration_seconds: How long to run for, or None for indefinite running
        """
        interval = 1.0 / events_per_second
        start_time = time.time()
        event_count = 0
        
        logger.info(f"Starting continuous production at ~{events_per_second} events/second")
        
        try:
            for event in self.generator.generate_continuous(interval=interval):
                self.produce_event(event)
                event_count += 1
                
                # Log progress periodically
                if event_count % 1000 == 0:
                    elapsed = time.time() - start_time
                    rate = event_count / elapsed
                    logger.info(f"Produced {event_count} events at {rate:.2f} events/second")
                
                # Check if we've been running long enough
                if duration_seconds and (time.time() - start_time) >= duration_seconds:
                    break
                
                # Poll to trigger delivery callbacks
                if event_count % 100 == 0:
                    self.producer.poll(0)
        
        except KeyboardInterrupt:
            logger.info("Production interrupted by user")
        
        finally:
            # Ensure all messages are delivered
            remaining = self.producer.flush(timeout=10)
            logger.info(f"Production completed. {event_count} events produced.")
            if remaining > 0:
                logger.warning(f"{remaining} messages may not have been delivered")

if __name__ == "__main__":
    # Example usage
    kafka_producer = ClickstreamProducer(
        bootstrap_servers='localhost:9092',
        topic_name='clickstream'
    )
    
    # Produce events continuously at 20 events per second for 60 seconds
    kafka_producer.start_continuous_production(events_per_second=1, duration_seconds=100)