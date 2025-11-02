import json
import structlog
from confluent_kafka import Producer, KafkaError
from typing import List
from src.common.events import BaseEvent

logger = structlog.get_logger()


class EventProducer:
    def __init__(self, bootstrap_servers: str, user_topic: str, content_topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.user_topic = user_topic
        self.content_topic = content_topic
        
        # Kafka producer config
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'feature-store-producer',
            'compression.type': 'snappy',
            'linger.ms': 10,
            'batch.size': 16384,
            'socket.timeout.ms': 10000,
            'api.version.request': True,
        }
        
        try:
            self.producer = Producer(conf)
            logger.info("Kafka producer initialized", servers=bootstrap_servers)
            
            # Test connection by getting metadata
            metadata = self.producer.list_topics(timeout=5)
            logger.info("Successfully connected to Kafka", 
                       broker_count=len(metadata.brokers),
                       topic_count=len(metadata.topics))
        except Exception as e:
            logger.error("Failed to initialize Kafka producer", error=str(e))
            raise
    
    def delivery_callback(self, err, msg):
        """Callback for message delivery confirmation"""
        if err:
            logger.error("Message delivery failed", 
                        error=str(err),
                        topic=msg.topic() if msg else None)
        else:
            logger.debug("Message delivered",
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset())
    
    def send_event(self, event: BaseEvent):
        """Send a single event to appropriate Kafka topic"""
        # Determine topic based on event type
        from src.common.events import UserEvent, ContentEvent
        
        if isinstance(event, UserEvent):
            topic = self.user_topic
            key = event.user_id
        elif isinstance(event, ContentEvent):
            topic = self.content_topic
            key = event.post_id
        else:
            logger.error("Unknown event type", event_type=type(event))
            return
        
        # Serialize event to JSON
        value = json.dumps(event.model_dump(), default=str)
        
        try:
            # Send to Kafka
            self.producer.produce(
                topic=topic,
                key=key.encode('utf-8'),
                value=value.encode('utf-8'),
                callback=self.delivery_callback
            )
            
            # Trigger callbacks (non-blocking)
            self.producer.poll(0)
        except BufferError:
            logger.warning("Local producer queue is full, waiting...")
            self.producer.flush()
            # Retry
            self.producer.produce(
                topic=topic,
                key=key.encode('utf-8'),
                value=value.encode('utf-8'),
                callback=self.delivery_callback
            )
        except Exception as e:
            logger.error("Failed to produce message", error=str(e), topic=topic)
    
    def send_batch(self, events: List[BaseEvent]):
        """Send a batch of events"""
        for event in events:
            self.send_event(event)
        
        # Flush to ensure all messages are sent
        remaining = self.producer.flush(timeout=10)
        if remaining > 0:
            logger.warning("Failed to flush all messages", remaining=remaining)
        else:
            logger.info("Batch sent", num_events=len(events))
    
    def close(self):
        """Close the producer and flush remaining messages"""
        remaining = self.producer.flush(timeout=10)
        if remaining > 0:
            logger.warning("Some messages were not delivered", count=remaining)
        logger.info("Kafka producer closed")