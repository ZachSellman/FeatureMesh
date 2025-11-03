import structlog
from confluent_kafka import Consumer, KafkaError
from src.storage.redis_client import RedisClient
from src.storage.postgres_client import PostgresClient
from src.streaming.user_engagement_processor import UserEngagementProcessor

logger = structlog.get_logger()


class StreamConsumer:
    def __init__(self, bootstrap_servers: str, group_id: str, topics: list):
        self.topics = topics
        
        # Kafka consumer config
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000,
        }
        
        self.consumer = Consumer(conf)
        self.consumer.subscribe(topics)
        
        # Initialize Redis and PostgreSQL
        self.redis = RedisClient()
        self.postgres = PostgresClient()
        self.user_processor = UserEngagementProcessor(self.redis, self.postgres)
        
        logger.info("Stream consumer initialized with dual storage",
                   bootstrap_servers=bootstrap_servers,
                   group_id=group_id,
                   topics=topics)
    
    def run(self):
        """Start consuming and processing messages"""
        logger.info("Starting stream processing...")
        
        try:
            message_count = 0
            
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug("Reached end of partition")
                    else:
                        logger.error("Consumer error", error=msg.error())
                    continue
                
                # Process the message
                topic = msg.topic()
                value = msg.value().decode('utf-8')
                
                if topic == 'user-events':
                    self.user_processor.process_event(value)
                elif topic == 'content-events':
                    # We'll add content processor later
                    pass
                
                message_count += 1
                
                if message_count % 100 == 0:
                    logger.info("Processed messages", count=message_count)
                
        except KeyboardInterrupt:
            logger.info("Shutting down stream consumer")
        finally:
            self.consumer.close()
            logger.info("Stream consumer closed")


def main():
    consumer = StreamConsumer(
        bootstrap_servers='localhost:19092',
        group_id='feature-store-stream-processor',
        topics=['user-events', 'content-events']
    )
    consumer.run()


if __name__ == "__main__":
    main()