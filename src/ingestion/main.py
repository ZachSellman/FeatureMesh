import time
import structlog
from src.common.config import Config
from src.common.event_generator import EventGenerator
from src.ingestion.kafka_producer import EventProducer

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer()  # Changed for better readability
    ]
)

logger = structlog.get_logger()


def main():
    config = Config()
    
    logger.info("Starting event producer",
                events_per_second=config.generator.events_per_second,
                bootstrap_servers=config.kafka.bootstrap_servers)
    
    # Initialize generator and producer
    generator = EventGenerator(
        num_users=config.generator.num_users,
        num_posts=config.generator.num_posts
    )
    
    producer = EventProducer(
        bootstrap_servers=config.kafka.bootstrap_servers,
        user_topic=config.kafka.user_events_topic,
        content_topic=config.kafka.content_events_topic
    )
    
    try:
        # Calculate sleep time between batches
        batch_size = 10
        sleep_time = batch_size / config.generator.events_per_second
        
        event_count = 0
        batch_num = 0
        
        while True:
            # Generate and send batch
            events = generator.generate_batch(
                batch_size,
                user_ratio=config.generator.user_event_ratio
            )
            
            logger.info("Generated events batch", 
                       batch_num=batch_num,
                       num_events=len(events),
                       user_events=sum(1 for e in events if hasattr(e, 'user_id')),
                       content_events=sum(1 for e in events if hasattr(e, 'author_id')))
            
            producer.send_batch(events)
            
            event_count += len(events)
            batch_num += 1
            
            if event_count % 100 == 0:
                logger.info("Events produced", total=event_count, batches=batch_num)
            
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        logger.info("Shutting down producer")
    finally:
        producer.close()


if __name__ == "__main__":
    main()