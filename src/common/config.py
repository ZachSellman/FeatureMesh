from pydantic_settings import BaseSettings
from typing import Dict


class KafkaConfig(BaseSettings):
    bootstrap_servers: str = "localhost:19092"
    user_events_topic: str = "user-events"
    content_events_topic: str = "content-events"

class GeneratorConfig(BaseSettings):
    num_users: int = 1000
    num_posts: int = 5000
    events_per_second: int = 100
    user_event_ratio: float = 0.9

class Config(BaseSettings):
    kafka: KafkaConfig = KafkaConfig()
    generator: GeneratorConfig = GeneratorConfig()

    class Config:
        env_file = ".env"
        env_nested_delimiter = "__"