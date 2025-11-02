import redis
import json
import structlog
from typing import Optional, Dict, Any
from src.common.features import FeatureDefinition

logger = structlog.get_logger()


class RedisClient:
    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0):
        self.client = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True,   # Auto-decode bytes to strings
            socket_connect_timeout=5,
            socket_keepalive=True
        )


        # Test connection
        try:
            self.client.ping()
            logger.info("Redis client connected", host=host, port=port,)
        except redis.ConnectionError as e:
            logger.error("Failed to connect to Redis", error=str(e))
            raise

    def set_feature(self, feature_def: FeatureDefinition, entity_id: str, value: Any, ttl: Optional[int] = None):
        """Set a feature value in Redis"""
        key = feature_def.get_redis_key(entity_id)

        # Use feature's TTL if not specified
        ttl = ttl or feature_def.ttl_seconds

        # Convert value to JSON if its complex
        if isinstance(value, (dict, list)):
            value = json.dumps(value)

        try:
            if ttl:
                self.client.setex(key, ttl, value)
            else:
                self.client.set(key, value)
            logger.debug("Feature set", feature=feature_def.name, entity_id=entity_id)
        
        except Exception as e:
            logger.error("Failed to set feature",
                         feature=feature_def.name,
                         entity_id=entity_id,
                         error=str(e))

    def get_feature(self, feature_def: FeatureDefinition, entity_id: str) -> Optional[Any]:
        """Get a feature value from Redis"""
        key = feature_def.get_redis_key(entity_id)

        try:
            value = self.client.get(key)
            if value is None:
                return None
            
            # Value is already a string because decode_responses=True
            # Try to parse as JSON
            try:
                return json.loads(value)  # type: ignore
            
            except (json.JSONDecodeError, TypeError):
                return value
        
        except Exception as e:
            logger.error("Failed to get feature",
                        feature=feature_def.name,
                        entity_id=entity_id,
                        error=str(e))
            return None
        
    def increment_counter(self, feature_def: FeatureDefinition, entity_id: str, amount: int = 1) -> int:
        """Increment a counter feature"""
        key = feature_def.get_redis_key(entity_id)

        try:
            # Increment and set TTL
            new_value = self.client.incr(key, amount)
            if feature_def.ttl_seconds:
                self.client.expire(key, feature_def.ttl_seconds)
            return int(new_value) # type: ignore
        
        except Exception as e:
            logger.error("Failed to increment counter",
                        feature=feature_def.name,
                        entity_id=entity_id,
                        error=str(e))
            
            return 0
            
    def get_multiple_features(self, feature_defs: list[FeatureDefinition], entity_id: str) -> Dict[str, Any]:
        """Get multiple features in one call"""
        pipeline = self.client.pipeline()


        # Queue all gets
        for feature_def in feature_defs:
            key = feature_def.get_redis_key(entity_id)
            pipeline.get(key)

        # Execute Pipeline
        results = pipeline.execute()

        # Build response dict
        features = {}
        for feature_def, value in zip(feature_defs, results):
            if value is not None:
                try:
                    features[feature_def.name] = json.loads(value)
                
                except (json.JSONDecodeError, TypeError):
                    features[feature_def.name] = value

            else:
                features[feature_def.name] = None

        return features

    def close(self):
        """Close Redis connection"""
        self.client.close()
        logger.info("Redis client closed")
                