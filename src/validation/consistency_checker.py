import structlog
import time
from typing import List, Dict, Any
from src.storage.redis_client import RedisClient
from src.storage.postgres_client import PostgresClient
from src.common.features import USER_FEATURES

logger = structlog.get_logger()


class ConsistencyChecker:
    """
    Validates that online (Redis) and offline (PostgreSQL) features match
    This is critical for ensuring training data integrity
    """
    
    def __init__(self):
        self.redis = RedisClient()
        self.postgres = PostgresClient()
        logger.info("ConsistencyChecker initialized")
    
    def check_feature_consistency(self, entity_id: str, entity_type: str,
                                  feature_name: str) -> Dict[str, Any]:
        """Check if online and offline values match for a feature"""
        
        # Get from Redis (online)
        from src.common.features import USER_FEATURES
        feature_def = USER_FEATURES.get(feature_name)
        if not feature_def:
            logger.error("Unknown feature", feature_name=feature_name)
            return {"error": "Unknown feature"}
        
        online_value = self.redis.get_feature(feature_def, entity_id)
        
        # Get from PostgreSQL (offline) - latest value
        offline_value = self.postgres.get_offline_feature(
            entity_id=entity_id,
            entity_type=entity_type,
            feature_name=feature_name
        )
        
        # Compare values
        is_consistent = str(online_value) == str(offline_value)
        
        result = {
            'entity_id': entity_id,
            'feature_name': feature_name,
            'online_value': online_value,
            'offline_value': offline_value,
            'is_consistent': is_consistent
        }
        
        # Record check result
        difference = None
        if not is_consistent:
            difference = f"Online: {online_value}, Offline: {offline_value}"
        
        self.postgres.record_consistency_check(
            entity_id=entity_id,
            entity_type=entity_type,
            feature_name=feature_name,
            online_value=online_value,
            offline_value=offline_value,
            is_consistent=is_consistent,
            difference=difference
        )
        
        return result
    
    def check_multiple_entities(self, entity_ids: List[str], 
                                entity_type: str = "user") -> Dict[str, Any]:
        """Check consistency for multiple entities"""
        results = []
        inconsistent_count = 0
        
        for entity_id in entity_ids:
            for feature_name in USER_FEATURES.keys():
                result = self.check_feature_consistency(
                    entity_id=entity_id,
                    entity_type=entity_type,
                    feature_name=feature_name
                )
                
                results.append(result)
                
                if not result.get('is_consistent', False):
                    inconsistent_count += 1
                    logger.warning("Inconsistency detected", **result)
        
        consistency_rate = 1.0 - (inconsistent_count / len(results)) if results else 0.0
        
        summary = {
            'total_checks': len(results),
            'consistent': len(results) - inconsistent_count,
            'inconsistent': inconsistent_count,
            'consistency_rate': consistency_rate,
            'results': results
        }
        
        logger.info("Consistency check complete", **summary)
        return summary
    
    def continuous_monitoring(self, interval_seconds: int = 60):
        """Continuously monitor consistency"""
        logger.info("Starting continuous consistency monitoring",
                   interval=interval_seconds)
        
        try:
            while True:
                # Sample random users
                sample_users = [f"user_{i}" for i in range(0, 10)]
                
                summary = self.check_multiple_entities(sample_users)
                
                logger.info("Consistency check cycle",
                           consistency_rate=summary['consistency_rate'],
                           inconsistent=summary['inconsistent'])
                
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("Stopping consistency monitoring")


def main():
    checker = ConsistencyChecker()
    
    # Run continuous monitoring
    checker.continuous_monitoring(interval_seconds=30)


if __name__ == "__main__":
    main()