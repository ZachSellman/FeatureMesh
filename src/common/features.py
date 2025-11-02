from enum import Enum
from dataclasses import dataclass
from typing import Optional


class FeatureType(str, Enum):
    REAL_TIME = "real_time"           # Updated continuously from stream
    NEAR_REAL_TIME = "near_real_time" # Updated every few minutes
    BATCH = "batch"                   # Updated daily
    STATIC = "static"                 # Rarely changes

@dataclass
class FeatureDefinition:
    name: str
    feature_type: FeatureType
    description: str
    ttl_seconds: Optional[int] = None # Time to live in Redis

    def get_redis_key(self, entity_id: str) -> str:
        """Generate Redis key for this feature"""
        return f"feature:{self.name}:{entity_id}"
    

# Features
USER_FEATURES = {
    "user_clicks_1h": FeatureDefinition(
        name="user_clicks_1h",
        feature_type=FeatureType.REAL_TIME,
        description="Number of clicks by user in last 1 hour",
        ttl_seconds=3600
    ),
    "user_views_1h": FeatureDefinition(
        name="user_views_1h",
        feature_type=FeatureType.REAL_TIME,
        description="Number of post views by user in the last 1 hour",
        ttl_seconds=3600
    ),
    "user_engagement_score": FeatureDefinition(
        name="user_engagement_score",
        feature_type=FeatureType.REAL_TIME,
        description="Weighted engagement score (views, clicks, votes)",
        ttl_seconds=3600
    )
}

CONTENT_FEATURES = {
    "post_views_10m": FeatureDefinition(
        name="post_views_10m",
        feature_type=FeatureType.REAL_TIME,
        description="Number of views for post in the last 10 minutes",
        ttl_seconds=600
    ),
    "post_velocity": FeatureDefinition(
        name="post_velocity",
        feature_type=FeatureType.REAL_TIME,
        description="Rate of engagement (views per minute)",
        ttl_seconds=600
    ),
    "post_upvote_ratio": FeatureDefinition(
        name="post_upvote_ratio",
        feature_type=FeatureType.REAL_TIME,
        description="Upvotes / (Upvotes + Downvotes)",
        ttl_seconds = 3600
    )
}