import json
import structlog
from datetime import datetime
from collections import defaultdict
from typing import Dict
from src.common.events import EventType
from src.common.features import USER_FEATURES
from src.storage.redis_client import RedisClient

logger = structlog.get_logger()


class UserEngagementProcessor:
    """
    Processes user events and computes real-time engagement features
    """
    
    def __init__(self, redis_client: RedisClient):
        self.redis = redis_client
        
        # In-memory state for windowed aggregations
        # In production, Flink would manage this state
        self.user_clicks_1h = defaultdict(int)
        self.user_views_1h = defaultdict(int)
        
        logger.info("UserEngagementProcessor initialized")
    
    def process_event(self, event_json: str):
        """Process a single user event"""
        try:
            event_data = json.loads(event_json)
            event_type = event_data.get('event_type')
            user_id = event_data.get('user_id')
            
            if not user_id:
                return
            
            # Update counters based on event type
            if event_type == EventType.USER_CLICK.value:
                self._process_click(user_id)
            elif event_type == EventType.USER_VIEW.value:
                self._process_view(user_id)
            elif event_type in [EventType.USER_UPVOTE.value, EventType.USER_DOWNVOTE.value]:
                self._process_vote(user_id, event_type)
            
            # Compute and store engagement score
            self._compute_engagement_score(user_id)
            
        except Exception as e:
            logger.error("Failed to process event", error=str(e), event=event_json[:100])
    
    def _process_click(self, user_id: str):
        """Process a click event"""
        feature_def = USER_FEATURES["user_clicks_1h"]
        self.redis.increment_counter(feature_def, user_id)
        logger.debug("Processed click", user_id=user_id)
    
    def _process_view(self, user_id: str):
        """Process a view event"""
        feature_def = USER_FEATURES["user_views_1h"]
        self.redis.increment_counter(feature_def, user_id)
        logger.debug("Processed view", user_id=user_id)
    
    def _process_vote(self, user_id: str, vote_type: str):
        """Process an upvote/downvote event"""
        # For now, just count as engagement
        pass
    
    def _compute_engagement_score(self, user_id: str):
        """
        Compute weighted engagement score
        Formula: (views * 1) + (clicks * 3) + (votes * 5)
        """
        # Get current counts from Redis
        clicks = self.redis.get_feature(USER_FEATURES["user_clicks_1h"], user_id) or 0
        views = self.redis.get_feature(USER_FEATURES["user_views_1h"], user_id) or 0
        
        # Convert to int if they're strings
        clicks = int(clicks) if clicks else 0
        views = int(views) if views else 0
        
        # Calculate weighted score
        engagement_score = (views * 1) + (clicks * 3)
        
        # Store in Redis
        feature_def = USER_FEATURES["user_engagement_score"]
        self.redis.set_feature(feature_def, user_id, engagement_score)
        
        logger.debug("Computed engagement score",
                    user_id=user_id,
                    score=engagement_score,
                    views=views,
                    clicks=clicks)