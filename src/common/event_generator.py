import random
from datetime import datetime, timedelta
from typing import List
from faker import Faker
from src.common.events import UserEvent, ContentEvent, EventType

fake = Faker()


class EventGenerator:
    def __init__(self, num_users: int = 1000, num_posts: int = 5000):
        self.num_users = num_users
        self.num_posts = num_posts

        # Pre-generate user and post IDs for consistency
        self.user_ids = [f"user_{i}" for i in range(num_users)]
        self.post_ids = [f"post_{i}" for i in range(num_posts)]

        # Popular subreddits for examples
        self.subreddits = [
            "AskReddit", "funny", "gaming", "aww", "pics", "science", "worldnews", "todayilearned", "videos", "movies"]
        
        self.device_types = ["mobile", "desktop", "tablet"]
        self.content_types = ["text", "link", "image", "video"]

        # Tracking of "active sessions"
        self.active_sessions = {}

    def generate_user_event(self) -> UserEvent:
        """Generate a random user interaction event"""
        user_id = random.choice(self.user_ids)
        post_id = random.choice(self.post_ids)

        # Get or create session for this user
        if user_id not in self.active_sessions or random.random() < 0.1:
            # New session (10% chance or first time)
            self.active_sessions[user_id] = {
                'session_id': fake.uuid4(),
                'device': random.choice(self.device_types)
            }

        session = self.active_sessions[user_id]

        # Weight event types so they're more realistic
        event_type = random.choices(
            [EventType.USER_VIEW, EventType.USER_CLICK, EventType.USER_UPVOTE, EventType.USER_DOWNVOTE, EventType.USER_COMMENT],
            weights=[60, 20, 10, 5, 5]
        )[0]

        event = UserEvent(
            event_type=event_type,
            user_id=user_id,
            post_id=post_id,
            subreddit=random.choice(self.subreddits),
            session_id=session['session_id'],
            device_type=session['device']
        )

        # Add duration for view events
        if event_type == EventType.USER_VIEW:
            # Log-normal distribution for view duration (most short, some longer)
            event.duration_seconds = max(1.0, random.lognormvariate(2.0, 1.5))

        return event
    
    def generate_content_event(self) -> ContentEvent:
        """Generate a random content creation/modification event"""
        post_id = random.choice(self.post_ids)
        author_id = random.choice(self.user_ids)

        # Weight event types (creation is most common)
        event_type = random.choices(
            [EventType.POST_CREATED, EventType.POST_EDITED, EventType.POST_DELETED], weights=[70, 25, 5]
        )[0]

        content_type = random.choice(self.content_types)

        event = ContentEvent(
            event_type=event_type,
            post_id=post_id,
            author_id=author_id,
            subreddit=random.choice(self.subreddits),
            title=fake.sentence(nb_words=8),
            content_type=content_type
        )

        # Add word count for text posts
        if content_type == "text":
            event.word_count = random.randint(10, 2000)
        
        return event
    
    def generate_batch(self, num_events: int, user_ratio: float = 0.9) -> List:
        """Generate a batch of mixed events"""
        events = []
        for _ in range(num_events):
            if random.random() < user_ratio:
                events.append(self.generate_user_event())
            else:
                events.append(self.generate_content_event())

        return events