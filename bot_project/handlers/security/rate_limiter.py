from datetime import datetime, timedelta
import logging
from typing import Dict, Optional, Tuple
from utils.config import CACHE_KEY
from redis.asyncio import Redis
import json

ratelimit = "limit_data"


class RateLimiter:
    """Rate limiter with enhanced security features using Redis."""
    
    def __init__(self, redis_client: Redis, duration: int = 60, max_requests: int = 5):
        self.redis = redis_client
        self.RATE_LIMIT_DURATION = duration  # Duration in seconds
        self.MAX_REQUESTS = max_requests  # Max requests per duration
        self.logger = logging.getLogger(__name__)

    async def limit(self, key: str, user_id: Optional[str] = None, max_requests: Optional[int] = None) -> bool:
        """
        Check if the rate limit has been exceeded for a given key and user_id (if provided).
        Returns True if request is allowed, False if rate limit exceeded.
        """
        try:
            now = datetime.now().timestamp()
            rate_key = f"{CACHE_KEY}{ratelimit}:{user_id}:{key}" if user_id else f"{CACHE_KEY}{ratelimit}:{key}"
            
            current = await self.redis.get(rate_key)
            if not current:
                await self.redis.set(rate_key, json.dumps({"count": 1, "start_time": now}), ex=self.RATE_LIMIT_DURATION)
                return True
                
            try:
                data = json.loads(current)
                count = int(data["count"])
                start_time = float(data["start_time"])
            except (ValueError, KeyError, json.JSONDecodeError) as e:
                self.logger.warning(f"Failed to parse rate limit data: {e}. Resetting.")
                await self.redis.set(rate_key, json.dumps({"count": 1, "start_time": now}), ex=self.RATE_LIMIT_DURATION)
                return True

            if not isinstance(count, int) or not isinstance(start_time, float):
                self.logger.warning(f"Unexpected rate limit data format for key {rate_key}. Resetting.")
                await self.redis.set(rate_key, json.dumps({"count": 1, "start_time": now}), ex=self.RATE_LIMIT_DURATION)
                return True

            max_reqs = max_requests or self.MAX_REQUESTS
            
            if count >= int(max_reqs):
                raise RateLimitExceeded(f"Rate limit exceeded for {'user ' + user_id if user_id else 'bot'}")
                
            data = {"count": count + 1, "start_time": start_time}
            await self.redis.set(rate_key, json.dumps(data), ex=self.RATE_LIMIT_DURATION)
            return True
            
        except RateLimitExceeded:
            return False
        except Exception as e:
            self.logger.error(f"Rate limit check failed: {e}")
            return True  # Fail open to avoid blocking legitimate traffic

    async def remaining_limit(self, key: str, user_id: Optional[str] = None, max_requests: Optional[int] = None) -> Tuple[int, float]:
        """Get remaining number of requests allowed and time until reset for a key and user_id (if provided)."""
        try:
            rate_key = f"{CACHE_KEY}{ratelimit}:{user_id}:{key}" if user_id else f"{CACHE_KEY}{ratelimit}:{key}"
            current = await self.redis.get(rate_key)
            if not current:
                return max_requests or self.MAX_REQUESTS, self.RATE_LIMIT_DURATION
                
            try:
                data = json.loads(current)
                count = int(data["count"])
                start_time = float(data["start_time"])
            except (ValueError, KeyError, json.JSONDecodeError) as e:
                self.logger.warning(f"Failed to parse rate limit data: {e}. Resetting.")
                await self.redis.delete(rate_key)
                return max_requests or self.MAX_REQUESTS, self.RATE_LIMIT_DURATION

            max_reqs = max_requests or self.MAX_REQUESTS
            remaining_requests = max(0, int(max_reqs) - count)
            time_elapsed = datetime.now().timestamp() - start_time
            time_remaining = max(0, self.RATE_LIMIT_DURATION - time_elapsed)
            
            return remaining_requests, time_remaining
            
        except Exception as e:
            self.logger.error(f"Failed to get remaining limit: {e}")
            return 0, 0

    async def reset_limit(self, key: str, user_id: Optional[str] = None):
        """Reset rate limit for a given key and user_id (if provided)."""
        try:
            rate_key = f"{CACHE_KEY}{ratelimit}:{user_id}:{key}" if user_id else f"{CACHE_KEY}{ratelimit}:{key}"
            await self.redis.delete(rate_key)
        except Exception as e:
            self.logger.error(f"Failed to reset rate limit: {e}")

class RateLimitExceeded(Exception):
    """Exception raised when rate limit is exceeded."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)