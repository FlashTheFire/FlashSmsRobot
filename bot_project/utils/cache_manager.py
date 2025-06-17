import json
import logging
from typing import Any, Optional, Dict
from telebot.types import InlineKeyboardMarkup
from utils.redis_manager import redis_manager
from datetime import datetime, timedelta

class CachePrefix:
    cache = "cache-data"
    SEARCH = f"{cache}:search_cache:"
    INLINE = f"{cache}:inline_cache:"
    SERVER = f"{cache}:server_cache:"
    BUTTONS = f"{cache}:buttons_cache:"
    COUNTRY = f"{cache}:country_cache:"
    MODIFY = f"{cache}:modify_cache:"
    SERVICE = f"{cache}:service_cache:"
    APP = f"{cache}:app_cache:"
    USER   = f"{cache}:user_cache:"
    ORDER  = f"{cache}:order_cache:"
    DEPOSIT = f"{cache}:deposit_cache:"
    TEMP   = f"{cache}:temp_cache:"

class CacheManager:
    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self.redis_client = redis_manager.redis_client
    
    def _build_key(self, prefix: str, key: str) -> str:
        """Build a standardized cache key"""
        return f"{prefix}{key}" if prefix else key
    

    def get_expire(self) -> int:
        """Get seconds from now to the next 12AM or 12PM (whichever is sooner)"""
        now = datetime.now()
        next_am = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        next_pm = now.replace(hour=12, minute=0, second=0, microsecond=0)
        if now >= next_pm:
            next_pm += timedelta(days=1)
        next_time = min(next_am, next_pm)
        return int((next_time - now).total_seconds())

    async def get(self, key: str, prefix: str = "") -> Optional[Any]:
        """Retrieve from Redis cache with prefix support"""
        full_key = self._build_key(prefix, key)
        try:
            data = await self.redis_client.get(full_key) or {}
            if not data:
                return {}

            if isinstance(data, bytes):
                data = data.decode('utf-8')

            parsed = json.loads(data)
            return parsed.get("data")

        except json.JSONDecodeError as e:
            self._logger.error(f"JSON decode error for key {full_key}: {e}")
        except AttributeError as e:
            if "object has no attribute 'get'" in str(e):
                self._logger.error(f"Redis get error for key {full_key}: {e}")
            else:
                raise
        except Exception as e:
            self._logger.error(f"Redis get error for key {full_key}: {e}")
        return {}

    async def set(
        self,
        key: str,
        data: Any,
        prefix: str = "",
        expire_time: Optional[int] = None
    ) -> bool:
        """Set cache with prefix support and optional expiration"""
        full_key = self._build_key(prefix, key)
        try:
            # Convert special types
            if isinstance(data, InlineKeyboardMarkup):
                data = data.to_dict()

            expire_time = expire_time or self.get_expire()

            cache_data = {
                "data": data,
                "cached_at": datetime.utcnow().isoformat(),
                "cache_key": full_key
            }

            json_data = json.dumps(cache_data, default=str)
            await self.redis_client.setex(full_key, expire_time, json_data)
            return True

        except Exception as e:
            self._logger.error(f"Cache set error for key {full_key}: {e}")
            return False

    async def delete(self, key: str, prefix: str = "") -> bool:
        """Delete a cache entry"""
        full_key = self._build_key(prefix, key)
        try:
            await self.redis_client.delete(full_key)
            return True
        except Exception as e:
            self._logger.error(f"Cache delete error for key {full_key}: {e}")
            return False

    async def get_many(self, redis_client, keys: list, prefix: str = "") -> Dict[str, Any]:
        """Get multiple cache entries at once"""
        result = {}
        for key in keys:
            data = await self.get(redis_client, key, prefix)
            if data:
                result[key] = data
        return result

    async def set_many(
        self, 
        redis_client, 
        data_dict: Dict[str, Any], 
        expire_time: int = None,
        prefix: str = ""
    ) -> bool:
        """Set multiple cache entries at once"""
        success = True
        for key, value in data_dict.items():
            if not await self.set(redis_client, key, value, expire_time, prefix):
                success = False
        return success

    async def clear_prefix(self, redis_client, prefix: str) -> bool:
        """Clear all cache entries with given prefix"""
        try:
            pattern = f"{prefix}*"
            keys = await redis_client.keys(pattern)
            if keys:
                await redis_client.delete(*keys)
            return True
        except Exception as e:
            self._logger.error(f"Error clearing cache prefix {prefix}: {e}")
            return False

cache_manager = CacheManager() 