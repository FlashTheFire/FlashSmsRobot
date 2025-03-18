import json
import logging
from typing import Any, Optional, Dict
from telebot.types import InlineKeyboardMarkup
from datetime import datetime, timedelta

class CachePrefix:
    INLINE = "cache:inline_cache:"
    SERVER = "cache:server_cache:"
    SEARCH = "cache:search_cache:"
    USER   = "cache:user_cache:"
    ORDER  = "cache:order_cache:"
    TEMP   = "cache:temp_cache:"

class CacheManager:
    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self.redis_client = None
        self.default_expire = 86400  # 24 hours

    def set_redis_client(self, redis_client):
        self.redis_client = redis_client

    def _build_key(self, prefix: str, key: str) -> str:
        """Build a standardized cache key"""
        return f"{prefix}{key}"

    async def get(self, redis_client, key: str, prefix: str = "") -> Optional[Any]:
        """Retrieve from Redis cache with prefix support"""
        try:
            full_key = self._build_key(prefix, key)
            data = await redis_client.get(full_key)
            if data:
                if isinstance(data, bytes):
                    data = data.decode('utf-8')
                return json.loads(data)
        except json.JSONDecodeError as e:
            self._logger.error(f"JSON decode error for key {full_key}: {e}")
        except Exception as e:
            self._logger.error(f"Redis get error for key {full_key}: {e}")
        return None

    async def set(
        self, 
        redis_client, 
        key: str, 
        data: Any, 
        expire_time: int = None,
        prefix: str = ""
    ) -> bool:
        """Set cache with prefix support and validation"""
        try:
            full_key = self._build_key(prefix, key)
            
            # Handle special types
            if isinstance(data, InlineKeyboardMarkup):
                data = data.to_dict()
            
            # Add metadata
            cache_data = {
                "data": data,
                "cached_at": datetime.utcnow().isoformat(),
                "cache_key": full_key
            }
            
            json_data = json.dumps(cache_data, default=str)
            if expire_time is None:
                expire_time = self.default_expire
                
            await redis_client.setex(full_key, expire_time, json_data)
            return True
            
        except Exception as e:
            self._logger.error(f"Cache set error for key {self._build_key(prefix, key)}: {e}")
            return False

    async def delete(self, redis_client, key: str, prefix: str = "") -> bool:
        """Delete a cache entry"""
        try:
            full_key = self._build_key(prefix, key)
            await redis_client.delete(full_key)
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