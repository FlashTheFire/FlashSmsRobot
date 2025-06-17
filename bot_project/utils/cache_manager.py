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
        return f"{prefix}{key}" if prefix else key

    def get_expire(self) -> int:
        now = datetime.now()
        next_am = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        next_pm = now.replace(hour=12, minute=0, second=0, microsecond=0)
        if now >= next_pm:
            next_pm += timedelta(days=1)
        return int((min(next_am, next_pm) - now).total_seconds())

    async def get(self, key: str, prefix: str = "") -> Optional[Any]:
        full_key = self._build_key(prefix, key)
        try:
            raw = await self.redis_client.get(full_key)
            if not raw:
                return {}
            if isinstance(raw, bytes):
                raw = raw.decode('utf-8')
            parsed = json.loads(raw)
            return parsed.get("data")
        except json.JSONDecodeError as e:
            self._logger.error(f"JSON decode error for key {full_key}: {e}")
            return {}
        except Exception as e:
            if "has no attribute 'get'" in str(e):
                self._logger.error(f"Redis get error for key {full_key}: {e}")
                return {}
            self._logger.error(f"Unknown Redis get error for key {full_key}: {e}")
        return {}

    async def set(self, key: str, data: Any, prefix: str = "", expire_time: Optional[int] = None) -> bool:
        full_key = self._build_key(prefix, key)
        try:
            expire = expire_time or self.get_expire()
            payload = data.to_dict() if isinstance(data, InlineKeyboardMarkup) else data
            cache_data = {
                "data": payload,
                "cached_at": datetime.utcnow().isoformat(),
                "cache_key": full_key
            }
            await self.redis_client.setex(full_key, expire, json.dumps(cache_data, default=str))
            return True
        except Exception as e:
            self._logger.error(f"Cache set error for key {full_key}: {e}")
            return False

    async def delete(self, key: str, prefix: str = "") -> bool:
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