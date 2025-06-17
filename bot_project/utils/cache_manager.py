import json
import logging
from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime, timedelta

from telebot.types import InlineKeyboardMarkup
from utils.redis_manager import redis_manager

class CachePrefix:
    BASE       = "cache-data"
    SEARCH     = f"{BASE}:search_cache:"
    INLINE     = f"{BASE}:inline_cache:"
    SERVER     = f"{BASE}:server_cache:"
    BUTTONS    = f"{BASE}:buttons_cache:"
    COUNTRY    = f"{BASE}:country_cache:"
    MODIFY     = f"{BASE}:modify_cache:"
    SERVICE    = f"{BASE}:service_cache:"
    APP        = f"{BASE}:app_cache:"
    USER       = f"{BASE}:user_cache:"
    ORDER      = f"{BASE}:order_cache:"
    DEPOSIT    = f"{BASE}:deposit_cache:"
    TEMP       = f"{BASE}:temp_cache:"


class CacheManager:
    """
    Async Redis-backed cache manager with JSON serialization.
    """

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._redis = None

    async def get_redis(self):
        if self._redis is None:
            self._redis = await redis_manager.get_client()
        return self._redis

    def _full_key(self, prefix: str, key: str) -> str:
        return f"{prefix}{key}"

    def _get_expiry(self) -> int:
        """
        Cache expires at the next 00:00 or 12:00 UTC boundary, whichever comes first.
        """
        now = datetime.utcnow()
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        noon     = now.replace(hour=12, minute=0, second=0, microsecond=0)
        if now >= noon:
            noon += timedelta(days=1)
        # seconds until the earlier of midnight or noon
        return int((min(midnight, noon) - now).total_seconds())

    async def get(self, key: str, prefix: str = "") -> Any:
        """
        Retrieve a single cache entry. Returns parsed data or an empty dict.
        """
        if not self.redis:
            return {}
        full_key = self._full_key(prefix, key)
        try:
            raw = await self.redis.get(full_key)
            if not raw:
                return {}
            # decode and parse
            payload = raw.decode() if isinstance(raw, (bytes, bytearray)) else raw
            entry = json.loads(payload)
            return entry.get("data", {})
        except json.JSONDecodeError as e:
            self._logger.error(f"[get] JSON decode error for {full_key[:10]}...: {e}"[:100])
            return {}
        except Exception as e:
            self._logger.error(f"[get] Redis error for {full_key[:10]}...: {e}"[:100])
            return {}

    async def set(
        self,
        key: str,
        data: Union[InlineKeyboardMarkup, Any],
        prefix: str = "",
        expire: Optional[int] = None,
    ) -> bool:
        """
        Set one cache entry with JSON wrapping, returns True on success.
        """
        if not self.redis:
            return False
        full_key = self._full_key(prefix, key)
        to_store = data.to_dict() if isinstance(data, InlineKeyboardMarkup) else data
        payload = {
            "data": to_store,
            "cached_at": datetime.utcnow().isoformat(),
            "cache_key": full_key,
        }
        try:
            ttl = expire or self._get_expiry()
            await self.redis.setex(full_key, ttl, json.dumps(payload, default=str))
            return True
        except Exception as e:
            self._logger.error(f"[set] Redis error for {full_key[:10]}...: {e}"[:100])
            return False

    async def delete(self, key: str, prefix: str = "") -> bool:
        """
        Delete a single cache entry. Returns True if delete command succeeds.
        """
        if not self.redis:
            return False
        full_key = self._full_key(prefix, key)
        try:
            await self.redis.delete(full_key)
            return True
        except Exception as e:
            self._logger.error(f"[delete] Redis error for {full_key[:10]}...: {e}"[:100])
            return False

    async def get_many(self, keys: List[str], prefix: str = "") -> Dict[str, Any]:
        """
        Bulk-get multiple keys using MGET and JSON parse each.
        """
        result: Dict[str, Any] = {}
        if not self.redis or not keys:
            return result

        full_keys = [self._full_key(prefix, k) for k in keys]
        try:
            raw_values = await self.redis.mget(*full_keys)
            for orig, raw in zip(keys, raw_values):
                if not raw:
                    continue
                text = raw.decode() if isinstance(raw, (bytes, bytearray)) else raw
                try:
                    entry = json.loads(text)
                    result[orig] = entry.get("data", {})
                except json.JSONDecodeError:
                    self._logger.warning(f"[get_many] JSON decode failed for {orig}")
            return result
        except Exception as e:
            self._logger.error(f"[get_many] Redis error: {e}")
            return result

    async def set_many(
        self,
        data_map: Dict[str, Any],
        prefix: str = "",
        expire: Optional[int] = None
    ) -> bool:
        """
        Bulk-set multiple keys in a pipeline. Returns True if all succeed.
        """
        if not self.redis or not data_map:
            return False

        ttl = expire or self._get_expiry()
        try:
            pipe = self.redis.pipeline()
            for key, data in data_map.items():
                full = self._full_key(prefix, key)
                payload = {
                    "data": data.to_dict() if isinstance(data, InlineKeyboardMarkup) else data,
                    "cached_at": datetime.utcnow().isoformat(),
                    "cache_key": full,
                }
                pipe.setex(full, ttl, json.dumps(payload, default=str))
            await pipe.execute()
            return True
        except Exception as e:
            self._logger.error(f"[set_many] Redis pipeline error: {e}")
            return False

    async def clear_prefix(self, prefix: str) -> bool:
        """
        Delete all keys matching a prefix.
        """
        if not self.redis:
            return False
        try:
            cursor = b"0"
            while cursor:
                cursor, keys = await self.redis.scan(cursor=cursor, match=f"{prefix}*")
                if keys:
                    await self.redis.delete(*keys)
            return True
        except Exception as e:
            self._logger.error(f"[clear_prefix] Redis error for prefix {prefix}: {e}")
            return False


# Singleton instance to import elsewhere
cache_manager = CacheManager()
