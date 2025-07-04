import asyncio
import json
import logging
import time
import hashlib
import random
from datetime import datetime
from functools import wraps
from io import BytesIO
from termcolor import colored
import os
import re

import asyncio
import json
import logging
import time
from pathlib import Path
from utils.cache_manager import CacheManager, CachePrefix, cache_manager
from functools import wraps
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict

from aiohttp import web
import aiohttp
import aiofiles
import redis.asyncio as redis

from utils.redis_manager import redis_manager
import json
import logging
import cloudinary
import cloudinary.uploader
from typing import Tuple, Dict
from redis.asyncio import Redis
import aiohttp
from PIL import Image
import numpy as np

from telebot.async_telebot import AsyncTeleBot
from telebot.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from typing import Union, Optional, Dict, Any, List, Tuple

from redis import WatchError
from redis.asyncio import Redis
from pydantic import BaseModel, ValidationError

from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query
from redis.exceptions import RedisError
from datetime import datetime, timedelta

from forex_python.converter import CurrencyRates

from utils.config import (
    SERVICE_INDEX, SERVICE_PREFIX,
    INLINE_CACHE_PREFIX, CACHE_DURATION,
    CACHE_RESULTS_PER_PAGE, CACHE_EXPIRY,
    APP_COUNT, BOT_TOKEN, CHANNEL_ID
)
from utils.functions import small_caps, decode_barcode_id, encode_order_id, AdvancedLogger, convert_usd_to_rub, convert_rub_to_usd
from utils.redis_manager import RedisManager, redis_manager
from utils.redis_keys import RedisKeys
from handlers.manager.operation_lock import  OperationType, AsyncOperationContext, operation_lock_manager

SMS_BOWER_TAX = 1.27
GRIZZLY_SMS_TAX = 1.25
SMS_ACTIVATE_TAX = 1.14
FIVE_SIM_TAX = 1.12

# ---------------- Global Constants ----------------
ORDER_INFO_INDEX = "order_index"
USER_INFO_INDEX = "user_index"
ORDER_INFO_PREFIX = "order_data:"
USER_INFO_PREFIX = "user_data:"
DEPOSIT_INFO_INDEX = "deposit_index"
DEPOSIT_INFO_PREFIX = "deposit_data:"
user_key_profile = "user_data:{user_id}:profile:main"

ORDER_PREFIX = "987654321"
# Configure Cloudinary
cloudinary.config(
    cloud_name="djfsvvzto",
    api_key="291392939686751",
    api_secret="t5YvGkbk7ez71mzMS-3ZZoBFlFQ"
)

# ---------------- Asynchronous Logging ----------------
class AsyncHandler(logging.Handler):
    def emit(self, record):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        msg = self.format(record)
        loop.run_in_executor(None, print, msg)

_advanced_logger: Optional[AdvancedLogger] = None

async def get_async_logger(enable_logging: bool = True) -> AdvancedLogger:
    """Get or create an AdvancedLogger instance with colored formatting."""
    global _advanced_logger
    if _advanced_logger is None:
        _advanced_logger = AdvancedLogger(where_logger="operation.py")
    return _advanced_logger

# ---------------- Exception Decorator ----------------
def handle_redis_exceptions(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        self_obj = args[0]  # assume first arg is 'self'
        try:
            return await func(*args, **kwargs)
        except RedisError as e:
            if not self_obj.logger:
                self_obj.logger = await get_async_logger()
            await self_obj.logger.error(f"Redis operation error in {func.__name__}: {e}")
            return {'response': False, 'error': str(e)}
        except Exception as e:
            if not self_obj.logger:
                self_obj.logger = await get_async_logger()
            await self_obj.logger.error(f"Error in {func.__name__}: {e}")
            return {'response': False, 'error': str(e)}
    return wrapper

# ---------------- Data Serialization Utilities ----------------
async def serialize_data(data: Any) -> str:
    return await asyncio.get_event_loop().run_in_executor(None, json.dumps, data)
async def deserialize_data(data: Optional[str]) -> Optional[Dict]:
    if not data:
        return None
    try:
        return await asyncio.get_event_loop().run_in_executor(None, json.loads, data)
    except json.JSONDecodeError:
        logger = await get_async_logger()
        await logger.error("Error deserializing data")
        return None

# ---------------- OrderManagement Class ----------------
class OrderManagement:
    """Manage order operations with Redis asynchronously."""
    
    def __init__(self, redis_manager: RedisManager, enable_logging: bool = True):
        self.redis_manager = redis_manager
        self.redis_keys = None
        self._initialized = False

        self.logger: Optional[AdvancedLogger] = None
        self.enable_logging = enable_logging
        self.CANDIDATES_KEY = "free_numbers:list"
        self.FIELD_MAP = {
            "PRICE": "order_amount",
            "DATE":  "recorded_at"
            }

    async def _init_logger(self):
        if not self.logger:
            self.logger = await get_async_logger(self.enable_logging)

    async def ensure_initialized(self):
        """Ensure Redis keys are initialized asynchronously."""
        if not self._initialized:
            self.redis_keys = RedisKeys()
            self._initialized = True

    async def build_query(self, filters: dict) -> str:
        """Build a structured query string from a dictionary of filters."""
        query_parts = []
        for field, value in filters.items():
            if value is None:
                continue
            if isinstance(value, (list, tuple)):
                if isinstance(value, list) and value:
                    options = ' | '.join(map(str, value))
                    query_parts.append(f"@{field}:({options})")
                elif isinstance(value, tuple) and len(value) == 2:
                    start, end = value
                    query_parts.append(f"@{field}:[{start} {end}]")
            else:
                query_parts.append(f"@{field}:{value}")
        return ' '.join(query_parts) if query_parts else '*'
    
    async def extract_order_number(self, order_id: str) -> str:
        """
        Extracts the numeric part of the order_id string
        """
        match = re.search(r'\d+', order_id)
        return match.group() if match else ""

    @handle_redis_exceptions
    async def ensure_connection(self):
        """Ensure Redis connection is established asynchronously."""
        await self.ensure_initialized()
        return await self.redis_manager.get_client()

    @handle_redis_exceptions
    async def _init_search_indexes(self):
        """Initialize Redis search indexes for orders asynchronously."""
        await self._init_logger()
        redis_client = await self.ensure_connection()
        try:
            try:
                await redis_client.ft(ORDER_INFO_INDEX).dropindex()
            except RedisError:
                pass

            schema = (
                TextField("order_id", sortable=True),
                TextField("message_id", sortable=True),
                TextField("user_id", sortable=True),
                TextField("server_id", sortable=True),
                TextField("country_id", sortable=True),
                TextField("country_code", sortable=True),
                TextField("app_id", sortable=True),
                TextField("app_name", weight=5.0),
                NumericField("order_amount", sortable=True),
                TextField("order_number"),
                TextField("order_status"),
                TextField("refund_status"),
                TextField("sms_list"),
                TextField("order_history"),
                TextField("search_tags", weight=1.0),
                NumericField("recorded_at", sortable=True)
            )

            await redis_client.ft(ORDER_INFO_INDEX).create_index(
                schema,
                definition=IndexDefinition(
                    prefix=[ORDER_INFO_PREFIX],
                    language="english"
                )
            )

            await self.logger.info("OrderManagement indexes created successfully")
        except Exception as e:
            await self.logger.error(f"Error creating search indexes: {e}")

    @handle_redis_exceptions
    async def create_order_id(self, user_id: str) -> dict:
        """Generate unique order ID asynchronously."""
        base_order_id = await self.redis_manager.redis_client.incr("main_data:order_id")
        timestamp = int(time.time())
        combined = f"{user_id}-{base_order_id}-{timestamp}"
        order_id = int(hashlib.sha256(combined.encode()).hexdigest(), 16) % (10**16)
        return {'response': True, 'result': order_id} if order_id else {'response': False, 'error': 'Failed to generate order ID'}

    @handle_redis_exceptions
    async def add_order_data(self, order_id: str, user_id: str, data: dict) -> dict:
        """Add new order with search indexing asynchronously."""
        await self._init_logger()
        redis_client = await self.ensure_connection()
        
        order_info_key = f"{ORDER_INFO_PREFIX}info:{order_id}"
        
        current_time = time.time()
        data.setdefault('recorded_at', current_time)
        data['search_tags'] = " ".join(filter(None, [
            data.get('app_name', ''),
            data.get('order_status', ''),
            data.get('country_code', ''),
            str(data.get('server_id', '')),
            str(order_id),
            str(user_id)
        ]))
        async with redis_client.pipeline(transaction=True) as pipe:
            await pipe.hset(order_info_key, mapping=data)
            await pipe.execute()

        return {'response': True, 'message': "ORDER-ADDED", 'order_id': order_id}

    @handle_redis_exceptions
    async def get_order_data(self, order_id: str) -> dict:
        """Get order details asynchronously."""
        await self._init_logger()
        key = f"{ORDER_INFO_PREFIX}info:{order_id}"
        order_data = await self.redis_manager.redis_client.hgetall(key)
        if order_data:
            order_data['id'] = key
            return {'response': True, 'result': order_data}
        else:
            return {'response': False, 'error': 'ORDER-NOT-FOUND'}

    @handle_redis_exceptions
    async def update_order_status(self, order_id: str, status: str) -> dict:
        """Update order status with validation asynchronously."""
        await self._init_logger()
        valid_statuses = {'PENDING', 'COMPLETED', 'CANCELLED', 'FAILED', 'TIMEOUT', 'PROCESSING'}
        if status not in valid_statuses:
            return {'response': False, 'error': 'Invalid status'}

        order_data = await self.get_order_data(order_id)
        if not order_data.get('response'):
            return {'response': False, 'error': 'Order not found'}

        order_info_key = f"{ORDER_INFO_PREFIX}info:{order_id}"
        update_data = {'order_status': status}
        
        await self.redis_manager.redis_client.hset(order_info_key, mapping=update_data)
        return {'response': True, 'message': f'Order status updated to {status}'}

    @handle_redis_exceptions
    async def update_order_fields(self, order_id: str, fields: dict) -> dict:
        """Update specific fields of an order asynchronously."""
        await self._init_logger()
        order_data = await self.get_order_data(order_id)
        if not order_data.get('response'):
            return {'response': False, 'error': 'Order not found'}

        order_info_key = f"{ORDER_INFO_PREFIX}info:{order_id}"
        await self.redis_manager.redis_client.hset(order_info_key, mapping=fields)
        return {'response': True, 'message': 'Order fields updated successfully'}

    @handle_redis_exceptions
    async def update_order_success(self, order_id: str, sms: str, timeout: float, order_status: str, refund_status: str) -> dict:
        """Update success of an order using Redis pipeline asynchronously."""
        await self._init_logger()
        redis_client = await self.ensure_connection()
        order_info_key = f"{ORDER_INFO_PREFIX}info:{order_id}"
        
        order_data = await self.get_order_data(order_id)
        if not order_data.get('response'):
            return {'response': False, 'error': 'Order not found'}
        
        order_info = order_data.get('result', {})
        try:
            current_sms_list = json.loads(order_info.get('sms_list', '[]'))
        except Exception:
            current_sms_list = []
            await self.logger.warning(f'Invalid sms_list format: {order_info.get("sms_list", "[]")}')
        try:
            current_history = json.loads(order_info.get('order_history', '[]'))
        except Exception:
            current_history = []
            await self.logger.warning(f'Invalid order_history format: {order_info.get("order_history", "[]")}')

        if not isinstance(current_sms_list, list):
            await self.logger.warning(f'current_sms_list is not a list: {current_sms_list}')
            current_sms_list = []
        if not isinstance(current_history, list):
            await self.logger.warning(f'current_history is not a list: {current_history}')
            current_history = []
        
        sms_list = current_sms_list + [sms]
        current_history.append({
            "timestamp": time.time(),
            "action": "SMS_RECEIVED",
            "sms": sms
        })
        
        updates = {
            'last_sms': sms,
            'sms_list': json.dumps(sms_list),
            'sms_count': len(sms_list),
            'order_history': json.dumps(current_history),
            'refund_status': refund_status,
            'order_status': order_status,
            'timeout': timeout
        }
        
        await redis_client.hset(order_info_key, mapping=updates)
        return {'response': True, 'message': 'Order updated successfully'}

    @handle_redis_exceptions
    async def cancel_order(self, order_id: str, user_id: str, status: str = 'CANCELLED') -> dict:
        """Cancel an order and process refund asynchronously."""
        await self._init_logger()
        await self.logger.info(f"Attempting to cancel order {order_id} for user {user_id}")
        
        order_data = await self.get_order_data(order_id)
        if not order_data.get('response'):
            await self.logger.warning(f"Order {order_id} not found during cancellation")
            return {'response': False, 'error': 'Order not found'}

        order_info = order_data.get('result', {})
        if order_info.get('order_status') in ['CANCELLED', 'TIMEOUT']:
            await self.logger.info(f"Order {order_id} was already {status}")
            return {'response': False, 'error': f'Order already {status}'}

        order_info_key = f"{ORDER_INFO_PREFIX}info:{order_id}"
        
        await self.logger.info(f"Updating order status to {status.lower()} for order {order_id}")
        
        if order_info.get('refund_status') == 'true':
            return {'response': False, 'error': 'Order is already refunded'}
        if order_info.get('order_status') == 'PROCESSING':
            return {'response': False, 'error': 'Order status is PROCESSING'}
        if order_info.get('sms_list', '[]') != '[]':
            return {'response': False, 'error': 'Order has SMS'}
        if order_info.get('last_sms'):
            return {'response': False, 'error': 'Order has SMS'}

        try:
            history = json.loads(order_info.get('order_history', '[]'))
        except Exception:
            await self.logger.warning("Failed to load order_history, initializing new history list")
            history = []
        history.append({
            "timestamp": time.time(),
            "action": f"ORDER_{status}"
        })

        updates = {
            'order_status': status,
            'refund_status': 'true',
            'cancelled_at': datetime.utcnow().isoformat(),
            'order_history': json.dumps(history)
        }
        
        async with self.redis_manager.redis_client.pipeline(transaction=True) as pipe:
            await pipe.hset(order_info_key, mapping=updates)
            await pipe.execute()

        await self.logger.info(f"Successfully {status.lower()} order {order_id} with refund")
        return {'response': True, 'message': f'Order {status} and refunded successfully'}

    @handle_redis_exceptions
    async def search_orders_advanced(self, filters: dict, sort_by: str = None, sort_asc: bool = True, offset: int = 0, limit: int = 10) -> dict:
        """Search orders with advanced filtering."""
        await self._init_logger()
        redis_client = await self.ensure_connection()
        query_str = await self.build_query(filters)
        
        await self.logger.info(f"Searching orders with query: {query_str}")
        query = Query(query_str).paging(offset, limit)
        if sort_by:
            query.sort_by(sort_by, asc=sort_asc)

        results = await redis_client.ft(ORDER_INFO_INDEX).search(query)
        orders = await asyncio.gather(*[self.process_doc(doc) for doc in results.docs])
        return {'response': True, 'total_orders': results.total, 'results': orders}

    @handle_redis_exceptions
    async def search_current_orders(self, query_str: str = "*", sort_by: str = None, sort_asc: bool = True, limit: int = 10, offset: int = 0) -> dict:
        """Search current orders with advanced filtering."""
        await self._init_logger()
        redis_client = await self.ensure_connection()
        
        base_query = "(@order_status:(PENDING|PROCESSING))"
        if query_str != "*":
            base_query += f" ({query_str})"
        
        query = Query(base_query).paging(offset, limit)
        if sort_by:
            query.sort_by(sort_by, asc=sort_asc)
        
        results = await redis_client.ft(ORDER_INFO_INDEX).search(query)
        orders = await asyncio.gather(*[self.process_doc(doc) for doc in results.docs])
        return {'response': True, 'total': results.total, 'results': orders}

    async def process_doc(self, doc) -> dict:
        """Process individual document from search results."""
        return {k: v for k, v in doc.__dict__.items() if not k.startswith('__')}

    async def aggregate_orders(
        self,
        filters: Dict[str, Any],
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Ultra-fast order aggregation using RediSearch.

        Returns:
            {
                "total_amount": float,
                "count": int,
                "order_ids": List[[float order_amount,
                                   float recorded_at,
                                   str order_number]]
            }
        """
        return_ids = filters.pop("_return_ids", False)
        sort_specs = filters.pop("sort", [])

        try:
            # 1) Build query
            query_str = await self.build_query(filters)

            # 2) Aggregation command
            agg_cmd = [
                "FT.AGGREGATE",
                ORDER_INFO_INDEX,
                query_str,
                "GROUPBY", "0",
                "REDUCE", "SUM", "1", "@order_amount", "AS", "total_amount",
                "REDUCE", "COUNT", "0", "AS", "count"
            ]

            # 3) Optional: build ID fetch command using FT.AGGREGATE
            id_cmd: Optional[List[Any]] = None
            if return_ids:
                id_cmd = [
                    "FT.AGGREGATE",
                    ORDER_INFO_INDEX,
                    query_str,
                    "LOAD", "3", "__key", "order_amount", "recorded_at"
                ]

                if sort_specs:
                    id_cmd += ["SORTBY", str(len(sort_specs) * 2)]
                    for spec in sort_specs:
                        redis_field = self.FIELD_MAP[spec["field"]]
                        id_cmd += [f"@{redis_field}", spec["direction"]]

                if limit is not None:
                    id_cmd += ["LIMIT", "0", str(limit)]

            # 4) Pipeline both commands
            pipe = self.redis_manager.redis_client.pipeline(transaction=False)
            pipe.execute_command(*agg_cmd)
            if id_cmd:
                pipe.execute_command(*id_cmd)

            results = await pipe.execute()

            # 5) Parse aggregation result
            output = {"total_amount": 0.0, "count": 0}
            if results[0] and len(results[0]) > 1:
                row = results[0][1]
                data = {row[i]: row[i+1] for i in range(0, len(row), 2)}
                output["total_amount"] = float(data.get("total_amount", 0))
                output["count"] = int(data.get("count", 0))

            # 6) Add order IDs if requested
            if return_ids and len(results) > 1:
                _, *rows = results[1]
                order_rows = []
                for row in rows:
                    row_dict = {row[i]: row[i+1] for i in range(0, len(row), 2)}
                    raw_key = row_dict.get("__key")
                    if raw_key:
                        order_number = await self.extract_order_number(raw_key)
                        order_rows.append([
                            ("amount", float(row_dict.get("order_amount", 0))),
                            ("timestamp", float(row_dict.get("recorded_at", 0))),
                            ("order_number", order_number)
                        ])

                output["order_ids"] = order_rows

            return output

        except Exception as e:
            print(f"[aggregate_orders] {e}")
            return {
                "total_amount": 0.0,
                "count": 0,
                "order_ids": [] if return_ids else None,
            }

    async def manage_number_order(self,
        redis_client: Redis = None,
        country_id: int = None,
        server_id: int = None,
        app_id: str = None,
        operator: str = None,
        order_id: Optional[str] = None,
        action: str = "reserve",   # reserve | add | status | cancel
        user_id: Optional[int] = None,
        sms_code: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Reserve: pick a random free number with HRANDFIELD, mark it reserved
        Add:    embed the SMS code into that number via its order_id
        Status: look up status by stripping prefix→num
        Cancel: same, but reset that field
        """
        numbers_key = f"free_numbers:{country_id}:{server_id}:{app_id}:{operator}"
        print(numbers_key)
        # helper to decode a single hash-field:
        async def get_data(num: str) -> Dict[str, Any]:
            raw = await redis_client.hget(numbers_key, num)
            return json.loads(raw) if raw else {}

        # helper to write back a single field
        async def set_data(num: str, data: Dict[str, Any]):
            await redis_client.hset(numbers_key, num, json.dumps(data))

        # ────────────── RESERVE ──────────────
        if action == "reserve":
            now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

            # try up to N times to find a free number
            for _ in range(1000):  
                num = await redis_client.hrandfield(numbers_key)
                if not num:
                    return {"status": False, "message": "NO_NUMBERS"}

                # Redis returns bytes if decode_responses=False; normalize:
                if isinstance(num, bytes):
                    num = num.decode()

                print(f"[manage_number_order] Attempting to reserve {num}")
                data = await get_data(num)
                print(f"[manage_number_order] Data for {num}: {data}")
                # skip if already used
                if data.get("sms_received"):
                    print(f"[manage_number_order] {num} already has SMS")
                    continue
                print(f"[manage_number_order] user_id: {user_id}")
                if int(user_id) in data.get("user_ids", []):
                    print(f"[manage_number_order] {num} already has user {user_id}")
                    continue

                # now reserve this `num`
                new_order = order_id or f"{ORDER_PREFIX}{num}"
                print(f"[manage_number_order] Reserved {num} to {new_order}")
                data.update({
                    "order_id":    new_order,
                    "sms_received": True,
                    "sms_waiting":  "STATUS_WAIT_CODE",
                    "reserved_at":  now_iso,
                    # track multiple reservers if you want:
                    "user_ids":    data.get("user_ids", []) + ([user_id] if user_id else [])
                })
                await self.add_candidates(num)
                await set_data(num, data)

                return {
                    "status":   True,
                    "number":   num,
                    "order_id": new_order,
                    "details":  data
                }

            return {"status": False, "message": "NO_NUMBERS"}

        # For add/status/cancel we reconstruct the number from the order_id:
        if not order_id or not order_id.startswith(ORDER_PREFIX):
            return {"status": False, "message": "INVALID_ORDER_ID"}

        num = order_id[len(ORDER_PREFIX):]  # strip prefix to get the phone

        data = await get_data(num)
        if not data:
            return {"status": False, "message": "STATUS_WAIT_CODE"}

        # ────────────── ADD SMS CODE ──────────────
        if action == "add":
            if not sms_code:
                return {"status": False, "message": "NO_SMS_CODE"}
    
            data["sms_waiting"] = f"STATUS_OK:{sms_code}"
            await set_data(num, data)

            return {
                "status":      True,
                "number":      num,
                "order_id":    order_id,
                "sms_waiting": data["sms_waiting"]
            }

        # ────────────── STATUS ──────────────
        if action == "status":
            sms_waiting = data.get("sms_waiting", "STATUS_WAIT_CODE")
            reserved_at = data.get("reserved_at")

            # auto-cancel after 10 minutes
            if reserved_at:
                try:
                    then = datetime.strptime(reserved_at, "%Y-%m-%dT%H:%M:%SZ")
                    if sms_waiting == "STATUS_WAIT_CODE" and datetime.utcnow() - then > timedelta(minutes=10):
                        data["sms_waiting"] = "STATUS_CANCEL"
                        await set_data(num, data)
                        sms_waiting = "STATUS_CANCEL"
                except ValueError:
                    pass

            return {
                "status":      True,
                "order_id":    order_id,
                "number":      num,
                "sms_waiting": sms_waiting
            }

        # ────────────── CANCEL ──────────────
        if action == "cancel":
            if data.get("sms_waiting") != "STATUS_WAIT_CODE":
                return {"status": False, "message": "STATUS_CANCEL"}

            data.update({
                "order_id":     "",
                "sms_received": False,
                "sms_waiting":  "",
                "reserved_at":  "",
                "user_ids":     []
            })
            await set_data(num, data)

            return {
                "status":  True,
                "message": "Number canceled successfully",
                "number":  num
            }

        return {"status": False, "message": "INVALID_ACTION"}
    
    async def get_candidates(self) -> List[str]:
        """
        Fetches the JSON‐encoded list of candidate numbers from Redis.
        Returns an empty list if key is missing or invalid.
        """
        raw = await self.redis_manager.redis_client.get(self.CANDIDATES_KEY)
        if not raw:
            return []
        # raw might be bytes or str
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="ignore")
        try:
            data = json.loads(raw)
            # ensure it's a list of strings
            if isinstance(data, list):
                return [str(item) for item in data]
        except json.JSONDecodeError:
            pass
        return []

    async def add_candidates(self, new: Union[str, List[str]]) -> None:
        """
        Adds one or more new candidate numbers to the Redis list,
        avoiding duplicates, and re‐saves as JSON.
        """
        # normalize to a flat list of strings
        if isinstance(new, str):
            to_add = [new]
        else:
            to_add = [str(x) for x in new]

        current = await self.get_candidates()
        # union while preserving order
        updated = current[:]
        for num in to_add:
            if num not in updated:
                updated.append(num)

        # save back to Redis
        await self.redis_manager.redis_client.set(
            self.CANDIDATES_KEY,
            json.dumps(updated)
        )

class UserManagement:
    """Manage user operations with Redis asynchronously."""
    
    def __init__(self, redis_manager: RedisManager, bot_token: Optional[str] = None, channel_id: Optional[str] = None, enable_logging: bool = True):
        self.redis_manager = redis_manager
        self.redis_keys = None
        self._initialized = False
        self.bot_token = bot_token
        self.channel_id = channel_id
        self.logger: Optional[AdvancedLogger] = None
        self.enable_logging = enable_logging
        self.lock_manager = operation_lock_manager

    async def ensure_connection(self):
        """Ensure Redis connection is established asynchronously."""
        await self.ensure_initialized()
        return await self.redis_manager.get_client()

    async def _init_logger(self):
        if not self.logger:
            self.logger = await get_async_logger(self.enable_logging)

    async def ensure_initialized(self):
        """Ensure Redis keys are initialized asynchronously."""
        if not self._initialized:
            self.redis_keys = RedisKeys()
            self._initialized = True

    async def _send_telegram_request(self, method: str, payload: dict) -> Optional[dict]:
        url = f'https://api.telegram.org/bot{self.bot_token}/{method}'
        headers = {'Content-Type': 'application/json'}
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 200:
                    return await response.json()
        return None

    async def get_random_safe_emoji_id(self):
        url = f"https://api.telegram.org/bot{self.bot_token}/getForumTopicIconStickers"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return None
                data = await response.json()

        if not data.get("ok"):
            return None

        restricted_emojis = ["🍆", "🍑", "🔞", "🥃", "🍺", "🍷", "🍸", "🚬"]
        safe_stickers = [sticker for sticker in data.get("result", [])
                         if sticker.get("emoji") not in restricted_emojis]
        
        return random.choice(safe_stickers).get("custom_emoji_id") if safe_stickers else None

    async def user_metrics_report(self, bot: AsyncTeleBot, method: str, user_id: str, channel_id: str, forum_id: Optional[str] = None) -> Optional[int]:
        await self._init_logger()
        try:
            # Assume financial_summary_mgr is defined elsewhere
            data = await financial_mgr.get_user(user_id)
            if not data or not data.get('response'):
                await self.logger.error("User data response indicated failure.")
                return None

            if forum_id is None:
                profile_key = f"user_data:{user_id}:profile:main"
                forum_id = await self.redis_manager.redis_client.hget(profile_key, "forum_id")

            username = data['user_profile'][:15]
            metrics = data['metrics']
            balance = metrics['current_balance']
            total_spend = metrics['spend_balance']
            total_deposited = metrics['deposits']['total_amount']
            deposit_count = metrics['deposits']['count']
            total_orders = metrics['orders']['count']
            total_order_value = metrics['orders']['total_amount']
            message = (
                f" 👤 <b>Usᴇʀ:</b> <code>{username}</code> <b>||</b> <code>{user_id}</code>\n\n"
                "<b>╭─────────────────────╮</b>\n"
                "<code>│</code><b>     📊 Usᴇʀ Mᴇᴛʀɪᴄs Rᴇᴘᴏʀᴛ         </b><code>│</code>\n"
                "<b>╰─────────────────────╯</b>\n\n"
                "<b>╭─────────────────────╮</b>\n"
                "<b>│ 💰 Bᴀʟᴀɴᴄᴇ Sᴜᴍᴍᴀʀʏ!                 │</b>\n"
                "<b>├─────────────────────┤</b>\n"
                f"<b>│ 💵 Bᴀʟᴀɴᴄᴇ:</b> <code>{balance:.2f}</code> Pᴏɪɴᴛ{'s' if balance != 1 else ''}\n"
                f"<b>│ 💸 Tᴏᴛᴀʟ Sᴘᴇɴᴅ:</b> <code>{total_spend:.2f}</code> Pᴏɪɴᴛ{'s' if total_spend != 1 else ''}\n"
                "<b>╰─────────────────────╯</b>\n\n"
                "<b>╭─────────────────────╮</b>\n"
                "<b>│ 📥 Dᴇᴘᴏsɪᴛ Sᴜᴍᴍᴀʀʏ!                  │</b>\n"
                "<b>├─────────────────────┤</b>\n"
                f"<b>│ 💰 Tᴏᴛᴀʟ Dᴇᴘᴏsɪᴛᴇᴅ:</b> <code>{total_deposited:.2f}</code> 💎\n"
                f"<b>│ 🔄 Dᴇᴘᴏsɪᴛ Cᴏᴜɴᴛ:</b> <code>{deposit_count}</code> Tɪᴍᴇ{'s' if deposit_count != 1 else ''}\n"
                "<b>╰─────────────────────╯</b>\n\n"
                "<b>╭─────────────────────╮</b>\n"
                "<b>│ 🛒 Oʀᴅᴇʀ Sᴜᴍᴍᴀʀʏ!                     │</b>\n"
                "<b>├─────────────────────┤</b>\n"
                f"<b>│ 🛍 Tᴏᴛᴀʟ Oʀᴅᴇʀs:</b> <code>{total_orders}</code> Oʀᴅᴇʀ{'s' if total_orders != 1 else ''}\n"
                f"<b>│ 🏷 Tᴏᴛᴀʟ Oʀᴅᴇʀ :</b> <code>{total_order_value:.2f}</code> Pᴏɪɴᴛ{'s' if total_order_value != 1 else ''}\n"
                "<b>╰─────────────────────╯</b>\n\n"
                f" ✅ <code>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</code>"
            )
            admin_keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton('↻ Rᴇғʀᴇsʜ', callback_data=f'#RᴇғʀᴇsʜMᴇᴛʀɪᴄs:{user_id}'),
                    InlineKeyboardButton('🔗 Usᴇʀ', url=f'tg://openmessage?user_id={user_id}')
                ]
            ])
            
            try:
                if method == 'edit_message_text':
                    profile_key = f"user_data:{user_id}:profile:main"
                    forum_message_id = await self.redis_manager.redis_client.hget(profile_key, "forum_message_id")
                    if forum_message_id is None:
                        await self.logger.error("Forum message ID is None.")
                        return None
                    result = await bot.edit_message_text(
                        chat_id=channel_id,
                        message_id=int(forum_message_id),
                        text=message,
                        reply_markup=admin_keyboard,                
                        parse_mode='HTML'
                    )
                else:
                    result = await bot.send_message(
                        chat_id=channel_id,
                        text=message,
                        reply_markup=admin_keyboard,           
                        message_thread_id=forum_id,
                        parse_mode='HTML'
                    )
                    if result:
                        try:
                            await bot.pin_chat_message(
                                chat_id=channel_id,
                                message_id=result.message_id,
                                disable_notification=True
                            )
                        except Exception as e:
                            print(f"Error pinning message: {str(e)}")
                return result.message_id if result else None
            except Exception as e:
                print(f"Error in user_metrics_report: {str(e)}")
                return None
        except Exception as e:
            print(f"Error in user_metrics_report: {str(e)}")
            return None

    async def send_order_report(self, bot: AsyncTeleBot, method: str, order_id: str, user_id: str, channel_id: str, details: dict, is_api: bool = False) -> Optional[int]:
        await self._init_logger()
        try:
            await self.logger.info(f"Sending order report for order_id: {order_id}, user_id: {user_id}")
            
            profile_key = f"user_data:{user_id}:profile:main"
            forum_id = await self.redis_manager.redis_client.hget(profile_key, "forum_id")
            await self.logger.info(f"Retrieved forum_id: {forum_id}")

            message = "<b>#Usᴇʀ_Oʀᴅᴇʀ_Dᴇᴛᴀɪʟs ❯</b>\n\n<b>Tʀᴀɴsᴀᴄᴛɪᴏɴ Dᴇᴛᴀɪʟs »</b>\n" if int(details.get('msg_id')) != int("0") else "<b>#Aᴘɪ_Oʀᴅᴇʀ_Dᴇᴛᴀɪʟs ❯</b>\n\n<b>Tʀᴀɴsᴀᴄᴛɪᴏɴ Dᴇᴛᴀɪʟs »</b>\n"
            valid_status = details.get('valid_status' if method == 'edit_message_text' else 'valid_until', '')
            if valid_status in ['⏱️ Oʀᴅᴇʀ Is Cᴀɴᴄᴇʟʟᴇᴅ', '⏱️ Oʀᴅᴇʀ Hᴀs Exᴘɪʀᴇᴅ', '✅ Oʀᴅᴇʀ Hᴀs Cᴏᴍᴘʟᴇᴛᴇᴅ'] or ':' in valid_status:
                message += "<blockquote expandable>"
            
            barcode_id = await encode_order_id(str(order_id))
            message += (
                f"📦 <b>Aᴘᴘ Nᴀᴍᴇ »</b> <code>{details.get('app_name', 'N/A').translate(await small_caps())}</code>\n"
                f"💰 <b>Pʀɪᴄᴇ »</b> <code>{details.get('app_price', 'N/A')}</code> 💎 [ <code>{details.get('server_id', 'N/A')}</code> ]\n"
                f"🌍 <b>Rᴇɢɪᴏɴ »</b> <code>{details.get('country_name', 'N/A').translate(await small_caps())}</code> [ <code>{details.get('country_code', '🌍')}</code> ]\n\n"
                f"<b>Cᴏɴᴛᴀᴄᴛ Dᴇᴛᴀɪʟs »</b>\n"
                f"💳 <code>{order_id}</code>\n"
                f"📞 <code>{details.get('code', 'N/A')}</code> <code>{details.get('number', 'N/A')}</code>\n"
                f"⎚ Cᴏᴅᴇ » <code>{barcode_id}</code>\n"
            )
            if details.get('sms_list', 'N/A') != 'N/A':
                message += f"🔐 <b>Cᴏᴅᴇs »</b> {details.get('sms_list', 'N/A')}\n"
            
            if valid_status in ['⏱️ Oʀᴅᴇʀ Is Cᴀɴᴄᴇʟʟᴇᴅ', '⏱️ Oʀᴅᴇʀ Hᴀs Exᴘɪʀᴇᴅ', '✅ Oʀᴅᴇʀ Hᴀs Cᴏᴍᴘʟᴇᴛᴇᴅ'] or ':' in valid_status:
                message += "</blockquote>"
            
            if details.get('valid_until', 'N/A') != 'N/A':
                message += f"\n⏱️ <b>Uɴᴛɪʟ »</b> {details.get('valid_until', 'N/A')}"
            elif details.get('valid_status', 'N/A') != 'N/A':
                message += f"\n<b>{details.get('valid_status', 'N/A')}</b>"
            
            admin_keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton('🔗 Usᴇʀ', url=f'tg://openmessage?user_id={user_id}'),
                    InlineKeyboardButton('⌕ Dᴇᴛᴀɪʟs', callback_data='placeholder')
                ]
            ])
            
            try:
                if method == 'edit_message_text':
                    profile_key = f"{ORDER_INFO_PREFIX}info:{order_id}"
                    forum_message_id = await self.redis_manager.redis_client.hget(profile_key, "forum_message_id")
                    if forum_message_id is None:
                        await self.logger.error("Forum message ID is None.")
                        return None
                    await self.logger.info(f"Editing message with ID: {forum_message_id}")
                    result = await bot.edit_message_text(
                        chat_id=channel_id,
                        message_id=int(forum_message_id),
                        text=message,
                        reply_markup=admin_keyboard,
                        parse_mode='HTML'
                    )
                else:
                    await self.logger.info(f"Sending new message to channel: {channel_id}")
                    result = await bot.send_message(
                        chat_id=channel_id,
                        text=message,
                        reply_markup=admin_keyboard,           
                        message_thread_id=forum_id,
                        parse_mode='HTML'
                    )
                    message_id = result.message_id if result else None
                    order_info_key = f"{ORDER_INFO_PREFIX}info:{order_id}"
                    await self.logger.info(f"Storing message_id: {message_id} for order: {order_id}")
                    await self.redis_manager.redis_client.hset(order_info_key, "forum_message_id", message_id)

                return result.message_id if result else None
            except Exception as e:
                await self.logger.error(f"Error in send_order_report: {str(e)}")
                return None
        except Exception as e:
            await self.logger.error(f"Error in send_order_report: {str(e)}")
            return None

    async def create_forum_topic(self, user_id: str, topic_name: str) -> Optional[dict]:
        await self._init_logger()
        random_colors = [0x6FB9F0, 0xFFD67E, 0xCB86DB, 0x8EEE98, 0xFF93B2, 0xFB6F5F]
        icon_color = random.choice(random_colors)
        custom_emoji_id = await self.get_random_safe_emoji_id()
        
        payload = {
            'chat_id': self.channel_id,
            'name': topic_name,
            "icon_custom_emoji_id": custom_emoji_id,
            'icon_color': icon_color
        }
        
        result = await self._send_telegram_request('createForumTopic', payload)
        if result and result.get('ok'):
            forum_data = result.get('result')
            profile_key = f"user_data:{user_id}:profile:main"
            await self.redis_manager.redis_client.hset(profile_key, "forum_id", forum_data.get("message_thread_id"))
            return forum_data
        return None

    async def update_forum_topic(self, user_id: str, new_name: Optional[str] = None, new_icon_color: Optional[str] = None) -> Optional[dict]:
        await self._init_logger()
        profile_key = f"user_data:{user_id}:profile:main"
        forum_id = await self.redis_manager.redis_client.hget(profile_key, "forum_id")
        if not forum_id:
            return None

        payload = {"chat_id": self.channel_id, "message_thread_id": forum_id}
        if new_name:
            payload["name"] = new_name
        if new_icon_color:
            payload["icon_color"] = new_icon_color

        if len(payload) > 2:  # more than just chat_id and message_thread_id
            result = await self._send_telegram_request('editForumTopic', payload)
            return result.get("result") if result and result.get("ok") else None
        return None

    async def list_forum_topics(self) -> dict:
        await self._init_logger()
        pattern = "user_data:*:profile:main"
        keys = await self.redis_manager.redis_client.keys(pattern)
        topics = {}
        for key in keys:
            forum_id = await self.redis_manager.redis_client.hget(key, "forum_id")
            if forum_id:
                topics[key] = {"forum_id": forum_id}
        return topics

    async def archive_forum_topic(self, user_id: str) -> Optional[dict]:
        await self._init_logger()
        profile_key = f"user_data:{user_id}:profile:main"
        forum_id = await self.redis_manager.redis_client.hget(profile_key, "forum_id")
        if not forum_id:
            return None
        
        payload = {"chat_id": self.channel_id, "message_thread_id": forum_id}
        result = await self._send_telegram_request('closeForumTopic', payload)
        
        if result and result.get("ok"):
            await self.redis_manager.redis_client.hset(profile_key, "forum_archived", "true")
            return result.get("result")
        return None

    async def reopen_forum_topic(self, user_id: str) -> Optional[dict]:
        await self._init_logger()
        profile_key = f"user_data:{user_id}:profile:main"
        forum_id = await self.redis_manager.redis_client.hget(profile_key, "forum_id")
        if not forum_id:
            return None
        
        payload = {"chat_id": self.channel_id, "message_thread_id": forum_id}
        result = await self._send_telegram_request('reopenForumTopic', payload)
        
        if result and result.get("ok"):
            await self.redis_manager.redis_client.hset(profile_key, "forum_archived", "false")
            return result.get("result")
        return None

    async def get_forum_topic_details(self, user_id: str) -> dict:
        await self._init_logger()
        profile_key = f"user_data:{user_id}:profile:main"
        forum_id = await self.redis_manager.redis_client.hget(profile_key, "forum_id")
        return {"forum_id": forum_id}
    
    # -------------- User Management Async Methods --------------
    @handle_redis_exceptions
    async def _init_search_indexes(self):
        """Creates RediSearch indexes with the defined schemas."""
        await self._init_logger()
        redis_client = await self.ensure_connection()

        async def create_index(index_name: str, schema: list, prefix: str):
            try:
                await redis_client.ft(index_name).dropindex(delete_documents=True)
            except Exception as e:
                await self.logger.warning(f"Index '{index_name}' did not exist or could not be dropped: {e}")
            definition = IndexDefinition(prefix=[prefix], index_type=IndexType.HASH)
            await redis_client.ft(index_name).create_index(fields=schema, definition=definition)

        user_schema = [
            TextField("user_id", sortable=True),
            TextField("username", sortable=True),
            TextField("first_name", sortable=True),
            TextField("last_name", sortable=True),
            TextField("language_code", sortable=True),
            TextField("forum_id"),
            TextField("status", sortable=True),
            TextField("registration_date", sortable=True)
        ]

        service_schema = [
            TextField("record_id", sortable=True),
            TextField("search_tags", weight=1.0),
            TextField("is_show_server", weight=1.0),
            TextField("is_show_app", weight=1.0),
            TextField("is_show_country", weight=1.0),
            TextField("country_name", sortable=True),
            TextField("country_code", sortable=True),
            TextField("country_id"),
            TextField("server_name", sortable=True),
            TextField("server_id", sortable=True),
            TextField("app_id"),
            TextField("app_name", weight=5.0),
            TextField("app_code"),
            NumericField("app_price", sortable=True),
            NumericField("app_count", sortable=True)
        ]

        try:
            # Try USER_INFO_INDEX
            try:
                await redis_client.ft(USER_INFO_INDEX).info()
            except Exception as e:
                await self.logger.warning(f"USER_INFO_INDEX did not exist or could not be dropped: {e}")
                await create_index(USER_INFO_INDEX, user_schema, USER_INFO_PREFIX)
                

            # Try SERVICE_INDEX
            try:
                await redis_client.ft(SERVICE_INDEX).info()
            except Exception as e:
                await self.logger.warning(f"SERVICE_INDEX did not exist or could not be dropped: {e}")
                await create_index(SERVICE_INDEX, service_schema, SERVICE_PREFIX)
                

            await self.logger.info("UserManagement and Service indexes verified/created successfully")

        except RedisError as e:
            await self.logger.error(f"Redis error while creating indexes: {e}")
            raise

    @handle_redis_exceptions
    async def update_user_data(self, user_id: str, user_data: dict) -> dict:
        """Update user data with enhanced validation and security checks."""
        async with AsyncOperationContext(operation_lock_manager, OperationType.PROFILE_UPDATE, user_id):
            await self._init_logger()
            redis_client = await self.ensure_connection()
            
            try:
                # Get existing user data
                existing_data = await self.get_user_data(user_id)
                if not existing_data.get('response'):
                    return {'response': False, 'error': 'User not found'}

                # Update only allowed fields
                allowed_fields = {'username', 'email', 'settings', 'preferences'}
                update_data = {k: v for k, v in user_data.items() if k in allowed_fields}
                
                if not update_data:
                    return {'response': False, 'error': 'No valid fields to update'}

                # Update the user data
                key = f"{USER_INFO_PREFIX}{user_id}"
                await redis_client.hset(key, mapping=update_data)
                
                return {'response': True, 'result': 'User data updated successfully'}
            except Exception as e:
                await self.logger.error(f"Error updating user data: {e}")
                return {'response': False, 'error': str(e)}

    @handle_redis_exceptions
    async def _atomic_balance_update(self, user_id: str, amount: float, transaction_type: str) -> dict:
        """Perform atomic balance updates with validation and logging."""
        async with AsyncOperationContext(operation_lock_manager, OperationType.BALANCE_UPDATE, user_id):
            await self._init_logger()
            redis_client = await self.ensure_connection()
            
            try:
                # Get current balance
                current_balance = float(await redis_client.hget(f"{USER_INFO_PREFIX}{user_id}", "balance") or 0)
                
                # Validate transaction
                if transaction_type == 'debit' and current_balance < amount:
                    return {'response': False, 'error': 'Insufficient balance'}
                
                # Calculate new balance
                new_balance = current_balance + amount if transaction_type == 'credit' else current_balance - amount
                
                # Update balance atomically
                await redis_client.hset(f"{USER_INFO_PREFIX}{user_id}", "balance", str(new_balance))
                
                # Log transaction
                transaction_data = {
                    'user_id': user_id,
                    'amount': amount,
                    'type': transaction_type,
                    'previous_balance': current_balance,
                    'new_balance': new_balance,
                    'timestamp': datetime.now().isoformat()
                }
                await redis_client.rpush(f"transaction_history:{user_id}", json.dumps(transaction_data))
                
                return {'response': True, 'result': {'new_balance': new_balance}}
            except Exception as e:
                await self.logger.error(f"Error in atomic balance update: {e}")
                return {'response': False, 'error': str(e)}

    @handle_redis_exceptions
    async def create_user(self, user_data: dict) -> dict:
        """Create a new user with search indexing."""
        await self._init_logger()
        user_id = str(user_data.get('user_id'))
        if not user_id:
            return {'response': False, 'error': 'User ID is required'}

        user_data['search_tags'] = " ".join(filter(None, [
            user_data.get('username', ''),
            user_data.get('first_name', ''),
            user_data.get('last_name', ''),
            str(user_id)
        ]))

        now = datetime.utcnow().isoformat()
        user_data['registration_date'] = user_data.get('registration_date', now)
        user_data['last_activity'] = now

        user_key = f"user_data:{user_id}:profile:main"
        try:
            redis_client = await self.ensure_connection()
            await redis_client.hset(user_key, mapping=user_data)
            return {'response': True, 'message': "USER-CREATED", 'user_id': user_id}
        except Exception as e:
            await self.logger.error(f"Error creating user: {e}")
            return {'response': False, 'error': str(e)}

    @handle_redis_exceptions
    async def get_user_data(self, user_id: str) -> Dict[str, Any]:
        """Fetch user profile data from Redis using HGETALL."""
        await self._init_logger()
        user_key = f"user_data:{user_id}:profile:main"
        try:
            redis_client = await self.ensure_connection()
            user_data = await redis_client.hgetall(user_key)
            if not user_data:
                return {"response": False, "error": "USER-NOT-FOUND"}
            return {
                "response": True,
                "result": {k: v for k, v in user_data.items()}
            }
        except Exception as e:
            await self.logger.error(f"Error fetching user data for {user_id}: {e}")
            return {"response": False, "error": str(e)}

    @handle_redis_exceptions
    async def update_user_status(self, user_id: str, new_status: str) -> dict:
        """Update user status."""
        await self._init_logger()
        if new_status not in ["ACTIVE", "BANNED", "SUSPENDED", "INACTIVE"]:
            return {'response': False, 'error': 'Invalid status'}

        user_key = f"user_data:{user_id}:profile:main"
        try:
            redis_client = await self.ensure_connection()
            async with redis_client.pipeline(transaction=True) as pipe:
                await pipe.hset(user_key, "status", new_status)
                await pipe.hset(user_key, "last_activity", datetime.utcnow().isoformat())
                await pipe.execute()
            return {'response': True, 'message': f"User {user_id} status updated to '{new_status}'"}
        except Exception as e:
            await self.logger.error(f"Error updating user status for {user_id}: {e}")
            return {'response': False, 'error': str(e)}

    @handle_redis_exceptions
    async def search_users(self, query_str: str = "*", sort_by: str = None, sort_asc: bool = True, limit: int = 10) -> dict:
        """Search users with advanced filtering."""
        await self._init_logger()
        try:
            redis_client = await self.ensure_connection()
            query = Query(query_str).paging(0, limit)
            if sort_by:
                query.sort_by(sort_by, asc=sort_asc)
            results = await redis_client.ft(USER_INFO_INDEX).search(query)
            users = [{k: v for k, v in doc.__dict__.items() if not k.startswith('__')} for doc in results.docs]
            return {'response': True, 'total': results.total, 'results': users}
        except Exception as e:
            await self.logger.error(f"Error searching users: {e}")
            return {'response': False, 'error': str(e)}

    @handle_redis_exceptions
    async def get_user_value(self, user_id: str, field: str) -> dict:
        """Get a specific user field."""
        await self._init_logger()
        user_key = f"user_data:{user_id}:profile:main"
        try:
            redis_client = await self.ensure_connection()
            value = await redis_client.hget(user_key, field)
            return {'response': True, 'result': value}
        except Exception as e:
            await self.logger.error(f"Error getting user value for {user_id}: {e}")
            return {'response': False, 'error': str(e)}

    @handle_redis_exceptions
    async def set_user_value(self, user_id: str, field: str, value) -> dict:
        """Set a specific user field."""
        await self._init_logger()
        user_key = f"user_data:{user_id}:profile:main"
        try:
            redis_client = await self.ensure_connection()
            await redis_client.hset(user_key, field, value)
            return {'response': True, 'result': True}
        except Exception as e:
            await self.logger.error(f"Error setting user value for {user_id}: {e}")
            return {'response': False, 'error': str(e)}

    @handle_redis_exceptions
    async def update_user_data(self, user_id: str, user_data: dict) -> dict:
        """Update user data with enhanced validation and security checks."""
        async with AsyncOperationContext(operation_lock_manager, OperationType.PROFILE_UPDATE, user_id):
            await self._init_logger()
            user_key = f"user_data:{user_id}:profile:main"
            try:
                redis_client = await self.ensure_connection()
                async with redis_client.pipeline(transaction=True) as pipe:
                    await pipe.hset(user_key, mapping=user_data)
                    await pipe.hset(user_key, "last_updated", str(time.time()))
                    await pipe.execute()
                return {'response': True, 'message': f"User data updated for {user_id}", 'data': user_data}
            except Exception as e:
                await self.logger.error(f"Error updating user data for {user_id}: {e}")
                return {'response': False, 'error': str(e)}
    
    @handle_redis_exceptions
    async def _run_aggregate_cursor(
        self,
        cmd: List[Any],
        index: str,
        batch_size: int = 10_000
    ) -> List[List[Any]]:
        """
        Executes FT.AGGREGATE with WITHCURSOR, returns a flat list of rows.
        """
        cache_key = f"cursor_data:{batch_size}:{index}_" + "_".join(map(str, cmd))
        cache_data = await cache_manager.get(cache_key, prefix=CachePrefix.TEMP)
        if cache_data:
            return cache_data

        all_rows: List[List[Any]] = []

        # Add WITHCURSOR clause
        cmd_ext = [*cmd, "WITHCURSOR", "COUNT", str(batch_size)]

        try:
            response = await self.redis_manager.redis_client.execute_command(*cmd_ext)
            if not isinstance(response, list) or len(response) != 2:
                raise RuntimeError(f"Unexpected Redis response format: {response}")
            results, cursor = response
        except RedisError as e:
            print("Aggregation init failed:", e)
            raise RuntimeError("Redis aggregation initialization error") from e

        # First page
        if isinstance(results, list) and len(results) > 1:
            all_rows.extend(results[1:])

        # Paginated cursor reads
        while cursor:
            try:
                page = await self.redis_manager.redis_client.execute_command(
                    "FT.CURSOR", "READ", index, cursor
                )
                if not isinstance(page, list) or len(page) != 2:
                    raise RuntimeError(f"Unexpected cursor page format: {page}")
                rows, cursor = page
                if len(rows) > 1:
                    all_rows.extend(rows[1:])
            except RedisError as e:
                print("Cursor read failed:", e)
                raise RuntimeError("Redis cursor read error") from e

        await cache_manager.set(cache_key, all_rows, prefix=CachePrefix.TEMP)
        return all_rows

# ---------------- DepositManagement Class ----------------
class DepositManagement:
    """Manage deposit operations with Redis asynchronously."""
    
    def __init__(self, redis_manager: RedisManager, enable_logging: bool = True):
        """
        Initialize with a redis_manager instance.
        
        Args:
            redis_manager: An instance that provides an asynchronous Redis client.
        """
        self.redis_manager = redis_manager
        self.redis_keys = None
        self._initialized = False
        self.logger: Optional[AdvancedLogger] = None
        self.enable_logging = enable_logging
        self.lock_manager = operation_lock_manager

    async def _init_logger(self):
        if not self.logger:
            self.logger = await get_async_logger(self.enable_logging)

    async def ensure_initialized(self) -> None:
        """Ensure deposit-specific keys are initialized asynchronously."""
        if not self._initialized:
            self.redis_keys = RedisKeys()
            self._initialized = True

    async def build_query(self, filters: dict) -> str:
        """
        Build a structured query string from a dictionary of filters asynchronously.
        """
        async def process_filter(field: str, value: Any) -> Optional[str]:
            if value is None:
                return None
            if isinstance(value, list) and value:
                options = '|'.join(f'"{v}"' if ' ' in str(v) else str(v) for v in value)
                return f"@{field}:({options})"
            elif isinstance(value, tuple) and len(value) == 2:
                start, end = value
                return f"@{field}:[{start} {end}]"
            else:
                return f'@{field}:"{value}"' if ' ' in str(value) else f'@{field}:{value}'

        tasks = [process_filter(field, value) for field, value in filters.items()]
        query_parts = await asyncio.gather(*tasks)
        query_parts = [part for part in query_parts if part is not None]
        return ' '.join(query_parts) if query_parts else '*'

    async def process_deposit_doc(self, doc) -> dict:
        """Process individual deposit document from search results asynchronously."""
        return {k: v for k, v in doc.__dict__.items() if not k.startswith('__')}

    @handle_redis_exceptions
    async def ensure_connection(self) -> Any:
        """Ensure that a Redis connection is established asynchronously."""
        await self.ensure_initialized()
        return await self.redis_manager.get_client()

    @handle_redis_exceptions
    async def _init_search_indexes(self) -> None:
        """Initialize Redis search indexes for deposits asynchronously."""
        await self._init_logger()
        try:
            redis_client = await self.ensure_connection()
            try:
                await redis_client.ft(DEPOSIT_INFO_INDEX).dropindex()
            except Exception:
                pass

            schema = (
                TextField("deposit_id", sortable=True),
                TextField("message_id", sortable=True),
                TextField("user_id", sortable=True),
                TextField("server_id", sortable=True),
                NumericField("deposit_amount", sortable=True),
                TextField("deposit_status", sortable=True),
                TextField("search_tags", weight=1.0),
                NumericField("recorded_at", sortable=True)
            )

            await redis_client.ft(DEPOSIT_INFO_INDEX).create_index(
                schema,
                definition=IndexDefinition(
                    prefix=[DEPOSIT_INFO_PREFIX],
                    language="english"
                )
            )
            await self.logger.info("DepositManagement indexes created successfully")
        except Exception as e:
            await self.logger.error(f"Error creating deposit search indexes: {e}")

    @handle_redis_exceptions
    async def create_deposit_id(self, user_id: str) -> dict:
        """Generate a unique deposit ID asynchronously."""
        redis_client = await self.ensure_connection()
        base_deposit_id = await redis_client.incr("main_data:deposit_id")
        timestamp = int(time.time())
        combined = f"{user_id}-{base_deposit_id}-{timestamp}"
        deposit_id = int(hashlib.sha256(combined.encode()).hexdigest(), 16) % (10**16)
        return {'response': True, 'result': deposit_id} if deposit_id else {'response': False, 'error': 'Failed to generate deposit ID'}

    @handle_redis_exceptions
    async def add_deposit_data(self, deposit_id: str, user_id: str, data: Dict[str, Any]) -> dict:
        """Add a new deposit record with search indexing."""
        try:
            redis_client = await self.ensure_connection()
            deposit_info_key = f"{DEPOSIT_INFO_PREFIX}info:{deposit_id}"

            data.setdefault('recorded_at', time.time())
            data['search_tags'] = " ".join(filter(None, [
                data.get('deposit_status', ''),
                str(data.get('amount', '')),
                str(deposit_id),
                str(user_id)
            ]))

            async with redis_client.pipeline() as pipe:
                await pipe.hset(deposit_info_key, mapping=data)
                await pipe.execute()

            return {'response': True, 'message': "DEPOSIT-ADDED", 'deposit_id': deposit_id, 'user_id': user_id, 'result': data}
        except Exception as e:
            await self.logger.error(f"Error adding deposit data: {e}")
            return {'response': False, 'error': str(e)}

    @handle_redis_exceptions
    async def get_deposit_data(self, deposit_id: str) -> dict:
        """Retrieve deposit details asynchronously."""
        await self._init_logger()
        try:
            redis_client = await self.ensure_connection()
            deposit_data = await redis_client.hgetall(f"{DEPOSIT_INFO_PREFIX}info:{deposit_id}")
            if deposit_data:
                await self.logger.info(f"Successfully retrieved deposit data for ID: {deposit_id}")
                return {'response': True, 'result': deposit_data}
            else:
                await self.logger.warning(f"Deposit not found for ID: {deposit_id}")
                return {'response': False, 'error': 'DEPOSIT-NOT-FOUND'}
        except Exception as e:
            await self.logger.error(f"Error retrieving deposit data for ID {deposit_id}: {e}")
            return {'response': False, 'error': str(e)}

    @handle_redis_exceptions
    async def update_deposit_status(self, deposit_id: str, status: str) -> dict:
        """Update the status of a deposit after validating the new status."""
        try:
            valid_statuses = ['PENDING', 'COMPLETED', 'CANCELLED', 'FAILED', 'TIMEOUT']
            if status not in valid_statuses:
                return {'response': False, 'error': 'Invalid status'}

            deposit_data = await self.get_deposit_data(deposit_id)
            if not deposit_data['response']:
                return {'response': False, 'error': 'Deposit not found'}

            deposit_info_key = f"{DEPOSIT_INFO_PREFIX}info:{deposit_id}"
            redis_client = await self.ensure_connection()
            await redis_client.hset(deposit_info_key, 'deposit_status', status)

            return {'response': True, 'message': f'Deposit status updated to {status}'}
        except Exception as e:
            await self.logger.error(f"Error updating deposit status: {str(e)}")
            return {'response': False, 'error': str(e)}

    @handle_redis_exceptions
    async def update_deposit_fields(self, deposit_id: str, fields: Dict[str, Any]) -> dict:
        """Update specific fields of a deposit record."""
        try:
            deposit_data = await self.get_deposit_data(deposit_id)
            if not deposit_data['response']:
                return {'response': False, 'error': 'Deposit not found'}

            deposit_info_key = f"{DEPOSIT_INFO_PREFIX}info:{deposit_id}"
            redis_client = await self.ensure_connection()
            await redis_client.hset(deposit_info_key, mapping=fields)

            return {'response': True, 'message': 'Deposit fields updated successfully'}
        except Exception as e:
            await self.logger.error(f"Error updating deposit fields: {str(e)}")
            return {'response': False, 'error': str(e)}



    @handle_redis_exceptions
    async def update_deposit_success(self, bot, deposit_id: str, deposit_amount: str, timeout: float, api_status: Dict, deposit_status: str, valid_until: str) -> dict:
        """Update deposit success details (when deposit is confirmed)."""
        try:
            await self.logger.info(f"Dᴇᴘᴏsɪᴛ: Updating deposit success for deposit_id {deposit_id}")
            redis_client = await self.ensure_connection()
            deposit_info_key = f"{DEPOSIT_INFO_PREFIX}info:{deposit_id}"

            deposit_data = await self.get_deposit_data(deposit_id)
            if not deposit_data.get('response'):
                await self.logger.error(f"Dᴇᴘᴏsɪᴛ: Deposit not found for deposit_id {deposit_id}")
                return {'response': False, 'error': 'Deposit not found'}

            deposit_info = deposit_data.get('result', {})
            user_id = deposit_info.get('user_id')

            if not user_id:
                await self.logger.error(f"Dᴇᴘᴏsɪᴛ: User ID missing in deposit info for deposit_id {deposit_id}")
                return {'response': False, 'error': 'User ID missing in deposit info'}

            await self.logger.debug(f"Dᴇᴘᴏsɪᴛ: Handling deposit history for deposit_id {deposit_id}")
            try:
                current_history = json.loads(deposit_info.get('deposit_history', '[]'))
            except json.JSONDecodeError:
                await self.logger.warning(f"Dᴇᴘᴏsɪᴛ: Invalid JSON in deposit history for deposit_id {deposit_id}")
                current_history = []

            current_history.append({
                "timestamp": time.time(),
                "action": "DEPOSIT_CONFIRMED"
            })

            updates = {
                'deposit_amount': deposit_amount,
                'deposit_status': deposit_status,
                'timeout': str(timeout),
                'refund_status': 'false',
                'user_id': user_id,
                'api_status': json.dumps(api_status),
                'deposit_history': json.dumps(current_history)
            }

            await self.logger.info(f"Dᴇᴘᴏsɪᴛ: Updating Redis with new deposit info for deposit_id {deposit_id}")
            await redis_client.hset(deposit_info_key, mapping=updates)

            await self.logger.info(f"Dᴇᴘᴏsɪᴛ: Sending deposit notification for deposit_id {deposit_id}")
            await self.send_deposit_notification(
                bot,
                user_id,
                deposit_amount,
                deposit_id,
                api_status.get('gateway_name', 'N/A'),
                api_status.get('payment_mode', 'N/A'),
                valid_until
            )

            await self.logger.info(f"Dᴇᴘᴏsɪᴛ: Successfully updated deposit for deposit_id {deposit_id}")
            return {'response': True, 'message': 'Deposit updated successfully'}
        except Exception as e:
            await self.logger.error(f"Dᴇᴘᴏsɪᴛ: Error updating deposit for deposit_id {deposit_id}: {str(e)}", exc_info=True)
            return {'response': False, 'error': str(e)}

    @handle_redis_exceptions
    async def send_deposit_notification(self, bot: AsyncTeleBot, user_id: str, amount: float, deposit_id: str, paid_from: str, paid_type: str, valid_until: str) -> None:
        """Send a deposit notification message to both the user and the update channel."""
        try:
            await self.logger.info(f"Sending deposit notification for user {user_id}")
            
            data = await financial_mgr.get_user(user_id)
            if not isinstance(data, dict) or not data.get('response'):
                await self.logger.error(f"Failed to retrieve user data for user {user_id}")
                return

            metrics = data.get("metrics", {})
            user_name = data.get("user_profile", {})
            
            if metrics.get("deposits", {}).get("count", 0) == 1:
                forum_topic = await user_mgr.create_forum_topic(user_id, f"❯ {user_name} [{user_id}]")
                if forum_topic:
                    await self.logger.info(f"Created forum topic for first-time depositor: {forum_topic}")
            else:
                forum_topic = False

            profile_key = f"user_data:{user_id}:profile:main"
            forum_id = await self.redis_manager.redis_client.hget(profile_key, "forum_id")
    
            if forum_id:
                if not forum_topic:
                    message_id = await user_mgr.user_metrics_report(bot, 'edit_message_text', user_id, '-1002203139746')
                elif forum_topic:
                    from handlers.main.show_wallet import wallet_manager
                    message_id, _ = await asyncio.gather(
                        user_mgr.user_metrics_report(bot, 'sendMessage', user_id, '-1002203139746', forum_id),
                        wallet_manager.process_wallet_update(user_id),
                    )
                    message_id = await self.redis_manager.redis_client.hset(profile_key, "forum_message_id", str(message_id))
                    
                admin_text = (
                    f"<b>#Uᴘɪ_Cᴀʀᴅ_Dᴇᴘᴏsɪᴛ ❯</b>\n\n"
                    f"<b>Tʀᴀɴsᴀᴄᴛɪᴏɴ Dᴇᴛᴀɪʟs »</b>\n"
                    f"<blockquote expandable>"
                    f"<b>💰 Aᴍᴏᴜɴᴛ »</b> <code>{amount}</code> 💎\n"
                    f"<b>👤 Pᴀɪᴅ Fʀᴏᴍ »</b> <code>{paid_from}</code>\n"
                    f"<b>🕊 Pᴀʏᴍᴇɴᴛ Tʏᴘᴇ »</b> <code>{paid_type}</code>\n\n"
                    f"<b>Bᴀʟᴀɴᴄᴇ Uᴘᴅᴀᴛᴇ »</b>\n"
                    f"<b>🏛</b> <code>{deposit_id}</code>\n"
                    f"<b>⏱️ Tɪᴍᴇ »</b> {valid_until}\n"
                    f"</blockquote>\n"
                    f"<b>Sᴜᴄᴄᴇssғᴜʟʟʏ Cʀᴇᴅɪᴛᴇᴅ</b>"
                )
                admin_keyboard = InlineKeyboardMarkup()
                admin_keyboard.row(
                    InlineKeyboardButton('🔗 Usᴇʀ', url=f'tg://openmessage?user_id={user_id}'),
                    InlineKeyboardButton('⌕ Dᴇᴛᴀɪʟs', callback_data='placeholder')
                )
    
                try:
                    msg = await bot.send_message(
                        chat_id='-1002203139746',
                        text=admin_text,
                        reply_markup=admin_keyboard,
                        message_thread_id=int(forum_id),
                        parse_mode='HTML'
                    )
                except Exception as e:
                    await self.logger.error(f"Failed to send admin notification: {e}", exc_info=True)
                    return
    
                if msg:
                    message_id = msg.message_id
                    chat_id = msg.chat.id
                    if str(chat_id).startswith('-100'):
                        chat_id = 'c/' + str(chat_id)[4:]
    
                    link = f'https://t.me/{chat_id}/{forum_id}/{message_id}'
                    admin_keyboard.keyboard[0][1].url = link
    
                text = f'<b>💎 #Uᴘɪ_Cᴀʀᴅ_Dᴇᴘᴏsɪᴛ ❯</b>\n[<code>{paid_type}</code>][<code>{user_id}</code>][<code>{amount}</code>]'
    
                try:
                    await bot.send_message(
                        chat_id='-1002203139746',
                        text=text,
                        reply_markup=admin_keyboard,
                        parse_mode='HTML'
                    )
                except Exception as e:
                    await self.logger.error(f"Failed to send final notification: {str(e)}")
        except Exception as e:
            await self.logger.error(f"Error sending deposit notification: {str(e)}")
        await self.logger.info("Deposit notification process completed")

    @handle_redis_exceptions
    async def aggregate_deposits(self, filters: Dict[str, Any]) -> Dict[str, float]:
        """
        Perform a RediSearch aggregation query to compute total deposit amount and count asynchronously.
        """
        await self._init_logger()
        try:
            query_str = await self.build_query(filters)
            await self.logger.info(f"Aggregation query: {query_str}")

            aggregation_query = [
                "FT.AGGREGATE", DEPOSIT_INFO_INDEX, query_str,
                "GROUPBY", "0",
                "REDUCE", "SUM", "1", "@deposit_amount", "AS", "total_amount",
                "REDUCE", "COUNT", "0", "AS", "count"
            ]

            result = await self.redis_manager.redis_client.execute_command(*aggregation_query)
            if not result or len(result) < 2:
                return {"total_amount": 0.0, "count": 0}

            total_amount = float(result[1][1]) if result[1][1] else 0.0
            count = int(result[1][3]) if result[1][3] else 0

            return {"total_amount": total_amount, "count": count}
        except Exception as e:
            await self.logger.error(f"Error aggregating deposits: {e}")
            return {"total_amount": 0.0, "count": 0}

    @handle_redis_exceptions
    async def cancel_deposit(self, deposit_id: str, user_id: str, status: str = 'CANCELLED') -> dict:
        """
        Cancel a deposit asynchronously (and process any refund logic if applicable).
        """
        await self._init_logger()
        await self.logger.info(f"Attempting to cancel deposit {deposit_id} for user {user_id}")

        deposit_data = await self.get_deposit_data(deposit_id)
        if not deposit_data.get('response'):
            await self.logger.warning(f"Deposit {deposit_id} not found during cancellation")
            return {'response': False, 'error': 'Deposit not found'}

        deposit_info = deposit_data.get('result', {})
        if deposit_info.get('deposit_status') in ['CANCELLED', 'TIMEOUT']:
            await self.logger.info(f"Deposit {deposit_id} was already {status}")
            return {'response': False, 'error': f'Deposit already {status}'}

        deposit_info_key = f"{DEPOSIT_INFO_PREFIX}info:{deposit_id}"
        await self.logger.info(f"Updating deposit status to {status.lower()} for deposit {deposit_id}")

        try:
            history = json.loads(deposit_info.get('deposit_history', '[]'))
        except Exception:
            await self.logger.warning("Failed to load deposit history, initializing new history list")
            history = []
        history.append({
            "timestamp": time.time(),
            "action": f"DEPOSIT_{status}"
        })

        updates = {
            'deposit_status': status,
            'cancelled_at': datetime.utcnow().isoformat(),
            'deposit_history': json.dumps(history)
        }

        redis_client = await self.ensure_connection()
        async with redis_client.pipeline(transaction=True) as pipe:
            await pipe.hset(deposit_info_key, mapping=updates)
            await pipe.execute()

        await self.logger.info(f"Successfully {status.lower()} deposit {deposit_id}")
        return {'response': True, 'message': f'Deposit {status} successfully'}

    @handle_redis_exceptions
    async def search_deposits_advanced(self, filters: dict, sort_by: str = None, sort_asc: bool = True, offset: int = 0, limit: int = 10) -> dict:
        """Search deposits with advanced filtering asynchronously."""
        await self._init_logger()
        try:
            redis_client = await self.ensure_connection()
            query_str = await self.build_query(filters)
            await self.logger.info(f"Searching deposits with query: {query_str}")

            query = Query(query_str).paging(offset, limit)
            if sort_by:
                query.sort_by(sort_by, asc=sort_asc)

            results = await redis_client.ft(DEPOSIT_INFO_INDEX).search(query)
            deposits = await asyncio.gather(*[
                asyncio.create_task(self.process_deposit_doc(doc))
                for doc in results.docs
            ])
            return {'response': True, 'total_deposits': results.total, 'results': deposits}
        except Exception as e:
            await self.logger.error(f"Error searching deposits: {e}")
            return {'response': False, 'error': str(e)}

    @handle_redis_exceptions
    async def search_current_deposits(self, query_str: str = "*", sort_by: str = None, sort_asc: bool = True, limit: int = 10, offset: int = 0) -> dict:
        """
        Search for current deposits using advanced filtering asynchronously.
        """
        await self._init_logger()
        try:
            redis_client = await self.ensure_connection()
            base_query = "(@deposit_status:(PENDING))"
            if query_str != "*":
                base_query += f" ({query_str})"

            query = Query(base_query).paging(offset, limit)
            if sort_by:
                query.sort_by(sort_by, asc=sort_asc)

            results = await redis_client.ft(DEPOSIT_INFO_INDEX).search(query)
            deposits = await asyncio.gather(*[
                asyncio.create_task(self.process_deposit_doc(doc))
                for doc in results.docs
            ])
            return {'response': True, 'total': results.total, 'results': deposits}
        except Exception as e:
            await self.logger.error(f"Error searching current deposits: {e}")
            return {'response': False, 'error': str(e)}

class FinancialManagement:
    """
    High-performance asynchronous financial summary aggregator.
    Utilizes Redis aggregations and concurrent execution for optimal performance.
    """

    def __init__(self, deposit_mgr=None, order_mgr=None, user_mgr=None, enable_logging: bool = True):
        self.order_mgr: OrderManagement = order_mgr
        self.deposit_mgr: DepositManagement = deposit_mgr
        self.user_mgr: UserManagement = user_mgr
        self.logger: Optional[AdvancedLogger] = None
        self.enable_logging = enable_logging

    async def _init_logger(self):
        if not self.logger:
            self.logger = await get_async_logger(self.enable_logging)

    async def get_user(
        self,
        user_id: str,
        start_timestamp: Optional[float] = None,
        end_timestamp: Optional[float] = None,
        deposit_types: Optional[List[str]] = None,
        order_types: Optional[List[str]] = None,
        return_order_ids: bool = False,
        limit: Optional[int] = None,
        is_tool: bool = False,
        sort_fields: Optional[List[Tuple[str, str]]] = None,
        app_price: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Asynchronously retrieve a financial summary for the specified user with optimized data processing.
        """
        await self._init_logger()
        try:
            user_profile_task = asyncio.create_task(self.user_mgr.get_user_data(user_id))

            deposit_filters = await self._build_deposit_filters(user_id, start_timestamp, end_timestamp, deposit_types)
            order_filters = await self._build_order_filters(
                user_id, start_timestamp, end_timestamp, order_types,
                return_order_ids, app_price, sort_fields
            )

            deposit_task = asyncio.create_task(self.deposit_mgr.aggregate_deposits(deposit_filters))
            order_task = asyncio.create_task(self.order_mgr.aggregate_orders(order_filters, limit=limit))

            user_profile_response, deposit_agg, order_agg = await asyncio.gather(
                user_profile_task, deposit_task, order_task
            )

            if not user_profile_response.get("response"):
                await self.logger.warning(f"Failed to retrieve user profile for user {user_id}")
                return {"response": False, "error": "User profile not found"}

            user_profile = user_profile_response.get("result", {})
            if not user_profile.get("first_name"):
                await self.logger.warning(f"Invalid user profile for user {user_id}")
                return {"response": False, "error": "Invalid user profile"}

            current_balance = deposit_agg["total_amount"] - order_agg["total_amount"]
            spend_balance = order_agg["total_amount"]
            if is_tool:
                if start_timestamp or end_timestamp:
                    return {
                        "response": True,
                        "full_name": user_profile.get("first_name", ""),
                        "metrics": {
                            "spend_balance": spend_balance,
                            "deposits": {
                                "total_amount": deposit_agg["total_amount"],
                                "count": deposit_agg["count"],
                            },
                            "orders": {
                                "total_amount": order_agg["total_amount"],
                                "count": order_agg["count"],
                                "order_ids": order_agg.get("order_ids", [])
                            },
                        },
                        "updated_at": time.time()
                    }
                else:
                    return {
                        "response": True,
                        "full_name": user_profile.get("first_name", ""),
                        "metrics": {
                            "current_balance": current_balance,
                            "spend_balance": spend_balance,
                            "deposits": {
                                "total_amount": deposit_agg["total_amount"],
                                "count": deposit_agg["count"],
                            },
                            "orders": {
                                "total_amount": order_agg["total_amount"],
                                "count": order_agg["count"],
                                "order_ids": order_agg.get("order_ids", [])
                            },
                        },
                        "updated_at": time.time()
                    }
            return {
                "response": True,
                "user_profile": user_profile.get("first_name", ""),
                "metrics": {
                    "current_balance": current_balance,
                    "spend_balance": spend_balance,
                    "deposits": {
                        "total_amount": deposit_agg["total_amount"],
                        "count": deposit_agg["count"],
                    },
                    "orders": {
                        "total_amount": order_agg["total_amount"],
                        "count": order_agg["count"],
                        "order_ids": order_agg.get("order_ids", [])
                    },
                },
                "timestamp": datetime.utcnow().isoformat(),
            }

        except Exception as e:
            await self.logger.error(f"Error generating financial summary for user {user_id}: {e}")
            return {"response": False, "error": str(e)}

    async def _build_deposit_filters(
        self,
        user_id: str,
        start_timestamp: Optional[float],
        end_timestamp: Optional[float],
        deposit_types: Optional[List[str]]
    ) -> Dict[str, Any]:
        filters = {"user_id": user_id, "deposit_status": ["COMPLETED", "PROCESSING"]}
        if start_timestamp and end_timestamp:
            filters["recorded_at"] = (start_timestamp, end_timestamp)
        if deposit_types:
            filters["deposit_type"] = deposit_types
        return filters

    async def _build_order_filters(
        self,
        user_id: str,
        start_timestamp: Optional[float] = None,
        end_timestamp:   Optional[float] = None,
        order_types:     Optional[List[str]]  = None,
        include_order_ids: bool               = False,
        app_price:       Optional[str]         = "[0.01 +inf]",
        sort_fields:     Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        """
        Construct the filters dict for RediSearch:
          - user_id       (exact)
          - order_status  (COMPLETED, PROCESSING, PENDING)
          - recorded_at   (timestamp range)
          - order_type    (list of types)
          - order_amount     (price range)
          - sort          (List[{"field":"PRICE"|"DATE","direction":"ASC"|"DESC"}])
          - _return_ids   (internal flag)
        """
        filters: Dict[str, Any] = {
            "user_id":      user_id,
            "order_status": ["COMPLETED", "PROCESSING", "PENDING"],
            "order_amount":    app_price,
        }

        # Date range filter
        if start_timestamp is not None and end_timestamp is not None:
            filters["recorded_at"] = (start_timestamp, end_timestamp)

        # Specific order types
        if order_types:
            filters["order_type"] = order_types

        # Multi‑field sort
        if sort_fields:
            valid_f = {"PRICE", "DATE"}
            valid_o = {"ASC", "DESC"}
            sorts: List[Dict[str, str]] = []
            for sort in sort_fields:
                f, o = sort["field"].upper(), sort["direction"].upper()
                if f in valid_f and o in valid_o:
                    sorts.append({"field": f, "direction": o})
            if sorts:
                filters["sort"] = sorts

        # Internal: return raw IDs?
        filters["_return_ids"] = include_order_ids
        return filters

class CountryFlagUpdater:
    def __init__(self, redis_client: Redis):
        self.redis_client = redis_client

    def convert_svg_to_png_upload(self, svg_url: str) -> str:
        result = cloudinary.uploader.upload(svg_url, resource_type="image", overwrite=True)
        return cloudinary.CloudinaryImage(result["public_id"]).build_url(format="png")

    def emoji_to_country_code(self, flag_emoji: str) -> str:
        return ''.join(chr(ord(c) - 127397) for c in flag_emoji).lower()

    async def get_country_data(self, country_id: str = None) -> dict:
        try:
            whole_country_data = await self.redis_client.json().get('main_data:details:country_data') or {}
            return whole_country_data.get(country_id, {}) if country_id else whole_country_data
        except Exception as e:
            print(f"Error fetching country data: {e}")
            return {}

    async def update_flag_urls(self):
        country_data = await self.get_country_data()
        for key, val in country_data.items():
            flag_emoji = val.get("country_code")
            if not flag_emoji:
                continue
            country_code = self.emoji_to_country_code(flag_emoji)
            svg_url = f"https://hatscripts.github.io/circle-flags/flags/{country_code}.svg"
            try:
                png_url = self.convert_svg_to_png_upload(svg_url)
                val["flag_url"] = png_url
                await self.redis_client.json().set('main_data:details:country_data', f'.{key}.flag_url', png_url)
                print(f"Updated country {key} with flag URL: {png_url}")
            except Exception as e:
                print(f"Error converting flag for country {key}: {e}")

    async def load_mappings(
        self,
        is_country_return: bool = False,
        is_app_return: bool = False
    ) -> Union[Dict, Tuple[Dict, Dict]]:
        try:
            countries_dict = app_mapping = None

            # Load only what is needed
            countries_dict = await self.redis_client.json().get('main_data:details:country_data') or None
            if is_country_return or not countries_dict:
                if not countries_dict:
                    with open(os.path.join(os.path.dirname(__file__), 'file', 'country_code.json'), 'r', encoding='utf-8') as f:
                        countries_list = json.load(f)
                        countries_dict = {
                            country["record_id"]: {
                                "country_name": country["name"],
                                "country_code": country["code"]
                            }
                            for country in countries_list
                        }
                        await self.redis_client.json().set('main_data:details:country_data', '$', countries_dict)
                        await self.update_flag_urls()

                country_mapping = {
                    country_data["country_name"].lower(): int(key)
                    for key, country_data in countries_dict.items()
                }
            else:
                country_mapping = {}
            
            app_mapping = await self.redis_client.json().get('main_data:service:app_data') or None
            if is_app_return or not app_mapping:
                if not app_mapping:
                    with open(os.path.join(os.path.dirname(__file__),  "file", "app_code.json"), 'r', encoding='utf-8') as f:
                        app_mapping = json.load(f)
                        await self.redis_client.json().set('main_data:service:app_data', '$', app_mapping)

                reverse_map = {}
                for app_name, details in app_mapping.items():
                    codes = details.get("code")
                    if isinstance(codes, list) and codes:
                        reverse_map[app_name.lower().replace(" ", "")] = codes[0]
                    elif isinstance(codes, str):
                        reverse_map[app_name.lower().replace(" ", "")] = codes
            else:
                reverse_map = {}

            # Return according to requested params
            if is_country_return and is_app_return:
                return country_mapping, reverse_map
            elif is_country_return:
                return country_mapping
            elif is_app_return:
                return reverse_map
            else:
                return {}, {}

        except Exception as e:
            logging.error(f"Error loading mappings: {e}")
            if is_country_return and is_app_return:
                return {}, {}
            elif is_country_return:
                return {}
            elif is_app_return:
                return {}
            else:
                return {}, {}



# ---------------- Initialize Managers ----------------
deposit_mgr = DepositManagement(redis_manager)
order_mgr = OrderManagement(redis_manager)
user_mgr = UserManagement(redis_manager, BOT_TOKEN, CHANNEL_ID)
financial_mgr = FinancialManagement(deposit_mgr, order_mgr, user_mgr)

FinancialSummaryAggregator = financial_mgr





from utils.api import SMS_ACTIVATE as SMSACTIVATE_API_KEY, FIVE_SIM as FIVE_SIM_API_KEY, FAST_SMS as FAST_SMS_API_KEY, SMS_HUB as SMS_HUB_API_KEY, GRIZZLY_SMS as GRIZZLY_API_KEY, SMS_BOWER as SMSBOWER_API_KEY, VAK_SMS as VAKSMS_API_KEY, TIGER_SMS as TIGERSMS_API_KEY



# -------------------- logging Configuration --------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class FiveSimManagement:
    BASE_URL = (
        f"http://api1.5sim.net"
    )

    def __init__(self, max_concurrent_requests: int = 5):
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.selected_servers: Dict[str, Any] = {}

    async def __aenter__(self) -> "FiveSimManagement":
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def fetch_json(self, url: str, retries: int = 1) -> Optional[Dict[str, Any]]:
        import requests
        headers = {
            "accept": "application/json",
            "Authorization": "Bearer eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3Nzk4NTc1ODEsImlhdCI6MTc0ODMyMTU4MSwicmF5IjoiNzJiOWYxNGY4NzhjNzQ2ZDkyZTZiMWFjYjZlMDQyYTIiLCJzdWIiOjE1NjUxNzB9.AIN-uJ2d9_f_7xfsTyDXLzKFsSKCaGRQfwyV_HV7rVDwzXLwkpET1foIQU0VnCYJRzqH3T_W7RQTHbscGEBLwLPkZoMs-JDxlcBa99N5bIa75crUqRzhgReHghVscnyMeSuNtAk6xbgLLffj_hKeQO_qERB_oqO20hkIHL7YOtpk400cSJnHzgIn5ZqCkw4xXuTV5YWQe7KB_L5FVjhOzwi7Tit-N-lt1t2iphieuBAW1MdLSXMMkPrP83q1shxSyufHF_GNIdbz5GYc6AqXnsbGIGgMzE7W-cI2-YUjz30D4yBjyxuLYSkNLweat3WjDhhKQi9pwtVIkbeLXK-Ymw"
        }
        timeout = 10
        session = requests.Session()
        for attempt in range(1, retries + 1):
            try:
                response = session.get(url, headers=headers, timeout=timeout)
                if response.status_code != 200:
                    retry_after = response.headers.get("Retry-After")
                    wait_time = int(retry_after) if retry_after and retry_after.isdigit() else 1
                    logging.warning(
                        f"Rate limited when accessing {url}. Waiting {wait_time} seconds (attempt {attempt}/{retries})."
                    )
                    time.sleep(wait_time)
                    continue
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                logging.error(f"Error fetching {url}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error fetching {url}: {e}")
            if attempt < retries:
                backoff = 2 ** (attempt - 1)
                time.sleep(backoff)
        return None

    async def get_app_code_from_mapping(self, app_name: str, app_mapping: Dict[str, str]) -> str:
        """Retrieve the app code given an app name from the mapping."""
        normalized_name = app_name.lower().replace(" ", "")  # Normalize input
        if str(normalized_name) == str("other"):
            normalized_name = "anyother"
        return app_mapping.get(normalized_name, app_name)  # Return code or original input

    async def transform_json_structure(
        self,
        data: Dict[str, Any],
        is_service_request: bool = False,
        is_server_request: bool = False
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Returns a tuple (selected_service, selected_servers), each possibly empty.
        """
        selected_service: Dict[str, Any] = {}
        selected_servers: Dict[str, Any] = {}
        
        # if neither requested, just short‐circuit
        if not (is_service_request or is_server_request):
            return {}, {}

        try:
            # 1) load country codes
            updater = CountryFlagUpdater(redis_manager.redis_client)
            country_mapping = await updater.load_mappings(is_country_return=True)

            # 2) fetch and invert products → app_mapping
            loop = asyncio.get_event_loop()
            products = await loop.run_in_executor(
                None, self.fetch_json, "https://5sim.net/v1/user/list/products/old"
            )
            if products is None:
                logging.error("Could not fetch products list")
                return {}, {}
            app_mapping = {v: k for k, v in products.items()}

            # 3) main loop
            for country_name, services in data.items():
                if not isinstance(services, dict):
                    logging.error(
                        f"Skipping country '{country_name}': expected dict, got {type(services).__name__}"
                    )
                    continue

                if country_name in ['status', 'msg']:
                    continue

                # look up the two‐letter country_code (or “none”)
                country_code = country_mapping.get(country_name.lower(), "none")
                if is_service_request:
                    selected_service.setdefault(country_code, {})

                for service_name, servers in services.items():
                    service_code = app_mapping.get(service_name)
                    if not service_code:
                        logging.error(f"Invalid service key: {service_name!r}")
                        continue
                    if not isinstance(servers, dict):
                        logging.error(
                            f"Skipping service '{service_name}' in '{country_name}': expected dict, got {type(servers).__name__}"
                        )
                        continue

                    # collect only servers with count > -1
                    valid = []
                    for srv_name, details in servers.items():
                        if not isinstance(details, dict):
                            continue
                        cost = float(details.get("cost", 0))
                        count = int(details.get("count", 0))
                        if count > -1:
                            valid.append((cost, count, srv_name))

                    if not valid:
                        logging.error(f"No valid servers for service {service_name!r}")
                        continue

                    # pick best‐value server
                    avg_cost = sum(c for c, _, _ in valid) / len(valid)
                    low_cost = [s for s in valid if s[0] < avg_cost]
                    if low_cost:
                        avg_count = sum(cnt for _, cnt, _ in low_cost) / len(low_cost)
                        candidates = [s for s in low_cost if s[1] > avg_count]
                    else:
                        candidates = []

                    best = max(
                        candidates or valid,
                        key=lambda x: x[1] / x[0] if x[0] else float("inf")
                    )
                    cost, count, srv_name = best

                    if is_service_request:
                        selected_service[country_code][service_code] = {
                            f"{cost:.2f}": str(count)
                        }
                    if is_server_request:
                        selected_servers[service_code] = srv_name

            # *** always return the 2-tuple ***
            return selected_service, selected_servers

        except Exception as e:
            logging.error(f"Error in transform_json_structure: {e}")
            # still return the 2-tuple, even on failure
            return {}, {}

    async def get_prices(
        self,
        country_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fetch price data and return only the `selected_service` mapping.
        """
        async with self.semaphore:
            remote_url = (
                f"{self.BASE_URL}/stubs/handler_api.php?"
                f"country={country_id or '22'}&api_key={FIVE_SIM_API_KEY}&action=getPrices"
            )
            data = await self.fetch_with_retry(remote_url, retries=2)
            if data is None:
                logging.error("get_prices: failed to fetch data")
                return {}

            # unpack the 2-tuple; we only care about services here
            selected_service, _ = await self.transform_json_structure(
                data,
                is_service_request=True,
                is_server_request=False
            )
            return selected_service

    async def get_servers(
        self,
        country_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fetch price data and return only the `selected_servers` mapping.
        """
        # if we've already loaded servers earlier, just return the cache
        if hasattr(self, 'selected_servers') and self.selected_servers:
            return self.selected_servers

        async with self.semaphore:
            remote_url = (
                f"{self.BASE_URL}/stubs/handler_api.php?"
                f"country={country_id or '22'}&api_key={FIVE_SIM_API_KEY}&action=getPrices"
            )
            data = await self.fetch_with_retry(remote_url, retries=2)
            if data is None:
                print("get_servers: failed to fetch data")
                return {}

            # unpack the 2-tuple; we only care about servers here
            _, selected_servers = await self.transform_json_structure(
                data,
                is_service_request=False,
                is_server_request=True
            )
            self.selected_servers = selected_servers
            return selected_servers

    async def fetch_with_retry(self, url: str, retries: int = 1):
        """
        Fetch JSON from a URL with asynchronous retry logic.
        Implements timeout handling, rate-limit checks, and exponential backoff.
        """
        timeout_duration = 20
        async with aiohttp.ClientSession() as session:
            for attempt in range(1, retries + 1):
                timeout = aiohttp.ClientTimeout(total=timeout_duration)
                try:
                    async with session.get(url, timeout=timeout) as resp:
                        if resp.status == 500:
                            return 'NO_NUMBER'
                        if resp.status != 200:
                            retry_after = resp.headers.get("Retry-After")
                            wait_time = int(retry_after) if retry_after and retry_after.isdigit() else 1
                            logging.warning(
                                f"Rate limited when accessing {url}. Waiting {wait_time} seconds (attempt {attempt}/{retries})."
                            )
                            await asyncio.sleep(wait_time)
                            continue
                        resp.raise_for_status()
                        raw_response = await resp.text()
                        return json.loads(raw_response)
                except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
                    print(f"Error fetching {url}: {e}")
                    if retries == 0:
                        logging.error(f"Failed to fetch JSON from {url}")
                        return None
                    logging.error(f"Error fetching {url}: {e}")
                    if attempt < retries:
                        backoff = 2 ** (attempt - 1)
                        logging.info(f"Retrying {url} in {backoff} seconds (attempt {attempt}/{retries})...")
                        await asyncio.sleep(backoff)
                        timeout_duration *= 1.5
                except Exception as e:
                    if retries == 0:
                        logging.error(f"Failed to fetch JSON from {url}")
                        return None
                    logging.error(f"Unexpected error fetching {url}: {e}")
                    if attempt < retries:
                        backoff = 2 ** (attempt - 1)
                        logging.info(f"Retrying {url} in {backoff} seconds (attempt {attempt}/{retries})...")
                        await asyncio.sleep(backoff)
                        timeout_duration *= 1.5

    async def get_countries(self) -> Optional[Dict[str, Any]]:
        countries = {"0": "russia", "1": "ukraine", "2": "kazakhstan", "4": "philippines",
                "6": "indonesia", "7": "malaysia", "8": "kenya", "9": "tanzania",
                "10": "vietnam", "11": "kyrgyzstan", "12": "usa", "13": "israel", "14": "hongkong",
                "15": "poland", "16": "england", "17": "madagascar", "18": "dcongo",
                "19": "nigeria", "20": "macao", "21": "egypt", "22": "india",
                "23": "ireland", "24": "cambodia", "25": "laos", "26": "haiti",
                "27": "ivory", "28": "gambia", "29": "serbia", "31": "southafrica",
                "32": "romania", "33": "colombia", "34": "estonia", "35": "azerbaijan",
                "36": "canada", "37": "morocco", "38": "ghana", "39": "argentina",
                "40": "uzbekistan", "41": "cameroon", "42": "chad", "43": "germany",
                "44": "lithuania", "45": "croatia", "46": "sweden", "48": "netherlands",
                "49": "latvia", "50": "austria", "51": "belarus", "52": "thailand",
                "53": "saudiarabia", "54": "mexico", "55": "taiwan", "56": "spain",
                "58": "algeria", "59": "slovenia", "60": "bangladesh", "61": "senegal",
                "63": "czech", "64": "srilanka", "65": "peru", "66": "pakistan",
                "67": "newzealand", "68": "guinea", "70": "venezuela", "71": "ethiopia",
                "72": "mongolia", "73": "brazil", "74": "afghanistan", "75": "uganda",
                "76": "angola", "77": "cyprus", "78": "france", "79": "papua",
                "80": "mozambique", "81": "nepal", "82": "belgium", "83": "bulgaria",
                "84": "hungary", "85": "moldova", "86": "italy", "87": "paraguay",
                "88": "honduras", "89": "tunisia", "90": "nicaragua", "91": "timorleste",
                "92": "bolivia", "93": "costarica", "94": "guatemala", "97": "puertorico",
                "99": "togo", "100": "kuwait", "101": "salvador", "103": "jamaica",
                "104": "trinidad", "105": "ecuador", "106": "swaziland", "107": "oman",
                "108": "bosnia", "109": "dominican", "112": "panama", "114": "mauritania",
                "115": "sierraleone", "116": "jordan", "117": "portugal", "118": "barbados",
                "119": "burundi", "120": "benin", "123": "botswana", "128": "georgia",
                "129": "greece", "130": "guineabissau", "131": "guyana", "134": "saintkitts",
                "135": "liberia", "136": "lesotho", "137": "malawi", "138": "namibia",
                "140": "rwanda", "141": "slovakia", "142": "suriname", "143": "tajikistan",
                "145": "bahrain", "146": "reunion", "147": "zambia", "148": "armenia",
                "152": "burkinafaso", "154": "gabon", "155": "albania", "156": "uruguay",
                "157": "mauritius", "158": "bhutan", "159": "maldives", "160": "guadeloupe",
                "161": "turkmenistan", "162": "frenchguiana", "163": "finland",
                "164": "saintlucia", "165": "luxembourg", "166": "saintvincentgrenadines",
                "167": "equatorialguinea", "168": "djibouti", "169": "antiguabarbuda",
                "171": "montenegro", "172": "denmark", "173": "switzerland", "174": "norway",
                "175": "australia", "179": "aruba", "183": "northmacedonia",
                "184": "seychelles", "185": "newcaledonia", "186": "capeverde",
                "201": "gibraltar"}
        return countries

    async def fetch_all_data(self) -> Dict[str, Any]:
        countries_data = await self.get_countries()
        if not countries_data:
            #logging.error("Failed to fetch countries.")
            return {}
        tasks = [self.get_prices(country_id) for country_id in countries_data.keys()]
        prices_results = await asyncio.gather(*tasks)
        return {
        k: {
            kk: {
                sk: int(sv) if isinstance(sv, str) and sv.isdigit() else sv
                for sk, sv in vv.items()
            }
            for kk, vv in v.items()
        }
        for d in prices_results
        for k, v in d.items()
    }


    @staticmethod
    def select_best_service(data: Dict[str, Any]) -> Dict[str, Any]:
        for country_id, services in data.items():
            for service, details in services.items():
                if isinstance(details, dict) and details:
                    cost_str, count = next(iter(details.items()))
                    try:
                        cost = float(cost_str) * float(FIVE_SIM_TAX)
                    except ValueError:
                        cost = 0.0
                    services[service] = {f"{cost:.2f}": str(count)}
        return data


class FastSmsManagement:
    BASE_URL = "https://fastsms.su/stubs/handler_api.php"

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def fetch_json(self, url: str, retries: int = 3) -> Optional[Dict[str, Any]]:
        if not self.session:
            raise RuntimeError("Session not initialized. Use 'async with' context.")
        for attempt in range(1, retries + 1):
            try:
                async with self.session.get(url) as response:
                    if response.status != 200:
                        retry_after = response.headers.get("Retry-After")
                        wait_time = int(retry_after) if retry_after and retry_after.isdigit() else 1
                        #logging.warning(f"Rate limited when accessing {url}. Waiting {wait_time} seconds (attempt {attempt}/{retries}).")
                        await asyncio.sleep(wait_time)
                        continue
                    response.raise_for_status()
                    text = await response.text()
                    if not text.strip():
                        #logging.error(f"Empty response from {url}")
                        return None
                    if text.strip() == "NO_NUMBERS":
                        #logging.warning(f"Received 'NO_NUMBERS' response from {url}")
                        return {}
                    
                    data = json.loads(text)
                    # Handle string response (usually error messages)
                    if isinstance(data, str):
                        #logging.warning(f"Received string response from {url}: {data}")
                        return {}
                    
                    # For countries endpoint
                    if all(str(k).isdigit() for k in data.keys()):
                        return {str(k): v for k, v in data.items()}
                    
                    # For prices endpoint
                    if isinstance(data, dict) and "0" in data:
                        return data
                        
                    return data
            except (aiohttp.ClientResponseError, aiohttp.ContentTypeError, json.JSONDecodeError) as e:
                logging.error(f"Error fetching {url}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error fetching {url}: {e}")
            if attempt < retries:
                backoff = 2 ** (attempt - 1)
                #logging.info(f"Retrying {url} in {backoff} seconds (attempt {attempt}/{retries})...")
                await asyncio.sleep(backoff)
        #logging.error(f"Failed to fetch JSON from {url} after {retries} attempts.")
        return None

    async def get_countries(self) -> Optional[Dict[str, Any]]:
        url = f"{self.BASE_URL}?api_key={FAST_SMS_API_KEY}&action=getCountries"
        return await self.fetch_json(url)

    async def get_prices(self, country_id: str) -> Optional[Dict[str, Any]]:
        url = f"{self.BASE_URL}?api_key={FAST_SMS_API_KEY}&action=getPrices&country={country_id}"
        return await self.fetch_json(url)

    async def fetch_all_data(self) -> Dict[str, Any]:
        countries_data = await self.get_countries()
        if not countries_data:
            #logging.error("Failed to fetch countries.")
            return {}

        tasks = [self.get_prices(country_id) for country_id in countries_data.keys()]
        prices_results = await asyncio.gather(*tasks)

        combined_data = {}
        for country_id, prices in zip(countries_data.keys(), prices_results):
            if isinstance(prices, dict):
                combined_data[country_id] = prices.get(country_id, {})
            else:
                logging.warning(f"Ignoring country '{country_id}' due to invalid response: {prices}")

        return combined_data
class SmsHubManagement:
    """
    SMSHub management without separate countries endpoint.
    Fetches price data directly for provided country IDs.
    """
    SMS_HUB_BASE_URL = "https://smshub.org/stubs/handler_api.php"

    def __init__(self, max_concurrent_requests: int = 5, timeout_seconds: int = 120):
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    async def __aenter__(self) -> "SmsHubManagement":
        self.session = aiohttp.ClientSession(timeout=self.timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def fetch_json(self, url: str, retries: int = 3) -> Optional[Dict[str, Any]]:
        if not self.session:
            raise RuntimeError("Session not initialized. Use 'async with' context.")
        for attempt in range(1, retries + 1):
            try:
                async with self.session.get(url) as response:
                    if response.status != 200:
                        retry_after = response.headers.get("Retry-After")
                        wait_time = int(retry_after) if retry_after and retry_after.isdigit() else 1
                        await asyncio.sleep(wait_time)
                        continue
                    response.raise_for_status()
                    text = await response.text()
                    if not text.strip():
                        return None
                    data = json.loads(text)
                    # Handle common error responses
                    if isinstance(data, str):
                        return {}
                    return data
            except (aiohttp.ClientResponseError, aiohttp.ContentTypeError, json.JSONDecodeError) as e:
                logging.error(f"Error fetching {url}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error fetching {url}: {e}")
            if attempt < retries:
                backoff = 2 ** (attempt - 1)
                await asyncio.sleep(backoff)
        return None

    async def get_prices(self, country_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Fetch prices data. If a country_id is provided, it is appended to the query string.
        """
        async with self.semaphore:
            if country_id:
                url = f"{self.SMS_HUB_BASE_URL}?api_key={SMS_HUB_API_KEY}&action=getPrices&country={country_id}"
            else:
                url = f"{self.SMS_HUB_BASE_URL}?api_key={SMS_HUB_API_KEY}&action=getPrices"
            return await self.fetch_json(url)

    async def fetch_all_data(self) -> Dict[str, Any]:
        """
        Retrieves all data from the SMSHub API.
        """
        data = await self.get_prices()
        if data is None:
            print("Failed to fetch data from SMSHub API.")
            return {}
        print(f"Data fetched from SMSHub API : {len(data)}")
        return data


    @staticmethod
    def select_best_service(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        For each country and service, select the best server based on stock/price ratio.
        """
        for country_id, services in data.items():
            for service, servers in list(services.items()):
                try:
                    server_items = [(float(price), int(stock)) for price, stock in servers.items()]
                    if not server_items:
                        continue
                    avg_price = sum(price for price, _ in server_items) / len(server_items)
                    low_price_servers = [(price, stock) for price, stock in server_items if price < avg_price]
                    if low_price_servers:
                        avg_stock = sum(stock for _, stock in low_price_servers) / len(low_price_servers)
                        candidates = [(price, stock) for price, stock in low_price_servers if stock > avg_stock]
                    else:
                        candidates = []
                    best_server = (
                        max(candidates, key=lambda x: x[1] / x[0])
                        if candidates
                        else min(server_items, key=lambda x: x[0])
                    )
                    services[service] = {f"{convert_usd_to_rub(best_server[0]):.8f}": str(best_server[1])}
                except Exception as e:
                    logging.error(f"Error selecting best server for country '{country_id}', service '{service}': {e}")
        return data
class GrizzlySmsManagement:
    GRIZZLY_BASE_URL = "https://api.grizzlysms.com/stubs/handler_api.php"

    def __init__(self, max_concurrent_requests: int = 5, timeout_seconds: int = 120):
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        # Configure a total timeout for all requests
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    async def __aenter__(self) -> "GrizzlySmsManagement":
        # Initialize session with 2-minute timeout
        self.session = aiohttp.ClientSession(timeout=self.timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def fetch_json(self, url: str, retries: int = 3) -> Optional[Dict[str, Any]]:
        if not self.session:
            raise RuntimeError("Session not initialized. Use 'async with' context.")
        for attempt in range(1, retries + 1):
            try:
                # Perform GET with configured timeout
                async with self.session.get(url) as response:
                    if response.status != 200:
                        retry_after = response.headers.get("Retry-After")
                        wait_time = int(retry_after) if retry_after and retry_after.isdigit() else 1
                        await asyncio.sleep(wait_time)
                        continue
                    response.raise_for_status()
                    text = await response.text()
                    if not text.strip():
                        return None
                    if text.strip() in ("NO_NUMBERS", "BAD_COUNTRY"):
                        return {}
                    return json.loads(text)
            except (aiohttp.ClientResponseError, aiohttp.ClientError, json.JSONDecodeError) as e:
                logging.error(f"Error fetching {url}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error fetching {url}: {e}")
            if attempt < retries:
                backoff = 2 ** (attempt - 1)
                await asyncio.sleep(backoff)
        return None

    async def get_prices(self, country_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Fetch prices data. If a country_id is provided, it is appended to the query string.
        """
        async with self.semaphore:
            if country_id:
                url = f"{self.GRIZZLY_BASE_URL}?api_key={GRIZZLY_API_KEY}&action=getPrices&country={country_id}"
            else:
                url = f"{self.GRIZZLY_BASE_URL}?api_key={GRIZZLY_API_KEY}&action=getPrices"
            return await self.fetch_json(url)

    async def fetch_all_data(self) -> Dict[str, Any]:
        """
        Retrieves all data from the GrizzlySms API.
        """
        data = await self.get_prices()
        if data is None:
            print("Failed to fetch data from GrizzlySms API.")
            return {}
        print(f"Data fetched from GrizzlySms API : {len(data)}")
        return data


    @staticmethod
    def select_best_service(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Reformats each service's data to mirror the structure from class GrizzlySmsManagement.
        """
        formatted: Dict[str, Dict[str, Any]] = {}
        for country_id, services in data.items():
            updated_services: Dict[str, Any] = {}
            for service_key, details in services.items():
                # Normalize service name
                service_name = (service_key
                                .replace("gr_", "")
                                .replace("_", "")
                                .replace(" ", ""))
                cost = round(float(details.get("cost", 0)) * float(GRIZZLY_SMS_TAX), 4)
                count = details.get("count", 0)
                updated_services[service_name] = {f"{cost:.4f}": str(count)}
            formatted[country_id] = updated_services
        return formatted
class SmsBowerManagement:
    SMSBOWER_BASE_URL = "https://smsbower.online/stubs/handler_api.php"

    def __init__(self, max_concurrent_requests: int = 5):
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)

    async def __aenter__(self) -> "SmsBowerManagement":
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def fetch_json(self, url: str, retries: int = 3) -> Optional[Dict[str, Any]]:
        if not self.session:
            raise RuntimeError("Session not initialized. Use 'async with' context.")
        for attempt in range(1, retries + 1):
            try:
                async with self.session.get(url) as response:
                    if response.status != 200:
                        retry_after = response.headers.get("Retry-After")
                        wait_time = int(retry_after) if retry_after and retry_after.isdigit() else 1
                        #logging.warning(f"Rate limited when accessing {url}. Waiting {wait_time} seconds (attempt {attempt}/{retries}).")
                        await asyncio.sleep(wait_time)
                        continue
                    response.raise_for_status()
                    text = await response.text()
                    if not text.strip():
                        #logging.error(f"Empty response from {url}")
                        return None
                    if text.strip() == "NO_NUMBERS":
                        ##logging.warning(f"Received 'NO_NUMBERS' response from {url}")
                        return {}
                    if text.strip() == "BAD_COUNTRY":
                        ##logging.warning(f"Received 'BAD_COUNTRY' response from {url}")
                        return {}
                    return json.loads(text)
            except (aiohttp.ClientResponseError, aiohttp.ClientError, json.JSONDecodeError) as e:
                logging.error(f"Error fetching {url}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error fetching {url}: {e}")
            if attempt < retries:
                backoff = 2 ** (attempt - 1)
                #logging.info(f"Retrying {url} in {backoff} seconds (attempt {attempt}/{retries})...")
                await asyncio.sleep(backoff)
        #logging.error(f"Failed to fetch JSON from {url} after {retries} attempts.")
        return None

    async def get_prices(self, country_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Fetch prices data. If a country_id is provided, it is appended to the query string.
        """
        async with self.semaphore:
            if country_id:
                url = f"{self.SMSBOWER_BASE_URL}?api_key={SMSBOWER_API_KEY}&action=getPrices&country={country_id}"
            else:
                url = f"{self.SMSBOWER_BASE_URL}?api_key={SMSBOWER_API_KEY}&action=getPrices"
            return await self.fetch_json(url)

    async def fetch_all_data(self) -> Dict[str, Any]:
        """
        Retrieves all data from the SMSBower API.
        """
        data = await self.get_prices()
        if data is None:
            print("Failed to fetch data from SMSBower API.")
            return {}
        print(f"Data fetched from SMSBower API : {len(data)}")
        return data

    @staticmethod
    def select_best_service(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Reformats each service's data to mirror the structure from GrizzlySmsManagement.
        """
        for country_id, services in data.items():
            updated_services = {}
            for service, details in services.items():
                cost = round(float(details.get("cost", 0)) * float(SMS_BOWER_TAX), 4)
                count = details.get("count", 0)
                updated_services[service] = {f"{cost:.4f}": str(count)}
            data[country_id] = updated_services
        return data
class VakSmsManagement:
    VAKSMS_BASE_URL = "https://vak-sms.com/stubs/handler_api.php"

    def __init__(self, max_concurrent_requests: int = 5):
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)

    async def __aenter__(self) -> "VakSmsManagement":
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def fetch_json(self, url: str, retries: int = 3) -> Optional[Dict[str, Any]]:
        if not self.session:
            raise RuntimeError("Session not initialized. Use 'async with' context.")
        for attempt in range(1, retries + 1):
            try:
                async with self.session.get(url) as response:
                    if response.status != 200:
                        retry_after = response.headers.get("Retry-After")
                        wait_time = int(retry_after) if retry_after and retry_after.isdigit() else 1
                        #logging.warning(f"Rate limited when accessing {url}. Waiting {wait_time} seconds (attempt {attempt}/{retries}).")
                        await asyncio.sleep(wait_time)
                        continue
                    response.raise_for_status()
                    text = await response.text()
                    if not text.strip():
                        #logging.error(f"Empty response from {url}")
                        return None
                    if text.strip() == "NO_NUMBERS":
                        return {}
                    if text.strip() == "BAD_COUNTRY":
                        return {}
                    return json.loads(text)
            except (aiohttp.ClientResponseError, aiohttp.ClientError, json.JSONDecodeError) as e:
                logging.error(f"Error fetching {url}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error fetching {url}: {e}")
            if attempt < retries:
                backoff = 2 ** (attempt - 1)
                #logging.info(f"Retrying {url} in {backoff} seconds (attempt {attempt}/{retries})...")
                await asyncio.sleep(backoff)
        #logging.error(f"Failed to fetch JSON from {url} after {retries} attempts.")
        return None

    async def get_prices(self, country_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Fetch prices data. If a country_id is provided, it is appended to the query string.
        """
        async with self.semaphore:
            if country_id:
                url = f"{self.VAKSMS_BASE_URL}?api_key={VAKSMS_API_KEY}&action=getPrices&country={country_id}"
            else:
                url = f"{self.VAKSMS_BASE_URL}?api_key={VAKSMS_API_KEY}&action=getPrices"
            return await self.fetch_json(url)

    async def fetch_all_data(self) -> Dict[str, Any]:
        """
        Retrieves all data from the VakSms API.
        """
        data = await self.get_prices()
        if data is None:
            #logging.error("Failed to fetch data from VakSms API.")
            return {}
        return data

    @staticmethod
    def select_best_service(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Reformats each service's data by selecting the best based on cost and count.
        """
        for country_id, services in data.items():
            for service, details in services.items():
                cost = details.get("cost", 0)
                count = details.get("count", 0)
                services[service] = {f"{cost:.4f}": str(count)}
        return data
class TigerSmsManagement:
    TIGERSMS_BASE_URL = "https://api.tiger-sms.com/stubs/handler_api.php"  # Assuming similar endpoint structure

    def __init__(self, max_concurrent_requests: int = 5):
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)

    async def __aenter__(self) -> "TigerSmsManagement":
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def fetch_json(self, url: str, retries: int = 3) -> Optional[Dict[str, Any]]:
        if not self.session:
            raise RuntimeError("Session not initialized. Use 'async with' context.")
        for attempt in range(1, retries + 1):
            try:
                async with self.session.get(url) as response:
                    if response.status != 200:
                        retry_after = response.headers.get("Retry-After")
                        wait_time = int(retry_after) if retry_after and retry_after.isdigit() else 1
                        #logging.warning(f"Rate limited when accessing {url}. Waiting {wait_time} seconds (attempt {attempt}/{retries}).")
                        await asyncio.sleep(wait_time)
                        continue
                    response.raise_for_status()
                    text = await response.text()
                    if not text.strip():
                        #logging.error(f"Empty response from {url}")
                        return None
                    # If the API returns specific responses, handle them here
                    if text.strip() in ("NO_NUMBERS", "BAD_COUNTRY"):
                        return {}
                    return json.loads(text)
            except (aiohttp.ClientResponseError, aiohttp.ClientError, json.JSONDecodeError) as e:
                logging.error(f"Error fetching {url}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error fetching {url}: {e}")
            if attempt < retries:
                backoff = 2 ** (attempt - 1)
                #logging.info(f"Retrying {url} in {backoff} seconds (attempt {attempt}/{retries})...")
                await asyncio.sleep(backoff)
        #logging.error(f"Failed to fetch JSON from {url} after {retries} attempts.")
        return None

    async def get_prices(self, country_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Fetch prices data. If a country_id is provided, it is appended to the query string.
        """
        async with self.semaphore:
            if country_id:
                url = f"{self.TIGERSMS_BASE_URL}?api_key={TIGERSMS_API_KEY}&action=getPrices&country={country_id}"
            else:
                url = f"{self.TIGERSMS_BASE_URL}?api_key={TIGERSMS_API_KEY}&action=getPrices"
            return await self.fetch_json(url)

    async def fetch_all_data(self) -> Dict[str, Any]:
        """
        Retrieves all data from the TigerSms API.
        """
        data = await self.get_prices()
        if data is None:
            #logging.error("Failed to fetch data from TigerSms API.")
            return {}
        return data

    @staticmethod
    def select_best_service(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Reformats each service's data by selecting the best based on cost and count.
        Expected API response structure (example):
        {
            "22": {
                "kt": {"cost": "9.18", "count": 99},
                "nv": {"cost": "5.63", "count": 60},
                "oi": {"cost": "11.83", "count": 84},
                "ig": {"cost": "4.11", "count": 90},
                "tg": {"cost": "29.53", "count": 92},
                "dh": {"cost": "5.25", "count": 84},
                "vi": {"cost": "6.09", "count": 53},
                "fb": {"cost": "7.99", "count": 100}
            }
        }
        This method reformats the inner service details to the structure:
        {formatted_cost: "count"}
        """
        for country_id, services in data.items():
            for service, details in services.items():
                cost = float(details.get("cost", 0))
                count = details.get("count", 0)
                services[service] = {f"{cost:.4f}": str(count)}
        return data
class SmsActivateManagement:
    SMSACTIVATE_BASE_URL = "https://api.sms-activate.ae/stubs/handler_api.php"  # Assuming similar endpoint structure

    def __init__(self, max_concurrent_requests: int = 5):
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)

    async def __aenter__(self) -> "SmsActivateManagement":
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def fetch_json(self, url: str, retries: int = 3) -> Optional[Dict[str, Any]]:
        if not self.session:
            raise RuntimeError("Session not initialized. Use 'async with' context.")
        for attempt in range(1, retries + 1):
            try:
                async with self.session.get(url) as response:
                    if response.status != 200:
                        retry_after = response.headers.get("Retry-After")
                        wait_time = int(retry_after) if retry_after and retry_after.isdigit() else 1
                        #logging.warning(f"Rate limited when accessing {url}. Waiting {wait_time} seconds (attempt {attempt}/{retries}).")
                        await asyncio.sleep(wait_time)
                        continue
                    response.raise_for_status()
                    text = await response.text()
                    if not text.strip():
                        #logging.error(f"Empty response from {url}")
                        return None
                    # If the API returns specific responses, handle them here
                    if text.strip() in ("NO_NUMBERS", "BAD_COUNTRY"):
                        return {}
                    return json.loads(text)
            except (aiohttp.ClientResponseError, aiohttp.ClientError, json.JSONDecodeError) as e:
                logging.error(f"Error fetching {url}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error fetching {url}: {e}")
            if attempt < retries:
                backoff = 2 ** (attempt - 1)
                #logging.info(f"Retrying {url} in {backoff} seconds (attempt {attempt}/{retries})...")
                await asyncio.sleep(backoff)
        #logging.error(f"Failed to fetch JSON from {url} after {retries} attempts.")
        return None

    async def get_prices(self, country_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Fetch prices data. If a country_id is provided, it is appended to the query string.
        """
        async with self.semaphore:
            if country_id:
                url = f"{self.SMSACTIVATE_BASE_URL}?api_key={SMSACTIVATE_API_KEY}&action=getPrices&country={country_id}"
            else:
                url = f"{self.SMSACTIVATE_BASE_URL}?api_key={SMSACTIVATE_API_KEY}&action=getPrices"
            return await self.fetch_json(url)

    async def fetch_all_data(self) -> Dict[str, Any]:
        """
        Retrieves all data from the SmsActivate API.
        """
        data = await self.get_prices()
        if data is None:
            #logging.error("Failed to fetch data from SmsActivate API.")
            return {}
        return data

    @staticmethod
    def select_best_service(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Reformats each service's data by selecting the best based on cost and count.
        Expected API response structure (example):
        {
            "22": {
                "kt": {"cost": "9.18", "count": 99},
                "nv": {"cost": "5.63", "count": 60},
                "oi": {"cost": "11.83", "count": 84},
                "ig": {"cost": "4.11", "count": 90},
                "tg": {"cost": "29.53", "count": 92},
                "dh": {"cost": "5.25", "count": 84},
                "vi": {"cost": "6.09", "count": 53},
                "fb": {"cost": "7.99", "count": 100}
            }
        }
        This method reformats the inner service details to the structure:
        {formatted_cost: "count"}
        """
        for country_id, services in data.items():
            for service, details in services.items():
                cost = float(details.get("cost", 0))
                count = details.get("count", 0)
                services[service] = {f"{convert_usd_to_rub(cost):.8f}": str(count)}
        return data