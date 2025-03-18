import asyncio
import json
import logging
import time
import hashlib
import random
from datetime import datetime
from functools import wraps
from io import BytesIO
import os

import aiohttp
from PIL import Image
import numpy as np

from telebot.async_telebot import AsyncTeleBot
from telebot.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from typing import Union, Optional, Dict, Any, List, Tuple

from pydantic import BaseModel, ValidationError

from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query
from redis.exceptions import RedisError

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
from handlers.manager.operation_lock import OperationLockManager, OperationType, AsyncOperationContext, operation_lock_manager
from utils.config import  WEBHOOK_HOST as FIVE_SIM_URL

# ---------------- Global Constants ----------------
ORDER_INFO_INDEX = "order_index"
USER_INFO_INDEX = "user_index"
ORDER_CURRENT_PREFIX = "order:current:"
ORDER_INFO_PREFIX = "order_data:"
USER_INFO_PREFIX = "user_data:"
DEPOSIT_INFO_INDEX = "deposit_index"
DEPOSIT_INFO_PREFIX = "deposit_data:"
user_key_profile = "user_data:{user_id}:profile:main"
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
            #user_order_key = f"{USER_INFO_PREFIX}{user_id}:order:{order_id}"
            #filtered_data = {k: v for k, v in data.items() if k not in ('recorded_at', 'search_tags')}
            #await pipe.hset(user_order_key, mapping=filtered_data)
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
        user_order_key = f"{USER_INFO_PREFIX}{user_id}:order:{order_id}"
        
        await self.logger.info(f"Updating order status to {status.lower()} for order {order_id}")
        
        if order_info.get('refund_status') == 'true':
            return {'response': False, 'error': 'Order is already refunded'}
        if order_info.get('order_status') == 'PROCESSING':
            return {'response': False, 'error': 'Order status is PROCESSING'}
        if order_info.get('sms_list', '[]') != '[]':
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
            await pipe.hset(user_order_key, mapping=updates)
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

    async def aggregate_orders(self, filters: Dict[str, Any]) -> Dict[str, float]:
        """Perform a RediSearch aggregation query to compute total order amount and count asynchronously."""
        await self._init_logger()
        try:
            query_str = await self.build_query(filters)
            aggregation_query = [
                "FT.AGGREGATE", ORDER_INFO_INDEX, query_str,
                "GROUPBY", "0",
                "REDUCE", "SUM", "1", "@order_amount", "AS", "total_amount",
                "REDUCE", "COUNT", "0", "AS", "count"
            ]
            result = await self.redis_manager.redis_client.execute_command(*aggregation_query)
            await self.logger.info(f"Aggregation result: {result}")
            
            if not result or len(result) < 2:
                return {"total_amount": 0.0, "count": 0}

            total_amount = float(result[1][1]) if result[1][1] else 0.0
            count = int(result[1][3]) if result[1][3] else 0

            return {"total_amount": total_amount, "count": count}
        except Exception as e:
            await self.logger.error(f"Error aggregating orders: {e}")
            return {"total_amount": 0.0, "count": 0}

# ---------------- UserManagement Class ----------------
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
                        await bot.pin_chat_message(
                            chat_id=channel_id,
                            message_id=result.message_id,
                            disable_notification=True
                        )
                return result.message_id if result else None
            except Exception as e:
                await self.logger.error(f"Error in user_metrics_report: {str(e)}")
                return None
        except Exception as e:
            await self.logger.error(f"Error in user_metrics_report: {str(e)}")
            return None

    async def send_order_report(self, bot: AsyncTeleBot, method: str, order_id: str, user_id: str, channel_id: str, details: dict) -> Optional[int]:
        await self._init_logger()
        try:
            await self.logger.info(f"Sending order report for order_id: {order_id}, user_id: {user_id}")
            
            profile_key = f"user_data:{user_id}:profile:main"
            forum_id = await self.redis_manager.redis_client.hget(profile_key, "forum_id")
            await self.logger.info(f"Retrieved forum_id: {forum_id}")

            message = "<b>#Usᴇʀ_Oʀᴅᴇʀ_Dᴇᴛᴀɪʟs ❯</b>\n\n<b>Tʀᴀɴsᴀᴄᴛɪᴏɴ Dᴇᴛᴀɪʟs »</b>\n"
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
                    InlineKeyboardButton('🔍 Dᴇᴛᴀɪʟs', callback_data='placeholder')
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
                await self.logger.error(f"Error dropping index {index_name}: {e}")
                pass
            definition = IndexDefinition(prefix=[prefix], index_type=IndexType.HASH)
            await redis_client.ft(index_name).create_index(fields=schema, definition=definition)

        user_schema = [
            TextField("user_id", sortable=True),
            TextField("username", sortable=True),
            TextField("first_name", sortable=True),
            TextField("last_name", sortable=True),
            TextField("language_code", sortable=True),
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
            await asyncio.gather(
                create_index(USER_INFO_INDEX, user_schema, USER_INFO_PREFIX),
                create_index(SERVICE_INDEX, service_schema, SERVICE_PREFIX)
            )
            await self.logger.info("UserManagement and Service indexes created successfully")
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
            user_deposit_key = f"user_data:{user_id}:deposit:{deposit_id}"

            data.setdefault('recorded_at', time.time())
            data['search_tags'] = " ".join(filter(None, [
                data.get('deposit_status', ''),
                str(data.get('amount', '')),
                str(deposit_id),
                str(user_id)
            ]))

            async with redis_client.pipeline() as pipe:
                await pipe.hset(deposit_info_key, mapping=data)
                await pipe.hset(user_deposit_key, mapping=data)
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
                    InlineKeyboardButton('🔍 Dᴇᴛᴀɪʟs', callback_data='placeholder')
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
        user_deposit_key = f"user_data:{user_id}:deposit:{deposit_id}"
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
            await pipe.hset(user_deposit_key, mapping=updates)
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
        region: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Asynchronously retrieve a financial summary for the specified user with optimized data processing.
        """
        await self._init_logger()
        try:
            user_profile_task = asyncio.create_task(self.user_mgr.get_user_data(user_id))

            deposit_filters = await self._build_deposit_filters(user_id, start_timestamp, end_timestamp, deposit_types, region)
            order_filters = await self._build_order_filters(user_id, start_timestamp, end_timestamp, order_types, region)

            deposit_task = asyncio.create_task(self.deposit_mgr.aggregate_deposits(deposit_filters))
            order_task = asyncio.create_task(self.order_mgr.aggregate_orders(order_filters))

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
        deposit_types: Optional[List[str]],
        region: Optional[str],
    ) -> Dict[str, Any]:
        filters = {"user_id": user_id, "deposit_status": ["COMPLETED", "PROCESSING"]}
        if start_timestamp and end_timestamp:
            filters["recorded_at"] = (start_timestamp, end_timestamp)
        if deposit_types:
            filters["deposit_type"] = deposit_types
        if region:
            filters["region"] = region
        return filters

    async def _build_order_filters(
        self,
        user_id: str,
        start_timestamp: Optional[float],
        end_timestamp: Optional[float],
        order_types: Optional[List[str]],
        region: Optional[str],
    ) -> Dict[str, Any]:
        filters = {"user_id": user_id, "order_status": ["COMPLETED", "PROCESSING", "PENDING"]}
        if start_timestamp and end_timestamp:
            filters["recorded_at"] = (start_timestamp, end_timestamp)
        if order_types:
            filters["order_type"] = order_types
        if region:
            filters["region"] = region
        return filters

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
        f"{FIVE_SIM_URL}/stubs/handler_api.php"
    )

    def __init__(self, max_concurrent_requests: int = 5):
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)

    async def __aenter__(self) -> "FiveSimManagement":
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def fetch_json(self, url: str, retries: int = 1) -> Optional[Dict[str, Any]]:
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

    async def get_countries(self) -> Optional[Dict[str, Any]]:
        url = f"{self.BASE_URL}?api_key={FIVE_SIM_API_KEY}&action=getCountries"
        return await self.fetch_json(url)

    async def get_prices(self, country_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        async with self.semaphore:
            if country_id:
                url = f"{self.BASE_URL}?api_key={FIVE_SIM_API_KEY}&action=getPrices&country={country_id}"
            else:
                url = f"{self.BASE_URL}?api_key={SMS_HUB_API_KEY}&action=getPrices"
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

    @staticmethod
    def select_best_service(data: Dict[str, Any]) -> Dict[str, Any]:
        for country_id, services in data.items():
            for service, details in services.items():
                if isinstance(details, dict) and details:
                    cost_str, count = next(iter(details.items()))
                    try:
                        cost = float(cost_str)
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
    SMS_SHUB_BASE_URL = "https://smshub.org/stubs/handler_api.php"
    FASTSMS_BASE_URL = "https://fastsms.su/stubs/handler_api.php"

    def __init__(self, max_concurrent_requests: int = 5):
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)

    async def __aenter__(self) -> "SmsHubManagement":
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
        url = f"{self.FASTSMS_BASE_URL}?api_key={FAST_SMS_API_KEY}&action=getCountries"
        return await self.fetch_json(url)

    async def get_prices(self, country_id: str) -> Optional[Dict[str, Any]]:
        url = f"{self.SMS_SHUB_BASE_URL}?api_key={SMS_HUB_API_KEY}&action=getPrices&country={country_id}"
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

    @staticmethod
    def select_best_service(data: Dict[str, Any]) -> Dict[str, Any]:
        for country_id, services in data.items():
            for service, servers in services.items():
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
                    data[country_id][service] = {f"{convert_usd_to_rub(best_server[0]):.8f}": str(best_server[1])}
                except Exception as e:
                    logging.error(f"Error selecting best server for country '{country_id}', service '{service}': {e}")
        return data
class GrizzlySmsManagement:
    GRIZZLY_BASE_URL = "https://api.grizzlysms.com/stubs/handler_api.php"

    def __init__(self, max_concurrent_requests: int = 5):
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)

    async def __aenter__(self) -> "GrizzlySmsManagement":
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
                url = f"{self.GRIZZLY_BASE_URL}?api_key={GRIZZLY_API_KEY}&action=getPrices&country={country_id}"
            else:
                url = f"{self.GRIZZLY_BASE_URL}?api_key={GRIZZLY_API_KEY}&action=getPrices"
            return await self.fetch_json(url)

    async def fetch_all_data(self) -> Dict[str, Any]:
        """
        Retrieves all data from the GrizzlySMS API.
        """
        data = await self.get_prices()
        if data is None:
            #logging.error("Failed to fetch data from GrizzlySMS API.")
            return {}
        return data

    @staticmethod
    def select_best_service(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Reformats each service's data to mirror the structure from class GrizzlySmsManagement.
        """
        for country_id, services in data.items():
            for service, details in services.items():
                cost = details.get("cost", 0)
                count = details.get("count", 0)
                services[service] = {f"{cost:.4f}": str(count)}
        return data
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
            #logging.error("Failed to fetch data from SMSBower API.")
            return {}
        return data

    @staticmethod
    def select_best_service(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Reformats each service's data to mirror the structure from GrizzlySmsManagement.
        """
        for country_id, services in data.items():
            for service, details in services.items():
                cost = details.get("cost", 0)
                count = details.get("count", 0)
                services[service] = {f"{cost:.4f}": str(count)}
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