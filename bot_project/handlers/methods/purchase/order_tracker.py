import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional, List, Awaitable
import aiohttp
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import contextlib
from collections import deque

# Local imports
from handlers.manager.operation import OrderManagement, UserManagement
from handlers.security import TransactionGuard
from handlers.methods.purchase.order_status import purchase_status
from utils.functions import get_api_info, AfterMin, get_sms_text_by_code, small_caps, encode_base62, decode_base62, encode_order_id, decode_barcode_id
from handlers.methods.purchase.order_status import purchase_status
from utils.config import UPDATE_INTERVAL, BASE_TIMEOUT, EXTENDED_TIMEOUT, CHECK_INTERVAL, BATCH_SIZE
from handlers.manager.operation import user_mgr as user_manager
from handlers.main.show_wallet import wallet_manager
from utils.redis_manager import RedisManager, redis_manager

logger = logging.getLogger(__name__)

code = f"{int(time.time() * 1000) % 1000000:06d}"  # 6-digit OTP based on current time
response_next = None #'ACCESS_RETRY_GET'
response_code = None #f'STATUS_OK:{code}' #f'STATUS_WAIT_CODE' # 
class UserOrderTrackerManagement:
    """High-performance order tracking system with real-time capabilities"""
    __slots__ = (
        'check_interval', 'base_timeout', 'extended_timeout', 'update_interval',
        'logging', 'bot', 'order_manager', '_tracking_task', 'transaction_guard',
        '_initialized', '_keyboard_cache', '_adaptive_batch_size',
        '_load_window', '_circuit_state', '_circuit_errors', '_semaphore', 'redis_client'
    )

    def __init__(self, check_interval: int = 5) -> None:
        self.check_interval = max(int(CHECK_INTERVAL), check_interval)
        self.base_timeout = int(BASE_TIMEOUT)
        self.extended_timeout = int(EXTENDED_TIMEOUT)
        self.update_interval = int(UPDATE_INTERVAL)
        self.logging = False #True #
        self._semaphore = asyncio.Semaphore(int(BATCH_SIZE))
        self.redis_client: Optional[RedisManager] = None

        self.bot: Optional[AsyncTeleBot] = None
        self.order_manager: Optional[OrderManagement] = None
        self.transaction_guard: Optional[TransactionGuard] = None
        
        self._tracking_task: Optional[asyncio.Task] = None
        self._initialized = False
        self._keyboard_cache: Dict[str, InlineKeyboardMarkup] = {}
        
        self._adaptive_batch_size = int(BATCH_SIZE)
        self._load_window = deque(maxlen=10)
        self._circuit_state = "closed"
        self._circuit_errors = 0 
    async def init_managers(self, order_mgr: OrderManagement, bot: AsyncTeleBot) -> bool:
        """Initialize with atomic checks and type validation"""
        try:
            if not isinstance(order_mgr, OrderManagement) or not isinstance(bot, AsyncTeleBot):
                logger.error("Invalid manager types")
                return False

            self.order_manager = order_mgr
            self.redis_client = await redis_manager.get_client()
            self.bot = bot
            self.transaction_guard = bot.transaction_guard
            self._initialized = True
            logger.info("Order tracker initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Unexpected error initializing order tracker: {e}")
            return False

    async def start(self) -> None:
        """Start tracking with connection pooling and jitter control"""
        if not self._initialized:
            await self._log('error', "Tracker not initialized")
            return
            
        if not self._tracking_task or self._tracking_task.done():
            self._tracking_task = asyncio.create_task(self._processing_pipeline())
            await self._log('info', "Tracker started")
    async def stop(self) -> None:
        """Graceful shutdown with resource cleanup"""
        if self._tracking_task:
            self._tracking_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._tracking_task
        await self._log('info', "Tracker stopped")

    async def _handle_new_sms(self, order_id: str, api_status: Dict, order_info: Dict) -> None:
        """Handle new SMS without changing created_at"""
        try:
            number_parts = json.loads(order_info['order_number']) if isinstance(order_info.get('order_number'), str) else []
            number_part1 = number_parts[0] if len(number_parts) > 0 else ""
            number_part2 = number_parts[1] if len(number_parts) > 1 else ""
            sms_list = json.loads(order_info.get('sms_list', '[]'))
            sms_list.append(api_status['code'])
            text = "<code>" + "</code>\n<code>        </code><b>•</b> <code>".join(sms_list) + "</code>"
            details = {
                "status": True,
                "order_id": order_id,
                "number": number_part2,
                "code": number_part1,
                "app_id": order_info['app_id'],
                "app_name": order_info['app_name'],
                "server_id": order_info['server_id'],
                "app_price": order_info['order_amount'],
                "country_id": order_info['country_id'],
                "country_code": order_info['country_code'],
                "country_name": order_info['country_name'],
                "user_id": order_info['user_id'],
                "valid_status": order_info['valid_until'],
                "valid_until": order_info['valid_until'],
                "sms_list": text
            }
            tasks = [
                self.order_manager.update_order_success(
                    order_id,
                    api_status['code'],
                    self.extended_timeout,
                    'PROCESSING',
                    'false'
                ),
                self._send_sms_notification(order_id, api_status['code']),
                wallet_manager.process_wallet_update(order_info['user_id']),
                user_manager.send_order_report(self.bot, "edit_message_text", order_id, order_info['user_id'], '-1002203139746', details),
                user_manager.user_metrics_report(self.bot, 'edit_message_text', order_info['user_id'], '-1002203139746')
            ]
            if not sms_list:
                tasks.append(self._update_order_ui(order_info, is_timeout=None))
            await asyncio.gather(*tasks)
        except Exception as e:
            await self._log('error', f"SMS handling failed: {e}")
    async def _handle_retry(self, order_id: str) -> None:
        """Handle retry state"""
        await self.order_manager.update_order_fields(
            order_id,
            fields={'order_status': 'PROCESSING'}
        )


    async def _process_orders_batch(self, batch_size: int) -> None:
        """Process orders with strict timeout enforcement"""
        if self._circuit_state != "closed":
            return

        try:
            offset = 0
            while True:
                orders = await self._fetch_orders_batch(batch_size, offset)
                if not orders:
                    break

                valid, expired = await self._categorize_orders(orders)
                await self._log('debug', f"Processing batch: {len(valid)} valid, {len(expired)} expired")

                async with self._semaphore:
                    processing_tasks = [
                        *[self._process_single_order(o, True) for o in expired],
                        *[self._process_single_order(o, False) for o in valid]
                    ]
                    if processing_tasks:
                        await asyncio.gather(*processing_tasks, return_exceptions=True)

                if len(orders) < batch_size:
                    break
                offset += batch_size

        except Exception as e:
            await self._log('error', f"Batch processing failed: {repr(e)}")
            self._circuit_errors += 1
            if self._circuit_errors >= 3:
                await self._trip_circuit()
    async def _fetch_orders_batch(self, batch_size: int, offset: int) -> List[Dict]:
        """Optimized Redis batch fetch with validation"""
        response = await self.order_manager.search_current_orders(
            query_str="*", 
            limit=batch_size,
            offset=offset
        )
        return response.get('results', []) if isinstance(response, dict) else []


    async def _categorize_orders(self, orders: List[Dict]) -> tuple[List[Dict], List[Dict]]:
        """Categorize orders based on strict timeout rules"""
        current_time = datetime.utcnow()
        validation_tasks = [self._validate_order(o, current_time) for o in orders]
        results = await asyncio.gather(*validation_tasks, return_exceptions=True)
        
        valid, expired = [], []
        for order, result in zip(orders, results):
            if isinstance(result, Exception) or not result:
                continue
            if result['timeout']:
                expired.append(order)
            else:
                valid.append(order)
            
        return valid, expired
    async def _validate_order(self, order: Dict, current_time: datetime) -> Optional[Dict]:
        """Validate timeouts for both PENDING and PROCESSING orders"""
        try:
            created_at = datetime.fromisoformat(order['created_at'])
            status = order.get('order_status', 'PENDING')
        
            # Set timeout based on order status
            if status == 'PENDING':
                timeout = self.base_timeout
            elif status == 'PROCESSING':
                timeout = self.extended_timeout
            else:
                return {'timeout': False}  # No timeout for other statuses

            elapsed = (current_time - created_at).total_seconds()
            return {'timeout': elapsed > timeout * 60}

        except KeyError as e:
            await self._log('error', f"Validation error: {e}")
            return None


    async def _processing_pipeline(self) -> None:
        """Precision-scheduled processing core"""
        next_check = time.monotonic()
        next_update = next_check
        last_load_adjust = next_check

        await self._log('info', "╔══════════════════════════════════════════════════════════════╗")
        await self._log('info', "║ ᴘʀᴏᴄᴇꜱꜱɪɴɢ ᴘɪᴘᴇʟɪɴᴇ ɪɴɪᴛɪᴀʟɪᴢᴇᴅ                                  ║")
        await self._log('info', "╚══════════════════════════════════════════════════════════════╝")

        while True:
            try:
                now = time.monotonic()
                
                if now - last_load_adjust > 30:
                    await self._log('info', "► ᴀᴅᴀᴘᴛɪᴠᴇ ꜱʏꜱᴛᴇᴍ ᴛᴜɴɪɴɢ")
                    await self._adjust_processing_parameters()
                    last_load_adjust = now

                if self._circuit_state == "closed" and now >= next_check:
                    await self._log('info', "► ᴘʀᴏᴄᴇꜱꜱɪɴɢ ᴏʀᴅᴇʀꜱ")
                    await self._process_orders_batch(self._adaptive_batch_size)
                    next_check += self.check_interval

                if now >= next_update:
                    await self._log('info', "► ᴜᴘᴅᴀᴛɪɴɢ ᴜɪ")
                    await self._execute_with_overspill_protection(
                        self._batch_update_countdowns(self._adaptive_batch_size)
                    )
                    next_update += self.update_interval

                sleep_time = min(next_check, next_update) - now
                await self._log('debug', f"→ ꜱʟᴇᴇᴘɪɴɢ ꜰᴏʀ {sleep_time:.2f}ꜱ")
                await asyncio.sleep(max(0, sleep_time))

            except asyncio.CancelledError:
                await self._log('info', "╔══════════════════════════════════════════════════════════════╗")
                await self._log('info', "║ Pʀᴏᴄᴇꜱꜱɪɴɢ ᴘɪᴘᴇʟɪɴᴇ ᴄᴀɴᴄᴇʟʟᴇᴅ                                 ║")
                await self._log('info', "╚══════════════════════════════════════════════════════════════╝")
                break
            except Exception as e:
                await self._log('error', f"╔══════════════════════════════════════════════════════════════╗")
                await self._log('error', f"║ ᴘɪᴘᴇʟɪɴᴇ ᴇʀʀᴏʀ: {repr(e)[:100]} ║")
                await self._log('error', f"╚══════════════════════════════════════════════════════════════╝")
                await asyncio.sleep(min(5, self.check_interval))
    async def _execute_parallel_tasks(self, tasks: List[Awaitable]) -> None:
        """Batch task execution with error suppression"""
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                await self._log('error', f"Task error: {result}")

    async def _parse_api_response(self, response: str) -> Dict[str, Any]:
        """Asynchronous cached response mapping with pattern matching"""
        status_map = {
            'STATUS_OK': 'RECEIVED',
            'STATUS_WAIT_CODE': 'PENDING',
            'STATUS_WAIT_RETRY': 'PROCESSING',
            'STATUS_WAIT_RESEND': 'PROCESSING',
            'ACCESS_CANCEL': 'CANCELLED',
            'STATUS_CANCEL': 'CANCELLED',
            'NO_ACTIVATION': 'CANCELLED',
            'TIMEOUT': 'FINISHED',
            'BAD_KEY': 'FAILED',
            'BAD_ACTION': 'FAILED',
        }
        
        clean_response = response.split(':', 1)[0].strip()
        order_status = status_map.get(clean_response, 'FAILED')
        
        result = {
            'status': True,
            'order_status': order_status,
            'raw_response': response
        }
        
        if order_status == 'RECEIVED':
            result['code'] = response.split(':', 1)[1].strip() if ':' in response else ''
        
        return result
    async def _check_api_status(self, order: Dict) -> Dict:
        """Robust API checker with retry logic"""
        retries = 3
        timeout = 2.0
        
        for attempt in range(retries):
            try:
                server_id = int(order['server_id'])
                server_name, api_key = await get_api_info(server_id)
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"https://{server_name}/stubs/handler_api.php",
                        params={
                            'api_key': api_key,
                            'action': 'getStatus',
                            'id': order['order_id']
                        },
                        timeout=timeout
                    ) as resp:
                        resp.raise_for_status()
                        raw_response = await resp.text()
                        if response_code:
                            raw_response = response_code
                        return await self._parse_api_response(raw_response)
                    
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt == retries - 1:
                    return {'status': False, 'order_status': 'FAILED'}
                timeout *= 1.5
                continue

        return {'status': False, 'order_status': 'FAILED'}


    async def _complete_order(self, order_id: str, order_info: Dict, reason: str = 'API_COMPLETION') -> None:
        """Finalize order completion with audit trail and error handling"""
        try:
            # Validate order_info
            if not order_info or not isinstance(order_info, dict):
                await self._log('error', f"Invalid order_info for {order_id}")
                return

            # Prepare completion data
            current_time = time.time()
            completion_data = {
                "timestamp": current_time,
                "action": "ORDER_COMPLETED",
                "reason": reason
            }

            # Safely update order history
            try:
                order_history = json.loads(order_info.get('order_history', '[]'))
                if not isinstance(order_history, list):
                    order_history = []
            except json.JSONDecodeError:
                order_history = []

            fields = {
                'order_status': 'COMPLETED',
                'completed_at': current_time,
                'order_history': json.dumps([*order_history, completion_data])
            }

            # Atomic updates
            await self.order_manager.update_order_fields(order_id, fields=fields)
            order_info = (await self.order_manager.get_order_data(order_id))['result']
            number_parts = json.loads(order_info['order_number']) if isinstance(order_info.get('order_number'), str) else []
            number_part1 = number_parts[0] if len(number_parts) > 0 else ""
            number_part2 = number_parts[1] if len(number_parts) > 1 else ""
            sms_list = json.loads(order_info.get('sms_list', '[]'))
            sms_list = "<code>" + "</code>\n<code>        </code><b>•</b> <code>".join(sms_list) + "</code>"
            details = {
                "status": True,
                "order_id": order_id,
                "number": number_part2,
                "code": number_part1,
                "app_id": order_info['app_id'],
                "app_name": order_info['app_name'],
                "server_id": order_info['server_id'],
                "app_price": order_info['order_amount'],
                "country_id": order_info['country_id'],
                "country_code": order_info['country_code'],
                "country_name": order_info['country_name'],
                "user_id": order_info['user_id'],
                "valid_status": "✅ Oʀᴅᴇʀ Hᴀs Cᴏᴍᴘʟᴇᴛᴇᴅ",
                "sms_list": sms_list
            }
            await asyncio.gather(
                self._update_order_ui(order_info, is_timeout=None),
                user_manager.user_metrics_report(self.bot, 'edit_message_text', order_info['user_id'], '-1002203139746'),
                wallet_manager.process_wallet_update(order_info['user_id']),
                user_manager.send_order_report(self.bot, "edit_message_text", order_id, order_info['user_id'], '-1002203139746', details)
            )

            await self._log('info', f"Order {order_id} completed: {reason}")

        except KeyError as e:
            await self._log('error', f"Missing key in order_info for {order_id}: {e}")
        except json.JSONDecodeError as e:
            await self._log('error', f"Invalid order_history JSON for {order_id}: {e}")
        except Exception as e:
            await self._log('error', f"Failed to complete order {order_id}: {e}")
    async def _handle_order_timeout(self, order_id: str) -> None:
        """Handle timeout ONLY for orders with no SMS received"""
        async with self.transaction_guard:
            order_data = await self.order_manager.get_order_data(order_id)
            if not order_data.get('result'):
                return
        
            order_info = order_data['result']
        
            # Critical check: Only process PENDING orders with no SMS
            if order_info['order_status'] != 'PENDING':
                await self._log('debug', f"Skipping timeout - Order {order_id} has SMS")
                return

            # Verify no SMS was ever received
            sms_list = json.loads(order_info.get('sms_list', '[]'))
            if len(sms_list) > 0:
                await self._log('warning', f"Invalid timeout attempt - Order {order_id} has SMS")
                return

            # Proceed with cancellation
            api_result = await purchase_status.cancel_number_api(
                order_info['server_id'],
                order_info['order_id']
            )
        
            if not api_result.get('response', False):
                await self._log('error', f"API cancellation failed for {order_id}")
                return
            number_parts = json.loads(order_info['order_number']) if isinstance(order_info.get('order_number'), str) else []
            number_part1 = number_parts[0] if len(number_parts) > 0 else ""
            number_part2 = number_parts[1] if len(number_parts) > 1 else ""
            details = {
                "status": True,
                "order_id": order_id,
                "number": number_part2,
                "code": number_part1,
                "app_id": order_info['app_id'],
                "app_name": order_info['app_name'],
                "server_id": order_info['server_id'],
                "app_price": order_info['order_amount'],
                "country_id": order_info['country_id'],
                "country_code": order_info['country_code'],
                "country_name": order_info['country_name'],
                "user_id": order_info['user_id'],
                "valid_status": "⏱️ Oʀᴅᴇʀ Hᴀs Exᴘɪʀᴇᴅ"
            }

            await asyncio.gather(
                self.order_manager.cancel_order(order_id, order_info['user_id'], 'TIMEOUT'),
                self._update_order_ui(order_info, is_timeout=True),
                user_manager.send_order_report(self.bot, "edit_message_text", order_id, order_info['user_id'], '-1002203139746', details),
                user_manager.user_metrics_report(self.bot, 'edit_message_text', order_info['user_id'], '-1002203139746'),
                wallet_manager.process_wallet_update(order_info['user_id'])
                
            )


    async def _request_sms_retry(self, order_id: str) -> None:
        """Compulsory retry request"""
        try:
            order_data = (await self.order_manager.get_order_data(order_id))['result']
            server_name, api_key = await get_api_info(int(order_data['server_id']))
            
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"https://{server_name}/stubs/handler_api.php",
                    params={
                        'api_key': api_key,
                        'action': 'setStatus',
                        'id': order_data['order_id'],
                        'status': 3
                    },
                    timeout=5
                ) as response:
                    response_text = await response.text()
                    if response_next:
                        response_text = response_next
                    if response_text != 'ACCESS_RETRY_GET':
                        await self._complete_order(order_id, order_data, 'API_COMPLETION')

        except Exception as e:
            logger.error(f"SMS retry request failed: {e}")
    async def _update_order_ui(self, order_info: Dict, is_timeout: bool = False, current_time: str = None) -> None:
        """Robust UI update with error handling and type checking"""
        try:
            # Change order ID extraction to handle Redis key format
            order_id = order_info["id"].split(":")[-1] if order_info["id"].startswith("order_data:info:") else ''
            order_info['order_amount'] = float(order_info.get('order_amount', 0))
            order_info['order_id'] = order_id
            
            template = await self._get_message_template(order_info=order_info, is_timeout=is_timeout, current_time=current_time)
            keyboard = await self._get_cached_keyboard(order_info, is_timeout)
 
            await self._log('info', f"┌───────────────────────────────────┐")
            await self._log('info', f"│ ***ᴜᴘᴅᴀᴛɪɴɢ ᴜɪ ғᴏʀ ᴏʀᴅᴇʀ {order_id} │")
            await self._log('info', f"└───────────────────────────────────┘")
            await self.bot.edit_message_text(
                chat_id=order_info['user_id'],
                message_id=order_info['message_id'],
                parse_mode='HTML',
                reply_markup=keyboard,
                **template
            )
            if is_timeout is True:
                #print(f"┌─────────────────────────────────────┐")
                #print(f"│ ***sᴇɴᴅɪɴɢ ᴛɪᴍᴇᴏᴜᴛ ᴍᴇssᴀɢᴇ ғᴏʀ {order_id} │")
                #print(f"└─────────────────────────────────────┘")
                await self.bot.send_message(chat_id=order_info['user_id'], reply_to_message_id=order_info['message_id'], text='<blockquote><b>⌛ Tʜɪs Oʀᴅᴇʀ Hᴀs Exᴘɪʀᴇᴅ, Aɴᴅ Tʜᴇ Rᴇғᴜɴᴅ Hᴀs Bᴇᴇɴ Cʀᴇᴅɪᴛᴇᴅ Tᴏ Yᴏᴜʀ Aᴄᴄᴏᴜɴᴛ!</b></blockquote>', parse_mode='HTML')
        except KeyError as e:
            print(f"┌───────────────────────────────────────┐")
            print(f"│ ***ᴜɪ ᴜᴘᴅᴀᴛᴇ ᴋᴇʏ ᴇʀʀᴏʀ ғᴏʀ {order_id}: {e} │")
            print(f"└───────────────────────────────────────┘")
        except ValueError as e:
            print(f"┌────────────────────────────────────────────┐")
            print(f"│ ***ᴜɪ ᴜᴘᴅᴀᴛᴇ ᴠᴀʟᴜᴇ ᴇʀʀᴏʀ ғᴏʀ {order_id}: {e} │")
            print(f"└────────────────────────────────────────────┘")
        except Exception as e:
            print(f"┌─────────────────────────────────────┐")
            print(f"│ ***ᴜɪ ᴜᴘᴅᴀᴛᴇ ғᴀɪʟᴇᴅ ғᴏʀ {order_id}: {e} │")
            print(f"└─────────────────────────────────────┘")


    async def _get_cached_keyboard(self, order_info: Dict, is_timeout: bool) -> InlineKeyboardMarkup:
        """Asynchronous, non-blocking keyboard creation with order ID validation"""
        try:
            order_id = int(order_info.get('order_id', ''))
            barcode_id = await encode_order_id(order_id)
            status = order_info.get('order_status', 'unknown').upper()
            valid_status = status if status in ['PENDING', 'PROCESSING', 'COMPLETED'] else 'unknown'
            keyboard = InlineKeyboardMarkup()
            recorded_at = float(order_info.get('recorded_at', 0))
            current_time = time.time()
            order_amount = order_info.get('order_amount', 0)
            extra = ''
            if current_time - recorded_at < 60:
                extra = ' '

            buy_again_btn = InlineKeyboardButton(
                "↻ Bᴜʏ Aɢᴀɪɴ", 
                callback_data=f"purchase:{order_info.get('app_id', '')}:{order_amount}:{order_info.get('server_id', '')}:{order_info.get('country_id', '')}:{order_info.get('country_code', '')}{extra}"
            )
            
            if is_timeout:
                if valid_status == 'PENDING':
                    keyboard.row(
                        InlineKeyboardButton("↻ Cʜᴀɴɢᴇ Cᴏᴜɴᴛʀʏ", switch_inline_query_current_chat=f"#AᴘᴘIᴅ:{order_info.get('app_id', '')}"),
                        buy_again_btn
                    )
                elif valid_status in {'COMPLETED', 'PROCESSING'}:
                    keyboard.row(
                        InlineKeyboardButton("✆ Sᴍs Lɪsᴛ", switch_inline_query_current_chat=f"#BᴀʀCᴏᴅᴇ-{barcode_id}"),
                        buy_again_btn
                    )
                else:
                    keyboard.row(buy_again_btn)
            else:
                if valid_status == 'PENDING':
                    keyboard.row(
                        InlineKeyboardButton("✘ Cᴀɴᴄᴇʟ", callback_data=f"status_cancel:{order_id}:{order_info.get('user_id', '')}"),
                        buy_again_btn
                    )
                elif valid_status in {'COMPLETED', 'PROCESSING'}:
                    keyboard.row(
                        InlineKeyboardButton("✆ Sᴍs Lɪsᴛ", switch_inline_query_current_chat=f"#BᴀʀCᴏᴅᴇ-{barcode_id}"),
                        buy_again_btn
                    )
                else:
                    keyboard.row(buy_again_btn)
            
            return keyboard
        
        except Exception as e:
            #awaitt self._log_async('error', f"Keyboard fallback: {str(e)}")
            return InlineKeyboardMarkup(row_width=2).add(
                InlineKeyboardButton("❌ Error - Contact Support", url="t.me/your_support")
            )
    async def _get_message_template(self, order_info: Dict, is_timeout: bool, current_time: str = None) -> Dict:
        """Asynchronous precomputed message templates with lazy formatting"""
        number_parts = json.loads(order_info['order_number'])
        base_template = (
            f"<blockquote><b>📦 {order_info['app_name'].translate(await small_caps())} [</b> 💎 "
            f"<code>{order_info['order_amount']}</code> <b>][</b> <code>{order_info['country_code']}</code> "
            f"<b>][</b> <code>{order_info['server_id']}</code> <b>]</b></blockquote>\n\n"
            f"<b>📞 Nᴜᴍʙᴇʀ »</b> <code>{number_parts[0]}</code> <code>{number_parts[1]}</code>\n\n"
        )

        if is_timeout is True:
            text = f"{base_template}<b>⏱️ Oʀᴅᴇʀ Hᴀs Exᴘɪʀᴇᴅ [</b><code>Rᴇғᴜɴᴅᴇᴅ</code><b>]</b>"
        elif is_timeout is None or order_info.get('order_status') == 'COMPLETED':
            text = f"{base_template}<b>✅ Oʀᴅᴇʀ Hᴀs Cᴏᴍᴘʟᴇᴛᴇᴅ.</b>"
        else:
            text = f"{base_template}⏱ <b>Vᴀʟɪᴅ Uɴᴛɪʟ »</b> {order_info.get('valid_until', 'N/A')} <b>[</b>{current_time}<b>]</b>"
        
        return {'text': text}

    async def _get_timeout(self, order_info: Dict) -> int:
        try:
            return int(order_info.get('timeout', self.base_timeout))
        except (ValueError, TypeError):
            await self._log('warning', f"Invalid timeout value in order {order_info.get('id')}")
            return self.base_timeout
    async def _calculate_remaining_time(self, created_at: str, timeout: int) -> str:
        elapsed = (datetime.utcnow() - datetime.fromisoformat(created_at)).total_seconds()
        remaining = max(0, timeout * 60 - elapsed)
        mins, secs = divmod(int(remaining), 60)
        return f"<code>{mins:02}</code> <code>Mɪɴ</code>" #f"<code>{mins:02}</code>:<code>{secs:02}</code>" if self.update_interval < 60 else


    async def _batch_update_countdowns(self, batch_size: int, max_workers: int = 10) -> None:
        """Ultra-fast priority-based UI updater with optimized concurrency & batching"""

        before = time.time()
        await self._log('info', "╔══════════════════════════════════════════════════╗")
        await self._log('info', "║ ### STARTING BATCH COUNTDOWN UPDATE              ║")
        await self._log('info', "╚══════════════════════════════════════════════════╝")

        async def worker(queue: asyncio.Queue):
            """Worker function to process orders efficiently"""
            try:
                while True:
                    order = await queue.get()
                    if order is None:  # Stop signal
                        queue.task_done()  # Ensure the stop signal is marked as done
                        break

                    if order.get('order_status') in {'PENDING', 'PROCESSING'}:
                        try:
                            timeout = await self._get_timeout(order)
                            remaining = await self._calculate_remaining_time(order['created_at'], timeout)
                            current_time = (remaining.replace(' ', '')
                                                     .replace('Mɪɴ', '')
                                                     .replace('s', '')
                                                     .replace('<code>', '')
                                                     .replace('</code>', ''))
                            last_updated = order.get('last_updated', '')
                            if int(last_updated) != int(current_time):
                                await self._update_order_ui(order_info=order, current_time=remaining)
                                await self.redis_client.hset(order['id'], 'last_updated', str(current_time))
                        except Exception as e:
                            await self._log('error', f"Error updating order {order.get('id')}: {str(e)}")

                    queue.task_done()
            except asyncio.CancelledError:
                # If cancelled, ensure we exit cleanly
                raise

        order_queue = asyncio.Queue(maxsize=batch_size)
        workers = [asyncio.create_task(worker(order_queue)) for _ in range(max_workers)]

        try:
            offset = 0
            while True:
                orders = await self._fetch_orders_batch(batch_size, offset)
                if not orders:
                    break

                await self._log('info', f"► Processing {len(orders)} orders")
                orders.sort(
                    key=lambda o: datetime.fromisoformat(o['created_at']).timestamp(),
                    reverse=True
                )

                for order in orders:
                    await order_queue.put(order)

                offset += batch_size
                if len(orders) < batch_size:
                    break  # No more orders left

            # Wait for the queue to be fully processed
            await order_queue.join()

            # Signal workers to stop by adding a stop signal for each worker
            for _ in range(max_workers):
                await order_queue.put(None)

            # Wait for all workers to exit gracefully
            await asyncio.gather(*workers, return_exceptions=True)

            after = time.time()
            await self._log('info', f"Batch update time: {after - before:.2f}s")
            await self._log('info', "╔══════════════════════════════════════════════════╗")
            await self._log('info', "║ ### BATCH COUNTDOWN UPDATE COMPLETED            ║")
            await self._log('info', "╚══════════════════════════════════════════════════╝")
        except Exception as e:
            await self._log('error', f"✖ Countdown update failed: {repr(e)}")
            # In case of error, cancel any remaining worker tasks
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
        finally:
            # Ensure all worker tasks are cancelled if still running
            for task in workers:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
    async def _trip_circuit(self) -> None:
        """Overload protection mechanism"""
        self._circuit_state = "open"
        await self._log('critical', "Circuit tripped - entering cooldown")
        asyncio.create_task(self._reset_circuit())
    async def _reset_circuit(self) -> None:
        """Automatic system recovery"""
        await asyncio.sleep(60)
        self._circuit_state = "half-open"
        await asyncio.sleep(30)
        self._circuit_state = "closed"
        self._circuit_errors = 0

    
    async def _handle_sms_reception(self, order_id: str, api_status: Dict) -> None:
        """SMS handling pipeline with atomic updates"""
        try:
            await asyncio.gather(
                self.order_manager.update_order_success(order_id, api_status['code'], self.extended_timeout, 'PROCESSING', 'false'),
                self._send_sms_notification(order_id, api_status['code']),
                self._request_sms_retry(order_id)
            )
        except Exception as e:
            await self._log('error', f"SMS handling failed: {e}")
    async def _send_sms_notification(self, order_id: str, code: str) -> None:
        """Batched SMS notifications with template reuse"""
        try:
            order_data = (await self.order_manager.get_order_data(order_id))['result']
            text = (
                f"<blockquote><b>🗨️ Nᴇᴡ Mᴇssᴀɢᴇ Rᴇᴄᴇɪᴠᴇᴅ [ "
                f"<code>{json.loads(order_data['order_number'])[0]}</code> "
                f"<code>{json.loads(order_data['order_number'])[1]}</code> ]</b></blockquote>"
                f"<pre><code class=\"language-• Sᴍs ❯ \">{code}</code></pre>"
            )
            
            await self.bot.send_message(
                chat_id=order_data['user_id'],
                text=text,
                reply_to_message_id=order_data['message_id'],
                parse_mode='HTML'
            )
        except Exception as e:
            await self._log('error', f"SMS notification failed: {e}")
    

    async def _execute_with_overspill_protection(self, coroutine) -> None:
        """Execute a coroutine with timeout protection and load shedding."""
        try:
            # Set a timeout (e.g., 70% of the update interval)
            await asyncio.wait_for(
                coroutine,
                timeout=self.update_interval * 0.7  # Timeout after 70% of the interval
            )
        except asyncio.TimeoutError:
            # Log a warning and reduce batch size to shed load
            await self._log('warning', "╔═════════════════════════════════════════╗")
            await self._log('warning', "║ !!! Sʜᴇᴅᴅɪɴɢ ʟᴏᴀᴅ ᴛᴏ ᴍᴀɪɴᴛᴀɪɴ ʀᴇᴀʟ-ᴛɪᴍᴇ ║")
            await self._log('warning', "║    ᴘᴇʀғᴏʀᴍᴀɴᴄᴇ                          ║")
            await self._log('warning', "╚═════════════════════════════════════════╝")
            if self._adaptive_batch_size > 100:
                self._adaptive_batch_size = int(self._adaptive_batch_size * 0.9)  # Reduce batch size by 10%
        except Exception as e:
            # Log any other errors
            await self._log('error', f"Task execution failed: {repr(e)}")
    async def _process_single_order(self, order: Dict, is_expired: bool) -> None:
        """Handle expired orders based on their status"""
        order_id = order.get('id', '').split(':')[-1]
        if not order_id:
            return

        try:
            await self._log('info', f"Processing order {order_id}")
            if is_expired:
                await self._log('info', f"Order {order_id} is expired, getting fresh data")
                order_data = await self.order_manager.get_order_data(order_id)
                if not order_data.get('result'):
                    await self._log('warning', f"No data found for expired order {order_id}")
                    return
                
                current_status = order_data['result'].get('order_status', 'PENDING')
                sms_list = json.loads(order_data['result'].get('sms_list', '[]'))
                await self._log('info', f"Expired order {order_id} status: {current_status}, SMS count: {len(sms_list)}")

                if current_status == 'PENDING' and not sms_list:
                    await self._log('info', f"Handling timeout with refund for order {order_id}")
                    await self._handle_order_timeout(order_id)
                elif current_status == 'PROCESSING':
                    await self._log('info', f"Completing processing order {order_id} due to extended timeout")
                    await self._complete_order(order_id, order_data['result'], reason='EXTENDED_TIMEOUT')
                return

            await self._log('info', f"Checking API status for order {order_id}")
            api_status = await self._check_api_status(order)
            if not api_status['status']:
                await self._log('warning', f"API status check failed for order {order_id}")
                #await self.order_manager.update_order_status(order_id, 'FAILED')
                return

            status = api_status['order_status']
            await self._log('info', f"Order {order_id} API status: {status}")

            if status == 'RECEIVED':
                await self._log('info', f"Handling new SMS for order {order_id}")
                await self._handle_new_sms(order_id, api_status, order)
                await self._request_sms_retry(order_id)
            elif status == 'COMPLETED':
                await self._log('info', f"Completing order {order_id}")
                order_data = (await self.order_manager.get_order_data(order_id))['result']
                await self._complete_order(order_id, order, reason='API_COMPLETION')
            elif status == 'ACCESS_RETRY_GET':
                await self._log('info', f"Handling retry for order {order_id}")
                await self._handle_retry(order_id)
            elif status == 'CANCELLED':
                await self._log('info', f"Handling timeout for cancelled order {order_id}")
                await self._handle_order_timeout(order_id)

        except Exception as e:
            await self._log('error', f"Order {order_id} processing failed: {e}")


    async def _adjust_processing_parameters(self) -> None:
        """Self-optimizing batch size controller"""
        if len(self._load_window) < 5:
            return

        avg_time = sum(self._load_window) / len(self._load_window)
        safety_margin = self.check_interval * 0.3
        
        if avg_time > self.check_interval - safety_margin:
            new_size = max(100, int(self._adaptive_batch_size * 0.8))
        else:
            new_size = min(200, int(self._adaptive_batch_size * 1.2))
        
        self._adaptive_batch_size = new_size
        await self._log('debug', f"Adjusted batch size to {new_size} based on avg {avg_time:.2f}s")
    async def _log(self, level: str, message: str, *args, **kwargs) -> None:
        """Optimized logging with rate limiting"""
        if self.logging:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: getattr(logger, level, logger.info)(message[:150], *args, **kwargs))


order_tracker = UserOrderTrackerManagement()

async def init_managers(order_manager: OrderManagement, user_manager: UserManagement, bot: AsyncTeleBot) -> bool:
    try:
        if not isinstance(order_manager, OrderManagement):
            logger.error("Invalid order manager type")
            return False
        
        if not hasattr(order_tracker, 'init_managers') or not callable(order_tracker.init_managers):
            logger.error("Order tracker does not have a valid init_managers method")
            return False

        success = await order_tracker.init_managers(order_manager, bot)
        if success:
            logger.info("Order tracker initialized successfully")
            return True
        else:
            logger.error("Failed to initialize order tracker")
            return False
    except Exception as e:
        logger.error(f"Unexpected error initializing order tracker: {e}")
        return False

async def register_handlers(bot: AsyncTeleBot) -> None:
    try:
        if not isinstance(bot, AsyncTeleBot):
            logger.error("Invalid bot instance")
            return
        
        order_tracker.bot = bot
        order_tracker.transaction_guard = getattr(bot, 'transaction_guard', None)
        async with aiohttp.ClientSession() as session:
            # Perform any necessary operations with the session
            await order_tracker.start()
        logger.info("Order tracker initialized successfully")
    except Exception as e:
        logger.error(f"Failed to register order tracker handlers: {e}")

__all__ = ['init_managers', 'register_handlers', 'order_tracker']
