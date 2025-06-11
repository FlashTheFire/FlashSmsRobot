import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional, List, Awaitable
import aiohttp
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto
import contextlib
from collections import deque

# Local imports
from handlers.manager.operation import OrderManagement, UserManagement, DepositManagement
from handlers.security import TransactionGuard
from utils.config import MIN_DEPOSIT
from utils.functions import (
    get_api_info, AfterMin, small_caps, encode_base62, decode_base62,
    format_currency, qr_code
)

logger = logging.getLogger(__name__)

class DepositTrackerManagement:
    """
    High-performance deposit tracking system with real-time capabilities.
    This class polls the deposit API, categorizes deposit records, updates UI elements,
    and handles timeouts and completion events.
    """
    __slots__ = (
        'check_interval', 'base_timeout', 'update_interval',
        'logging', 'bot', 'deposit_manager', 'transaction_guard', '_tracking_task',
        '_keyboard_cache', '_adaptive_batch_size', '_load_window',
        '_circuit_state', '_circuit_errors', '_semaphore', '_initialized'
    )

    def __init__(self, check_interval: int = 5) -> None:
        self.check_interval = max(5, check_interval)
        self.base_timeout = 10  # minutes
        self.update_interval = 60  # seconds
        self.logging =  False#True#
        self._semaphore = asyncio.Semaphore(100)

        # Service references
        self.bot: Optional[AsyncTeleBot] = None
        self.deposit_manager: Optional[DepositManagement] = None
        self.transaction_guard: Optional[TransactionGuard] = None
        
        # Runtime state
        self._tracking_task: Optional[asyncio.Task] = None
        self._initialized = False
        self._keyboard_cache: Dict[str, InlineKeyboardMarkup] = {}
        
        # Adaptive performance controls
        self._adaptive_batch_size = 100
        self._load_window = deque(maxlen=10)
        self._circuit_state = "closed"
        self._circuit_errors = 0
    async def init_managers(self, deposit_mgr: DepositManagement, bot: AsyncTeleBot) -> bool:
        """Initialize with atomic checks and type validation"""
        try:
            if not isinstance(deposit_mgr, DepositManagement) or not isinstance(bot, AsyncTeleBot):
                await self._log('error', "Invalid manager types")
                return False
            self.deposit_manager = deposit_mgr
            self.bot = bot
            self.transaction_guard = bot.transaction_guard
            self._initialized = True
            await self._log('info', "Deposit tracker initialized successfully")
            return True
        except Exception as e:
            await self._log('error', f"Unexpected error initializing deposit tracker: {e}")
            return False
    
    async def _get_timeout(self, deposit_info: Dict) -> int:
        return self.base_timeout
    async def _log(self, level: str, message: str, *args, **kwargs) -> None:
        """Optimized logging with rate limiting"""
        if self.logging:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: getattr(logger, level, logger.info)(message[:150], *args, **kwargs))

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
        self._tracking_task = None
        await self._log('info', "Tracker stopped")

    '''async def _processing_pipeline(self) -> None:
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=100, ssl=False),
            timeout=aiohttp.ClientTimeout(total=10)
        ) as session:
            self.session = session
            while True:
                try:
                    await self._process_deposits_batch(self._adaptive_batch_size)
                    await asyncio.sleep(self.check_interval)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    await self._log('error', f"Error in processing pipeline: {e}")
                    self._circuit_errors += 1
                    if self._circuit_errors >= 3:
                        await self._trip_circuit()
        self.session = None'''
    
    async def _complete_deposit(self, deposit_id: str, api_status: Dict, reason: str = 'API_COMPLETION') -> None:
        try:
            deposit_amount = float(api_status.get('deposit_amount', 0))

            if not api_status or not isinstance(api_status, dict):
                await self._log('error', f"Invalid deposit_info for {deposit_id}")
                return

            timeout = 'false'
            deposit_status = api_status.get('deposit_status', 'ERROR')
            
            deposit_info = (await self.deposit_manager.get_deposit_data(deposit_id))['result']
            valid_until = deposit_info.get('valid_until', None)
            deposit_info['deposit_status'] = 'COMPLETED'
            await asyncio.gather(
                self._update_deposit_ui(deposit_info, is_timeout=None, api_status=api_status),
                self.deposit_manager.update_deposit_success(
                    self.bot,
                    deposit_id=deposit_id,
                    deposit_amount=deposit_amount,
                    timeout=timeout,
                    deposit_status=deposit_status,
                    api_status=api_status,
                    valid_until=valid_until
                )
            )
            await self._log('info', f"Deposit {deposit_id} completed: {reason}")
        except KeyError as e:
            await self._log('error', f"Missing key in deposit_info for {deposit_id}: {str(e)}")
        except Exception as e:
            await self._log('error', f"Failed to complete deposit {deposit_id}: {str(e)}")

    async def _processing_pipeline(self) -> None:
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
                    await self._log('info', "► ᴘʀᴏᴄᴇꜱꜱɪɴɢ ᴅᴇᴘᴏꜱɪᴛꜱ")
                    await self._process_deposits_batch(self._adaptive_batch_size)
                    next_check += self.check_interval

                if now >= next_update:
                    await self._log('info', "► ᴜᴘᴅᴀᴛɪɴɢ ᴅᴇᴘᴏꜱɪᴛ ᴜɪ")
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
                await self._log('error', f"Error in processing pipeline: {e}")
                self._circuit_errors += 1
                if self._circuit_errors >= 3:
                    await self._trip_circuit()
                await asyncio.sleep(min(5, self.check_interval))
    async def _process_deposits_batch(self, batch_size: int) -> None:
        if self._circuit_state != "closed":
            return

        try:
            offset = 0
            while True:
                deposits = await self._fetch_deposits_batch(batch_size, offset)
                if not deposits:
                    break

                valid, expired = await self._categorize_deposits(deposits)
                await self._log('debug', f"Processing batch: {len(valid)} valid, {len(expired)} expired")

                async with self._semaphore:
                    processing_tasks = [
                        *[self._process_single_deposit(d, True) for d in expired],
                        *[self._process_single_deposit(d, False) for d in valid]
                    ]
                    if processing_tasks:
                        await asyncio.gather(*processing_tasks, return_exceptions=True)

                if len(deposits) < batch_size:
                    break
                offset += batch_size
        except Exception as e:
            await self._log('error', f"Batch processing failed: {repr(e)}")
            self._circuit_errors += 1
            if self._circuit_errors >= 3:
                await self._trip_circuit()

    async def _fetch_deposits_batch(self, batch_size: int, offset: int) -> List[Dict]:
        response = await self.deposit_manager.search_current_deposits(
            query_str="*", 
            limit=batch_size,
            offset=offset
        )
        return response.get('results', []) if isinstance(response, dict) else []
    async def _categorize_deposits(self, deposits: List[Dict]) -> tuple[List[Dict], List[Dict]]:
        current_time = datetime.utcnow()
        tasks = [self._validate_deposit(d, current_time) for d in deposits]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        valid, expired = [], []
        for deposit, result in zip(deposits, results):
            if isinstance(result, Exception) or not result:
                continue
            if result['timeout']:
                expired.append(deposit)
            else:
                valid.append(deposit)
        return valid, expired

    async def _validate_deposit(self, deposit: Dict, current_time: datetime) -> Optional[Dict]:
        try:
            created_at = datetime.fromisoformat(deposit['created_at'])
            status = deposit.get('deposit_status', 'PENDING')
            if status == 'PENDING':
                timeout = self.base_timeout
            else:
                return {'timeout': False}
            elapsed = (current_time - created_at).total_seconds()
            return {'timeout': elapsed > timeout * 60}
        except KeyError as e:
            await self._log('error', f"Validation error: {e}")
            return None
    async def _execute_parallel_tasks(self, tasks: List[Awaitable]) -> None:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                await self._log('error', f"Task error: {result}")

    async def _check_api_status(self, deposit: Dict) -> Dict:
        retries = 3
        timeout = 2.0

        for attempt in range(retries):
            try:
                server_name, api_key = ('paytm.udayscriptsx.workers.dev', 'UWjSzy23711328951174')
                url = f"https://{server_name}/"
                params = {'mid': api_key, 'id': deposit['deposit_id']}
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=timeout) as resp:
                        resp.raise_for_status()
                        raw_response = await resp.text()
                        return await self._parse_api_response(raw_response)
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                await self._log('error', f"API request failed (attempt {attempt + 1}/{retries}): {str(e)}")
                if attempt == retries - 1:
                    return {'status': False, 'deposit_status': 'FAILED'}
                timeout *= 1.5
                continue
        await self._log('error', "All API request attempts failed")
        return {'status': False, 'deposit_status': 'FAILED'}
    async def _parse_api_response(self, response: str) -> Dict[str, Any]:
        try:
            data = await asyncio.to_thread(json.loads, response)
            await self._log('info', f"API Response: {data}")
            status = data.get('STATUS', '')
            deposit_status = 'COMPLETED' if status == 'TXN_SUCCESS' else 'PENDING'

            result = {
                'status': True,
                'deposit_status': deposit_status,
                'raw_response': response
            }

            if deposit_status == 'COMPLETED':
                result.update({
                    'order_id': data.get('ORDERID', ''),
                    'deposit_amount': data.get('TXNAMOUNT', ''),
                    'gateway_name': data.get('GATEWAYNAME', ''),
                    'payment_mode': data.get('PAYMENTMODE', ''),
                    'deposit_date': data.get('TXNDATE', '')
                })
            return result
        except json.JSONDecodeError:
            await self._log('error', "Failed to parse API response")
            return {'status': False, 'deposit_status': 'FAILED', 'raw_response': response}
        except Exception as e:
            await self._log('error', f"Unexpected error parsing API response: {str(e)}")
            return {'status': False, 'deposit_status': 'ERROR', 'raw_response': response}

    async def _handle_deposit_timeout(self, deposit_id: str) -> None:
        async with self.transaction_guard:
            deposit_data = await self.deposit_manager.get_deposit_data(deposit_id)
            if not deposit_data.get('result'):
                return

            deposit_info = deposit_data['result']
            if deposit_info['deposit_status'] != 'PENDING':
                await self._log('debug', f"Skipping timeout - Deposit {deposit_id} is not PENDING")
                return

            api_result = await self.deposit_manager.cancel_deposit(deposit_id, deposit_info['user_id'], 'TIMEOUT')
            if not api_result.get('response', False):
                await self._log('error', f"API cancellation failed for {deposit_id}")
                return

            await asyncio.gather(
                self.deposit_manager.cancel_deposit(deposit_id, deposit_info['user_id'], 'TIMEOUT'),
                self._update_deposit_ui(deposit_info, is_timeout=True)
            )
    async def _update_deposit_ui(self, deposit_info: Dict, is_timeout: bool = False, api_status: Dict = {}) -> None:
        try:
            deposit_id = deposit_info.get('deposit_id', 'unknown')
            if not deposit_id.isdigit():
                deposit_id = ''.join(filter(str.isdigit, deposit_id)) or '0000'

            template = await self._get_message_template(deposit_info, is_timeout)
            deposit_status = api_status.get('deposit_status', 'FAILED')

            qr_image = deposit_info.get('file_id', 'https://i.postimg.cc/hGZ2G2v5/IMG-20240620-025944-733.jpg')
            
            if deposit_status == 'COMPLETED':
                keyboard_for_send = InlineKeyboardMarkup()
                keyboard_for_send.row(InlineKeyboardButton("🛒 Bᴜʏ Sᴇʀᴠɪᴄᴇ Nᴏᴡ",switch_inline_query_current_chat=''))
                text = await self._get_completed_deposit_text(api_status, deposit_info)
                await self.bot.send_message(
                    chat_id=deposit_info['user_id'],
                    text=text,
                    parse_mode='HTML',
                    reply_markup=keyboard_for_send
                )
                keyboard_for_edit = InlineKeyboardMarkup()
                keyboard_for_edit.row(
                    InlineKeyboardButton("⌕ Dᴇᴘᴏsɪᴛ Hɪsᴛᴏʀʏ", switch_inline_query_current_chat='#Hɪsᴛᴏʀʏ-Dᴇᴘᴏsɪᴛ'),
                    InlineKeyboardButton("ⓘ Hᴇʟᴘ & Sᴜᴘᴘᴏʀᴛ", callback_data="USER:HELP")
                )
                await self.bot.edit_message_media(
                    media=InputMediaPhoto(
                        media='https://st2.depositphotos.com/1006899/9688/i/450/depositphotos_96887528-stock-photo-deposit-word-hanging-on-string.jpg',
                        **template,
                        parse_mode='HTML'
                    ),
                    chat_id=deposit_info['user_id'],
                    message_id=deposit_info['message_id'],
                    reply_markup=keyboard_for_edit
                )
                
                # Send deposit notification
                user_data = {'id': deposit_info['user_id'], 'username': deposit_info.get('username', 'N/A')}
            elif is_timeout:
                keyboard_for_edit = InlineKeyboardMarkup()
                keyboard_for_edit.row(
                    InlineKeyboardButton("💰 Dᴇᴘᴏsɪᴛ Aɢᴀɪɴ", callback_data='USER:DEPOSIT'),
                    InlineKeyboardButton("ⓘ Hᴇʟᴘ & Sᴜᴘᴘᴏʀᴛ", callback_data="USER:HELP")
                )
                await self.bot.edit_message_media(
                    media=InputMediaPhoto(
                        media='https://st2.depositphotos.com/1006899/9688/i/450/depositphotos_96887528-stock-photo-deposit-word-hanging-on-string.jpg',
                        **template,
                        parse_mode='HTML'
                    ),
                    chat_id=deposit_info['user_id'],
                    message_id=deposit_info['message_id'],
                    reply_markup=keyboard_for_edit
                ) 
                await self.bot.send_message(
                    chat_id=deposit_info['user_id'],
                    reply_to_message_id=deposit_info['message_id'],
                    text='<blockquote><b>⌛ Tʜɪs Dᴇᴘᴏsɪᴛ Hᴀs Exᴘɪʀᴇᴅ, Aɴᴅ Tʜᴇ Aᴍᴏᴜɴᴛ Hᴀs Nᴏᴛ Bᴇᴇɴ Rᴇᴄᴇɪᴠᴇᴅ Iɴ Yᴏᴜʀ Aᴄᴄᴏᴜɴᴛ!</b></blockquote>',
                    parse_mode='HTML'
                )
            else:
                keyboard = await self._get_cached_keyboard(deposit_info, is_timeout)
                await self.bot.edit_message_media(
                    media=InputMediaPhoto(
                        media=qr_image,
                        **template,
                        parse_mode='HTML'
                    ),
                    chat_id=deposit_info['user_id'],
                    message_id=deposit_info['message_id'],
                    reply_markup=keyboard
                )
        except KeyError as e:
            await self._log('error', f"Key error in UI update: {e}")
        except Exception as e:
            await self._log('error', f"Failed to update deposit UI: {e}")

    async def _get_cached_keyboard(self, deposit_info: Dict, is_timeout: bool) -> InlineKeyboardMarkup:
        try:
            deposit_id = str(deposit_info.get('deposit_id', '0000')).split(':')[-1]
            
            status = deposit_info.get('deposit_status', 'unknown').upper()
            valid_status = status if status in ['PENDING', 'COMPLETED'] else 'unknown'
            keyboard = InlineKeyboardMarkup()
            
            cancel_btn = InlineKeyboardButton("✘ Cᴀɴᴄᴇʟ Dᴇᴘᴏsɪᴛ", switch_inline_query_current_chat='#Hɪsᴛᴏʀʏ-Dᴇᴘᴏsɪᴛ')
            history_btn = InlineKeyboardButton("⌕ Dᴇᴘᴏsɪᴛ Hɪsᴛᴏʀʏ", switch_inline_query_current_chat='#Hɪsᴛᴏʀʏ-Dᴇᴘᴏsɪᴛ')
            help_btn = InlineKeyboardButton("ⓘ Hᴇʟᴘ & Sᴜᴘᴘᴏʀᴛ", callback_data="USER:HELP")
            back = InlineKeyboardButton("« Bᴀᴄᴋ Tᴏ Dᴇᴘᴏsɪᴛ Pᴀɢᴇ", callback_data='USER:DEPOSIT')

            if is_timeout or valid_status in ['PENDING', 'COMPLETED']:
                keyboard.row(cancel_btn, help_btn)
            else:
                keyboard.row(history_btn, help_btn)
            
            '''cache_key = f"{deposit_id}:{is_timeout}"
            
            if cache_key in self._keyboard_cache:
                return self._keyboard_cache[cache_key]

            encoded_id = await encode_base62(int(deposit_id) if deposit_id.isdigit() else abs(hash(deposit_id)) % (10**8))
            self._keyboard_cache[cache_key] = keyboard
            if len(self._keyboard_cache) > 100:
                self._keyboard_cache.pop(next(iter(self._keyboard_cache)))'''
            
            return keyboard
            
        except Exception as e:
            await self._log('error', f"Keyboard generation failed: {str(e)}")
            return InlineKeyboardMarkup().add(
                InlineKeyboardButton("❌ Error - Contact Support", url="t.me/your_support")
            )
    async def _get_message_template(self, deposit_info: Dict, is_timeout: bool) -> Dict:
        base_template = (
            "<b>🔥 Yᴏᴜʀ Fʟᴀsʜ Qʀ-Cᴏᴅᴇ 》</b>\n\n"
            "💰 <b>Mɪɴ Aᴍᴏᴜɴᴛ  »</b>  <code>₹{}</code>  <code>〚</code><code>💎 {}</code><code>〛</code>\n"
            "💳 <b>Dᴇᴘᴏsɪᴛ Iᴅ  »</b>  [ <code>{}</code> ]\n"
        )

        if is_timeout:
            return {'caption': f"{base_template.format(MIN_DEPOSIT, MIN_DEPOSIT, deposit_info.get('deposit_id', ''))}\n<b>⏱️ Dᴇᴘᴏsɪᴛ Hᴀs Exᴘɪʀᴇᴅ [</b><code>Rᴇғᴜɴᴅᴇᴅ</code><b>]</b>"}
        
        if deposit_info.get('deposit_status') == 'COMPLETED':
            return {'caption': f"{base_template.format(MIN_DEPOSIT, MIN_DEPOSIT, deposit_info.get('deposit_id', ''))}\n<b>✅ Dᴇᴘᴏsɪᴛ Hᴀs Cᴏᴍᴘʟᴇᴛᴇᴅ. [</b><code>Cʀᴇᴅɪᴛᴇᴅ</code><b>]</b>"}
        
        timeout = await self._get_timeout(deposit_info)
        remaining = await self._calculate_remaining_time(deposit_info['created_at'], timeout)
        return {
            'caption': (
                f"{base_template.format(MIN_DEPOSIT, MIN_DEPOSIT, deposit_info.get('deposit_id', ''))}"
                f"⏱ <b>Vᴀʟɪᴅ Uɴᴛɪʟ »</b> {deposit_info.get('valid_until', 'N/A')} <b>[</b>{remaining}<b>]</b>\n\n"
                "📌 <b>Sᴄᴀɴ Tʜɪs Qʀ Aɴᴅ Pᴀʏ Fʀᴏᴍ Aɴʏ Pᴀʏᴍᴇɴᴛ Aᴘᴘ.</b>"
            )
        }

    async def _get_completed_deposit_text(self, api_status: Dict, deposit_info: Dict) -> str:
        return (
            "<b>#Uᴘɪ_Cᴀʀᴅ_Dᴇᴘᴏsɪᴛ ❯</b>\n\n"
            "<b>Tʀᴀɴsᴀᴄᴛɪᴏɴ Dᴇᴛᴀɪʟs</b>\n"
            f"<b>💰 Aᴍᴏᴜɴᴛ Cʀᴇᴅɪᴛᴇᴅ »</b> <code>{api_status.get('deposit_amount', '0')}</code> 💎\n"
            f"<b>💳 Dᴇᴘᴏsɪᴛ Iᴅ »</b> <code>{deposit_info.get('deposit_id', 'N/A')}</code>\n"
            f"<b>👤 Pᴀɪᴅ Fʀᴏᴍ »</b> <code>{api_status.get('gateway_name', 'N/A')}</code>\n"
            f"<b>🕊 Pᴀʏᴍᴇɴᴛ Tʏᴘᴇ »</b> <code>{api_status.get('payment_mode', 'N/A')}</code>\n\n"
            "<b>🏛 Bᴀʟᴀɴᴄᴇ Uᴘᴅᴀᴛᴇ 》</b>\n"
            f"<i>Sᴜᴄᴄᴇssғᴜʟʟʏ Cʀᴇᴅɪᴛᴇᴅ</i> <code>{api_status.get('deposit_amount', '0')}</code> 💎\n"
            "<i>Tᴏ Yᴏᴜʀ Aᴄᴄᴏᴜɴᴛ.</i>"
        )
    
    async def _calculate_remaining_time(self, created_at: str, timeout: int) -> str:
        try:
            timeout = int(timeout)
            created_at_dt = datetime.fromisoformat(created_at).replace(tzinfo=None)
            elapsed = (datetime.utcnow() - created_at_dt).total_seconds()
            remaining = max(0, timeout * 60 - elapsed)
            mins, secs = divmod(int(remaining), 60)
            return f"<code>{mins:02}</code>:<code>{secs:02}</code>" if self.update_interval < 60 else f"<code>{mins:02}</code> <code>Mɪɴ</code>"
        except Exception:
            return "<code>⩇⩇</code><code>:</code><code>⩇⩇</code>"

    async def _batch_update_countdowns(self, batch_size: int) -> None:
        """
        Update UI countdowns for active deposits with optimized batch processing.
        """
        try:
            offset = 0
            processed = 0
            start_time = time.time()
            
            while True:
                deposits = await self._fetch_deposits_batch(batch_size, offset)
                if not deposits:
                    break

                sorted_deposits = sorted(
                    deposits,
                    key=lambda d: datetime.fromisoformat(d['created_at']).timestamp(),
                    reverse=True
                )

                update_tasks = []
                for deposit in sorted_deposits:
                    if deposit.get('deposit_status') == 'PENDING':
                        update_tasks.append(self._update_deposit_ui(deposit))
                        processed += 1
                        
                        if len(update_tasks) >= 50:
                            results = await asyncio.gather(*update_tasks, return_exceptions=True)
                            for result in results:
                                if isinstance(result, Exception):
                                    await self._log('error', f"Task error: {result}")
                            update_tasks = []
                            await asyncio.sleep(0.1)

                if update_tasks:
                    results = await asyncio.gather(*update_tasks, return_exceptions=True)
                    for result in results:
                        if isinstance(result, Exception):
                            await self._log('error', f"Task error: {result}")

                if (time.time() - start_time) > self.update_interval * 0.8:
                    await self._log('warning', "Batch update approaching time limit, breaking early")
                    break
                    
                if len(deposits) < batch_size:
                    break
                offset += batch_size

            await self._log('info', f"Updated {processed} deposit UIs in {time.time()-start_time:.2f}s")
            
        except Exception as e:
            await self._log('error', f"Countdown update failed: {str(e)}")
            raise
    async def _adjust_processing_parameters(self) -> None:
        """
        Dynamically adjust batch size based on system load, asynchronously.
        """
        if len(self._load_window) < 5:
            await self._log('debug', "Not enough data to adjust processing parameters")
            return

        avg_process_time = sum(self._load_window) / len(self._load_window)
        target_time = self.check_interval * 0.7

        if avg_process_time > target_time:
            new_size = max(50, int(self._adaptive_batch_size * 0.8))
            await self._log('info', f"Decreasing batch size due to high load. New size: {new_size}")
        else:
            new_size = min(500, int(self._adaptive_batch_size * 1.2))
            await self._log('info', f"Increasing batch size due to low load. New size: {new_size}")

        self._adaptive_batch_size = new_size
        await self._log('debug', f"Adjusted batch size to {new_size} based on avg {avg_process_time:.2f}s")

    async def _trip_circuit(self) -> None:
        """
        Activate circuit breaker when error threshold is reached.
        """
        self._circuit_state = "open"
        await self._log('critical', "Circuit breaker tripped! Stopping processing")
        asyncio.create_task(self._reset_circuit())
    async def _reset_circuit(self) -> None:
        """
        Reset circuit breaker after cooldown period.
        """
        await asyncio.sleep(60)
        self._circuit_state = "half-open"
        await self._log('info', "Circuit in half-open state, testing...")
        
        # Test with small batch
        try:
            await self._process_deposits_batch(10)
            self._circuit_state = "closed"
            self._circuit_errors = 0
            await self._log('info', "Circuit reset to closed state")
        except Exception as e:
            await self._log('error', f"Circuit test failed: {str(e)}")
            await self._trip_circuit()

    async def _execute_with_overspill_protection(self, coroutine) -> None:
        """
        Execute task with timeout and automatic load adjustment.
        """
        try:
            start_time = time.monotonic()
            await asyncio.wait_for(coroutine, timeout=self.update_interval*0.9)
            process_time = time.monotonic() - start_time
            self._load_window.append(process_time)
            await self._log('debug', f"Task completed in {process_time:.2f}s")
        except asyncio.TimeoutError:
            await self._log('warning', "Task timeout, reducing batch size")
            self._adaptive_batch_size = max(50, int(self._adaptive_batch_size * 0.7))
            await self._log('info', f"Adjusted batch size to {self._adaptive_batch_size}")
        except Exception as e:
            await self._log('error', f"Protected task failed: {str(e)}")
            await self._log('debug', f"Error details: {traceback.format_exc()}")

    async def _process_single_deposit(self, deposit: Dict, is_expired: bool) -> None:
        """
        Process individual deposit with enhanced error handling and logging.
        """
        deposit_id = deposit.get('deposit_id', '').split(':')[-1]
        if not deposit_id:
            await self._log('warning', f"Invalid deposit ID: {deposit.get('deposit_id', 'Unknown')}")
            return

        try:
            async with self._semaphore:
                await self._log('info', f"Processing deposit {deposit_id}, expired: {is_expired}")
                
                if is_expired:
                    deposit_data = await self.deposit_manager.get_deposit_data(deposit_id)
                    if not deposit_data.get('result'):
                        await self._log('warning', f"No data found for expired deposit {deposit_id}")
                        return

                    current_status = deposit_data['result'].get('deposit_status', 'PENDING')
                    if current_status == 'PENDING':
                        await self._log('info', f"Handling timeout for expired deposit {deposit_id}")
                        await self._handle_deposit_timeout(deposit_id)
                    elif current_status == 'COMPLETED':
                        await self._log('info', f"Completing expired but completed deposit {deposit_id}")
                        await self._complete_deposit(deposit_id, deposit_data['result'], 'COMPLETED')
                    return

                # Active deposit processing
                await self._log('debug', f"Checking API status for deposit {deposit_id}")
                api_status = await self._check_api_status(deposit)
                if not api_status['status']:
                    await self._log('warning', f"API check failed for deposit {deposit_id}")
                    await self.deposit_manager.update_deposit_status(deposit_id, 'FAILED')
                    return

                status = api_status['deposit_status']
                await self._log('info', f"Deposit {deposit_id} status: {status}")
                
                if status == 'COMPLETED':
                    await self._log('info', f"Completing deposit {deposit_id}")
                    await self._complete_deposit(deposit_id, api_status, 'API_COMPLETION')
                elif status in {'FAILED', 'CANCELLED'}:
                    await self._log('info', f"Handling timeout for failed/cancelled deposit {deposit_id}")
                    await self._handle_deposit_timeout(deposit_id)

        except Exception as e:
            await self._log('error', f"Deposit {deposit_id} processing failed: {str(e)}")
            self._circuit_errors += 1
            if self._circuit_errors >= 3:
                await self._log('critical', "Circuit breaker threshold reached")
                await self._trip_circuit()

# Global instance of DepositTrackerManagement
deposit_tracker = DepositTrackerManagement()

async def init_managers(deposit_manager: DepositManagement, user_manager: UserManagement, bot: AsyncTeleBot) -> bool:
    try:
        if not isinstance(deposit_manager, DepositManagement):
            await deposit_tracker._log('error', "Invalid deposit manager type")
            return False
        
        if not hasattr(deposit_tracker, 'init_managers') or not callable(deposit_tracker.init_managers):
            await deposit_tracker._log('error', "Deposit tracker does not have a valid init_managers method")
            return False

        success = await deposit_tracker.init_managers(deposit_manager, bot)
        if success:
            await deposit_tracker._log('info', "Deposit tracker initialized successfully")
            return True
        else:
            await deposit_tracker._log('error', "Failed to initialize deposit tracker")
            return False
    except Exception as e:
        await deposit_tracker._log('error', f"Unexpected error initializing deposit tracker: {e}")
        return False

async def register_handlers(bot: AsyncTeleBot) -> None:
    try:
        if not isinstance(bot, AsyncTeleBot):
            await deposit_tracker._log('error', "Invalid bot instance")
            return
        
        deposit_tracker.bot = bot
        deposit_tracker.transaction_guard = getattr(bot, 'transaction_guard', None)
        async with aiohttp.ClientSession() as session:
            # Perform any necessary operations with the session
            await deposit_tracker.start()
        await deposit_tracker._log('info', "Deposit tracker initialized successfully")
    except Exception as e:
        await deposit_tracker._log('error', f"Failed to register deposit tracker handlers: {e}")

__all__ = ['init_managers', 'register_handlers', 'deposit_tracker']
