from typing import Dict, Optional, Any, List
import asyncio
#import await logging
import uuid
import json
import time
import hashlib
from datetime import datetime, timedelta

from telebot.async_telebot import AsyncTeleBot
from telebot.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputMediaPhoto,
    CallbackQuery,
    Message,
    InputMediaVideo
)

# Local imports – ensure these modules are available in your project.
from utils.redis_keys import RedisKeys
from utils.functions import AfterMin, format_currency, qr_code
from handlers.manager.operation import UserManagement, OrderManagement, DepositManagement
from handlers.security import RateLimiter, TransactionGuard
from utils.config import DEPOSIT_TIMEOUT, INR_RATE, PAYMENT_GATEWAY_API, PAYMENT_GATEWAY_API_KEY
from utils.redis_manager import redis_manager, RedisManager
from utils.cache_manager import cache_manager, CachePrefix
from utils.config import LOADING_GIF, MIN_DEPOSIT
from redis.asyncio.client import Redis
#logger = logging.getLogger(__name__)

class ShowDepositManager:
    """
    Real-time deposit tracking system with exponential backoff and circuit breaking.
    This class handles deposit record management (using Redis) and the QR deposit flow.
    """
    __slots__ = (
        'bot',
        'check_interval',
        'deposit_manager',       # retained for backward compatibility if needed
        'user_manager',
        'input_validator',
        'transaction_guard',
        'rate_limiter',
        'redis_client',
        '_initialized'
    )

    def __init__(self, check_interval: int = 30):
        self.check_interval = check_interval
        self.bot: Optional[AsyncTeleBot] = None
        self.deposit_manager: Optional[DepositManagement] = None  # no longer used for deposit ops
        self.user_manager: Optional[UserManagement] = None
        self.input_validator: Optional[Any] = None
        self.transaction_guard: Optional[TransactionGuard] = None
        self.rate_limiter: Optional[RateLimiter] = None
        self.redis_client: Optional[RedisManager] = None
        self._initialized = False

    async def init_managers(self, deposit_mgr: DepositManagement, user_mgr: UserManagement, bot: AsyncTeleBot) -> bool:
        """
        Initialize required components for deposit handling asynchronously.
        """
        try:
            if not all([deposit_mgr, user_mgr, bot]):
                #await logger.error("Missing required components for initialization")
                return False

            self.deposit_manager = deposit_mgr  # retained for compatibility if needed
            self.user_manager = user_mgr
            self.bot = bot

            # Retrieve additional attributes from the bot, if they exist.
            self.input_validator = getattr(bot, "input_validator", None)
            self.transaction_guard = getattr(bot, "transaction_guard", None)

            self.redis_client = await redis_manager.get_client()
            if not self.redis_client:
                raise ConnectionError("Failed to establish Redis connection")
            self.rate_limiter = RateLimiter(
                redis_client=self.redis_client,
                duration=60,
                max_requests=10
            )
            
            self._initialized = True
            #await logger.info("Deposit managers initialized successfully")
            return True
        except Exception as e:
            #await logger.error(f"Initialization error: {e}")
            return False

    async def handle_qr_deposit(self, call: CallbackQuery):
        """
        Handle the QR code deposit flow by editing the current message with deposit options.
        """
        try:
            keyboard = InlineKeyboardMarkup()
            keyboard.row(
                InlineKeyboardButton("🪙 Tʀx", callback_data="/Trx"),
                InlineKeyboardButton("🏆 Rᴇᴅᴇᴇᴍ", callback_data="/Redeem"),
                InlineKeyboardButton("💰 Iɴʀ", callback_data="USER:DEPOSIT:QR")
            )
            keyboard.row(
                InlineKeyboardButton("🔙 Bᴀᴄᴋ Tᴏ Hᴏᴍᴇ Pᴀɢᴇ", callback_data='start')
            )

            caption = (
                "<b>🔥 Fʟᴀsʜ Dᴇᴘᴏsɪᴛ Pᴀɢᴇ 》</b>\n"
                "<b>Hᴇʀᴇ Yᴏᴜ Cᴀɴ Aᴅᴅ Fᴜɴᴅs Tᴏ Yᴏᴜʀ Wᴀʟʟᴇᴛ!</b>\n\n"
                "<code>❒</code> <code>1</code> <b>Iɴʀ</b>   <b>»</b> <code>1</code> 💎 <b>||</b> "
                "<code>1</code> Tʀx  <b>»</b> <code>25</code> 💎\n\n"
                "➕ <b>Sᴇʟᴇᴄᴛ Dᴇᴘᴏsɪᴛ Mᴇᴛʜᴏᴅ, Aʟʟ Dᴇᴘᴏsɪᴛ Aᴍᴏᴜɴᴛ Wɪʟʟ Bᴇ Cᴏɴᴠᴇʀᴛᴇᴅ Tᴏ Pᴏɪɴᴛ</b>"
                "<code>(💎)</code>"
            )

            media = InputMediaPhoto(
                media='https://i.postimg.cc/hGZ2G2v5/IMG-20240620-025944-733.jpg',
                caption=caption,
                parse_mode='HTML'
            )

            await asyncio.gather(
                self.bot.edit_message_media(
                    media=media,
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    reply_markup=keyboard
                ),
                #await logg_info(f"QR deposit handled successfully for chat_id: {call.message.chat.id}")
            )
        except Exception as e:
            await asyncio.gather(
                #await logg_error(f"QR deposit handler error: {e}"),
                self.bot.answer_callback_query(
                    call.id, "🚫 Failed to process QR deposit", show_alert=True
                )
            )

    async def _build_deposit_data(self, data: Dict, deposit_id: str) -> Dict:
        """
        Build the deposit data structure from the provided input asynchronously.
        """
        utc_now = str(datetime.utcnow())

        return {
            "deposit_id": str(deposit_id),
            "message_id": str(data['message_id'].message_id) if hasattr(data['message_id'], 'message_id') else str(data['message_id']),
            "user_id": str(data['user_id']),
            "server_id": str(data['server_id']),
            "valid_until": data['valid_until'],
            "file_id": str(data['file_id']),
            "deposit_status": "PENDING",
            "deposit_history": json.dumps([{
                "timestamp": str(time.time()),
                "action": "DEPOSIT_CREATED"
            }]),
            "created_at": utc_now,
            "recorded_at": time.time(),
            "amount": data.get('amount', 0),
            "currency": data.get('currency', 'INR'),
            "payment_method": data.get('payment_method', 'QR')
        }

    async def _create_deposit_record(self, data: Dict, deposit_id: str) -> str:
        """
        Create a deposit record in the database asynchronously.
        """
        
        deposit_data = await self._build_deposit_data(data, deposit_id)
        response = await self.deposit_manager.add_deposit_data(deposit_id, data['user_id'], deposit_data)
        
        if not response.get('response'):
            raise Exception("⚠️ DEPOSIT DATA STORAGE FAILED")

        return response['result']

    async def start_deposit(self, call: CallbackQuery) -> None:
        """Initiate the deposit process by creating a deposit record and displaying the QR code for payment."""
        try:
            user_id = str(call.from_user.id)
            keyboard = InlineKeyboardMarkup()
            keyboard.row(
                InlineKeyboardButton("✘ Cᴀɴᴄᴇʟ Dᴇᴘᴏsɪᴛ", switch_inline_query_current_chat='#HɪsᴛᴏʀʏDᴇᴘᴏsɪᴛ'),
                InlineKeyboardButton("ⓘ Hᴇʟᴘ & Sᴜᴘᴘᴏʀᴛ", callback_data="USER:HELP")
            )

            caption = (
                "<b>🔥 Yᴏᴜʀ Fʟᴀsʜ Qʀ-Cᴏᴅᴇ 》</b>\n\n"
                "💰 <b>Mɪɴ Aᴍᴏᴜɴᴛ  »</b>  <code>₹{}</code>  <code>〚</code><code>💎 {}</code><code>〛</code>\n"
                "💳 <b>Dᴇᴘᴏsɪᴛ Iᴅ  »</b>  [ <code>{}</code> ]\n"
                "⏳ <b>Pᴀʏ Uɴᴅᴇʀ  »</b>  {} <b>[</b><code>{}</code> <code>Mɪɴ</code><b>]</b>\n\n"
                "📌 <b>Sᴄᴀɴ Tʜɪs Qʀ Aɴᴅ Pᴀʏ Fʀᴏᴍ Aɴʏ Pᴀʏᴍᴇɴᴛ Aᴘᴘ.</b>"
            )

            loading_msg = await self.bot.edit_message_media(
                media=InputMediaVideo(
                    media=LOADING_GIF, 
                    caption=caption.format('⩇⩇', '⩇⩇', '⩇⩇⩇⩇⩇⩇⩇⩇⩇⩇⩇⩇', '⩇⩇:⩇⩇ Pᴍ', '⩇⩇'), 
                    parse_mode="HTML"
                ),
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=keyboard
            )

            server_id = 1
            valid_until = await AfterMin(int(DEPOSIT_TIMEOUT))

            async with TransactionGuard(self.redis_client):
                deposit_id_resp = await self.deposit_manager.create_deposit_id(user_id=user_id)
                if not isinstance(deposit_id_resp, dict) or not deposit_id_resp.get('response'):
                    raise Exception("Failed to create deposit ID")

                deposit_id = deposit_id_resp['result']
                qr_image = await qr_code(deposit_id=deposit_id, size=380, position=(1470, 550), radius=20)

                msg = await self.bot.edit_message_media(
                    media=InputMediaPhoto(
                        media=qr_image, 
                        caption=caption.format(MIN_DEPOSIT, MIN_DEPOSIT, deposit_id, valid_until, int(DEPOSIT_TIMEOUT)), 
                        parse_mode="HTML"
                    ),
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    reply_markup=keyboard
                )

                deposit_data = {
                    "deposit_id": deposit_id,
                    "user_id": user_id,
                    "message_id": msg.message_id,
                    "server_id": server_id,
                    "valid_until": valid_until,
                    "file_id": msg.photo[-1].file_id 
                }

                await self._create_deposit_record(deposit_data, deposit_id)

                asyncio.create_task(self._delayed_message_edit(deposit_data, keyboard, caption, MIN_DEPOSIT, deposit_id, valid_until, int(DEPOSIT_TIMEOUT)-1))

        except Exception as e:
            print(f"🚫 Failed to start deposit: {str(e)}")

    async def _delayed_message_edit(self, deposit_data, keyboard, caption, MIN_DEPOSIT, deposit_id, valid_until, DEPOSIT_TIMEOUT):
        await asyncio.sleep(1)
        try:
            updated_caption = caption.format(MIN_DEPOSIT, MIN_DEPOSIT, deposit_id, valid_until, f"{int(DEPOSIT_TIMEOUT):02d}")
            
            await self.bot.edit_message_caption(
                chat_id=deposit_data['user_id'],
                message_id=deposit_data['message_id'],
                caption=updated_caption,
                parse_mode="HTML",
                reply_markup=keyboard
            )
        except Exception as e:
            print(e)
            pass
# Create a global instance of DepositManager.
deposit_manager = ShowDepositManager()


async def init_managers(order_manager: OrderManagement, user_manager: UserManagement, bot: AsyncTeleBot) -> bool:
    """
    Initialize the deposit management system asynchronously.
    """
    return await deposit_manager.init_managers(bot.deposit_manager, user_manager, bot)


async def register_handlers(bot: AsyncTeleBot) -> None:
    """
    Register deposit-related bot handlers asynchronously.
    """

    @bot.callback_query_handler(func=lambda call: call.data.startswith("USER:DEPOSIT"))
    async def handle_deposit_callback(call: CallbackQuery):
        try:
            if call.data == "USER:DEPOSIT":
                await deposit_manager.handle_qr_deposit(call)
            elif call.data == "USER:DEPOSIT:QR":
                await deposit_manager.start_deposit(call)
            elif call.data == "USER:DEPOSIT:CHECK":
                await bot.answer_callback_query(
                    call.id, "Payment check not implemented yet", show_alert=True
                )
            else:
                #await logger.warning("Unhandled deposit action: %s", call.data)
                await bot.answer_callback_query(
                    call.id, "🚫 Unhandled deposit action", show_alert=True
                )
        except ValueError as ve:
            #await logger.error("ValueError in deposit callback: %s", ve)
            await bot.answer_callback_query(
                call.id, "🚫 Invalid request format", show_alert=True
            )
        except Exception as e:
            #await logger.error(f"Deposit callback error: {e}")
            await bot.answer_callback_query(
                call.id, "🚫 System error occurred", show_alert=True
            )

    @bot.callback_query_handler(func=lambda call: call.data.startswith("USER:HELP"))
    async def handle_help_callback(call: CallbackQuery):
        try:
            help_text = (
                "<b>Deposit Help & Support</b>\n\n"
                "1. To make a deposit, select your deposit method.\n"
                "2. Follow the instructions provided to complete the payment.\n"
                "3. If you encounter issues, contact support."
            )
            await bot.answer_callback_query(call.id)
            await bot.send_message(call.message.chat.id, help_text, parse_mode='HTML')
        except Exception as e:
            #await logger.error(f"Help callback error: {e}")
            await bot.answer_callback_query(call.id, "🚫 Failed to display help", show_alert=True)


__all__ = ['init_managers', 'register_handlers']











'''
            keyboard = InlineKeyboardMarkup()
            keyboard.row(InlineKeyboardButton("🛒 Oʀᴅᴇʀ", switch_inline_query_current_chat='#HɪsᴛᴏʀʏOʀᴅᴇʀ'),InlineKeyboardButton("🔍 Aʟʟ Hɪsᴛᴏʀʏ",switch_inline_query_current_chat='#HɪsᴛᴏʀʏAʟʟ'),InlineKeyboardButton("💰 Dᴇᴘᴏsɪᴛ",switch_inline_query_current_chat='#HɪsᴛᴏʀʏDᴇᴘᴏsɪᴛ'))
            keyboard.row(InlineKeyboardButton("🔙 Bᴀᴄᴋ Tᴏ Pʀᴏғɪʟᴇ Pᴀɢᴇ [ Usᴇʀ-Pʀᴏғɪʟᴇ ] ", callback_data='USER:PROFILE'))
            caption = (
                "🔥 <b>Fʟᴀsʜ Tʀᴀɴsᴀᴄᴛɪᴏɴ Hɪsᴛᴏʀʏ 》</b>\n\n"
                "🔍 <b>Hᴇʀᴇ Yᴏᴜ Cᴀɴ Vɪᴇᴡ Aʟʟ Yᴏᴜʀ Pᴀsᴛ Tʀᴀɴsᴀᴄᴛɪᴏɴs.</b>\n\n"
                "<b>📅 Tʜɪs Wᴇᴇᴋ ❯</b>\n"
                f"💰 <b>Pᴜʀᴄʜᴀsᴇs  »</b>  <code>{number}</code> <code>Nᴜᴍʙᴇʀ{'s' if number > 1 else ''}</code>\n"
                f"📊 <b>Sᴘᴇɴᴅ  »</b>  <code>{amount:.2f}</code> 💎  〚$ <code>0.00</code>〛\n"
                f"📈 <b>Dᴇᴘᴏsɪᴛs  »</b>  <code>{deposit:.2f}</code> 💎  〚$ <code>0.00</code>〛\n\n"
                "🏛️ <b>Yᴏᴜ Cᴀɴ Sᴇᴀʀᴄʜ Yᴏᴜʀ Tʀᴀɴsᴀᴄᴛɪᴏɴs Bʏ Dᴀᴛᴇ Aɴᴅ Tʏᴘᴇ. Tʜɪs Wɪʟʟ Hᴇʟᴘ Yᴏᴜ Eᴀsɪʟʏ Aɴᴀʟʏᴢᴇ Yᴏᴜʀ Fᴜᴛᴜʀᴇ Fɪɴᴀɴᴄᴇs..</b>"
            )

            await self.bot.edit_message_media(
                media=InputMediaPhoto(
                    media='https://i.postimg.cc/HLWC80bf/20240628-092309.jpg',
                    caption=caption,
                    parse_mode='HTML'
                ),
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=keyboard
            )
'''