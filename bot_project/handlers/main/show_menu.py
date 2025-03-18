from telebot.async_telebot import AsyncTeleBot
from utils.redis_manager import redis_manager
from handlers.manager.operation import FinancialManagement, UserManagement, FinancialSummaryAggregator, get_async_logger
from telebot.types import InputMediaPhoto, Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from utils.config import START_PAGE
from utils.functions import create_keyboard, serialize_data
from handlers.security import InputValidator, TransactionGuard, RateLimiter
import time
import json
from datetime import datetime
import re
import html
import logging
from typing import Optional, Dict, Any, Tuple, List
import asyncio

class UserStartManager:
    """Manager class for handling Telegram bot start commands."""
    
    def __init__(self):
        self.user_manager: Optional[UserManagement] = None
        self.rate_limiter: Optional[RateLimiter] = None
        self.input_validator: Optional[InputValidator] = None
        self.transaction_guard: Optional[TransactionGuard] = None
        self.bot: Optional[AsyncTeleBot] = None
        self.aggregator: Optional[FinancialManagement] = None
        self._initialized = False
        self.DEFAULT_VALUES = {
            'currency_code': 'Iɴʀ [₹]',
            'user_status': 'active',
            'forum_id': None,
            'is_premium': False,
            'referral_code': None,
            'referred_by': None,
            'total_referrals': 0
        }

    async def init_managers(self, user_mgr: UserManagement, bot: Optional[AsyncTeleBot] = None) -> bool:
        async_logger = await get_async_logger()
        try:
            if not user_mgr or not bot:
                await async_logger.error("User manager and bot instance are required")
                return False

            self.user_manager = user_mgr
            self.bot = bot
            self.input_validator = getattr(bot, 'input_validator', None)
            self.transaction_guard = getattr(bot, 'transaction_guard', None)
            self.aggregator = getattr(bot, 'aggregator', None)

            if not all([self.user_manager, self.input_validator, self.transaction_guard, self.aggregator]):
                missing = [name for name, comp in [
                    ('user_manager', self.user_manager),
                    ('input_validator', self.input_validator),
                    ('transaction_guard', self.transaction_guard),
                    ('aggregator', self.aggregator)
                ] if not comp]
                await async_logger.error(f"Missing required components: {', '.join(missing)}")
                return False

            self._initialized = True
            await async_logger.info("Handler Managers Initialized Successfully!")
            return True

        except Exception as e:
            await async_logger.error(f"Error initializing managers: {e}")
            return False

    async def _create_welcome_keyboard(self) -> InlineKeyboardMarkup:
        keyboard = InlineKeyboardMarkup()
        keyboard.row(
            InlineKeyboardButton("🛒 Sᴇʀᴠɪᴄᴇs", switch_inline_query_current_chat=""),
            InlineKeyboardButton("🔥 Tᴏᴘ Sᴇʀᴠɪᴄᴇs", callback_data="USER:TOPSERVICE")
        )
        keyboard.row(
            InlineKeyboardButton("👨‍💻 Wᴀʟʟᴇᴛ", callback_data="USER:WALLET"),
            InlineKeyboardButton("💰 Rᴇᴄʜᴀʀɢᴇ", callback_data="USER:DEPOSIT")
        )
        keyboard.row(
            InlineKeyboardButton("🔗 Rᴇғғᴇʀᴀʟ", callback_data="USER:REFFERAL"),
            InlineKeyboardButton("📑 Hɪsᴛᴏʀʏ", callback_data="USER:HISTORY")
        )
        keyboard.row(
            InlineKeyboardButton("⁉️ Hᴇʟᴘ", callback_data="USER:SUPPORT"),
            InlineKeyboardButton("⚙️ Sᴇᴛᴛɪɴɢs", callback_data="USER:SETTINGS:CURRENCY")
        )
        
        return keyboard

    async def _create_welcome_caption(self, first_name: str, current_balance: float, total_orders: int) -> str:
        return (
            f"<b>Hᴇʟʟᴏ</b> {first_name} <b>!</b>\n\n"
            f"<b>💰 Yᴏᴜʀ Bᴀʟᴀɴᴄᴇ :</b> <code>{current_balance:.2f}</code> 💎\n"
            f"<b>📊 Tᴏᴛᴀʟ Nᴜᴍʙᴇʀ Pᴜʀᴄʜᴀsᴇᴅ :</b> <code>{total_orders}</code>\n\n"
            "<b>📌 Rᴀɴᴋ Hᴇʟᴘs Tᴏ Iɴᴄʀᴇᴀsᴇ Dɪsᴄᴏᴜɴᴛ Oɴ Sᴇʀᴠɪᴄᴇs...</b>"
        )

    async def _send_welcome_message(self, bot: AsyncTeleBot, chat_id: int, caption: str,
                                    keyboard: InlineKeyboardMarkup, message_id: Optional[int] = None,
                                    request_type: str = "start") -> bool:
        async_logger = await get_async_logger()
        try:
            if request_type == "start":
                await bot.send_photo(
                    chat_id=chat_id,
                    photo=START_PAGE,
                    caption=caption,
                    parse_mode="HTML",
                    reply_markup=keyboard
                )
            elif request_type == "edit" and message_id:
                try:
                    await bot.edit_message_caption(
                        chat_id=chat_id,
                        message_id=message_id,
                        caption=caption,
                        parse_mode="HTML",
                        reply_markup=keyboard
                    )
                except Exception:
                    await bot.edit_message_media(
                        media=InputMediaPhoto(
                            media=START_PAGE,
                            caption=caption,
                            parse_mode="HTML"
                        ),
                        chat_id=chat_id,
                        message_id=message_id,
                        reply_markup=keyboard
                    )
            else:
                return False
            return True
        except Exception as e:
            await async_logger.error(f"Failed to send/edit message: {e}")
            return False

    async def _create_new_user_data(self, message: Message, first_name: str, username: str) -> Dict[str, Any]:
        return {
            "first_name": first_name,
            "username": username,
            "user_id": str(message.from_user.id),
            "language_code": message.from_user.language_code,
            **self.DEFAULT_VALUES,
            "created_at": datetime.utcnow().isoformat(),
            "last_updated": time.time()
        }

    async def handle_start_display(self, bot: AsyncTeleBot, message: Message,
                                   user_data: Dict[str, Any], request_type: str = "start") -> None:
        async_logger = await get_async_logger()
        try:
            chat_id = message.chat.id
            if not message or not hasattr(message, 'chat') or not self.input_validator.validate_user_id(chat_id):
                return

            first_name = user_data.get("user_profile")
            message_id = getattr(message, "message_id", None)
            current_balance = user_data.get("metrics", {}).get("current_balance", 0)
            total_orders = user_data.get("metrics", {}).get("orders", {}).get("count", 0)
            caption = await self._create_welcome_caption(first_name, current_balance, total_orders)
            keyboard = await self._create_welcome_keyboard()

            if not await self._send_welcome_message(bot, chat_id, caption, keyboard, message_id, request_type):
                await bot.send_message(
                    chat_id=chat_id,
                    text="Failed to process your request. Please try again later.",
                    parse_mode="HTML"
                )

        except Exception as e:
            await async_logger.error(f"Error in handle_start_display: {e}")

    async def handle_start_command(self, message: Message) -> None:
        bot = self.bot
        if not self._initialized or not message.from_user:
            await bot.reply_to(message, "Service unavailable. Please try again later.")
            return

        user_id = str(message.from_user.id)
        async_logger = await get_async_logger()

        try:
            first_name = self.input_validator.sanitize_text(message.from_user.first_name)
            username = self.input_validator.sanitize_text(message.from_user.username or "N/A")

            data = await self.aggregator.get_user(user_id)

            if not data["response"]:
                user_data = await self._create_new_user_data(message, first_name, username)
                validation_result = self.input_validator.validate_user_data(user_data)
                
                if not validation_result['valid']:
                    await async_logger.error(f"Invalid user data: {validation_result.get('error')}")
                    await bot.reply_to(message, "Failed to create account. Please try again later.")
                    return

                result = await self.user_manager.update_user_data(user_id, validation_result['data'])
                if not result["response"]:
                    await async_logger.error(f"Failed to create user: {result.get('error')}")
                    await bot.reply_to(message, "Failed to create account. Please try again later.")
                    return

                await bot.reply_to(message, f"Welcome, {first_name}! Your account has been created.")

            else:
                user_data = {
                    'first_name': str(first_name)[:50],
                    'username': str(username)[:50],
                    'last_updated': time.time()
                }
           
                update_result = await self.user_manager.update_user_data(user_id, user_data)
                if not update_result["response"]:
                    await async_logger.error(f"Failed to update user: {update_result.get('error')}")
                    await bot.reply_to(message, "Error updating your profile. Please try again later.")
                    return

            await self.handle_start_display(bot, message, data, "start")

        except Exception as e:
            await async_logger.error(f"Error in handle_start_command: {e}")
            await bot.reply_to(message, "An error occurred. Please try again later.")

    async def handle_start_callback(self, call: CallbackQuery) -> None:
        if not self._initialized or not call.from_user:
            await self.bot.answer_callback_query(call.id, "Service unavailable. Please try again later.")
            return

        user_id = str(call.from_user.id)
        async_logger = await get_async_logger()

        try:
            first_name = self.input_validator.sanitize_text(call.from_user.first_name)
            username = self.input_validator.sanitize_text(call.from_user.username or "N/A")

            data = await self.aggregator.get_user(user_id)

            if not data["response"]:
                user_data = await self._create_new_user_data(call.message, first_name, username)
                validation_result = self.input_validator.validate_user_data(user_data)
                
                if not validation_result['valid']:
                    await async_logger.error(f"Invalid user data: {validation_result.get('error')}")
                    await self.bot.answer_callback_query(call.id, "Failed to create account. Please try again later.")
                    return

                result = await self.user_manager.update_user_data(user_id, validation_result['data'])
                if not result["response"]:
                    await async_logger.error(f"Failed to create user: {result.get('error')}")
                    await self.bot.answer_callback_query(call.id, "Failed to create account. Please try again later.")
                    return

                await self.bot.answer_callback_query(call.id, f"Welcome, {first_name}! Your account has been created.")

            else:
                user_data = {
                    'first_name': str(first_name)[:50],
                    'username': str(username)[:50],
                    'last_updated': time.time()
                }
           
                update_result = await self.user_manager.update_user_data(user_id, user_data)
                if not update_result["response"]:
                    await async_logger.error(f"Failed to update user: {update_result.get('error')}")
                    await self.bot.answer_callback_query(call.id, "Error updating your profile. Please try again later.")
                    return

            await self.handle_start_display(self.bot, call.message, data, "edit")
            await self.bot.answer_callback_query(call.id)

        except Exception as e:
            await async_logger.error(f"Error in handle_start_callback: {e}")
            await self.bot.answer_callback_query(call.id, "An error occurred. Please try again later.")

    async def register_handlers(self, bot: AsyncTeleBot) -> None:
        async_logger = await get_async_logger()
        if not self._initialized:
            await async_logger.error("Cannot register handlers: manager not initialized")
            return

        try:
            bot.register_message_handler(
                self.handle_start_command,
                commands=['start'],
                pass_bot=False
            )
            
            bot.register_callback_query_handler(
                self.handle_start_callback,
                func=lambda call: call.data == "start",
                pass_bot=False
            )
            
            await async_logger.info("Start command and callback handlers registered successfully")
        except Exception as e:
            await async_logger.error(f"Failed to register start handlers: {e}")
            raise

start_manager = UserStartManager()

async def init_managers(user_manager: UserManagement, order_manager=None, bot: Optional[AsyncTeleBot] = None) -> bool:
    return await start_manager.init_managers(user_manager, bot)

async def register_handlers(bot: AsyncTeleBot) -> None:
    await start_manager.register_handlers(bot)

async def handle_start(bot: AsyncTeleBot, message: Message) -> None:
    await start_manager.handle_start_command(message)

__all__ = ['register_handlers', 'init_managers', 'handle_start']
