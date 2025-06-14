import time
import json
import logging
import asyncio
import re
import html
from datetime import datetime
from typing import Optional, Dict, Any, Set, List

from telebot.async_telebot import AsyncTeleBot
from telebot import types
from telebot.types import InputMediaPhoto, Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
import os
from dotenv import load_dotenv

from utils.redis_manager import redis_manager
from utils.config import START_PAGE, ADMIN_ID, ENV_FILE
from utils.functions import create_keyboard, serialize_data
from handlers.manager.operation import FinancialManagement, UserManagement, FinancialSummaryAggregator, get_async_logger
from handlers.security import InputValidator, TransactionGuard, RateLimiter

# Replace these with your actual bot token, numeric channel id, and invite link.
TOKEN = '6452050983:AAHmFN6jTjkpAD28qhkQkWNm9VEwN8fVgJk'
CHANNEL_ID = -1001886867129  # Numeric channel or supergroup id
INVITE_LINK = "https://t.me/+HXYCt94N-OM0MjU1"

# Set up logging for better traceability.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UserStartManager:
    """Manager class for handling Telegram bot start commands and join requests."""
    
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

            first_name = user_data.get("user_profile") or user_data.get("first_name", "User")
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

    # ----------------- Redis & Join Request Helper Methods -----------------
    async def _save_message_id(self, user_id: int, message_id: int) -> None:
        """Append a message_id to the user's list in Redis."""
        try:
            key = f'main_data:join_messages:{user_id}'
            existing_messages = await redis_manager.redis_client.json().get(key)

            if not isinstance(existing_messages, list):
                existing_messages = []
            existing_messages.append(message_id)
            await redis_manager.redis_client.json().set(key, '$', existing_messages)
        except Exception as e:
            logger.error(f"Error saving message to Redis for user {user_id}: {e}")

    async def _get_message_ids(self, user_id: int) -> Optional[List[int]]:
        """Retrieve all message_ids for a user from Redis."""
        try:
            key = f'main_data:join_messages:{user_id}'
            return await redis_manager.redis_client.json().get(key) or []
        except Exception as e:
            logger.error(f"Error retrieving messages from Redis for user {user_id}: {e}")
            return None

    async def _delete_all_messages(self, user_id: int) -> None:
        """Delete all stored messages for a user and remove them from Redis."""
        try:
            key = f'main_data:join_messages:{user_id}'
            message_ids = await self._get_message_ids(user_id)
            if message_ids:
                for message_id in message_ids:
                    try:
                        await self.bot.delete_message(user_id, message_id)
                    except Exception as e:
                        logger.error(f"Failed to delete message {message_id} for user {user_id}: {e}")
                await redis_manager.redis_client.json().delete(key)
        except Exception as e:
            logger.error(f"Error deleting messages from Redis for user {user_id}: {e}")

    # ----------------- New Handlers for Join Requests & /start Membership Check -----------------
    async def _load_success_requests(self) -> Set[int]:
        """Load success join requests from Redis."""
        try:
            data = await redis_manager.redis_client.json().get('main_data:details:success_requests') or {}
            return set(data.get('requests', []))
        except Exception as e:
            logger.error(f"Error loading success requests from Redis: {e}")
            return set()
    async def _save_success_requests(self, requests: Set[int]) -> None:
        """Save success join requests to Redis."""
        try:
            await redis_manager.redis_client.json().set(
                'main_data:details:success_requests',
                '$',
                {'requests': list(requests)}
            )
        except Exception as e:
            logger.error(f"Error saving success requests to Redis: {e}")

    async def handle_join_request(self, join_request: types.ChatJoinRequest) -> None:
        """
        Triggered when a user sends a join request for the channel/supergroup.
        Stores the user ID in Redis and runs handle_start_command.
        """
        user = join_request.from_user
        chat = join_request.chat
        logger.info(f"Received join request from user {user.id} for chat {chat.id}")

        # Load current success requests, add new user, and save back to Redis
        success_requests = await self._load_success_requests()
        success_requests.add(user.id)
        await self._save_success_requests(success_requests)
        # Create a pseudo-message using a JSON dictionary and de_json
        pseudo_message_data = {
            "message_id": 0,  # Dummy message ID
            "date": int(time.time()),
            "chat": {"id": user.id, "type": "private"},
            "from": user.to_dict(),  # Ensure your user object has a to_dict() method
            "content_type": "text",
            "options": {},
            "json_string": "{}",
            "text": ""  # Optional text field
        }
        pseudo_message = Message.de_json(pseudo_message_data)
        # Call the normal start command handling using the pseudo-message
        await asyncio.gather(
            self.handle_start_command(pseudo_message),
            self._delete_all_messages(user_id=user.id)
        )


    async def start_command_with_membership(self, message: Message) -> None:
        """
        Handles the /start command by checking if the user is a member of the channel.
        If not, shows an invite button.
        """
        user = message.from_user
        success_requests = await self._load_success_requests()

        try:
            member = await self.bot.get_chat_member(CHANNEL_ID, user.id)
            if member.status in ['member', 'administrator', 'creator']:
                await self.handle_start_command(message)
            elif user.id in success_requests:
                await self.handle_start_command(message)
            else:
                keyboard = types.InlineKeyboardMarkup()
                join_button = types.InlineKeyboardButton(
                    text="👑 Jᴏɪɴ Tʜᴇ Cʜᴀɴɴᴇʟ",
                    url=INVITE_LINK
                    )
                keyboard.add(join_button)
                msg = await self.bot.send_message(
                    message.from_user.id,
                    "🙅🏻‍♂️ <b>Yᴏᴜ Aʀᴇ Nᴏᴛ A Mᴇᴍʙᴇʀ Yᴇᴛ!\n\n🔔 Pʟᴇᴀsᴇ Jᴏɪɴ Oᴜʀ Cʜᴀɴɴᴇʟ Usɪɴɢ\nTʜᴇ Bᴜᴛᴛᴏɴ Bᴇʟᴏᴡ.</b>..",
                    parse_mode="html",
                    reply_markup=keyboard
                )
                await self._save_message_id(user_id=message.from_user.id, message_id=message.message_id)
                await self._save_message_id(user_id=message.from_user.id, message_id=msg.message_id)
        except Exception as e:
            logger.error(f"Error checking membership for user {user.id}: {e}")
            await self.bot.send_message(message.from_user.id, "There was an error checking your membership. Please try again later.")
    
    async def update_env_file(key, value):
        lines = []
        if os.path.exists(ENV_FILE):
            with open(ENV_FILE, "r") as f:
                lines = f.readlines()

        with open(ENV_FILE, "w") as f:
            key_found = False
            for line in lines:
                if line.strip().startswith(f"{key}="):
                    f.write(f"{key}={value}\n")
                    key_found = True
                else:
                    f.write(line)
            if not key_found:
                f.write(f"{key}={value}\n")

    async def handle_file_id(self, message: Message) -> None:
        if message.from_user.id != ADMIN_ID:
            return 

        if not message.caption:
            await self.bot.reply_to(message, "❌ Please send a caption. The caption will be used as the ENV key.")
            return

        # Get file ID depending on media type
        file_id = None
        if message.photo:
            file_id = message.photo[-1].file_id  # Largest photo
        elif message.video:
            file_id = message.video.file_id
        elif message.document:
            file_id = message.document.file_id
        elif message.audio:
            file_id = message.audio.file_id
        elif message.voice:
            file_id = message.voice.file_id
        elif message.animation:
                file_id = message.animation.file_id

        if not file_id:
            await self.bot.reply_to(message, "⚠️ Unable to retrieve file ID.")
            return

        key = message.caption.strip()
        await self.update_env_file(key, file_id)
        await self.bot.reply_to(message, f"✅ Saved `{key}={file_id}` to `.env`", parse_mode="Markdown")

    # ----------------- Handler Registration -----------------

    async def register_handlers(self, bot: AsyncTeleBot) -> None:
        async_logger = await get_async_logger()
        if not self._initialized:
            await async_logger.error("Cannot register handlers: manager not initialized")
            return

        try:
            # /start command
            bot.register_message_handler(
                self.start_command_with_membership,
                commands=['start'],
                pass_bot=False
            )

            # File handler registration (without decorator)
            async def handle_file_wrapper(message: Message) -> None:
                try:
                    await self.handle_file_id(message)
                except Exception as e:
                    await bot.send_message(message.chat.id, "⚠️ Error processing file.")
                    await async_logger.error(f"Error in handle_file_id: {e}")

            bot.register_message_handler(
                handle_file_wrapper,
                content_types=['photo', 'video', 'document', 'audio', 'voice', 'animation'],
                pass_bot=False
            )

            # Callback query: "start"
            bot.register_callback_query_handler(
                self.handle_start_callback,
                func=lambda call: call.data == "start",
                pass_bot=False
            )

            # Chat join request
            bot.register_chat_join_request_handler(
                self.handle_join_request,
                pass_bot=False
            )

            await async_logger.info("Handlers registered successfully.")
        except Exception as e:
            await async_logger.error(f"Failed to register handlers: {e}")
            raise
bot = AsyncTeleBot(TOKEN)
start_manager = UserStartManager()

# Global functions to initialize managers and register handlers.
async def init_managers(user_manager: UserManagement, order_manager=None, bot: Optional[AsyncTeleBot] = None) -> bool:
    return await start_manager.init_managers(user_manager, bot)

async def register_handlers(bot: AsyncTeleBot) -> None:
    await start_manager.register_handlers(bot)

async def handle_start(bot: AsyncTeleBot, message: Message) -> None:
    await start_manager.handle_start_command(message)

__all__ = ['register_handlers', 'init_managers', 'handle_start']
