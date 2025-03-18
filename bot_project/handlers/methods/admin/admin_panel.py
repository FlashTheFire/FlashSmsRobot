from typing import Dict, Optional, Any, List, Tuple, Set
import asyncio
import time
import logging
from datetime import datetime

from redis.commands.core import AsyncBasicKeyCommands
from telebot.async_telebot import AsyncTeleBot
from telebot.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    Message,
    CallbackQuery,
    InputMediaPhoto
)

from utils.redis_keys import RedisKeys
from utils.functions import format_currency
from handlers.manager.operation import UserManagement, OrderManagement, DepositManagement, FinancialManagement
from handlers.security import RateLimiter  # TransactionGuard imported but not used; remove if unnecessary
from utils.redis_manager import redis_manager
from utils.cache_manager import cache_manager, CachePrefix
from handlers.main.top_services import top_service_manager
# Configure logger for the module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def build_time_query(time_filter: dict) -> str:
    """If a time filter is provided (with key 'recorded_at'), build a Redis query fragment."""
    if time_filter and 'recorded_at' in time_filter:
        start, end = time_filter['recorded_at']
        # Redis expects numeric range as: [start end]
        return f" @recorded_at:[{start} {end}]"
    return ""


def parse_aggregate_result(result: Any, key: str, value_type: type = int) -> Any:
    """
    Parse a single-field aggregate result.
    Expects result format: [num_rows, [field, value, ...]]
    If result is an exception, return a default value.
    """
    if isinstance(result, Exception):
        return value_type(0)
    if result and len(result) > 1 and isinstance(result[1], list):
        row = result[1]
        for i in range(0, len(row), 2):
            if row[i] == key:
                try:
                    return value_type(row[i+1])
                except Exception:
                    return value_type(0)
    return value_type(0)


def parse_active_users(result: Any) -> Set:
    """
    Parse active user ids from an aggregate query grouping by @user_id.
    If result is an exception, return an empty set.
    """
    if isinstance(result, Exception):
        return set()
    users = set()
    if result and len(result) > 1:
        # result[0] is the header, each subsequent row is a list of field/value pairs.
        for row in result[1:]:
            for i in range(0, len(row), 2):
                if row[i] == "user_id":
                    users.add(row[i+1])
    return users


class AdminPanelManager:
    """Main class for handling admin panel functionality in a professional and advanced manner."""
    
    def __init__(self) -> None:
        self.bot: Optional[AsyncTeleBot] = None
        self.user_manager: Optional[UserManagement] = None
        self.order_manager: Optional[OrderManagement] = None
        self.deposit_manager: Optional[DepositManagement] = None
        self.financial_manager: Optional[FinancialManagement] = None
        self.redis_client: Optional[AsyncBasicKeyCommands] = None
        self.rate_limiter: Optional[RateLimiter] = None
        self._initialized: bool = False
        self.admin_id = '5716978793'

    async def init_managers(
        self, 
        user_mgr: UserManagement,
        order_mgr: OrderManagement,
        deposit_mgr: DepositManagement,
        bot: AsyncTeleBot
    ) -> bool:
        """
        Initialize required components for the admin panel.
        
        Returns:
            bool: True if initialization succeeded, False otherwise.
        """
        try:

            self.bot = bot
            self.user_manager = user_mgr
            self.order_manager = order_mgr
            self.deposit_manager = deposit_mgr
            self.financial_manager = FinancialManagement(
                deposit_mgr=deposit_mgr,
                order_mgr=order_mgr,
                user_mgr=user_mgr
            )
            
            self.redis_client = await redis_manager.get_client()
            if not self.redis_client:
                logger.error("Redis client is not available during initialization.")
                return False
                
            self.rate_limiter = RateLimiter(
                redis_client=self.redis_client,
                duration=60,
                max_requests=20
            )
            
            self._initialized = True
            logger.info("Admin panel managers initialized successfully.")
            return True
        except Exception as e:
            logger.exception("Exception during init_managers")
            return False
            
    async def register_handlers(self) -> bool:
        """
        Register all admin panel related handlers for the bot.
        
        Returns:
            bool: True if registration succeeded, False otherwise.
        """
        try:
            # Register the admin panel command handler.
            self.bot.register_message_handler(
                self.show_admin_panel,
                commands=['AdminPanel']
            )
            
            # Register callback query handler for inline buttons.
            self.bot.register_callback_query_handler(
                self.handle_callback_query,
                func=lambda call: call.data.startswith("admin:")
            )

            # Register message handler for inline query.
            self.bot.register_message_handler(
                self.show_service_details,
                func=lambda message: message.text.startswith("#Sᴇʀᴠɪᴄᴇ")
            )
            
            logger.info("Admin panel handlers registered successfully.")
            return True
        except Exception as e:
            logger.exception("Failed to register admin panel handlers")
            return False

    async def show_admin_panel(self, message: Message) -> None:
        """
        Display the main admin panel interface.
        """
        try:
            keyboard = InlineKeyboardMarkup()
            keyboard.row(
                InlineKeyboardButton("🛠 Sᴇʀᴠɪᴄᴇ", callback_data="admin:service_manager"),
                InlineKeyboardButton("💳 Pᴀʏᴍᴇɴᴛ", callback_data="admin:payment_manager")
            )
            keyboard.row(
                InlineKeyboardButton("📦 Oʀᴅᴇʀ", callback_data="admin:order_manager"),
                InlineKeyboardButton("💱 Tʀᴀɴsᴀᴄᴛɪᴏɴ", callback_data="admin:transaction_manager")
            )
            keyboard.row(
                InlineKeyboardButton("⚙️ Gᴇɴᴇʀᴀʟ", callback_data="admin:settings"),
                InlineKeyboardButton("🎮 Aᴘɪs", callback_data="admin:api_manager")
            )
            keyboard.row(
                InlineKeyboardButton("🔐 Bᴀᴄᴋᴜᴘ", callback_data="admin:backup_manager"),
                InlineKeyboardButton("💭 Sᴜᴘᴘᴏʀᴛ", callback_data="admin:support_manager")
            )
            keyboard.row(
                InlineKeyboardButton("🔙 Bᴀᴄᴋ Tᴏ Hᴏᴍᴇ", callback_data="start")
            )
            
            today_stats, overall_stats = await self._get_system_stats()

            caption = (
                "<b>👑 Aᴅᴍɪɴ Pᴀɴᴇʟ Oᴠᴇʀᴠɪᴇᴡ ❯</b>\n\n"
                "<blockquote expandable>"
                "<b>📊 Sʏsᴛᴇᴍ Sᴛᴀᴛɪsᴛɪᴄs [Tᴏᴅᴀʏ]</b>\n"
                f"<code>│</code>\n"
                f"<code>├</code> 👥 Aᴄᴛɪᴠᴇ Usᴇʀ{'s' if today_stats.get('active_users') != 1 else ''}  » <code>{today_stats.get('active_users')}</code> <code>Usᴇʀ{'s' if today_stats.get('active_users') != 1 else ''}</code>\n"
                f"<code>│</code>\n"
                f"<code>├</code> 📦 Pᴇɴᴅɪɴɢ Oʀᴅᴇʀs          » <code>{today_stats.get('pending_orders')}</code>\n"
                f"<code>├</code> 🏦 Pᴇɴᴅɪɴɢ Dᴇᴘᴏsɪᴛs       » <code>{today_stats.get('pending_deposits')}</code>\n"
                f"<code>│</code>\n"
                f"<code>├</code> ✅ Cᴏᴍᴘʟᴇᴛᴇᴅ Oʀᴅᴇʀs     » <code>{today_stats.get('completed_orders')}</code>\n"
                f"<code>├</code> 🏦 Cᴏᴍᴘʟᴇᴛᴇᴅ Dᴇᴘᴏsɪᴛs  » <code>{today_stats.get('completed_deposits')}</code>\n"
                f"<code>│</code>\n"
                f"<code>├</code> ❌ Cᴀɴᴄᴇʟʟᴇᴅ Oʀᴅᴇʀs      » <code>{today_stats.get('cancelled_orders')}</code>\n"
                f"<code>├</code> 🛑 Cᴀɴᴄᴇʟʟᴇᴅ Dᴇᴘᴏsɪᴛs   » <code>{today_stats.get('cancelled_deposits')}</code>\n"
                f"<code>│</code>\n"
                f"<code>├</code> 💰 Oʀᴅᴇʀ Aᴍᴏᴜɴᴛ            » <code>{round(today_stats.get('order_amount', 0), 2)}</code>\n"
                f"<code>└</code> 🏦 Dᴇᴘᴏsɪᴛ Aᴍᴏᴜɴᴛ         » <code>{round(today_stats.get('deposit_amount', 0), 2)}</code>"
                "</blockquote>\n\n"
                "<blockquote expandable>"
                "<b>📊 Sʏsᴛᴇᴍ Sᴛᴀᴛɪsᴛɪᴄs [Tᴏᴛᴀʟ]</b>\n"
                f"<code>│</code>\n"
                f"<code>├</code> 👥 Tᴏᴛᴀʟ Usᴇʀ{'s' if overall_stats.get('active_users') != 1 else ''}  » <code>{overall_stats.get('active_users')}</code> <code>Usᴇʀ{'s' if overall_stats.get('active_users') != 1 else ''}</code>\n"
                f"<code>│</code>\n"
                f"<code>├</code> ✅ Cᴏᴍᴘʟᴇᴛᴇᴅ Oʀᴅᴇʀs     » <code>{overall_stats.get('completed_orders')}</code>\n"
                f"<code>├</code> 🏦 Cᴏᴍᴘʟᴇᴛᴇᴅ Dᴇᴘᴏsɪᴛs  » <code>{overall_stats.get('completed_deposits')}</code>\n"
                f"<code>│</code>\n"
                f"<code>├</code> ❌ Cᴀɴᴄᴇʟʟᴇᴅ Oʀᴅᴇʀs      » <code>{overall_stats.get('cancelled_orders')}</code>\n"
                f"<code>├</code> 🛑 Cᴀɴᴄᴇʟʟᴇᴅ Dᴇᴘᴏsɪᴛs   » <code>{overall_stats.get('cancelled_deposits')}</code>\n"
                f"<code>│</code>\n"
                f"<code>├</code> 💰 Oʀᴅᴇʀ Aᴍᴏᴜɴᴛ            » <code>{round(overall_stats.get('order_amount', 0), 2)}</code>\n"
                f"<code>└</code> 🏦 Dᴇᴘᴏsɪᴛ Aᴍᴏᴜɴᴛ         » <code>{round(overall_stats.get('deposit_amount', 0), 2)}</code>"
                "</blockquote>\n\n"
                "<i>Sᴇʟᴇᴄᴛ A Mᴀɴᴀɢᴇᴍᴇɴᴛ Oᴘᴛɪᴏɴ...</i>"
            )
            
            await self.bot.send_message(
                chat_id=message.chat.id,
                text=caption,
                reply_markup=keyboard,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.exception("Failed to load admin panel")
            await self.bot.send_message(
                message.chat.id,
                "❌ Failed to load admin panel"
            )
    async def edit_admin_panel(self, call: CallbackQuery) -> None:
        """
        Display the main admin panel interface.
        """
        try:
            keyboard = InlineKeyboardMarkup()
            keyboard.row(
                InlineKeyboardButton("Sᴇʀᴠɪᴄᴇ", callback_data="admin:service_manager"),
                InlineKeyboardButton("Pᴀʏᴍᴇɴᴛ", callback_data="admin:payment_manager")
            )
            keyboard.row(
                InlineKeyboardButton("Oʀᴅᴇʀ", callback_data="admin:order_manager"),
                InlineKeyboardButton("Tʀᴀɴsᴀᴄᴛɪᴏɴ", callback_data="admin:transaction_manager")
            )
            keyboard.row(
                InlineKeyboardButton("Gᴇɴᴇʀᴀʟ", callback_data="admin:settings"),
                InlineKeyboardButton("Aᴘɪs", callback_data="admin:api_manager")
            )
            keyboard.row(
                InlineKeyboardButton("Bᴀᴄᴋᴜᴘ", callback_data="admin:backup_manager"),
                InlineKeyboardButton("Sᴜᴘᴘᴏʀᴛ", callback_data="admin:support_manager")
            )
            keyboard.row(
                InlineKeyboardButton("Bᴀᴄᴋ Tᴏ Hᴏᴍᴇ", callback_data="start")
            )
            today_stats, overall_stats = await self._get_system_stats()
            caption = (
                "<b>👑 Aᴅᴍɪɴ Pᴀɴᴇʟ Oᴠᴇʀᴠɪᴇᴡ ❯</b>\n\n"
                "<blockquote expandable>"
                "<b>📊 Sʏsᴛᴇᴍ Sᴛᴀᴛɪsᴛɪᴄs [Tᴏᴅᴀʏ]</b>\n"
                f"<code>│</code>\n"
                f"<code>├</code> 👥 Aᴄᴛɪᴠᴇ Usᴇʀ{'s' if today_stats.get('active_users') != 1 else ''}  » <code>{today_stats.get('active_users')}</code> <code>Usᴇʀ{'s' if today_stats.get('active_users') != 1 else ''}</code>\n"
                f"<code>│</code>\n"
                f"<code>├</code> 📦 Pᴇɴᴅɪɴɢ Oʀᴅᴇʀs          » <code>{today_stats.get('pending_orders')}</code>\n"
                f"<code>├</code> 🏦 Pᴇɴᴅɪɴɢ Dᴇᴘᴏsɪᴛs       » <code>{today_stats.get('pending_deposits')}</code>\n"
                f"<code>│</code>\n"
                f"<code>├</code> ✅ Cᴏᴍᴘʟᴇᴛᴇᴅ Oʀᴅᴇʀs     » <code>{today_stats.get('completed_orders')}</code>\n"
                f"<code>├</code> 🏦 Cᴏᴍᴘʟᴇᴛᴇᴅ Dᴇᴘᴏsɪᴛs  » <code>{today_stats.get('completed_deposits')}</code>\n"
                f"<code>│</code>\n"
                f"<code>├</code> ❌ Cᴀɴᴄᴇʟʟᴇᴅ Oʀᴅᴇʀs      » <code>{today_stats.get('cancelled_orders')}</code>\n"
                f"<code>├</code> 🛑 Cᴀɴᴄᴇʟʟᴇᴅ Dᴇᴘᴏsɪᴛs   » <code>{today_stats.get('cancelled_deposits')}</code>\n"
                f"<code>│</code>\n"
                f"<code>├</code> 💰 Oʀᴅᴇʀ Aᴍᴏᴜɴᴛ            » <code>{round(today_stats.get('order_amount', 0), 2)}</code>\n"
                f"<code>└</code> 🏦 Dᴇᴘᴏsɪᴛ Aᴍᴏᴜɴᴛ         » <code>{round(today_stats.get('deposit_amount', 0), 2)}</code>"
                "</blockquote>\n\n"
                "<blockquote expandable>"
                "<b>📊 Sʏsᴛᴇᴍ Sᴛᴀᴛɪsᴛɪᴄs [Tᴏᴛᴀʟ]</b>\n"
                f"<code>│</code>\n"
                f"<code>├</code> 👥 Tᴏᴛᴀʟ Usᴇʀ{'s' if overall_stats.get('active_users') != 1 else ''}  » <code>{overall_stats.get('active_users')}</code> <code>Usᴇʀ{'s' if overall_stats.get('active_users') != 1 else ''}</code>\n"
                f"<code>│</code>\n"
                f"<code>├</code> ✅ Cᴏᴍᴘʟᴇᴛᴇᴅ Oʀᴅᴇʀs     » <code>{overall_stats.get('completed_orders')}</code>\n"
                f"<code>├</code> 🏦 Cᴏᴍᴘʟᴇᴛᴇᴅ Dᴇᴘᴏsɪᴛs  » <code>{overall_stats.get('completed_deposits')}</code>\n"
                f"<code>│</code>\n"
                f"<code>├</code> ❌ Cᴀɴᴄᴇʟʟᴇᴅ Oʀᴅᴇʀs      » <code>{overall_stats.get('cancelled_orders')}</code>\n"
                f"<code>├</code> 🛑 Cᴀɴᴄᴇʟʟᴇᴅ Dᴇᴘᴏsɪᴛs   » <code>{overall_stats.get('cancelled_deposits')}</code>\n"
                f"<code>│</code>\n"
                f"<code>├</code> 💰 Oʀᴅᴇʀ Aᴍᴏᴜɴᴛ            » <code>{round(overall_stats.get('order_amount', 0), 2)}</code>\n"
                f"<code>└</code> 🏦 Dᴇᴘᴏsɪᴛ Aᴍᴏᴜɴᴛ         » <code>{round(overall_stats.get('deposit_amount', 0), 2)}</code>"
                "</blockquote>\n\n"
                "<i>Sᴇʟᴇᴄᴛ A Mᴀɴᴀɢᴇᴍᴇɴᴛ Oᴘᴛɪᴏɴ...</i>"
            )
            await self.bot.delete_message(call.message.chat.id, call.message.message_id)

            # Send a new text message with the caption
            await self.bot.send_message(
                chat_id=call.message.chat.id,
                text=caption,
                reply_markup=keyboard,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.exception("Failed to load admin panel")
            await self.bot.send_message(
                call.message.chat.id,
                "Failed to load admin panel"
            )


    async def handle_callback_query(self, call: CallbackQuery) -> None:
        """
        Handle callback queries for admin panel inline buttons.
        """
        try:
            # Process callback data – add your detailed logic for each option here.
            data = call.data
            if data == "admin:service_manager":
                await self.show_service_manager(call)
            elif data == "admin:edit_admin_panel":
                await self.edit_admin_panel(call)

            await self.bot.answer_callback_query(call.id, text="Option selected")
        except Exception as e:
            logger.exception("Error handling callback query")



    async def show_service_manager(self, call: CallbackQuery) -> None:
        try:
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            message_id = call.message.message_id
            # Check if the current user is an admin
            '''if str(user_id) != str(self.admin_id):
                await self.bot.answer_callback_query(call.id, text="🚫 Access denied: Admins only.")
                return'''

            await self.bot.answer_callback_query(call.id, text="✅ Admin access granted")
            caption = (
                "<b>👨🏻‍💻 Sᴇʀᴠɪᴄᴇ Mᴀɴᴀɢᴇʀ ❯</b>\n\n"
                "<blockquote expandable>"
                "📊 Tᴏᴛᴀʟ Sᴇʀᴠɪᴄᴇs       » <code>{}</code>\n"
                "🌎 Tᴏᴛᴀʟ Cᴏᴜɴᴛʀʏ       » <code>{}</code>\n"
                "💻 Pᴏᴘᴜʟᴀʀ Sᴇʀᴠɪᴄᴇs  » <code>{}</code>\n"
                "📈 Pᴏᴘᴜʟᴀʀ Cᴏᴜɴᴛʀʏ  »  <code>{}</code>"
                "</blockquote>\n\n"
                "<i>Sᴇʟᴇᴄᴛ A Sᴇʀᴠɪᴄᴇ Oᴘᴛɪᴏɴ</i><b>.</b>"
            )
            keyboard = InlineKeyboardMarkup(row_width=2)
            keyboard.add(
                InlineKeyboardButton("🔍 Sᴇʀᴠɪᴄᴇs", switch_inline_query_current_chat="#Sᴇʀᴠɪᴄᴇ "),
                InlineKeyboardButton("➕ Aᴅᴅ Sᴇʀᴠɪᴄᴇ", callback_data="remove_service")
            )
            keyboard.add(InlineKeyboardButton("🔙 Bᴀᴄᴋ Tᴏ Aᴅᴍɪɴ Pᴀɴᴇʟ", callback_data="admin:edit_admin_panel"))

            await self.bot.edit_message_reply_markup(
                chat_id=chat_id,
                reply_markup=keyboard,
                message_id=call.message.message_id
            )
            total_country_query = [
                "FT.AGGREGATE", "service_index", "*",
                "GROUPBY", "1", "@country_id",
                "REDUCE", "COUNT_DISTINCT", "1", "@country_id", "AS", "unique_countries"
            ]

            famous_country_query = [
                "FT.AGGREGATE", "order_index", "*",
                "GROUPBY", "1", "@country_id",
                "REDUCE", "COUNT_DISTINCT", "1", "@country_id", "AS", "unique_countries"
            ]

            total_service_query = [
                "FT.AGGREGATE", "service_index", "*",
                "GROUPBY", "1", "@app_id",
                "REDUCE", "COUNT_DISTINCT", "1", "@app_id", "AS", "unique_app_ids", 
                "LIMIT", "0", "1170"
            ]

            famous_service_query = [
                "FT.AGGREGATE", "order_index", "*",
                "GROUPBY", "1", "@app_id",
                "REDUCE", "COUNT_DISTINCT", "1", "@app_id", "AS", "unique_app_ids"
            ]

            tasks = [
                self.redis_client.execute_command(*total_country_query),
                self.redis_client.execute_command(*famous_country_query),
                self.redis_client.execute_command(*total_service_query),
                self.redis_client.execute_command(*famous_service_query)
            ]

            (total_country_res,
            famous_country_res,
            total_service_res,
            famous_service_res) = await asyncio.gather(*tasks, return_exceptions=True)
            await self.bot.edit_message_text(
                chat_id=chat_id, 
                text=caption.format(
                    total_service_res[0],
                    total_country_res[0],
                    famous_service_res[0],
                    famous_country_res[0]
                    ),
                    parse_mode="HTML",
                    message_id=message_id,
                    reply_markup=keyboard
                )
            '''cache_data = await top_service_manager._get_cached_leaderboard(must_return=True)
            if cache_data:
                file_id = cache_data.get("file_id")
                try:
                    if file_id:
                        await self.bot.edit_message_media(
                            media=InputMediaPhoto(media=file_id, caption=caption.format(
                                total_service_res[0],
                                famous_service_res[0],
                                total_country_res[0],
                                famous_country_res[0]
                                ), parse_mode="HTML"),
                            chat_id=chat_id,
                            message_id=message_id,
                            reply_markup=keyboard
                        )
                        return
                    else:
                        # Use cached file but get new file_id
                        with open(cache_data["file_path"], 'rb') as media_file:
                            result = await self.bot.edit_message_media(
                                media=InputMediaPhoto(media=media_file, caption=caption, parse_mode="HTML"),
                                chat_id=chat_id,
                                message_id=message_id,
                                reply_markup=keyboard
                            )
                            if hasattr(result, 'photo') and result.photo:
                                # Save the new file_id
                                new_file_id = result.photo[-1].file_id
                                await top_service_manager._save_file_id(new_file_id)
                            return
                except Exception as e:
                    print(f"Error using cached data: {e}")
                    # Fall through to generate new data
                    pass'''
        except Exception as e:
            logger.exception("Failed to show service manager")
            await self.bot.send_message(call.message.chat.id, "❌ Failed to show service manager")
    async def show_service_details(self, message: Message) -> None:
        try:
            if not self._initialized:
                logger.error("Cannot show service details: manager not initialized")
                await self.bot.send_message(message.chat.id, "❌ Manager not initialized")
                return
            
            
        except Exception as e:
            logger.exception("Failed to show service details")
            await self.bot.send_message(message.chat.id, "❌ Failed to show service details")



    async def _fetch_stats(self, time_filter: dict) -> Dict[str, Any]:
        """
        Fetch and calculate system statistics using Redis aggregate queries.
        If a time_filter is provided (with 'recorded_at' as a tuple of start and end timestamps),
        it is appended to each query.

        Returns:
            dict: A dictionary containing system statistics.
        """
        time_query = build_time_query(time_filter)

        # Build aggregate queries for order_index
        order_pending_query = [
            "FT.AGGREGATE", "order_index", f"@order_status:PENDING{time_query}",
            "GROUPBY", "0",
            "REDUCE", "COUNT", "0", "AS", "pending_orders"
        ]
        order_completed_query = [
            "FT.AGGREGATE", "order_index", f"@order_status:(COMPLETED|PROCESSING){time_query}",
            "GROUPBY", "0",
            "REDUCE", "COUNT", "0", "AS", "completed_orders"
        ]
        order_cancelled_query = [
            "FT.AGGREGATE", "order_index", f"@order_status:(CANCELLED|TIMEOUT){time_query}",
            "GROUPBY", "0",
            "REDUCE", "COUNT", "0", "AS", "cancelled_orders"
        ]
        order_amount_query = [
            "FT.AGGREGATE", "order_index", f"@order_status:(COMPLETED|PROCESSING){time_query}",
            "GROUPBY", "0",
            "REDUCE", "SUM", "1", "@order_amount", "AS", "order_amount"
        ]
        active_order_users_query = [
            "FT.AGGREGATE", "order_index", f"{time_query}",
            "GROUPBY", "1", "@user_id"
        ]

        # Build aggregate queries for deposit_index
        deposit_pending_query = [
            "FT.AGGREGATE", "deposit_index", f"@deposit_status:PENDING{time_query}",
            "GROUPBY", "0",
            "REDUCE", "COUNT", "0", "AS", "pending_deposits"
        ]
        deposit_completed_query = [
            "FT.AGGREGATE", "deposit_index", f"@deposit_status:COMPLETED{time_query}",
            "GROUPBY", "0",
            "REDUCE", "COUNT", "0", "AS", "completed_deposits"
        ]
        deposit_cancelled_query = [
            "FT.AGGREGATE", "deposit_index", f"@deposit_status:(CANCELLED|TIMEOUT){time_query}",
            "GROUPBY", "0",
            "REDUCE", "COUNT", "0", "AS", "cancelled_deposits"
        ]
        deposit_amount_query = [
            "FT.AGGREGATE", "deposit_index", f"@deposit_status:(COMPLETED|PROCESSING){time_query}",
            "GROUPBY", "0",
            "REDUCE", "SUM", "1", "@deposit_amount", "AS", "deposit_amount"
        ]
        active_deposit_users_query = [
            "FT.AGGREGATE", "deposit_index", f"{time_query}",
            "GROUPBY", "1", "@user_id"
        ]

        # Run all queries concurrently
        tasks_for_today = [
            self.redis_client.execute_command(*order_pending_query),
            self.redis_client.execute_command(*order_completed_query),
            self.redis_client.execute_command(*order_cancelled_query),
            self.redis_client.execute_command(*order_amount_query),
            self.redis_client.execute_command(*active_order_users_query)
        ]
        tasks_for_overall = [
            self.redis_client.execute_command(*deposit_pending_query),
            self.redis_client.execute_command(*deposit_completed_query),
            self.redis_client.execute_command(*deposit_cancelled_query),
            self.redis_client.execute_command(*deposit_amount_query),
            self.redis_client.execute_command(*active_deposit_users_query)
        ]

        (order_pending_res,
         order_completed_res,
         order_cancelled_res,
         order_amount_res,
         active_order_users_res) = await asyncio.gather(*tasks_for_today, return_exceptions=True)
        (deposit_pending_res,
         deposit_completed_res,
         deposit_cancelled_res,
         deposit_amount_res,
         active_deposit_users_res) = await asyncio.gather(*tasks_for_overall, return_exceptions=True)

        # Parse aggregate results
        pending_orders = parse_aggregate_result(order_pending_res, "pending_orders", int)
        completed_orders = parse_aggregate_result(order_completed_res, "completed_orders", int)
        cancelled_orders = parse_aggregate_result(order_cancelled_res, "cancelled_orders", int)
        try:
            order_amount = float(parse_aggregate_result(order_amount_res, "order_amount", float))
        except Exception:
            order_amount = 0.0

        pending_deposits = parse_aggregate_result(deposit_pending_res, "pending_deposits", int)
        completed_deposits = parse_aggregate_result(deposit_completed_res, "completed_deposits", int)
        cancelled_deposits = parse_aggregate_result(deposit_cancelled_res, "cancelled_deposits", int)
        try:
            deposit_amount = float(parse_aggregate_result(deposit_amount_res, "deposit_amount", float))
        except Exception:
            deposit_amount = 0.0

        # Parse active users by extracting user ids and taking a union
        print(active_order_users_res)
        print(active_deposit_users_res)
        active_order_users = parse_active_users(active_order_users_res)
        active_deposit_users = parse_active_users(active_deposit_users_res)
        active_users = len(active_order_users.union(active_deposit_users))

        return {
            'pending_orders': pending_orders,
            'pending_deposits': pending_deposits,
            'completed_orders': completed_orders,
            'completed_deposits': completed_deposits,
            'cancelled_orders': cancelled_orders,
            'cancelled_deposits': cancelled_deposits,
            'order_amount': order_amount,
            'deposit_amount': deposit_amount,
            'active_users': active_users
        }

    async def _get_system_stats(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Retrieve system statistics for today and overall (since the beginning of the year).
        
        Returns:
            tuple: A tuple (today_stats, overall_stats)
        """
        try:
            now = datetime.now()
            # Today's statistics: from start of day until now.
            today_start = datetime.combine(now.date(), datetime.min.time())
            today_filter = {
                'recorded_at': (today_start.timestamp(), now.timestamp())
            }
            today_stats = await self._fetch_stats(today_filter)
            
            # Overall statistics: from the start of the year until now.
            year_start = datetime(now.year, 1, 1)
            overall_filter = {
                'recorded_at': (year_start.timestamp(), now.timestamp())
            }
            overall_stats = await self._fetch_stats(overall_filter)
            
            return today_stats, overall_stats
        except Exception as e:
            logger.exception("Failed to get system statistics")
            # Return default stats on error
            default_stats = {
                'active_users': 0,
                'pending_orders': 0,
                'pending_deposits': 0,
                'completed_orders': 0,
                'completed_deposits': 0,
                'cancelled_orders': 0,
                'cancelled_deposits': 0,
                'order_amount': 0,
                'deposit_amount': 0
            }
            return default_stats, default_stats

# Instantiate a global admin panel manager.
admin_panel_manager = AdminPanelManager()

async def init_managers(user_manager: UserManagement, order_manager: OrderManagement, bot: AsyncTeleBot) -> bool:
    """
    Initialize the admin panel manager with the required managers.
    
    Returns:
        bool: True if initialization succeeded, False otherwise.
    """
    from handlers.manager.operation import deposit_mgr  # Import deposit_mgr from the appropriate module
    return await admin_panel_manager.init_managers(user_manager, order_manager, deposit_mgr, bot)

async def register_handlers(bot: AsyncTeleBot) -> bool:
    """
    Register admin panel handlers with the given bot.
    
    Returns:
        bool: True if registration succeeded, False otherwise.
    """
    return await admin_panel_manager.register_handlers()
