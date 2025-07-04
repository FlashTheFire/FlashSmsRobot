from typing import Dict, Optional, Any, List
import asyncio
import logging
import json
from datetime import datetime, timedelta
from telebot.async_telebot import AsyncTeleBot
from telebot.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputMediaPhoto,
    CallbackQuery,
    Message,
    InputMediaVideo,
    InputTextMessageContent,
    InlineQueryResultArticle
)
import asyncio
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from datetime import datetime, timedelta, date
import calendar
from typing import Dict, Optional
# Local imports
from utils.redis_manager import redis_manager
from handlers.manager.operation import (
    FinancialManagement, OrderManagement, DepositManagement,
    UserManagement, FinancialManagement, user_mgr
)
from handlers.security import RateLimiter
from utils.functions import small_caps, encode_order_id, decode_barcode_id, date_to_unix, large_caps, subscript_small_caps
from utils.config import LOADING_GIF
from redis.commands.search.query import Query
from functools import partial
from utils.redis_keys import RedisKeys
from handlers.security import RateLimiter, InputValidator, TransactionGuard

logger = logging.getLogger(__name__)



def time_ago(timestamp: float) -> str:
    """Calculate relative time ago from timestamp."""
    now = datetime.now().timestamp()
    diff = now - float(timestamp)

    if diff < 60:
        return "Jᴜsᴛ Nᴏᴡ"
    elif diff < 3600:
        minutes = int(diff // 60)
        seconds = int(diff % 60)
        return f"{minutes}ᴍ {seconds}s" if seconds else f"{minutes}ᴍ Aɢᴏ"
    elif diff < 86400:
        hours = int(diff // 3600)
        minutes = int((diff % 3600) // 60)
        return f"{hours}ʜ {minutes}ᴍ" if minutes else f"{hours}ʜ Aɢᴏ"
    elif diff < 604800:
        days = int(diff // 86400)
        hours = int((diff % 86400) // 3600)
        return f"{days}ᴅ {hours}ʜ" if hours else f"{days}ᴅ Aɢᴏ"
    elif diff < 2592000:
        weeks = int(diff // 604800)
        days = int((diff % 604800) // 86400)
        return f"{weeks}ᴡ {days}ᴅ" if days else f"{weeks}ᴡ Aɢᴏ"
    elif diff < 31536000:
        months = int(diff // 2592000)
        weeks = int((diff % 2592000) // 604800)
        return f"{months}ᴍᴏ {weeks}ᴡ" if weeks else f"{months}ᴍᴏ Aɢᴏ"
    else:
        years = int(diff // 31536000)
        months = int((diff % 31536000) // 2592000)
        return f"{years}ʏ {months}ᴍᴏ" if months else f"{years}ʏ Aɢᴏ"
def get_circled_number(n: int) -> str:
    """Return circled number (Unicode) for integers 1–31."""
    circled_map = {
        1: "①", 2: "②", 3: "③", 4: "④", 5: "⑤", 6: "⑥", 7: "⑦", 8: "⑧", 9: "⑨", 10: "⑩",
        11: "⑪", 12: "⑫", 13: "⑬", 14: "⑭", 15: "⑮", 16: "⑯", 17: "⑰", 18: "⑱", 19: "⑲", 20: "⑳",
        21: "㉑", 22: "㉒", 23: "㉓", 24: "㉔", 25: "㉕", 26: "㉖", 27: "㉗", 28: "㉘", 29: "㉙", 30: "㉚",
        31: "㉛"
    }
    return circled_map.get(n, str(n))  # fallback to normal number



RESULT_LIMIT = 10


class HistoryManager:
    """Advanced history management system with Redis integration."""
    __slots__ = ('bot', 'order_mgr', 'deposit_mgr', 'aggregator', 'redis_client', 'user_mgr', 'SELECTIONS', 'PREVIEW_URL', 'HEADER_TEXT_HTML', 'MIN_DATE')

    def __init__(self):
        self.bot: Optional[AsyncTeleBot] = None
        self.order_mgr: Optional[OrderManagement] = None
        self.deposit_mgr: Optional[DepositManagement] = None
        self.aggregator: Optional[FinancialManagement] = None
        self.user_mgr: Optional[UserManagement] = None
        self.redis_client = None
        self.SELECTIONS: Dict[int, Dict[str, Optional[str]]] = {}

        self.PREVIEW_URL = 'https://i.ibb.co/Xkb6XgFD/20250703-111741.jpg'
        self.HEADER_TEXT_HTML = f'<a href="{self.PREVIEW_URL}">📅</a> <b>Sᴇʟᴇᴄᴛ A Dᴀᴛᴇ Fʀᴏᴍ Tʜᴇ Cᴀʟᴇɴᴅᴀʀ Bᴇʟᴏᴡ</b>'
        self.MIN_DATE = datetime.strptime('2025-02-20', '%Y-%m-%d').date()

    async def init_managers(self, order_mgr: OrderManagement, user_mgr: UserManagement, deposit_mgr: DepositManagement, bot: AsyncTeleBot) -> bool:
        """Initialize required components for history handling asynchronously."""
        try:
            self.bot = bot
            self.order_mgr = order_mgr
            self.deposit_mgr = deposit_mgr
            self.user_mgr = user_mgr
            self.aggregator = bot.aggregator
            redis_client = await redis_manager.get_client()
            self.redis_client = redis_client
            
            # Using asyncio.to_thread to avoid blocking the event loop for logging.
            await asyncio.to_thread(logger.info, "History managers initialized successfully")
            return True
        except Exception as e:
            await asyncio.to_thread(logger.error, f"Initialization error: {e}")
            return False

    async def search_history(
        self,
        history_type: str,
        user_id: str,
        filters: Optional[Dict] = None,
        sort_by: Optional[str] = None,
        sort_asc: bool = True,
        offset: int = 0,
        limit: int = 1000
    ) -> dict:
        filters = filters or {}
        filters['user_id'] = user_id
        try:
            if history_type == 'OʀᴅᴇʀIᴅ':
                return await self.order_mgr.get_order_data(order_id=filters['order_id'])
            elif history_type == 'Oʀᴅᴇʀ':
                filters.setdefault('order_status', ['COMPLETED', 'PROCESSING', 'PENDING'])
                return await self.order_mgr.search_orders_advanced(filters, sort_by, sort_asc, offset, limit)
            elif history_type == 'Dᴇᴘᴏsɪᴛ':
                filters.setdefault('deposit_status', ['COMPLETED', 'PROCESSING'])
                return await self.deposit_mgr.search_deposits_advanced(filters, sort_by, sort_asc, offset, limit)
            elif history_type == 'Aʟʟ':
                order_task = asyncio.create_task(self.order_mgr.search_orders_advanced(
                    {**filters, 'order_status': ['COMPLETED', 'PROCESSING', 'PENDING']},
                    sort_by='recorded_at', sort_asc=False, offset=0, limit=1000
                ))
                deposit_task = asyncio.create_task(self.deposit_mgr.search_deposits_advanced(
                    {**filters, 'deposit_status': ['COMPLETED', 'PROCESSING']},
                    sort_by='recorded_at', sort_asc=False, offset=0, limit=1000
                ))
                order_result, deposit_result = await asyncio.gather(order_task, deposit_task)

                if not order_result.get('response') or not deposit_result.get('response'):
                    error_msg = order_result.get('error', deposit_result.get('error', 'Unknown error'))
                    return {'response': False, 'error': f'Search failed: {error_msg}'}

                combined = order_result.get('results', []) + deposit_result.get('results', [])
                combined.sort(key=lambda x: float(x.get('recorded_at', 0)), reverse=not sort_asc)
                results = combined[offset : offset + limit]
                return {'response': True, 'results': results}
            else:
                return {'response': False, 'error': 'Invalid history type'}
        except Exception as e:
            logger.error(f"History search error: {e}")
            return {'response': False, 'error': str(e)}

    async def _get_history_stats(
        self,
        user_id: str,
        order_filters: Optional[Dict] = None,
        deposit_filters: Optional[Dict] = None
    ) -> dict:
        """Get weekly history statistics for a user."""
        if order_filters is None:
            order_filters = {}
        if deposit_filters is None:
            deposit_filters = {}

        now = datetime.now()
        start_date = now - timedelta(days=7)
        start_timestamp = start_date.timestamp()
        end_timestamp = now.timestamp()
        
        order_filters = {
            'recorded_at': (start_timestamp, end_timestamp),
            'order_status': ['COMPLETED', 'PROCESSING', 'PENDING'],
            **order_filters
        }
        deposit_filters = {
            'recorded_at': (start_timestamp, end_timestamp),
            'deposit_status': ['COMPLETED', 'PROCESSING'],
            **deposit_filters
        }

        order_task = asyncio.create_task(self.search_history('Oʀᴅᴇʀ', user_id, order_filters))
        deposit_task = asyncio.create_task(self.search_history('Dᴇᴘᴏsɪᴛ', user_id, deposit_filters))
        orders, deposits = await asyncio.gather(order_task, deposit_task)

        return {
            'purchases': orders.get('total_orders', 0),
            'deposits': deposits.get('total_deposits', 0),
            'order_amount': sum(float(o.get('order_amount', 0)) for o in orders.get('results', [])),
            'deposit_amount': sum(float(d.get('deposit_amount', 0)) for d in deposits.get('results', []))
        }
    
    async def _get_cached_keyboard(self, order_info: Dict, is_timeout: bool, order_id: str) -> InlineKeyboardMarkup:
        """Asynchronous, non-blocking keyboard creation with order ID validation"""
        try:
            status = order_info.get('order_status', 'unknown').upper()
            valid_status = status if status in ['PENDING', 'PROCESSING', 'COMPLETED'] else 'unknown'
            barcode_id = await encode_order_id(int(order_id))

            keyboard = InlineKeyboardMarkup()
            buy_again_btn = InlineKeyboardButton(
                "↻ Bᴜʏ Aɢᴀɪɴ",
                callback_data=f"purchase:{order_info.get('app_id', '')}:{order_info.get('order_amount', '')}:{order_info.get('server_id', '')}:{order_info.get('country_id', '')}:{order_info.get('country_code', '')}"
            )
        
            if is_timeout:
                if valid_status == 'PENDING':
                    keyboard.row(
                        InlineKeyboardButton("⌕ Cʜᴀɴɢᴇ Cᴏᴜɴᴛʀʏ", switch_inline_query_current_chat=f"#AᴘᴘIᴅ:{order_info.get('app_id', '')} "),
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
                        InlineKeyboardButton("✘ Cᴀɴᴄᴇʟ", switch_inline_query_current_chat="#SᴛᴀᴛᴜsCᴀɴᴄᴇʟ"),
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
            logger.error(f"Keyboard fallback: {str(e)}")
            return InlineKeyboardMarkup(row_width=1).add(
                InlineKeyboardButton("❌ Error - Contact Support", url="t.me/your_support")
            )
    
    async def create_calendar(
        self,
        year: int,
        month: int,
        start_date: str | None = None,
        end_date: str | None = None
    ) -> InlineKeyboardMarkup:
        # Reset identical start/end
        if start_date and end_date and start_date == end_date:
            start_date = end_date = None

        today = date.today()
        first_of_month = date(year, month, 1)
        last_of_month = date(year, month, calendar.monthrange(year, month)[1])
        prev_month = first_of_month - timedelta(days=1)
        next_month = last_of_month + timedelta(days=1)

        allow_prev = (prev_month.year, prev_month.month) >= (self.MIN_DATE.year, self.MIN_DATE.month)
        allow_next = (next_month.year, next_month.month) <= (today.year, today.month)

        # Inline query search prefix
        search_prefix = '#Hɪsᴛᴏʀʏ-Aʟʟ'
        if start_date and end_date:
            search_query = f'{search_prefix} {start_date}|{end_date}'
        elif start_date or end_date:
            single = start_date or end_date
            search_query = f'{search_prefix} {single}'
        else:
            search_query = f'{search_prefix}'
        search_query = search_query.translate(await small_caps())

        markup = InlineKeyboardMarkup(row_width=7)
        # Header row
        title = f'📅 Calendar – {calendar.month_name[month]} {year}'.translate(await small_caps())
        markup.add(InlineKeyboardButton(text=title, callback_data='date_picker:ignore'))
        weekdays = ['Mᴏɴ','Tᴜᴇ','Wᴇᴅ','Tʜᴜ','Fʀɪ','Sᴀᴛ','Sᴜɴ']
        markup.add(*[InlineKeyboardButton(text=d, callback_data='date_picker:ignore') for d in weekdays])

        # Days grid
        weeks = calendar.monthcalendar(year, month)
        if len(weeks) == 5:
            weeks.append([0]*7)
        for week in weeks:
            row_buttons = []
            for day in week:
                if day == 0:
                    row_buttons.append(InlineKeyboardButton(' ', callback_data='date_picker:ignore'))
                    continue
                ds = f'{year:04d}-{month:02d}-{day:02d}'
                current = date(year, month, day)
                if current < self.MIN_DATE:
                    text = f'{day}'.translate(await subscript_small_caps())
                    cb = 'date_picker:NO-PREV'
                elif current > today:
                    text = f'{day}'.translate(await subscript_small_caps())
                    cb = 'date_picker:NO-NEXT'
                else:
                    # selection styling
                    if start_date and not end_date and ds == start_date:
                        disp = get_circled_number(int(day))
                    elif start_date and end_date:
                        if ds == start_date:
                            disp = f'»{day}'
                        elif ds == end_date:
                            disp = f'{day}«'
                        elif start_date < ds < end_date:
                            disp = '○'
                        else:
                            disp = str(day)
                    else:
                        disp = str(day)
                    text = disp.translate(await small_caps())
                    cb = f'date_picker:DAY:{ds}'
                row_buttons.append(InlineKeyboardButton(text=text, callback_data=cb))
            markup.add(*row_buttons)

        # Action row
        buttons: list[InlineKeyboardButton] = []
        if start_date and end_date:
            buttons.append(InlineKeyboardButton('✗ Rᴇsᴇᴛ Dᴀᴛᴇs', callback_data='date_picker:CLEAR'))
            buttons.append(InlineKeyboardButton(
                '🔍 Sᴇᴀʀᴄʜ Hɪsᴛᴏʀʏ', switch_inline_query_current_chat=search_query
            ))
        elif start_date or end_date:
            if allow_prev:
                pt = '❮ Pʀᴇᴠɪᴏᴜs Dᴀᴛᴇ' if not allow_next else '❮❮❮'
                buttons.append(InlineKeyboardButton(pt, callback_data=f'date_picker:PREV:{prev_month.year}-{prev_month.month}'))
            sl = '🔍 Sᴇᴀʀᴄʜ Hɪsᴛᴏʀʏ' if (not allow_prev or not allow_next) else '🔍 Sᴇᴀʀᴄʜ'
            buttons.append(InlineKeyboardButton(sl, switch_inline_query_current_chat=search_query))
            if allow_next:
                nt = 'Aғᴛᴇʀ ❯❯❯' if not allow_prev else '❯❯❯'
                buttons.append(InlineKeyboardButton(nt, callback_data=f'date_picker:NEXT:{next_month.year}-{next_month.month}'))
        else:
            if not allow_prev and allow_next:
                buttons.append(InlineKeyboardButton('🔍 Sᴇᴀʀᴄʜ Hɪsᴛᴏʀʏ', switch_inline_query_current_chat=search_query))
                buttons.append(InlineKeyboardButton('Aғᴛᴇʀ ❯❯❯', callback_data=f'date_picker:NEXT:{next_month.year}-{next_month.month}'))
            elif allow_prev and not allow_next:
                buttons.append(InlineKeyboardButton('❮ Pʀᴇᴠɪᴏᴜs Dᴀᴛᴇ', callback_data=f'date_picker:PREV:{prev_month.year}-{prev_month.month}'))
                buttons.append(InlineKeyboardButton('🔍 Sᴇᴀʀᴄʜ Hɪsᴛᴏʀʏ', switch_inline_query_current_chat=search_query))
            else:
                if allow_prev:
                    buttons.append(InlineKeyboardButton('❮❮❮', callback_data=f'date_picker:PREV:{prev_month.year}-{prev_month.month}'))
                buttons.append(InlineKeyboardButton('🔍 Sᴇᴀʀᴄʜ', switch_inline_query_current_chat=search_query))
                if allow_next:
                    buttons.append(InlineKeyboardButton('❯❯❯', callback_data=f'date_picker:NEXT:{next_month.year}-{next_month.month}'))
        markup.add(*buttons)
        return markup


    async def _acquire_transaction_lock(self, guard, transaction_key, input_data) -> bool:
        """Acquire transaction lock with error handling."""
        if not await guard.acquire_lock(transaction_key):
            try:
                if isinstance(input_data, CallbackQuery):
                    await self.bot.answer_callback_query(
                        input_data.id,
                        "🔒 Aɴᴏᴛʜᴇʀ Tʀᴀɴsᴀᴄᴛɪᴏɴ Iɴ Pʀᴏɢʀᴇss, Pʟᴇᴀsᴇ Wᴀɪᴛ...", 
                        show_alert=False
                    )
                else:
                    await self.bot.send_message(
                        input_data.chat.id,
                        "🔒 Aɴᴏᴛʜᴇʀ Tʀᴀɴsᴀᴄᴛɪᴏɴ Iɴ Pʀᴏɢʀᴇss, Pʟᴇᴀsᴇ Wᴀɪᴛ...",
                        parse_mode='html'
                    )
            except Exception as e:
                print(f"Error sending message: {e}")
            return False
        return True

    async def _show_loading_animation(self, call: CallbackQuery, chat_id: int, message_id: int, keyboard: InlineKeyboardMarkup) -> None:
        """Asynchronously display loading animation during data processing"""
        try:
            keyboard.row(
                InlineKeyboardButton("🔙 Bᴀᴄᴋ Tᴏ Mᴀɪɴ", callback_data='start'),
                InlineKeyboardButton("📅 Dᴀᴛᴇ Pɪᴄᴋᴇʀ", callback_data='date_picker')
            )
            caption = (
                "🔥 <b>Fʟᴀsʜ Tʀᴀɴsᴀᴄᴛɪᴏɴ Hɪsᴛᴏʀʏ 》</b>\n\n"
                "<b> ○ <u>Tʜɪs Wᴇᴇᴋ</u> ❯</b>\n"
                f"💰 <b>Pᴜʀᴄʜᴀsᴇs  »</b>  <code>0</code> <code>Oʀᴅᴇʀ</code>\n"
                f"📊 <b>Sᴘᴇɴᴅ  »</b>  <code>0.00</code> 💎  〚$ <code>0.00</code>〛\n"
                f"📈 <b>Dᴇᴘᴏsɪᴛs  »</b>  <code>0.00</code> 💎  〚$ <code>0.00</code>〛\n\n"
                "🏛️ <b>Yᴏᴜ Cᴀɴ Sᴇᴀʀᴄʜ Yᴏᴜʀ Tʀᴀɴsᴀᴄᴛɪᴏɴs Bʏ Dᴀᴛᴇ Aɴᴅ Tʏᴘᴇ. Tʜɪs Wɪʟʟ Hᴇʟᴘ Yᴏᴜ Eᴀsɪʟʏ Aɴᴀʟʏᴢᴇ Yᴏᴜʀ Fᴜᴛᴜʀᴇ Fɪɴᴀɴᴄᴇs..</b>"
            )
            await self.bot.edit_message_media(
                media=InputMediaVideo(
                    media=LOADING_GIF, 
                    caption=caption,
                    parse_mode="HTML"
                ),
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=keyboard
            )
        except Exception as e:
            logger.error(f"Error displaying loading animation: {e}")
    
    async def handle_history(self, call: CallbackQuery):
        """Handle the history interface and display transaction stats asynchronously."""
        try:
            user_id = str(call.from_user.id)
            message_id = call.message.message_id
            chat_id = call.message.chat.id
            transaction_key = RedisKeys.transaction_lock_key(chat_id, f"show_history:main")
            async with TransactionGuard(self.redis_client) as guard:
                if not await self._acquire_transaction_lock(guard, transaction_key, call):
                    return
                try:
                    keyboard = InlineKeyboardMarkup()
                    keyboard.row(
                        InlineKeyboardButton("🛒 Oʀᴅᴇʀ", switch_inline_query_current_chat='#Hɪsᴛᴏʀʏ-Oʀᴅᴇʀ'),
                        InlineKeyboardButton("⌕ Aʟʟ Hɪsᴛᴏʀʏ", switch_inline_query_current_chat='#Hɪsᴛᴏʀʏ-Aʟʟ'),
                        InlineKeyboardButton("💰 Dᴇᴘᴏsɪᴛ", switch_inline_query_current_chat='#Hɪsᴛᴏʀʏ-Dᴇᴘᴏsɪᴛ')
                    )

                    async def fetch_data():
                        return await asyncio.gather(
                            self._show_loading_animation(call, chat_id, message_id, keyboard),
                            self._get_history_stats(user_id)
                        )

                    # Run loading animation and history stats concurrently.
                    _, stats = await fetch_data()

                    keyboard = InlineKeyboardMarkup()
                    keyboard.row(
                        InlineKeyboardButton("🛒 Oʀᴅᴇʀ", switch_inline_query_current_chat='#Hɪsᴛᴏʀʏ-Oʀᴅᴇʀ'),
                        InlineKeyboardButton("⌕ Aʟʟ Hɪsᴛᴏʀʏ", switch_inline_query_current_chat='#Hɪsᴛᴏʀʏ-Aʟʟ'),
                        InlineKeyboardButton("💰 Dᴇᴘᴏsɪᴛ", switch_inline_query_current_chat='#Hɪsᴛᴏʀʏ-Dᴇᴘᴏsɪᴛ')
                    )
                    keyboard.row(
                            InlineKeyboardButton("🔙 Bᴀᴄᴋ Tᴏ Mᴀɪɴ", callback_data='start'),
                        InlineKeyboardButton("📅 Dᴀᴛᴇ Pɪᴄᴋᴇʀ", callback_data='date_picker:OPEN')
                    )

                    caption = (
                        "🔥 <b>Fʟᴀsʜ Tʀᴀɴsᴀᴄᴛɪᴏɴ Hɪsᴛᴏʀʏ 》</b>\n\n"
                        "<b> ○ <u>Tʜɪs Wᴇᴇᴋ</u> ❯</b>\n"
                        f"💰 <b>Pᴜʀᴄʜᴀsᴇs  »</b>  <code>{stats['purchases']}</code> <code>Oʀᴅᴇʀ{'s' if stats['purchases'] > 1 else ''}</code>\n"
                        f"📊 <b>Sᴘᴇɴᴅ  »</b>  <code>{stats['order_amount']:.2f}</code> 💎  〚$ <code>0.00</code>〛\n"
                        f"📈 <b>Dᴇᴘᴏsɪᴛs  »</b>  <code>{stats['deposit_amount']:.2f}</code> 💎  〚$ <code>0.00</code>〛\n\n"
                        "🏛️ <b>Yᴏᴜ Cᴀɴ Sᴇᴀʀᴄʜ Yᴏᴜʀ Tʀᴀɴsᴀᴄᴛɪᴏɴs Bʏ Dᴀᴛᴇ Aɴᴅ Tʏᴘᴇ. Tʜɪs Wɪʟʟ Hᴇʟᴘ Yᴏᴜ Eᴀsɪʟʏ Aɴᴀʟʏᴢᴇ Yᴏᴜʀ Fᴜᴛᴜʀᴇ Fɪɴᴀɴᴄᴇs..</b>"
                    )

                    async def update_message():
                        try:
                            await self.bot.edit_message_media(
                                media=InputMediaPhoto(
                                    media='https://i.postimg.cc/HLWC80bf/20240628-092309.jpg',
                                    caption=caption,
                                    parse_mode='HTML'
                                ),
                                chat_id=chat_id,
                                message_id=message_id,
                                reply_markup=keyboard
                            )
                        except Exception as e:
                            logger.error(f"Error updating message: {e}")
                            await self.bot.answer_callback_query(call.id, "❌ Failed to update history message", show_alert=True)

                    await update_message()
                except Exception as e:
                    print(f"Error processing buy command: {e}")
                    await self.bot.send_message(chat_id, "🚫 Eʀʀᴏʀ Gᴇɴᴇʀᴀᴛɪɴɢ Rᴇǫᴜᴇsᴛ.")
                    return
                finally:
                    await guard.release_lock(transaction_key)
        except Exception as e:
            logger.error(f"History handler error: {e}")
            await self.bot.answer_callback_query(call.id, "❌ Failed to load history", show_alert=True)


history_manager = HistoryManager()

async def init_managers(order_manager: OrderManagement, user_manager: UserManagement, bot: AsyncTeleBot) -> bool:
    """
    Initialize the history management system asynchronously.
    Note: It is assumed that the bot instance has an attribute `deposit_manager` for deposit operations.
    """
    deposit_mgr = bot.deposit_manager
    return await history_manager.init_managers(order_manager, user_manager, deposit_mgr, bot)


async def register_handlers(bot: AsyncTeleBot) -> None:
    """Register history-related bot handlers asynchronously."""

    @bot.callback_query_handler(func=lambda call: call.data.startswith("USER:HISTORY"))
    async def history_callback_handler(call: CallbackQuery):
        try:
            process_task = partial(history_manager.handle_history, call)
            asyncio.create_task(process_task())
        except ValueError:
            asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", show_alert=True))
        except Exception as e:
            #logging.error(f"Callback error: {e}")
            asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ...", show_alert=True))

    @bot.inline_handler(func=lambda query: query.query.startswith('#Hɪsᴛᴏʀʏ-'))
    async def handle_history_inline(inline_query):
        user_id = str(inline_query.from_user.id)
        query_parts = inline_query.query.split('#')

        try:
            if len(query_parts) < 2:
                return
            # Extract main part: e.g., "Hɪsᴛᴏʀʏ-Aʟʟ 2025-06-12|2025-06-20"
            main_part = query_parts[1].strip()
            action_and_date = main_part.split(" ", 1)

            history_type = action_and_date[0].split('-')[1].strip()  # e.g. Aʟʟ
            date_input = action_and_date[1].strip() if len(action_and_date) > 1 else None

        except Exception as e:
            logger.error(f"Error processing query: {e}")
            return

        filters = {'user_id': user_id}


        # Parse optional user_id and deposit_status filters from inline query
        if '@user_id:' in inline_query.query:
            user_id = inline_query.query.split('@user_id:')[1].split()[0]
            filters['user_id'] = user_id

        if '@deposit_status:' in inline_query.query:
            deposit_status = inline_query.query.split('@deposit_status:')[1].split()[0]
            filters['deposit_status'] = deposit_status.strip('()').split('|')

        # Add recorded_at range filter if date input is present
        if date_input:
            try:
                start_ts, end_ts = date_to_unix(str(date_input).translate(await large_caps()))
                if start_ts and end_ts:
                    filters['recorded_at'] = (start_ts, end_ts)
            except Exception as e:
                logger.error(f"Invalid date format in inline query: {e}")
                

        result = await history_manager.search_history(
            history_type=history_type,
            user_id=user_id,
            filters=filters,
            sort_by='recorded_at',
            sort_asc=False,
            offset=int(inline_query.offset or 0),
            limit=RESULT_LIMIT
        )
        if not result.get('response'):
            return await bot.answer_inline_query(inline_query.id, [])
        country_data = await redis_manager.redis_client.json().get('main_data:details:country_data') or {}
        inline_results = []
        deposit_keyboard = InlineKeyboardMarkup()
        deposit_keyboard.row(
            InlineKeyboardButton("🛒 Bᴜʏ", switch_inline_query_current_chat=''),
            InlineKeyboardButton("↻ Dᴇᴘᴏsɪᴛ", callback_data="USER:DEPOSIT")
        )
        key = "image_data:country-service"
        redis_client = await redis_manager.get_client()
        link_data = await redis_client.hgetall(key)
        for idx, item in enumerate(result['results'], 1):
            if item["id"].startswith("order_data"):
                recorded_at = float(item.get('recorded_at', 0))
                app_name = item.get('app_name', '')
                sms_list = json.loads(item.get('sms_list', '[]'))
                
                country_code = item.get('country_code', '')
                country_id = item.get('country_id', '')
                country_name = country_data.get(country_id, {}).get('country_name', '').translate(await small_caps())
                order_status = item.get('order_status', '')
                order_amount = float(item.get('order_amount', 0))
                
                app_id = item.get('app_id', '')
                server_id = item.get('server_id', '')
                order_id = item["id"].split(":")[-1] if item["id"].startswith("order_data:info:") else ''
                order_number = json.loads(item.get('order_number', '[]'))
                sms_list = [s.strip("'")[:10] + (",..." if len(s) > 10 else '') for s in sms_list]
                sms = "Nᴏᴛ Rᴇᴄᴇɪᴠᴇᴅ" if not sms_list else ", ".join(sms_list[:3] + (["..."] if len(sms_list) > 3 else []))
                thumbnail_url = link_data.get(f"{country_id}-{app_id}", "https://i.postimg.cc/13PMXbT7/Pngtree-hourglass-waiting-for-mouse-pointer-5453296.png")
                status = "⏳" if order_status == "PENDING" else "⌛" if order_status == "PROCESSING" else "✅" if order_status == "COMPLETED" else "🛑"
                order_status = "Aᴄᴛɪᴠᴇ" if order_status == "PENDING" else "Pʀᴏᴄᴇssɪɴɢ" if order_status == "PROCESSING" else "Cᴏᴍᴘʟᴇᴛᴇᴅ" if order_status == "COMPLETED" else "Iɴᴀᴄᴛɪᴠᴇ"
                order_at = time_ago(recorded_at)
                app = app_name.translate(await small_caps())
                title = f"{app} 💎 {order_amount:.2f} [{country_code}] [{server_id}]"
                description = (
                    f"📞 Nᴜᴍʙᴇʀ   » {order_number[0] if order_number else 'N/A'} {order_number[1] if len(order_number) > 1 else ''}\n"
                    f"💬 Sᴍs Lɪsᴛ  » {sms}\n"
                    f"{status} Oʀᴅᴇʀ Aᴛ » {order_at}..."
                )
                barcode_id = await encode_order_id(order_id)
                if len(sms_list) > 2:
                    text = "<code>" + "</code>\n<code>          </code><b>•</b> <code>".join(sms_list) + "</code>"
                    sms_section = f"<blockquote expandable>💬 <b>Sᴍs Lɪsᴛ »</b> {text}</blockquote>\n\n"
                elif len(sms_list) == 2:
                    sms_section = f"💬 <b>Sᴍs Lɪsᴛ »</b> <code>{sms_list[0]}</code><code>,</code> <code>{sms_list[1]}</code>\n\n"
                elif len(sms_list) == 1:
                    sms_section = f"💬 <b>Sᴍs Lɪsᴛ »</b> <code>{sms_list[0]}</code>\n\n"
                else:
                    sms_section = f"💬 <b>Sᴍs Lɪsᴛ »</b> <code>{sms}</code>\n\n"
                message_text = (
                    f"📜 <b>Oʀᴅᴇʀ Hɪsᴛᴏʀʏ</b> <code>[</code> <code>{app}</code> <code>]</code>\n\n"
                    f"📦 <b>Bᴀʀ-Cᴏᴅᴇ »</b> <code>{barcode_id}</code>\n"
                    f"{status} <b>Sᴛᴀᴛᴜs »</b> <code>{order_status}</code>\n\n"
                    f"💎 <b>Aᴍᴏᴜɴᴛ »</b> <code>{order_amount:.2f}</code> <code>Pᴏɪɴᴛs</code>\n"
                    f"🌍 <b>Rᴇɢɪᴏɴ »</b> <code>{country_name}</code> <b>[</b> <code>{country_code}</code> <b>]</b>\n\n"
                    f"📞 <b>Nᴜᴍʙᴇʀ »</b> <code>{order_number[0]}</code> <code>{order_number[1]}</code>\n"
                    f"{sms_section}"
                    f"🗓️ <b>Oʀᴅᴇʀ Tɪᴍᴇ »</b> <code>{order_at}</code>"
                )
                inline_results.append(InlineQueryResultArticle(
                    id=str(idx),
                    title=title,
                    description=description,
                    thumbnail_url=thumbnail_url,
                    input_message_content=InputTextMessageContent(message_text=message_text, parse_mode="HTML"),
                    reply_markup=await history_manager._get_cached_keyboard(item, is_timeout=False, order_id=order_id)
                ))
            elif item["id"].startswith("deposit_data"):
                recorded_at = float(item.get('recorded_at', 0))
                deposit_id = int(item.get('deposit_id', 0))
                method = item.get('method', 'Uᴘɪ')
                deposit_amount = float(item.get('deposit_amount', 0))
                deposit_status = item.get('deposit_status', 'UNKNOWN').upper()
                status_map = {
                    "PENDING": "Aᴄᴛɪᴠᴇ",
                    "PROCESSING": "Pʀᴏᴄᴇssɪɴɢ",
                    "COMPLETED": "Cᴏᴍᴘʟᴇᴛᴇᴅ"
                }
                deposit_status = status_map.get(deposit_status, "Iɴᴀᴄᴛɪᴠᴇ")
                deposit_time = time_ago(recorded_at)
                
                title = f"Dᴇᴘᴏsɪᴛ Hɪsᴛᴏʀʏ [{method}]"
                description = (
                    f"💰 Dᴇᴘᴏsɪᴛ Iᴅ ❯ {deposit_id}\n"
                    f"💎 Aᴍᴏᴜɴᴛ ❯ {deposit_amount:.2f} Pᴏɪɴᴛs\n"
                    f"🗓️ Dᴇᴘᴏsɪᴛ Tɪᴍᴇ ❯ {deposit_time}..."
                )
                thumbnail_url = "https://i.ibb.co/Y4sY9N6h/20250302-230204.png"
                message_text = (
                    f"📜 <b>Dᴇᴘᴏsɪᴛ Hɪsᴛᴏʀʏ</b> <code>[</code> <code>{method}</code> <code>]</code>\n\n"
                    f"📦 <b>Dᴇᴘᴏsɪᴛ Iᴅ »</b> <code>{deposit_id}</code>\n"
                    f"✅ <b>Sᴛᴀᴛᴜs »</b> <code>{deposit_status}</code>\n\n"
                    f"💎 <b>Aᴍᴏᴜɴᴛ »</b> <code>{deposit_amount:.2f}</code> <code>Pᴏɪɴᴛs</code>\n"
                    f"🗓️ <b>Dᴇᴘᴏsɪᴛ Tɪᴍᴇ »</b> <code>{deposit_time}</code>"
                )
                inline_results.append(InlineQueryResultArticle(
                    id=str(idx),
                    title=title,
                    description=description,
                    thumbnail_url=thumbnail_url,
                    input_message_content=InputTextMessageContent(
                        message_text=message_text,
                        parse_mode="HTML"
                    ),
                    reply_markup=deposit_keyboard
                ))

        if not inline_query.offset:
            if filters.get("recorded_at"):
                start_timestamp, end_timestamp = filters["recorded_at"]
                data = await history_manager.aggregator.get_user(user_id, start_timestamp=start_timestamp, end_timestamp=end_timestamp)
            else:
                data = await history_manager.aggregator.get_user(user_id)
            if data and data.get("response"):
                user_profile = data.get("user_profile")
                current_balance = data["metrics"]["current_balance"]
                spend_balance = data["metrics"]["spend_balance"]
                total_deposits = data["metrics"]["deposits"]["total_amount"]
                total_orders = data["metrics"]["orders"]["total_amount"]
                timestamp = data["timestamp"]

                summary_map = {
                    "Aʟʟ": (f"🛒 Tᴏᴛᴀʟ ❯ {data['metrics']['orders']['count']} Oʀᴅᴇʀ{'s' if data['metrics']['orders']['count'] != 1 else ''} [💎 {total_orders:.2f}]\n"
                            f"💰 Tᴏᴛᴀʟ ❯ {data['metrics']['deposits']['count']} Dᴇᴘᴏsɪᴛ{'s' if data['metrics']['deposits']['count'] != 1 else ''} [💎 {total_deposits:.2f}]"),
                    "Oʀᴅᴇʀ": (f"🛒 Tᴏᴛᴀʟ Oʀᴅᴇʀs ❯ {data['metrics']['orders']['count']} Oʀᴅᴇʀ{'s' if data['metrics']['orders']['count'] != 1 else ''}\n"
                              f"💰 Tᴏᴛᴀʟ Aᴍᴏᴜɴᴛ ❯ {total_orders:.2f} Pᴏɪɴᴛ{'s' if total_orders != 1 else ''}"),
                    "Dᴇᴘᴏsɪᴛ": (f"💰 Tᴏᴛᴀʟ Dᴇᴘᴏsɪᴛs ❯ {data['metrics']['deposits']['count']} Dᴇᴘᴏsɪᴛ{'s' if data['metrics']['deposits']['count'] != 1 else ''}\n"
                            f"💰 Tᴏᴛᴀʟ Aᴍᴏᴜɴᴛ ❯ {total_deposits:.2f} Pᴏɪɴᴛ{'s' if total_deposits != 1 else ''}")
                }
                summary_result = InlineQueryResultArticle(
                    id="summary",
                    title=f"{'🛍️ Oʀᴅᴇʀ & Dᴇᴘᴏsɪᴛ Hɪsᴛᴏʀʏ' if history_type == 'Aʟʟ' else '💎 ' + history_type.capitalize() + ' Hɪsᴛᴏʀʏ'}",
                    description=summary_map.get(history_type, ""),
                    input_message_content=InputTextMessageContent("/Buy_"),
                    thumbnail_url="https://i.postimg.cc/JhdcD1S6/ainvoice.png"
                )
                inline_results.insert(0, summary_result)

        next_offset = str(int(inline_query.offset or 0) + RESULT_LIMIT) if len(inline_results) >= RESULT_LIMIT else ""
        await bot.answer_inline_query(
            inline_query.id,
            results=inline_results,
            cache_time=0,
            next_offset=next_offset
        )
    
    @bot.callback_query_handler(func=lambda call: call.data.startswith('date_picker:'))
    async def handle_query(call: CallbackQuery):
        data = call.data.removeprefix('date_picker:')
        cid = call.message.chat.id
        mid = call.message.message_id
        state = history_manager.SELECTIONS.setdefault(cid, {'start': None, 'end': None})
        start, end = state['start'], state['end']

        if data == 'OPEN':
            now = datetime.now()
            mk = await history_manager.create_calendar(now.year, now.month)
            await bot.answer_callback_query(call.id, text="📅 Cᴀʟᴇɴᴅᴀʀ Iɴɪᴛɪᴀʟɪᴢᴇᴅ – Sᴇʟᴇᴄᴛ Yᴏᴜʀ Dᴀᴛᴇ Rᴀɴɢᴇ")
            await asyncio.gather(
                bot.send_message(
                    chat_id=cid,
                    text=f"{history_manager.HEADER_TEXT_HTML}",
                    parse_mode='HTML',
                    reply_markup=mk,
                    disable_web_page_preview=False
                ),
                bot.delete_message(cid, mid)
            )
            history_manager.SELECTIONS[cid] = {'start': None, 'end': None}

        elif data.startswith('DAY:'):
            date_str = data.split(':', 1)[1]
            if not start or (start and end):
                state['start'], state['end'] = date_str, None
                await bot.answer_callback_query(call.id, text=f"🟢 Sᴛᴀʀᴛ Dᴀᴛᴇ Sᴇʟᴇᴄᴛᴇᴅ – {date_str}")
            else:
                if date_str < start:
                    state['start'], date_str = date_str, start
                state['end'] = date_str
                await bot.answer_callback_query(call.id, text=f"🔴 Eɴᴅ Dᴀᴛᴇ Cᴏɴғɪʀᴍᴇᴅ – {date_str}")
            y, m = map(int, date_str.split('-')[:2])
            mk = await history_manager.create_calendar(y, m, state['start'], state['end'])
            await bot.edit_message_text(
                chat_id=cid,
                message_id=mid,
                text=f"{history_manager.HEADER_TEXT_HTML}",
                parse_mode='HTML',
                reply_markup=mk,
                disable_web_page_preview=False
            )

        elif data.startswith('PREV:') or data.startswith('NEXT:'):
            _, ym = data.split(':', 1)
            y, m = map(int, ym.split('-'))
            mk = await history_manager.create_calendar(y, m, state.get('start'), state.get('end'))
            await bot.edit_message_text(
                chat_id=cid,
                message_id=mid,
                text=f"{history_manager.HEADER_TEXT_HTML}",
                parse_mode='HTML',
                reply_markup=mk,
                disable_web_page_preview=False
            )
            await bot.answer_callback_query(call.id, text="🔁 Vɪᴇᴡ Uᴘᴅᴀᴛᴇᴅ – Nᴀᴠɪɢᴀᴛɪɴɢ Mᴏɴᴛʜs.")

        elif data == 'CLEAR':
            history_manager.SELECTIONS[cid] = {'start': None, 'end': None}
            now = datetime.now()
            mk = await history_manager.create_calendar(now.year, now.month)
            await bot.edit_message_text(
                chat_id=cid,
                message_id=mid,
                text=f"{history_manager.HEADER_TEXT_HTML}",
                parse_mode='HTML',
                reply_markup=mk,
                disable_web_page_preview=False
            )
            await bot.answer_callback_query(call.id, text="🧹 Sᴇʟᴇᴄᴛɪᴏɴ Rᴇsᴇᴛ – Sᴛᴀʀᴛ Aɢᴀɪɴ Fʀᴇsʜ.")
        elif data == 'NO-NEXT':
            await bot.answer_callback_query(
                call.id,
                text="📅 Yᴏᴜ Cᴀɴɴᴏᴛ Cʜᴏᴏsᴇ Uᴘᴄᴏᴍɪɴɢ Dᴀᴛᴇs – Sᴇʟᴇᴄᴛ A Vᴀʟɪᴅ Dᴀʏ."
            )
        elif data == 'NO-PREV':
            await bot.answer_callback_query(
                call.id,
                text="⏳ Dᴀᴛᴇ Bᴇғᴏʀᴇ Aʟʟᴏᴡᴇᴅ Rᴀɴɢᴇ – Pʟᴇᴀsᴇ Sᴇʟᴇᴄᴛ A Fʀᴏᴍ Dᴀʏ."
            )
        elif data == 'ignore':
            await bot.answer_callback_query(call.id)
        else:
            await bot.answer_callback_query(call.id, text="⚠️ Iɴᴠᴀʟɪᴅ Aᴄᴛɪᴏɴ – Pʟᴇᴀsᴇ Tʀʏ Aɢᴀɪɴ")


    @bot.callback_query_handler(func=lambda call: call.data.startswith("#RᴇғʀᴇsʜMᴇᴛʀɪᴄs"))
    async def refresh_metrics_handler(call: CallbackQuery):
        try:
            user_id = call.data.split(":")[1]
            await bot.answer_callback_query(call.id, "📊 Rᴇғʀᴇsʜɪɴɢ Mᴇᴛʀɪᴄs...")
            
            metrics_result = await history_manager.user_mgr.user_metrics_report(
                bot, "edit_message_text", user_id, "-1002203139746"
            )
            
            if metrics_result is not None:
                await bot.send_message(call.from_user.id, "📊 Mᴇᴛʀɪᴄs Rᴇғʀᴇsʜᴇᴅ Sᴜᴄᴄᴇssғᴜʟʟʏ")
            else:
                await bot.send_message(call.from_user.id, "⚠️ Fᴀɪʟᴇᴅ ᴛᴏ ʀᴇғʀᴇsʜ ᴍᴇᴛʀɪᴄs. Pʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ.")
        except Exception as e:
            logger.error(f"Error in refresh_metrics_handler: {e}")
            await bot.send_message(call.from_user.id, "🚫 Aɴ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ ᴡʜɪʟᴇ ʀᴇғʀᴇsʜɪɴɢ ᴍᴇᴛʀɪᴄs.")

    @bot.inline_handler(func=lambda query: query.query.startswith('#BᴀʀCᴏᴅᴇ-'))
    async def handle_barcode_inline(inline_query):
        logger.info(f"Received inline query: {inline_query.query}")
        user_id = str(inline_query.from_user.id)
        query_parts = inline_query.query.split('-')
        if len(query_parts) < 2:
            logger.error("Invalid query format")
            return
        barcode_id = query_parts[1].split(':')[0].strip()
        order_id = await decode_barcode_id(barcode_id)
        number_images = {
            "1": "https://i.ibb.co/1tFqHRDB/IMG-20250616-001326-425.png",
            "2": "https://i.ibb.co/B5kvxC4h/IMG-20250616-001438-747.png",
            "3": "https://i.ibb.co/XkLW1JMD/IMG-20250616-001509-853.png",
            "4": "https://i.ibb.co/BV4tmnzV/IMG-20250616-001539-153.png",
            "5": "https://i.ibb.co/7Jhkswbx/IMG-20250616-001600-754.png",
            "6": "https://i.ibb.co/vCyntfC0/IMG-20250616-001622-141.png",
            "7": "https://i.ibb.co/vv3673bF/IMG-20250616-001642-217.png",
            "8": "https://i.ibb.co/vx75SQnv/IMG-20250616-001701-946.png",
            "9": "https://i.ibb.co/HjfFzMS/IMG-20250616-001721-317.png",
            "10": "https://i.ibb.co/XrRWwv1N/IMG-20250616-001748-924.png",
            "11": "https://i.ibb.co/v4ytZMhB/IMG-20250616-001829-283.png",
            "12": "https://i.ibb.co/XxYNk92n/IMG-20250616-001854-594.png",
            "13": "https://i.ibb.co/Q7p9RYfL/IMG-20250616-001924-017.png",
            "14": "https://i.ibb.co/hRT1jhgM/IMG-20250616-001947-626.png",
            "15": "https://i.ibb.co/nM36KKm4/IMG-20250616-002014-687.png",
            "16": "https://i.ibb.co/hJCZLSYD/IMG-20250616-002040-979.png",
            "17": "https://i.ibb.co/bgNM03kX/IMG-20250616-002932-998.png",
            "18": "https://i.ibb.co/XkWdhpWs/IMG-20250616-002327-856.png",
            "19": "https://i.ibb.co/tpFKyQNp/IMG-20250616-002929-290.png",
            "20": "https://i.ibb.co/Rp1Btr5P/IMG-20250616-002625-707.png"
        }

        filters = {
            "user_id": user_id,
            "order_id": order_id,
            "order_status": ["COMPLETED", "PROCESSING"]
        }
        if ':' in inline_query.query:
            _, filter_part = inline_query.query.split(':', 1)
            for pair in filter_part.split('&'):
                if '=' in pair:
                    key, val = pair.split('=', 1)
                    if key in ("start", "end"):
                        filters[key] = float(val)
        result = await history_manager.search_history(
            history_type="OʀᴅᴇʀIᴅ",
            user_id=user_id,
            filters=filters,
        )
        if not result.get("response"):
            logger.warning(f"No results found for user {user_id} and order {order_id}")
            return await bot.answer_inline_query(inline_query.id, [])
        order_info = result["result"]
        try:
            order_history = json.loads(order_info.get("order_history", "[]"))
            sms_list = json.loads(order_info.get("sms_list", "[]"))
            order_number = json.loads(order_info.get("order_number", "[]"))
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            order_history, sms_list, order_number = [], [], []
        order_amount = float(order_info.get("order_amount", 0))
        order_amount_display = f"{order_amount:.2f}"
        app_name = order_info.get("app_name", "N/A")
        order_status = order_info.get("order_status", "")
        country_code = order_info.get("country_code", "")
        country_id = order_info.get("country_id", "")
        recorded_at = float(order_info.get("recorded_at", 0))
        server_id = order_info.get("server_id", 0)
        inline_results = []
        sms_count = 0
        country_data = await redis_manager.redis_client.json().get('main_data:details:country_data') or {}

        async def process_event(idx, event):
            nonlocal sms_count, order_amount_display
            event_timestamp = event.get("timestamp", "0")
            event_time = time_ago(event_timestamp)
            event_action = event.get("action", "")
            if "SMS_RECEIVED" in event_action:
                if sms_count == 1:
                    order_amount_display = "Fʀᴇᴇ"
                sms_count += 1
                event_sms = event.get("sms", "N/A")
                suffix = "sᴛ" if sms_count == 1 else "ɴᴅ" if sms_count == 2 else "ʀᴅ" if sms_count == 3 else "ᴛʜ"
                event_title = f"{sms_count}{suffix}. Sᴍs Rᴇᴄɪᴇᴠᴇᴅ [{event_sms}]"
                event_desc = f"💎 Pʀɪᴄᴇ ❯ {order_amount_display}\n⏳ Rᴇᴄɪᴇᴠᴇᴅ Aᴛ {event_time}"
                return InlineQueryResultArticle(
                    id=str(idx),
                    title=event_title,
                    description=event_desc,
                    thumbnail_url=number_images.get(str(sms_count), "https://i.postimg.cc/59q18wJT/image.png"),
                    input_message_content=InputTextMessageContent(
                        message_text=(
                            f"<b>Bᴀʀ-Cᴏᴅᴇ:</b> <code>{barcode_id}</code>\n"
                            f"<b>Eᴠᴇɴᴛ:</b> {event_title}\n\n"
                            f"<b>💎 Pʀɪᴄᴇ ❯</b> <code>{order_amount_display}</code>\n"
                            f"<b>⏳ Rᴇᴄɪᴠᴇᴅ Aᴛ</b> {event_time}"
                        ),
                        parse_mode="HTML"
                    ),
                    reply_markup=await history_manager._get_cached_keyboard(event, is_timeout=False, order_id=order_id)
                )
            return None

        tasks = [process_event(idx, event) for idx, event in enumerate(order_history, start=1)]
        results = await asyncio.gather(*tasks)
        inline_results = [result for result in results if result is not None]

        if not inline_query.offset:
            description = await asyncio.to_thread(lambda: (
                f"📞 Nᴜᴍʙᴇʀ   » {order_number[0] if order_number else 'N/A'} {order_number[1] if len(order_number) > 1 else ''}\n"
                f"⚡ Oʀᴅᴇʀ Bᴜʏᴇᴅ Aᴛ {time_ago(recorded_at)}\n"
                f"💬 Tᴏᴛᴀʟ Sᴍs Rᴇᴄɪᴇᴠᴇᴅ ❯ {sms_count} Sᴍs{'s' if sms_count > 1 else ''}"
            ))
            country_name = country_data.get(country_id, {}).get('country_name', '').translate(await small_caps())
            order_at = time_ago(recorded_at)
            status = "⏳" if order_status == "PENDING" else "⌛" if order_status == "PROCESSING" else "✅" if order_status == "COMPLETED" else "🛑"

            if len(sms_list) > 2:
                text = "<code>" + "</code>\n<code>          </code><b>•</b> <code>".join(sms_list) + "</code>"
                sms_section = f"<blockquote expandable>💬 <b>Sᴍs Lɪsᴛ »</b> {text}</blockquote>\n\n"
            elif len(sms_list) == 2:
                sms_section = f"💬 <b>Sᴍs Lɪsᴛ »</b> <code>{sms_list[0]}</code><code>,</code> <code>{sms_list[1]}</code>\n\n"
            elif len(sms_list) == 1:
                sms_section = f"💬 <b>Sᴍs Lɪsᴛ »</b> <code>{sms_list[0]}</code>\n\n"
            else:
                sms_section = "💬 <b>Sᴍs Lɪsᴛ »</b> <code>N/A</code>\n\n"
            message_text = (
                    f"📜 <b>Oʀᴅᴇʀ Hɪsᴛᴏʀʏ</b> <code>[</code> <code>{app_name.translate(await small_caps())}</code> <code>]</code>\n\n"
                    f"📦 <b>Bᴀʀ-Cᴏᴅᴇ »</b> <code>{barcode_id}</code>\n"
                    f"{status} <b>Sᴛᴀᴛᴜs »</b> <code>{order_status}</code>\n\n"
                    f"💎 <b>Aᴍᴏᴜɴᴛ »</b> <code>{order_amount_display}</code> <code>Pᴏɪɴᴛs</code>\n"
                    f"🌍 <b>Rᴇɢɪᴏɴ »</b> <code>{country_name}</code> <b>[</b> <code>{country_code}</code> <b>]</b>\n\n"
                    f"📞 <b>Nᴜᴍʙᴇʀ »</b> <code>{order_number[0]}</code> <code>{order_number[1]}</code>\n"
                    f"{sms_section}"
                    f"🗓️ <b>Oʀᴅᴇʀ Tɪᴍᴇ »</b> <code>{order_at}</code>"
                )
            summary_result = InlineQueryResultArticle(
                id="summary",
                title=f"🛍️ Oʀᴅᴇʀ Sᴍs Hɪsᴛᴏʀʏ [{app_name.translate(await small_caps())}]",
                description=description,
                input_message_content=InputTextMessageContent(message_text=message_text, parse_mode="HTML"),
                thumbnail_url="https://i.postimg.cc/JhdcD1S6/ainvoice.png",
                reply_markup=await history_manager._get_cached_keyboard(order_info, is_timeout=False, order_id=order_id)
            )
            inline_results.insert(0, summary_result)

        next_offset = str(int(inline_query.offset or 0) + 50) if len(inline_results) == 50 else ""
        await bot.answer_inline_query(
            inline_query.id,
            results=inline_results,
            cache_time=0,
            next_offset=next_offset
        )

        logger.info("Inline handler for #BᴀʀCᴏᴅᴇ- registered successfully")

    @bot.inline_handler(func=lambda query: query.query.startswith("#SᴛᴀᴛᴜsCᴀɴᴄᴇʟ"))
    async def handle_status_cancel_pending_inline(inline_query):
        user_id = str(inline_query.from_user.id)
        query_text = inline_query.query
        filters = {
            "user_id": user_id,
            "order_status": ["PENDING"]
        }
        result = await history_manager.search_history(
            history_type="Oʀᴅᴇʀ",
            user_id=user_id,
            filters=filters,
            sort_by="recorded_at",
            sort_asc=False,
            offset=int(inline_query.offset or 0),
            limit=RESULT_LIMIT
        )
        inline_results = []
        if result.get("response") and result.get("results"):
            for idx, order in enumerate(result["results"], 1):
                if order.get("order_status", "").upper() == "PENDING":
                    order_id = order["id"].split(":")[-1] if order["id"].startswith("order_data:info:") else ""
                    app_name = order.get("app_name", "Unknown").translate(await small_caps())
                    order_amount = order.get("order_amount", "N/A")
                    country_code = order.get("country_code", "N/A")
                    server_id = order.get("server_id", "N/A")
                    app_code = order.get("app_code", "N/A")
                    recorded_at = float(order.get("recorded_at", 0))
                    order_at = time_ago(recorded_at)
                    if app_code and app_code.startswith('['):
                        try:
                            app_code = app_code.strip('[]').split(',')[0].strip().strip("'\"")
                        except (IndexError, AttributeError):
                            app_code = app_code.strip('[]')
                    first_code = app_code.split(",")[0].strip().lower() if app_code and "," in app_code else app_code.lower() if app_code else ''
                    thumbnail_url = f"https://smsactivate.s3.eu-central-1.amazonaws.com/assets/ico/{first_code}0.webp"
                    encoded_order_id = await encode_order_id(order_id)
                    title = f"{app_name} 💎 {order_amount} [{country_code}]"
                    description = f"Oʀᴅᴇʀᴇᴅ {order_at} | Bᴀʀ-Cᴏᴅᴇ : {encoded_order_id}"
                    inline_results.append(
                        InlineQueryResultArticle(
                            id=str(idx),
                            title=title,
                            description=description,
                            thumbnail_url=thumbnail_url,
                            input_message_content=InputTextMessageContent(
                                message_text=f"#SᴛᴀᴛᴜsCᴀɴᴄᴇʟ:{encoded_order_id}",
                                parse_mode="HTML"
                            )
                        )
                    )
        if not inline_results:
            inline_results.append(
                InlineQueryResultArticle(
                    id="no_order",
                    title="No Order To Cancel",
                    description="No order to cancel",
                    input_message_content=InputTextMessageContent(
                        message_text="no order to cancel",
                        parse_mode="HTML"
                    )
                )
            )
        await bot.answer_inline_query(
            inline_query.id,
            results=inline_results,
            cache_time=1,
            next_offset=str(int(inline_query.offset or 0) + RESULT_LIMIT) if len(inline_results) >= RESULT_LIMIT else ""
        )


__all__ = ["init_managers", "register_handlers"]




#query_str = f'@app_id:{app_id}'
#query = Query(query_str).return_fields("app_code").dialect(2)
#search_result = await redis_client.ft(SERVICE_INDEX).search(query)
#app_code = search_result.docs[0]["app_code"].lower().strip() if search_result.docs else None'