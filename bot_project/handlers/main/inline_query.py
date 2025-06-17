import asyncio
import logging
import json
import time
from datetime import datetime
import re
from typing import Optional, Dict, Any, List, Tuple
import difflib
import asyncio
from functools import partial
import logging
from termcolor import colored
from colorama import Fore, Style, init as colorama_init
import uuid

import redis.asyncio as redis
from redis.exceptions import RedisError
from telebot.async_telebot import AsyncTeleBot
from telebot.types import (
    InlineQueryResultArticle,
    InputTextMessageContent,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    Message,
    InlineQuery,
    InlineQueryResultArticle,
    CallbackQuery,
)
from utils.functions import small_caps
from utils.config import SERVICE_INDEX, COMMISSION
from handlers.manager.operation import get_async_logger
from handlers.security import InputValidator
from handlers.manager.operation import UserManagement, user_mgr
from handlers.methods.purchase.show_country import country_management
from utils.redis_manager import RedisManager, redis_manager
from utils.cache_manager import cache_manager, CachePrefix

SERVICE_INDEX = "service_index"
CACHE_TTL = 240
CACHE_RESULTS_PER_PAGE = 50
RESULTS_PER_PAGE = 8

ALPHANUM_REGEX = re.compile(r'^[A-Za-z0-9 ]+$')

class UserSearchManagement:
    def __init__(self):
        self.redis_client: Optional[RedisManager] = None
        self.cache_ttl = CACHE_TTL
        self.user_manager: Optional[UserManagement] = None
        self.input_validator = InputValidator()
        self.bot = None
        self._initialized = False

    async def init_managers(self, user_mgr, bot: Optional[AsyncTeleBot] = None) -> bool:
        async_logger = await get_async_logger()
        try:
            if not user_mgr or not bot:
                await async_logger.error("User manager and bot instance are required")
                return False

            self.user_manager = user_mgr
            self.bot = bot
            self.input_validator = getattr(bot, 'input_validator', None)
            self.redis_client = await redis_manager.get_client()
            
            if not all([self.user_manager, self.input_validator, self.redis_client]):
                missing = [name for name, comp in [
                    ('user_manager', self.user_manager),
                    ('input_validator', self.input_validator),
                    ('redis_client', self.redis_client),
                ] if not comp]
                await async_logger.error(f"Missing required components: {', '.join(missing)}")
                return False

            self._initialized = True
            await async_logger.info("Handler managers initialized successfully")
            return True

        except Exception as e:
            await async_logger.error(f"Error initializing managers: {e}")
            return False

    async def register_handlers(self, bot: AsyncTeleBot) -> None:
        if not self._initialized:
            logging.error("Cannot register handlers: manager not initialized")
            return
        try:
            # 1️⃣ Only register this one inline handler—no manual register_inline_handler calls
            @bot.inline_handler(lambda q: getattr(q, 'chat_type', None) != 'sender')
            async def inline_referral(query: InlineQuery):
                # build your referral link dynamically
                me = await bot.get_me()
                bot_username   = me.username
                referral_link  = f"https://t.me/{bot_username}?start={query.from_user.id}"

                referral_text = (
                    "<b>⚡ <u>Fʟᴀsʜ Sᴍs Oᴛᴘ Bᴏᴛ</u> ❯</b>\n\n"
                    "<b>👉 Wᴀɴᴛ Tᴏ Rᴇᴄᴇɪᴠᴇ Oᴛᴘs Fʀᴏᴍ Aɴʏ Aᴘᴘ Oʀ "
                    "Wᴇʙsɪᴛᴇ Oɴ Uɴʟɪᴍɪᴛᴇᴅ Nᴜᴍʙᴇʀs?</b>\n"
                    f"🔗 <a href=\"{referral_link}\">Gᴇᴛ Sᴛᴀʀᴛᴇᴅ Wɪᴛʜ FʟᴀsʜSᴍs</a>\n\n"
                    "<b>🎯 Tᴏᴘ‑Rᴀᴛᴇᴅ Sᴇʀᴠɪᴄᴇs:</b>\n"
                    "<code>    </code><b>•</b>  <i>Tᴇʟᴇɢʀᴀᴍ</i>     <b>•</b> <i>Wʜᴀᴛsᴀᴘᴘ</i> <b>[✆]</b>\n"
                    "<code>    </code><b>•</b>  <i>Gᴍᴀɪʟ</i>            <b>•</b> <i>Fᴀᴄᴇʙᴏᴏᴋ</i> <b>[ⓕ]</b>\n"
                    "<code>    </code><b>•</b>  <i>Iɴsᴛᴀɢʀᴀᴍ</i>    <b>•</b> <i>Tᴡɪᴛᴛᴇʀ</i> <b>[𝕏]</b>\n"
                    "<code>    </code><b>•</b> <i>Wɪɴᴢᴏ, Rᴜᴍᴍʏ & Mᴀɴʏ Mᴏʀᴇ...</i>\n\n"
                    "<b>💼 Aᴠᴀɪʟᴀʙʟᴇ Iɴ</b> <code>170+</code> <b>Cᴏᴜɴᴛʀɪᴇs, "
                    "Sᴜᴘᴘᴏʀᴛɪɴɢ</b> <code>1500+</code> <b>Aᴘᴘs Wɪᴛʜ Pʀᴇᴍɪᴜᴍ Oᴘᴇʀᴀᴛᴏʀs</b>\n"
                    "<b>🚀 Fᴀsᴛ • Sᴇᴄᴜʀᴇ • 24/7 Aᴄᴄᴇss</b>"
                )
                kb = InlineKeyboardMarkup()
                kb.add(
                    InlineKeyboardButton(
                        text="⚡ Gᴇᴛ Oᴛᴘ Jᴜsᴛ Lɪᴋᴇ Fʟᴀsʜ ↗",
                        url=referral_link
                    )
                )


                result = InlineQueryResultArticle(
                    id="refer_and_earn",
                    title="💸 Rᴇғᴇʀ Aɴᴅ Eᴀʀɴ 💎",
                    description="Invite friends to FlashSMS and earn rewards!",
                    thumbnail_url="https://te.legra.ph/file/8f211c54558cd48392a5f.jpg",
                    thumbnail_width=100,
                    thumbnail_height=100,
                    reply_markup=kb,
                    input_message_content=InputTextMessageContent(
                        message_text=referral_text,
                        parse_mode="HTML"
                    )
                )

                # only answer if not private
                await bot.answer_inline_query(
                    query.id,
                    results=[result],
                    cache_time=0,
                    switch_pm_text="⚡ Gᴇᴛ Oᴛᴘ Jᴜsᴛ Lɪᴋᴇ Fʟᴀsʜ",
                    switch_pm_parameter="start"
                )
    
            bot.register_inline_handler(
                lambda inline_query: asyncio.create_task(self.handle_inline_query(inline_query))
                if not inline_query.query.startswith('#') else None,
                func=lambda inline_query: not inline_query.query.startswith('#')
            )
            bot.register_inline_handler(
                lambda inline_query: asyncio.create_task(self.handle_inline_query(inline_query, is_admin=True)),
                lambda inline_query: inline_query.query.startswith("#Sᴇʀᴠɪᴄᴇ")
            )
            
            bot.register_message_handler(self.handle_search_message, content_types=['text'])
            bot.register_callback_query_handler(self.handle_pagination, func=lambda call: call.data.startswith("search:"))

            logging.info("Inline query handlers registered successfully")
        except Exception as e:
            logging.error(f"Failed to register inline query handlers: {e}")
            raise

    @staticmethod
    def is_alphanumeric(name: str) -> bool:
        return bool(ALPHANUM_REGEX.match(name))

    @staticmethod
    def categorize(app_name: str, query: str) -> str:
        lower_name, lower_query = app_name.lower(), query.lower()
        if lower_name == lower_query:
            return "exact"
        elif lower_name.startswith(lower_query):
            return "prefix"
        elif lower_name.endswith(lower_query):
            return "suffix"
        elif lower_query in lower_name:
            return "substring"
        return "other"

    async def format_number_to_text(self, num: float) -> str:
        """
        Converts a number into a formatted text string using rounding.
    
        Rules:
          - If num < 100: return "Fᴇᴡ"
          - If 100 ≤ num < 1000: round the number and return with " Nᴜᴍʙᴇʀ" or " Nᴜᴍʙᴇʀ's"
          - If 1000 ≤ num < 100000: divide by 1000 and round to one decimal place, then append
              " Tʜᴏᴜsᴀɴᴅ" (if value is 1) or " Tʜᴏsᴀɴᴅ's" (if greater than 1)
          - If 100000 ≤ num < 10000000: divide by 100000, round to one decimal, and append " Lᴀᴋʜ" or " Lᴀᴋʜ's"
          - Otherwise (num ≥ 10000000): divide by 10000000, round to one decimal, and append " Cʀᴏʀᴇ" or " Cʀᴏʀᴇ's"
        """
        if num < 100:
            return "Fᴇᴡ Nᴜᴍʙᴇʀs"
        elif num < 1000:
            value = round(num)
            if value == 1:
                return f"{value} Nᴜᴍʙᴇʀ"
            else:
                return f"{value} Nᴜᴍʙᴇʀs"
        elif num < 100000:
            # Thousands range
            value = round(num / 1000, 1)
            # If rounding yields a whole number, convert to int
            if value.is_integer():
                value = int(value)
            if int(value) == 1:
                return f"{value} Tʜᴏsᴀɴᴅ"
            else:
                return f"{value} Tʜᴏsᴀɴᴅs"
        elif num < 10000000:
            # Lakhs range
            value = round(num / 100000, 1)
            if value.is_integer():
                value = int(value)
            if int(value) == 1:
                return f"{value} Lᴀᴋʜ"
            else:
                return f"{value} Lᴀᴋʜs"
        else:
            # Crores range
            value = round(num / 10000000, 1)
            if value.is_integer():
                value = int(value)
            if int(value) == 1:
                return f"{value} Cʀᴏʀᴇ"
            else:
                return f"{value} Cʀᴏʀᴇs"

    async def build_simple_advanced_query(self, user_input: str) -> str:
        """
        Build an advanced fuzzy query for the @app_name field.
        
        Any spaces in the input are permanently removed.
        
        For example:
          Input: "tata neu"  → becomes "tataneu"
          
          Generates: "%%tataneu%%|tataneu*|*tataneu|*tataneu*|tataneu"
          
          Final query: @app_name:(%%tataneu%%|tataneu*|*tataneu|*tataneu*|tataneu)
          
        If the query is empty or only spaces, or contains special characters, returns: *
        """
        # Remove spaces permanently and convert to lower-case.
        processed = user_input.strip().lower().replace(" ", "")
        
        if not processed or not self.is_alphanumeric(processed):
            return ""
        
        variant1 = "%%" + processed + "%%"   # Using %% wrapper for substring matching.
        variant2 = processed + "*"            # Trailing wildcard.
        variant3 = "*" + processed            # Leading wildcard.
        variant4 = "*" + processed + "*"      # Both sides wildcards.
        
        # Combine variants using OR (|)
        or_clause = f"{variant1}|{variant2}|{variant3}|{variant4}|{processed}"
        return f" @search_tags:({or_clause})"

    async def _search_pattern(
        self,
        pattern: str,
        app_count: str = None,
        app_price: str = None,
        tool_limit: int = 1500,
        sort_by: str = None,
        country_name_query: Optional[str] = None
    ) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Ultra-fast, non-blocking FT.AGGREGATE search with caching.
        """
        # 1) cache key
        parts = [pattern, app_count or 'any_count', app_price or 'any_price', str(tool_limit), sort_by or 'nosort', country_name_query or 'all']
        cache_key = 'search:' + '|'.join(parts)
        cached = await cache_manager.get(cache_key, CachePrefix.SEARCH)
        if cached:
            return cached

        # 2) build query
        tags = f"{pattern} @is_show_server:(True) @is_show_app:(True) @is_show_country:(True)"
        if country_name_query:
            tags += f" @country_name:(%{country_name_query}%|{country_name_query}*|{country_name_query})"
        tags += f" @app_price:{app_price or '[0.01 +inf]'}"

        cmd = [
            'FT.AGGREGATE', SERVICE_INDEX, tags,
            'LOAD', '3', '@app_name', '@app_code', '@app_price',
            'GROUPBY', '1', '@app_name',
            'REDUCE', 'MIN', '1', '@app_price', 'AS', 'MinPrice',
            'REDUCE', 'SUM', '1', '@app_count', 'AS', 'Total',
            'REDUCE', 'FIRST_VALUE', '4', '@app_id', 'BY', '@app_price', 'ASC', 'AS', 'app_id',
            'REDUCE', 'FIRST_VALUE', '1', '@app_code', 'AS', 'app_code'
        ]
        if sort_by:
            cmd.extend(['SORTBY', '2', '@MinPrice', sort_by.upper()])

        # 3) execute
        try:
            rows = await self.user_manager._run_aggregate_cursor(cmd, SERVICE_INDEX)
        except Exception:
            return []

        # 4) parse
        results: List[Tuple[str, Dict[str, Any]]] = []
        for rec in rows:
            try:
                results.append((rec[1], {
                    'lowest_price': float(rec[3]),
                    'total_stock': int(rec[5]),
                    'app_id': rec[7],
                    'app_code': rec[9]
                }))
            except:
                continue

        # 5) cache
        await cache_manager.set(cache_key, results, CachePrefix.SEARCH)
        return results

    async def search_advanced(
        self,
        query: str,
        offset: int = 0,
        limit: Optional[int] = 1500,
        app_count: str = None,
        app_price: str = None,
        sort_by: Optional[str] = None,
        country_name_query: Optional[str] = None,
        tool_limit: Optional[int] = 1500
    ) -> Dict[str, Any]:
        """
        Ultra-fast, parallelized advanced search with slicing and caching.
        """
        # 1) cache key
        parts = [f'q={query}', f'off={offset}', f'lim={limit or tool_limit}', f'ac={app_count or "any"}', 
                 f'ap={app_price or "any"}', f'so={sort_by or "none"}', f'cn={country_name_query or "all"}']
        cache_key = 'adv:' + '|'.join(parts)
        cached = await cache_manager.get(cache_key, CachePrefix.SEARCH)
        if cached:
            return cached

        # 2) build patterns list (could add more patterns)
        main_pattern = await self.build_simple_advanced_query(query)
        patterns = [main_pattern]

        # 3) run all patterns in parallel
        tasks = [
            self._search_pattern(p, app_count=app_count, app_price=app_price,
                                  tool_limit=tool_limit, sort_by=sort_by,
                                  country_name_query=country_name_query)
            for p in patterns
        ]
        results_list = await asyncio.gather(*tasks)

        # 4) merge & prioritize
        priority = {'exact':0,'prefix':1,'substring':2,'suffix':3,'other':4}
        unified: Dict[str, Dict[str, Any]] = {}
        for pat_idx, res in enumerate(results_list):
            for name, data in res:
                cat = self.categorize(name, query) if len(query)>1 else 'prefix'
                if name in unified:
                    if priority[cat] < priority[unified[name]['category']]:
                        unified[name]['category'] = cat
                else:
                    if str(data.get('app_id','')).isdigit():
                        unified[name] = {**data, 'app_name': name, 'category': cat}

        # 5) sort
        sorted_items = sorted(
            unified.values(),
            key=lambda d: (priority[d['category']], d['app_name'].lower())
        )

        total = len(sorted_items)
        sliced = sorted_items[offset: offset + (limit or tool_limit)]

        # 6) result dict
        result = {
            'total_results': total,
            'results': {d['app_name']: d for d in sliced},
            'cached_at': datetime.now().timestamp()
        }

        # 7) cache
        await cache_manager.set(cache_key, result, CachePrefix.SEARCH)
        return result

    async def validate_inline_query(self, user_id: str, query: str) -> Dict[str, Any]:
        try:
            sanitized_query = self.input_validator.sanitize_text(query, max_length=100)
            return {"valid": True, "sanitized_query": sanitized_query}
        except Exception as e:
            logging.error(f"Error validating inline query: {e}")
            return {"valid": False, "error": "Internal validation error"}

    async def query_apps(self, inline_query, is_admin: bool = False) -> None:
        try:
            raw_query = inline_query.query.strip().lower()
            query_text = raw_query.removeprefix("#sᴇʀᴠɪᴄᴇ").strip() if is_admin else raw_query
            offset = int(inline_query.offset or "0")

            cache_key = f"query_apps:q={query_text}|admin={int(is_admin)}|off={offset}"
            cached_blob = await cache_manager.get(cache_key, CachePrefix.SEARCH)
            if cached_blob:
                # Ensure total is always an int
                items = cached_blob.get("items", [])
                total = int(cached_blob.get("total", 0))
                # Reconstruct Telegram articles
                results = []
                for item in items:
                    art = InlineQueryResultArticle(
                        id=item["id"],
                        title=item["title"],
                        description=item["description"],
                        thumbnail_url=item["thumb"],
                        input_message_content=InputTextMessageContent(message_text=item["input_cmd"])
                    )
                    if item.get("switch"):
                        art.switch_inline_query_current_chat = item["switch"]
                    results.append(art)

                # Calculate next_offset safely
                next_offset = ""
                if total > offset + CACHE_RESULTS_PER_PAGE:
                    next_offset = str(offset + CACHE_RESULTS_PER_PAGE)

                await self.bot.answer_inline_query(
                    inline_query.id,
                    results,
                    cache_time=30,
                    next_offset=next_offset
                )
                return


            start_time = time.time()
            validation = await self.validate_inline_query(str(inline_query.from_user.id), query_text)
            if not validation["valid"]:
                error_text = validation["error"]
                await self.bot.answer_inline_query(
                    inline_query.id,
                    [InlineQueryResultArticle(
                        id="error",
                        title="Error",
                        description=error_text,
                        thumbnail_url="https://img.freepik.com/free-vector/bird-colorful-logo-gradient-vector_343694-1365.jpg",
                        input_message_content=InputTextMessageContent(message_text=error_text)
                    )],
                    cache_time=5
                )
                return

            query_text = validation.get("sanitized_query", "")
            search_data = await self.search_advanced(
                query=query_text,
                offset=0,
                limit=None,
                app_count=None,
                app_price=None,
                sort_by=None,
                country_name_query=None,
                tool_limit=None
            )
            apps = list(search_data.get("results", {}).items())
            total_count = len(apps)
            page_data = apps[offset:offset + CACHE_RESULTS_PER_PAGE]

            if not page_data:
                keyboard = InlineKeyboardMarkup().add(
                    InlineKeyboardButton("⌕ Contact Support", url="https://t.me/udaysupport")
                )
                await self.bot.answer_inline_query(
                    inline_query.id,
                    [InlineQueryResultArticle(
                        id="not_found",
                        title=" Nᴏ Sᴇʀᴠɪᴄᴇs Aᴠᴀɪʟᴀʙʟᴇ",
                        description="Wᴇ'ʀᴇ ᴄᴏɴsᴛᴀɴᴛʟʏ ᴜᴘᴅᴀᴛɪɴɢ ᴏᴜʀ sᴇʀᴠɪᴄᴇs. Tʀʏ ᴀɴᴏᴛʜᴇʀ sᴇᴀʀᴄʜ ᴏʀ ᴄᴏɴᴛᴀᴄᴛ sᴜᴘᴘᴏʀᴛ!",
                        thumbnail_url="https://img.freepik.com/free-vector/bird-colorful-logo-gradient-vector_343694-1365.jpg",
                        reply_markup=keyboard,
                        input_message_content=InputTextMessageContent(
                            message_text=(
                                "*Nᴏ Sᴇʀᴠɪᴄᴇs Fᴏᴜɴᴅ*\n\n"
                                "Yᴏᴜʀ Sᴇᴀʀᴄʜ Dɪᴅɴ'ᴛ Mᴀᴛᴄʜ Aɴʏ Aᴠᴀɪʟᴀʙʟᴇ Sᴇʀᴠɪᴄᴇs.\n"
                                "Sᴜɢɢᴇsᴛɪᴏɴs:\n"
                                "• Cʜᴇᴄᴋ Yᴏᴜʀ Sᴘᴇʟʟɪɴɢ\n"
                                "• Tʀʏ Mᴏʀᴇ Gᴇɴᴇʀᴀʟ Kᴇʏᴡᴏʀᴅs\n"
                                "• Cᴏɴᴛᴀᴄᴛ Oᴜʀ Sᴜᴘᴘᴏʀᴛ Tᴇᴀᴍ\n\n"
                                "Wᴇ'ʀᴇ Cᴏɴsᴛᴀɴᴛʟʏ Aᴅᴅɪɴɢ Nᴇᴡ Sᴇʀᴠɪᴄᴇs!"
                            ),
                            parse_mode="Markdown"
                        )
                    )],
                    cache_time=30
                )
                return

            price_country = await self.redis_client.json().get('main_data:price-country') or {}
            country_data = await self.redis_client.json().get('main_data:details:country_data') or {}
            results, raw_items = [], []

            for app_name, data in page_data:
                try:
                    app_id = str(data.get("app_id", app_name))
                    clean_name = self.input_validator.sanitize_text(app_name).title().translate(await small_caps())
                    total_stock = int(data.get("total_stock", 0))
                    lowest_price = float(data.get("lowest_price", 0.0)) * float(COMMISSION)
                    app_code = data.get("app_code", "")
                    first_code = app_code.split(",")[0].strip().lower() if "," in app_code else app_code.lower()

                    app_prices = price_country.get(app_id, {})
                    countries = {}
                    for p_str, cid in app_prices.items():
                        try:
                            price = float(p_str)
                            if price > 0 and (cid not in countries or price < countries[cid]):
                                countries[cid] = price
                        except:
                            continue

                    top_countries = sorted(countries.items(), key=lambda x: x[1])[:4]
                    has_more = len(countries) > 3
                    display_countries = [
                        country_data.get(cid, {}).get("country_code", "") for cid, _ in top_countries[:3]
                    ]
                    top_display = f"[{', '.join(display_countries)}{',...' if has_more else ''}]"

                    description = (
                        f"❯ Tʜᴇ Sᴛᴀʀᴛɪɴɢ Pʀɪᴄᴇ Is Oɴʟʏ {lowest_price:.2f} Pᴏɪɴᴛ's.\n"
                        f"• Aᴠᴀɪʟᴀʙʟᴇ Aᴄʀᴏss » {top_display}\n"
                        f"• Tᴏᴛᴀʟ Sᴛᴏᴄᴋ » {await self.format_number_to_text(total_stock)}"
                    )

                    input_cmd = f"#Sᴇʀᴠɪᴄᴇ|{app_id}" if is_admin else f"/Buy_{app_id}"
                    switch_query = "#Sᴇʀᴠɪᴄᴇ " if is_admin else ""

                    item = {
                        "id": str(uuid.uuid4()),
                        "title": clean_name,
                        "description": description,
                        "thumb": (
                            f"https://smsactivate.s3.eu-central-1.amazonaws.com/assets/ico/{first_code}0.webp"
                            if first_code else
                            "https://img.icons8.com/color/48/000000/shop.png"
                        ),
                        "input_cmd": input_cmd,
                        "switch": switch_query
                    }
                    raw_items.append(item)
                    results.append(
                        InlineQueryResultArticle(
                            id=item["id"],
                            title=item["title"],
                            description=item["description"],
                            thumbnail_url=item["thumb"],
                            input_message_content=InputTextMessageContent(message_text=item["input_cmd"]),
                            reply_markup=InlineKeyboardMarkup().add(
                                InlineKeyboardButton("🛒 Sᴇʀᴠɪᴄᴇs", switch_inline_query_current_chat=item["switch"])
                            )
                        )
                    )
                except Exception as e:
                    logging.error(f"App error: {e}")
                    continue

            await cache_manager.set(
                cache_key,
                {"items": raw_items, "total": total_count, "ts": time.time()},
                CachePrefix.SEARCH
            )

            # Send paginated response
            next_offset = ""
            if total_count > offset + CACHE_RESULTS_PER_PAGE:
                next_offset = str(offset + CACHE_RESULTS_PER_PAGE)

            await self.bot.answer_inline_query(
                inline_query.id,
                results,
                cache_time=30,
                next_offset=next_offset
            )

        except Exception as e:
            logging.error(f"query_apps failed: {e}")
            await self.bot.answer_inline_query(
                inline_query.id,
                [InlineQueryResultArticle(
                    id="error",
                    title="Error",
                    description="An error occurred. Please try again.",
                    input_message_content=InputTextMessageContent(message_text="Error occurred. Please try again.")
                )],
                cache_time=5
            )


    async def handle_inline_query(self, inline_query, is_admin=False) -> None:
        """Handle an incoming inline query by processing it."""
        return await self.query_apps(inline_query, is_admin)

    async def validate_search_query(self, user_id: str, query: str) -> dict:
        if len(query) > 20:
            return {"valid": False, "error": "Query must not exceed 20 characters."}
        # Further validation and sanitization logic...
        return {"valid": True, "sanitized_query": query}

    async def handle_search_message(
        self,
        message: Message,
        app_count: str = "[1 +inf]",
        app_price: str = "[0.01 +inf]",
        tool_limit: int = None,
        sort_by: Optional[str] = None,
        country_name_query: Optional[str] = None
    ) -> None:
        try:
            query_text = message.text.strip().lower()
            validation = await self.validate_search_query(str(message.from_user.id), query_text)
            if not validation["valid"]:
                return

            query_text = validation.get("sanitized_query", query_text)
            offset = 0  # initial offset

            # Build cache key
            cache_key = (
                f"search_msg:q={query_text}|"
                f"count={app_count}|price={app_price}|"
                f"sort={sort_by or 'none'}|country={country_name_query or 'all'}"
            )

            # Try cache
            cached = await cache_manager.get(cache_key, CachePrefix.SEARCH)
            if cached:
                return_message, return_keyboard = cached
                await self.bot.send_message(
                    message.chat.id,
                    return_message,
                    reply_markup=return_keyboard,
                    parse_mode='HTML'
                )
                return

            start_time = time.time()
            search_results = await self.search_advanced(
                query=query_text,
                offset=offset,
                limit=tool_limit or RESULTS_PER_PAGE,
                app_count=app_count,
                app_price=app_price,
                sort_by=sort_by,
                country_name_query=country_name_query,
                tool_limit=tool_limit
            )

            if not search_results or not search_results.get("results"):
                msg = message.chat.id
                if msg != 'tool':
                    result_message = (
                        "No Services Found.\n\nSuggestions:\n"
                        "• Check Your Spelling\n"
                        "• Try General Keywords\n"
                        "• Contact Support/Admin For Help."
                    ).translate(await small_caps())
                    await self.bot.send_message(msg, result_message)
                else:
                    return []
                return

            search_items = list(search_results["results"].items())[:RESULTS_PER_PAGE]
            exact_match = None
            for app_name, data in search_items:
                ratio = difflib.SequenceMatcher(None, query_text, app_name.lower()).ratio()
                if ratio >= 0.8:
                    exact_match = (app_name, data)
                    break

            if exact_match:
                app_name, data = exact_match
                app_id = str(data.get("app_id", app_name))
                new_text = f"/Buy_{app_id}"
                message.text = new_text
                if message.chat.id != 'tool':
                    task = partial(country_management.process_buy_command, message)
                    asyncio.create_task(task())
                else:
                    return [{"app_id": app_id, "app_name": app_name}]
                return

            result_text = ""
            result_objs = []
            for app_name, data in search_items:
                app_id = str(data.get("app_id", app_name))
                total_stock = int(data.get("total_stock", 0))
                lowest_price = float(data.get("lowest_price", 0.0)) * float(COMMISSION)
                result_text += await self.format_app_result(app_name, app_id, total_stock, lowest_price) + "\n\n"

            has_prev = False
            has_next = len(search_items) == RESULTS_PER_PAGE
            keyboard = InlineKeyboardMarkup()

            if not has_prev and not has_next:
                keyboard.row(
                    InlineKeyboardButton("⌕ Sᴇᴀʀᴄʜ", switch_inline_query_current_chat=f"{query_text}")
                )
            elif has_prev and has_next:
                keyboard.row(
                    InlineKeyboardButton("« Pʀᴇᴠɪoᴜs", callback_data=f"search:prev:{offset}:{query_text}"),
                    InlineKeyboardButton("⌕", switch_inline_query_current_chat=f"{query_text}"),
                    InlineKeyboardButton("Nᴇxᴛ »", callback_data=f"search:next:{offset}:{query_text}")
                )
            elif has_next:
                keyboard.row(
                    InlineKeyboardButton("⌕ Sᴇᴀʀᴄʜ", switch_inline_query_current_chat=f"{query_text}"),
                    InlineKeyboardButton("Nᴇxᴛ »", callback_data=f"search:next:{offset}:{query_text}")
                )
            else:
                keyboard.row(
                    InlineKeyboardButton("« Pʀᴇᴠɪoᴜs", callback_data=f"search:prev:{offset}:{query_text}"),
                    InlineKeyboardButton("⌕ Sᴇᴀʀᴄʜ", switch_inline_query_current_chat=f"{query_text}")
                )

            await self.bot.send_message(
                message.chat.id,
                result_text,
                reply_markup=keyboard,
                parse_mode='HTML'
            )

            # Save to cache
            await cache_manager.set(
                cache_key,
                (result_text, keyboard),
                CachePrefix.SEARCH
            )

            end_time = time.time()
            logging.info(f"Search message processing time: {end_time - start_time:.3f}s")
        except Exception as e:
            logging.error(f"Error in handle_search_message: {e}")
            await self.bot.send_message(
                message.chat.id,
                "An error occurred while processing your search. Please try again later."
            )

    async def handle_pagination(self, call: CallbackQuery):
        """
        Handle callback queries for pagination buttons.
        Callback data format: "search:<direction>:<current_offset>:<query_text>"
        """
        try:
            parts = call.data.split(":")
            if len(parts) < 4:
                await self.bot.answer_callback_query(call.id, text="Invalid callback data.")
                return

            direction, current_offset, query_text = parts[1], int(parts[2]), parts[3]
            if direction == "next":
                offset = current_offset + RESULTS_PER_PAGE
            elif direction == "prev":
                offset = max(current_offset - RESULTS_PER_PAGE, 0)
            else:
                await self.bot.answer_callback_query(call.id, text="Unknown direction.")
                return

            search_results = await self.search_advanced(query=query_text, offset=offset, limit=RESULTS_PER_PAGE)
            if not search_results or not search_results.get("results"):
                await self.bot.edit_message_text(
                    "No more results.",
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id
                )
                return

            search_items = list(search_results["results"].items())[:RESULTS_PER_PAGE]
            result_text = ""
            for app_name, data in search_items:
                app_id = str(data.get("app_id", app_name))
                total_stock = int(data.get("total_stock", 0))
                lowest_price = float(data.get("lowest_price", 0.0)) * float(COMMISSION)
                result_text += await self.format_app_result(app_name, app_id, total_stock, lowest_price) + "\n\n"

            # Determine pagination availability.
            has_prev = offset > 0
            has_next = len(search_items) >= RESULTS_PER_PAGE

            keyboard = InlineKeyboardMarkup()
            if not has_prev and not has_next:
                keyboard.row(
                    InlineKeyboardButton("⌕ Sᴇᴀʀᴄʜ", switch_inline_query_current_chat=f"{query_text}")
                )
            elif has_prev and has_next:
                keyboard.row(
                    InlineKeyboardButton("« Pʀᴇᴠɪoᴜs", callback_data=f"search:prev:{offset}:{query_text}"),
                    InlineKeyboardButton("⌕ Sᴇᴀʀᴄʜ", switch_inline_query_current_chat=f"{query_text}"),
                    InlineKeyboardButton("Nᴇxᴛ »", callback_data=f"search:next:{offset}:{query_text}")
                )
            elif has_next:
                keyboard.row(
                    InlineKeyboardButton("⌕ Sᴇᴀʀᴄʜ", switch_inline_query_current_chat=f"{query_text}"),
                    InlineKeyboardButton("Nᴇxᴛ »", callback_data=f"search:next:{offset}:{query_text}")
                )
            elif has_prev:
                keyboard.row(
                    InlineKeyboardButton("« Pʀᴇᴠɪoᴜs", callback_data=f"search:prev:{offset}:{query_text}"),
                    InlineKeyboardButton("⌕ Sᴇᴀʀᴄʜ", switch_inline_query_current_chat=f"{query_text}")
                )
            try:
                await self.bot.edit_message_text(
                    result_text,
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    reply_markup=keyboard,
                    parse_mode='html'
                )
                await self.bot.answer_callback_query(call.id)
            except Exception as e:
                logging.error(f"Error editing message: {e}")
                await self.bot.answer_callback_query(call.id, text="🔒 Aɴᴏᴛʜᴇʀ Tʀᴀɴsᴀᴄᴛɪᴏɴ Iɴ Pʀᴏɢʀᴇss, Pʟᴇᴀsᴇ Wᴀɪᴛ...", show_alert=False)
        except Exception as e:
            logging.error(f"Error in pagination handler: {e}")
            await self.bot.answer_callback_query(call.id, text="An error occurred while paginating.")

    async def format_app_result(self, app_name: str, app_id: str, total_stock: int, lowest_price: float) -> str:
        """
        Format a search result for display.
        Expects an async function small_caps() (defined elsewhere) for text translation.
        """
        try:
            caps_map = await small_caps()  # small_caps must be defined elsewhere.
            return (
                f"<u><b>{app_name.title().translate(caps_map)}</b></u> <b>[</b><i>{await self.format_number_to_text(total_stock)}</i><b>]</b>\n "
                f"   <code>❯</code> <i>Sᴛᴀʀᴛɪɴɢ Pʀɪᴄᴇ</i> <b>»</b> "
                f"<code>💎</code> <code>{f'{lowest_price:.2f}'.translate(caps_map)}</code> \n"
                f"    <b>•</b> <i>Cʟɪᴄᴋ Tᴏ Sᴇᴇ</i> <b>»</b> <i>/Buy_{app_id}</i>"
            )
        except Exception as e:
            logging.error(f"Error formatting result for {app_name}: {e}")
            return f"<b>{app_name.title()}</b> - <i>Error processing result</i>"




# Initialize the search manager instance for inline queries.
search_manager = UserSearchManagement()

async def init_managers(user_manager, order_manager=None, bot: Optional[AsyncTeleBot] = None) -> bool:
    """Initialize the search manager with the required components."""
    return await search_manager.init_managers(user_manager, bot)

async def register_handlers(bot: AsyncTeleBot) -> None:
    """Register inline query handlers with the provided bot instance."""
    await search_manager.register_handlers(bot)
