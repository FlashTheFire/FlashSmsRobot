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
        Search the Redis index using a custom FT.SEARCH query.
        
        :param pattern: Custom search query string.
        :param app_count: Optional count filter. Defaults to "[1 +inf]".
        :param app_price: Optional price filter. Defaults to "[0.01 +inf]".
        :param limit: Optional limit for the number of results. Defaults to 1500.
        :param sort_by: Optional field to sort the results by. Defaults to None.
        :return: List of tuples containing the app_id and a dictionary of app data.
        """
        redis_client = self.redis_client

        # Construct cache key
        cache_key_parts = [
            pattern,
            app_price or "default_price",
            str(tool_limit),
            sort_by or "default_sort",
            country_name_query or "all"
        ]
        cache_key = "search:" + "|".join(cache_key_parts)

        # Try to get from cache
        cache_data = await cache_manager.get(cache_key, CachePrefix.SEARCH)
        if cache_data:
            return cache_data

        # Build query string
        query_str = f'{pattern} @is_show_server:(True) @is_show_app:(True) @is_show_country:(True)'
        if country_name_query:
            query_str += f" @country_name:(%%{country_name_query}%%|{country_name_query}*|{country_name_query})"
        if app_price:
            query_str += f" @app_price:{app_price}"
        else:
            query_str += " @app_price:[0.01 +inf]"
        #if app_count:
        #    query_str += f" @app_count:{app_count}"

        redis_query = [
            'FT.AGGREGATE', SERVICE_INDEX,
            query_str,
            'LOAD', '3', '@app_name', '@app_code', '@app_price',
            'GROUPBY', '1', '@app_name',
            'REDUCE', 'MIN', '1', '@app_price', 'AS', 'MinPrice',
            'REDUCE', 'SUM', '1', '@app_count', 'AS', 'Total',
            'REDUCE', 'FIRST_VALUE', '4', '@app_id', 'BY', '@app_price', 'ASC', 'AS', 'app_id',
            'REDUCE', 'FIRST_VALUE', '1', '@app_code', 'AS', 'app_code',
        ]

        if sort_by is not None:
            redis_query += ['SORTBY', '2', '@MinPrice', sort_by]

        #redis_query += ['LIMIT', '0', str(tool_limit)]
        print(colored(redis_query, 'blue'))

        try:
            result = await self.user_manager._run_aggregate_cursor(redis_query, SERVICE_INDEX)
        except Exception as e:
            logging.error(f"Error executing search pattern with pattern {pattern}: {e}")
            return []

        items = []
        for i in range(1, len(result)):
            rec = result[i]
            try:
                app_name = rec[1]
                items.append((app_name, {
                    "lowest_price": float(rec[3]),
                    "total_stock": int(rec[5]),
                    "app_id": rec[7],
                    "app_code": rec[9]
                }))
            except Exception as e:
                logging.error(f"Error processing record: {rec} with error: {e}")
                continue

        # Store result in cache
        await cache_manager.set(cache_key, items, CachePrefix.SEARCH)

        return items

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
        try:
            # Construct a detailed cache key for the advanced search
            cache_key_parts = [
                f"q={query}",
                f"app_count={app_count or 'any'}",
                f"app_price={app_price or 'any'}",
                f"sort={sort_by or 'none'}",
                f"country={country_name_query or 'all'}",
                f"limit={tool_limit or '1500'}"
            ]
            cache_key = "search_advanced:" + "|".join(cache_key_parts)

            # Try getting from cache
            cached_result = await cache_manager.get(cache_key, CachePrefix.SEARCH)
            if cached_result:
                return cached_result

            # Build advanced query
            advanced_query = await self.build_simple_advanced_query(query)
            logging.debug(f"Advanced query: {advanced_query}")

            pattern_configs = [("advanced", advanced_query)]
            tasks = [
                self._search_pattern(
                    pattern,
                    app_count=app_count,
                    app_price=app_price,
                    sort_by=sort_by,
                    tool_limit=tool_limit,
                    country_name_query=country_name_query
                ) for _, pattern in pattern_configs
            ]
            results_by_pattern = await asyncio.gather(*tasks)

            priority = {"exact": 0, "prefix": 1, "substring": 2, "suffix": 3, "other": 4}
            processed_results: Dict[str, Dict[str, Any]] = {}

            for (cat_hint, _), pattern_results in zip(pattern_configs, results_by_pattern):
                if isinstance(pattern_results, Exception):
                    logging.error(f"Error processing pattern '{cat_hint}': {pattern_results}")
                    continue

                for app_name, data in pattern_results:
                    new_cat = self.categorize(app_name, query) if len(query.strip()) > 1 else "prefix"
                    if app_name in processed_results:
                        current_priority = priority.get(processed_results[app_name]['category'], 5)
                        new_priority = priority.get(new_cat, 5)
                        if new_priority < current_priority:
                            processed_results[app_name]['category'] = new_cat
                    else:
                        app_id = data.get('app_id', app_name)
                        if app_id.isdigit():
                            data['category'] = new_cat
                            data['app_name'] = app_name
                            processed_results[app_name] = data

            sorted_results = dict(sorted(
                processed_results.items(),
                key=lambda x: (priority.get(x[1]['category'], 5), x[0].lower())
            ))

            result_dict = {
                "total_results": len(sorted_results),
                "results": sorted_results,
                "cached_at": datetime.now().timestamp()
            }

            # Cache the full result set
            sorted_items = list(sorted_results.items())
            sliced_items = dict(sorted_items[offset: (offset + limit) if limit is not None else None])
            result_dict["results"] = sliced_items
            result_dict["total_results"] = len(sorted_items)
            result_dict["sliced_results"] = len(sliced_items)
            logging.debug(f"|| results {len(sorted_items)}")
            await cache_manager.set(cache_key, result_dict, CachePrefix.SEARCH)
            return result_dict

        except RedisError as e:
            logging.error(f"Redis error in search_advanced: {e}")
        except Exception as e:
            logging.error(f"Error in search_advanced: {e}")

        return {"total_results": 0, "results": {}, "cached_at": datetime.now().timestamp()}

    async def validate_inline_query(self, user_id: str, query: str) -> Dict[str, Any]:
        try:
            sanitized_query = self.input_validator.sanitize_text(query, max_length=100)
            return {"valid": True, "sanitized_query": sanitized_query}
        except Exception as e:
            logging.error(f"Error validating inline query: {e}")
            return {"valid": False, "error": "Internal validation error"}

    async def query_apps(self, inline_query, is_admin: bool = False) -> None:
        try:
            if not is_admin:
                query_text = inline_query.query.strip().lower()
            else:
                query_text = inline_query.query.strip().lower().removeprefix("#sᴇʀᴠɪᴄᴇ").strip()

            offset = int(inline_query.offset) if inline_query.offset else 0

            # Build deterministic cache key
            cache_key = (
                f"query_apps:"
                f"q={query_text}|"
                f"admin={int(is_admin)}|"
                f"off={offset}"
            )

            # Attempt to load from cache
            cached_blob = await cache_manager.get(cache_key, CachePrefix.SEARCH)
            if cached_blob:
                results, total_count, cached_at = cached_blob
                await self.bot.answer_inline_query(
                    inline_query.id,
                    results,
                    cache_time=30,
                    next_offset=str(offset + CACHE_RESULTS_PER_PAGE) if total_count > offset + CACHE_RESULTS_PER_PAGE else ""
                )
                return

            start_time = time.time()

            validation = await self.validate_inline_query(str(inline_query.from_user.id), query_text)
            if not validation["valid"]:
                await self.bot.answer_inline_query(
                    inline_query.id,
                    [
                        InlineQueryResultArticle(
                            id="error",
                            title="Error",
                            description=validation["error"],
                            thumbnail_url="https://img.freepik.com/free-vector/bird-colorful-logo-gradient-vector_343694-1365.jpg",
                            input_message_content=InputTextMessageContent(message_text=validation["error"])
                        )
                    ],
                    cache_time=5
                )
                return

            query_text = validation.get("sanitized_query", "")
            search_results = await self.search_advanced(
                query=query_text,
                offset=offset,
                limit=CACHE_RESULTS_PER_PAGE,
                app_count=None,
                app_price=None,
                sort_by=None,
                country_name_query=None,
                tool_limit=None
            )

            if not search_results or not search_results.get("results"):
                keyboard = InlineKeyboardMarkup()
                keyboard.add(
                    InlineKeyboardButton("⌕ Contact Support", url="https://t.me/udaysupport")
                )
                await self.bot.answer_inline_query(
                    inline_query.id,
                    [
                        InlineQueryResultArticle(
                            id="not_found",
                            title=" Nᴏ Sᴇʀᴠɪᴄᴇs Aᴠᴀɪʟᴀʙʟᴇ",
                            description="Wᴇ'ʀᴇ ᴄᴏɴsᴛᴀɴᴛʟʏ ᴜᴘᴅᴀᴛɪɴɢ ᴏᴜʀ sᴇʀᴠɪᴄᴇs. Tʀʏ ᴀɴᴏᴛʜᴇʀ sᴇᴀʀᴄʜ ᴏʀ ᴄᴏɴᴛᴀᴄᴛ sᴜᴘᴘᴏʀᴛ!",
                            thumbnail_url="https://img.freepik.com/free-vector/bird-colorful-logo-gradient-vector_343694-1365.jpg",
                            reply_markup=keyboard,
                            input_message_content=InputTextMessageContent(
                                message_text=(
                                    " *Nᴏ Sᴇʀᴠɪᴄᴇs Fᴏᴜɴᴅ*\n\n"
                                    " Yᴏᴜʀ Sᴇᴀʀᴄʜ Dɪᴅɴ'ᴛ Mᴀᴛᴄʜ Aɴʏ Aᴠᴀɪʟᴀʙʟᴇ Sᴇʀᴠɪᴄᴇs.\n"
                                    " Sᴜɢɢᴇsᴛɪᴏɴs:\n"
                                    "• Cʜᴇᴄᴋ Yᴏᴜʀ Sᴘᴇʟʟɪɴɢ\n"
                                    "• Tʀʏ Mᴏʀᴇ Gᴇɴᴇʀᴀʟ Kᴇʏᴡᴏʀᴅs\n"
                                    "• Cᴏɴᴛᴀᴄᴛ Oᴜʀ Sᴜᴘᴘᴏʀᴛ Tᴇᴀᴍ Fᴏʀ Assɪsᴛᴀɴᴄᴇ\n\n"
                                    " Wᴇ'ʀᴇ Cᴏɴsᴛᴀɴᴛʟʏ Aᴅᴅɪɴɢ Nᴇᴡ Sᴇʀᴠɪᴄᴇs!"
                                ),
                                parse_mode="Markdown"
                            )
                        )
                    ],
                    cache_time=30
                )
                return

            results = []
            used_ids = set()
            price_country = await self.redis_client.json().get('main_data:price-country') or {}
            country_data = await self.redis_client.json().get('main_data:details:country_data') or {}

            for app_name, data in search_results["results"].items():
                try:
                    app_id = str(data.get('app_id', app_name))
                    clean_name = self.input_validator.sanitize_text(app_name)
                    total_stock = int(data.get("total_stock", 0))
                    lowest_price = float(data.get("lowest_price", 0.0)) * float(COMMISSION)
                    app_code = data.get("app_code", "")
                    first_code = app_code.split(",")[0].strip().lower() if "," in app_code else app_code.lower()

                    app_price_data = price_country.get(app_id, {})
                    country_prices = {}
                    for price_str, cid in app_price_data.items():
                        try:
                            price = float(price_str)
                            if price <= 0:
                                continue
                            prev = country_prices.get(cid)
                            if prev is None or price < prev:
                                country_prices[cid] = price
                        except Exception:
                            continue

                    countries = [
                        cid for cid, _ in sorted(country_prices.items(), key=lambda x: x[1])[:4]
                    ]
                    has_more = len(country_prices) > 3
                    country_codes_display = [
                        country_data.get(cid, {}).get('country_code', '')
                        for cid in countries[:3]
                    ]
                    top_country_display = f"[{', '.join(country_codes_display)}{',...' if has_more else ''}]"

                    description = "".join([
                        f"❯ Tʜᴇ Sᴛᴀʀᴛɪɴɢ Pʀɪᴄᴇ Is Oɴʟʏ {lowest_price:.2f} Pᴏɪɴᴛ's.\n",
                        f"• Aᴠᴀɪʟᴀʙʟᴇ Aᴄʀᴏss » {top_country_display}\n",
                        f"• Tᴏᴛᴀʟ Sᴛᴏᴄᴋ » {await self.format_number_to_text(total_stock)}"
                    ])

                    result_id = str(uuid.uuid4())
                    input_cmd = f"#Sᴇʀᴠɪᴄᴇ|{app_id}" if is_admin else f"/Buy_{app_id}"
                    switch_query = "#Sᴇʀᴠɪᴄᴇ " if is_admin else ""

                    if result_id not in used_ids:
                        used_ids.add(result_id)
                        results.append(
                            InlineQueryResultArticle(
                                id=result_id,
                                title=clean_name.title().translate(await small_caps()),
                                description=description,
                                thumbnail_url=(
                                    f"https://smsactivate.s3.eu-central-1.amazonaws.com/assets/ico/{first_code}0.webp"
                                    if first_code else
                                    "https://img.icons8.com/color/48/000000/shop.png"
                                ),
                                input_message_content=InputTextMessageContent(input_cmd),
                                reply_markup=InlineKeyboardMarkup().add(
                                    InlineKeyboardButton("🛒 Sᴇʀᴠɪᴄᴇs", switch_inline_query_current_chat=switch_query)
                                )
                            )
                        )
                except Exception as e:
                    logging.error(f"Error processing app {app_name}: {e}")
                    continue

            total_count = len(results)
            end_time = time.time()
            logging.info(f"Total execution time: {end_time - start_time:.3f}s")

            # Save to cache
            await cache_manager.set(
                cache_key,
                (results, total_count, time.time()),
                CachePrefix.SEARCH
            )

            await self.bot.answer_inline_query(
                inline_query.id,
                results[:CACHE_RESULTS_PER_PAGE],
                cache_time=30,
                next_offset=str(offset + CACHE_RESULTS_PER_PAGE) if total_count > offset + CACHE_RESULTS_PER_PAGE else ""
            )

        except Exception as e:
            logging.error(f"Error in query_apps: {e}")
            await self.bot.answer_inline_query(
                inline_query.id,
                [
                    InlineQueryResultArticle(
                        id="error",
                        title="Error",
                        description="An error occurred while processing your request",
                        input_message_content=InputTextMessageContent(message_text="Error: Please try again later")
                    )
                ],
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
