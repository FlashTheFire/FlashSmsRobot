import asyncio
import logging
import json
import time
from datetime import datetime
import re
from typing import Optional, Dict, Any, List, Tuple

import redis.asyncio as redis
from redis.exceptions import RedisError
from telebot.async_telebot import AsyncTeleBot
from telebot.types import (
    InlineQueryResultArticle,
    InputTextMessageContent,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)
from utils.functions import small_caps
from utils.config import SERVICE_INDEX, COMMISSION
from handlers.manager.operation import get_async_logger
from handlers.security import InputValidator
from utils.redis_manager import RedisManager, redis_manager

SERVICE_INDEX = "service_index"
CACHE_TTL = 240
CACHE_RESULTS_PER_PAGE = 50

ALPHANUM_REGEX = re.compile(r'^[A-Za-z0-9 ]+$')

class UserSearchManagement:
    def __init__(self):
        self.redis_client: Optional[RedisManager] = None
        self.cache_ttl = CACHE_TTL
        self.user_manager = None
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
            bot.register_inline_handler(
                lambda inline_query: asyncio.create_task(self.handle_inline_query(inline_query))
                if not inline_query.query.startswith('#') else None,
                func=lambda inline_query: not inline_query.query.startswith('#')
            )
            bot.register_inline_handler(
                lambda inline_query: asyncio.create_task(self.show_service_manager(inline_query)),
                lambda inline_query: inline_query.query.startswith("#Sᴇʀᴠɪᴄᴇ")
            )
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

    async def _search_pattern(self, pattern: str) -> List[Tuple[str, Dict[str, Any]]]:
        redis_client = self.redis_client
        redis_query = [
            'FT.AGGREGATE', SERVICE_INDEX,
            f'@app_price:[0.01 +inf] @app_count:[1 +inf]{pattern} @is_show_server:(True) @is_show_app:(True) @is_show_country:(True)',
            'LOAD', '3', '@app_name', '@app_code', '@app_price',
            'GROUPBY', '1', '@app_name',
            'REDUCE', 'MIN', '1', '@app_price', 'AS', 'MinPrice',
            'REDUCE', 'SUM', '1', '@app_count', 'AS', 'Total',
            'REDUCE', 'FIRST_VALUE', '4', '@app_id', 'BY', '@app_price', 'ASC', 'AS', 'app_id',
            'REDUCE', 'FIRST_VALUE', '1', '@app_code', 'AS', 'app_code',
            'SORTBY', '2', '@app_name', 'ASC',
            'LIMIT', '0', '1500'
        ]
        print(redis_query)
        try:
            result = await redis_client.execute_command(*redis_query)
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
        
        return items

    async def search_advanced(self, query: str, offset: int = 0, limit: Optional[int] = None) -> Dict[str, Any]:
        try:
            redis_client = self.redis_client
            cache_key = f"search_cache:{query}"
            cached_result = await redis_client.get(cache_key)
            if cached_result:
                result_dict = json.loads(cached_result)
                sorted_items = list(result_dict["results"].items())
                sliced_items = dict(sorted_items[offset: (offset + limit) if limit is not None else None])
                result_dict["results"] = sliced_items
                result_dict["total_results"] = len(sorted_items)
                result_dict["sliced_results"] = len(sliced_items)
                return result_dict

            # Use the advanced query builder to create a single pattern.
            advanced_query = await self.build_simple_advanced_query(query)
            # For debugging, print the advanced query.
            logging.debug(f"Advanced query: {advanced_query}")

            # Build a single configuration tuple for the advanced query.
            pattern_configs = [("advanced", advanced_query)]
            
            # Launch the search task(s).
            tasks = [self._search_pattern(pattern) for _, pattern in pattern_configs]
            results_by_pattern = await asyncio.gather(*tasks)
            
            # Define a priority mapping for categorization.
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
            await redis_client.set(cache_key, json.dumps(result_dict), ex=self.cache_ttl)
            
            sorted_items = list(sorted_results.items())
            sliced_items = dict(sorted_items[offset: (offset + limit) if limit is not None else None])
            result_dict["results"] = sliced_items
            result_dict["total_results"] = len(sorted_items)
            result_dict["sliced_results"] = len(sliced_items)
            logging.debug(f"|| results {len(sorted_items)}")
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

    async def query_apps(self, inline_query) -> None:
        try:
            query_text = inline_query.query.strip().lower()
            offset = int(inline_query.offset) if inline_query.offset else 0
            start_time = time.time()

            validation_result = await self.validate_inline_query(str(inline_query.from_user.id), query_text)
            if not validation_result["valid"]:
                await self.bot.answer_inline_query(
                    inline_query.id,
                    [
                        InlineQueryResultArticle(
                            id="error",
                            title="Error",
                            description=validation_result["error"],
                            thumbnail_url="https://img.freepik.com/free-vector/bird-colorful-logo-gradient-vector_343694-1365.jpg",
                            input_message_content=InputTextMessageContent(message_text=validation_result["error"])
                        )
                    ],
                    cache_time=5
                )
                return

            query_text = validation_result.get("sanitized_query", "")
            search_results = await self.search_advanced(query=query_text, offset=offset, limit=CACHE_RESULTS_PER_PAGE)

            if not search_results or not search_results.get("results"):
                keyboard = InlineKeyboardMarkup()
                keyboard.add(
                    InlineKeyboardButton("🔍 Contact Support", url="https://t.me/udaysupport")
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
                                    " Yᴏᴜʀ sᴇᴀʀᴄʜ ᴅɪᴅɴ'ᴛ ᴍᴀᴛᴄʜ ᴀɴʏ ᴀᴠᴀɪʟᴀʙʟᴇ sᴇʀᴠɪᴄᴇs.\n"
                                    " Sᴜɢɢᴇsᴛɪᴏɴs:\n"
                                    "• Cʜᴇᴄᴋ ʏᴏᴜʀ sᴘᴇʟʟɪɴɢ\n"
                                    "• Tʀʏ ᴍᴏʀᴇ ɢᴇɴᴇʀᴀʟ ᴋᴇʏᴡᴏʀᴅs\n"
                                    "• Cᴏɴᴛᴀᴄᴛ ᴏᴜʀ sᴜᴘᴘᴏʀᴛ ᴛᴇᴀᴍ ғᴏʀ ᴀssɪsᴛᴀɴᴄᴇ\n\n"
                                    " Wᴇ'ʀᴇ ᴄᴏɴsᴛᴀɴᴛʟʏ ᴀᴅᴅɪɴɢ ɴᴇᴡ sᴇʀᴠɪᴄᴇs!"
                                ),
                                parse_mode="Markdown"
                            )
                        )
                    ],

                    cache_time=30
                )
                return
            results = []
            used_result_ids = set()
            # Cache price-country data for the session
            price_country_data = await self.redis_client.json().get('main_data:price-country') or {}
            
            for app_name, data in search_results["results"].items():
                try:
                    app_id = str(data.get('app_id', app_name))
                    clean_app_name = self.input_validator.sanitize_text(app_name)
                    total_stock = int(data.get("total_stock", 0))
                    lowest_price = float(data.get("lowest_price", 0.0)) * float(COMMISSION)
                    app_code = data.get("app_code", "")
                    first_code = app_code.split(",")[0].strip().lower() if "," in app_code else app_code.lower()
                    
                    # Fast price data lookup with default
                    app_price_data = price_country_data.get(app_id, {})
                    
                    # Ultra-fast country processing - single pass O(n)
                    countries = []
                    country_prices = {}  # {country: lowest_price}
                    
                    if app_price_data:
                        try:
                            # Single pass to get lowest price per country
                            for price_str, country in app_price_data.items():
                                try:
                                    price = float(price_str)
                                    if price <= 0:
                                        continue
                                        
                                    curr_price = country_prices.get(country)
                                    if curr_price is None or price < curr_price:
                                        country_prices[country] = price
                                except (ValueError, TypeError):
                                    continue

                            # Get top 3 countries by price - using sorted for cleaner code
                            if country_prices:
                                countries = [
                                    country for country, _ in sorted(
                                        country_prices.items(), 
                                        key=lambda x: x[1]
                                    )[:3]
                                ]

                        except Exception as e:
                            logging.error(f"Error processing price data for app {app_id}: {e}")
                    
                    # Format country display - only show ... if we have more unique countries
                    has_more = len(country_prices) > 3  # More efficient than using set
                    country_list = countries + (["..."] if has_more else [])
                    top_country_display = f"[{', '.join(country_list) or '🌍'}]"

                    # Build description with optimized string concatenation
                    description = "".join([
                        f"❯ Tʜᴇ Sᴛᴀʀᴛɪɴɢ Pʀɪᴄᴇ Is Oɴʟʏ {lowest_price:.2f} Pᴏɪɴᴛ's.\n",
                        f"• Aᴠᴀɪʟᴀʙʟᴇ Aᴄʀᴏss » {top_country_display}\n",
                        f"• Tᴏᴛᴀʟ Sᴛᴏᴄᴋ » {total_stock}"
                    ])

                    # Result ID generation without string interpolation
                    result_id = "_".join([app_name, str(total_stock), f"{lowest_price:.2f}"])
                    if result_id not in used_result_ids:
                        used_result_ids.add(result_id)
                        results.append(
                            InlineQueryResultArticle(
                                id=result_id,
                                title=clean_app_name.title().translate(await small_caps()),
                                description=description,
                                thumbnail_url=f"https://udayscripts.in/image/service/{first_code}.png" if first_code else "https://img.icons8.com/color/48/000000/shop.png",
                                input_message_content=InputTextMessageContent(f"/Buy_{app_id}"),
                                reply_markup=InlineKeyboardMarkup().add(
                                    InlineKeyboardButton("🛒 Sᴇʀᴠɪᴄᴇs", switch_inline_query_current_chat="")
                                )
                            )
                        )
                except Exception as e:
                    logging.error(f"Error processing app {app_name}: {e}")
                    continue

            end_time = time.time()
            logging.info(f"Total execution time: {end_time - start_time:.3f}s")

            await self.bot.answer_inline_query(
                inline_query.id,
                results[:50],
                cache_time=30,
                next_offset=str(offset + CACHE_RESULTS_PER_PAGE) if len(results) >= CACHE_RESULTS_PER_PAGE else ""
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

    async def handle_inline_query(self, inline_query) -> None:
        """Handle an incoming inline query by processing it."""
        return await self.query_apps(inline_query)

    async def query_apps_admin(self, inline_query) -> None:
        try:
            query_text = inline_query.query.strip().lower().removeprefix("#sᴇʀᴠɪᴄᴇ").strip()
            offset = int(inline_query.offset) if inline_query.offset else 0
            start_time = time.time()

            validation_result = await self.validate_inline_query(str(inline_query.from_user.id), query_text)
            if not validation_result["valid"]:
                await self.bot.answer_inline_query(
                    inline_query.id,
                    [
                        InlineQueryResultArticle(
                            id="error",
                            title="Error",
                            description=validation_result["error"],
                            thumbnail_url="https://img.freepik.com/free-vector/bird-colorful-logo-gradient-vector_343694-1365.jpg",
                            input_message_content=InputTextMessageContent(message_text=validation_result["error"])
                        )
                    ],
                    cache_time=5
                )
                return

            query_text = validation_result.get("sanitized_query", "")
            search_results = await self.search_advanced(query=query_text, offset=offset, limit=CACHE_RESULTS_PER_PAGE)

            if not search_results or not search_results.get("results"):
                keyboard = InlineKeyboardMarkup()
                keyboard.add(
                    InlineKeyboardButton("🔍 Contact Support", url="https://t.me/udaysupport")
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
                                    " Yᴏᴜʀ sᴇᴀʀᴄʜ ᴅɪᴅɴ'ᴛ ᴍᴀᴛᴄʜ ᴀɴʏ ᴀᴠᴀɪʟᴀʙʟᴇ sᴇʀᴠɪᴄᴇs.\n"
                                    " Sᴜɢɢᴇsᴛɪᴏɴs:\n"
                                    "• Cʜᴇᴄᴋ ʏᴏᴜʀ sᴘᴇʟʟɪɴɢ\n"
                                    "• Tʀʏ ᴍᴏʀᴇ ɢᴇɴᴇʀᴀʟ ᴋᴇʏᴡᴏʀᴅs\n"
                                    "• Cᴏɴᴛᴀᴄᴛ ᴏᴜʀ sᴜᴘᴘᴏʀᴛ ᴛᴇᴀᴍ ғᴏʀ ᴀssɪsᴛᴀɴᴄᴇ\n\n"
                                    " Wᴇ'ʀᴇ ᴄᴏɴsᴛᴀɴᴛʟʏ ᴀᴅᴅɪɴɢ ɴᴇᴡ sᴇʀᴠɪᴄᴇs!"
                                ),
                                parse_mode="Markdown"
                            )
                        )
                    ],

                    cache_time=30
                )
                return
            results = []
            used_result_ids = set()
            # Cache price-country data for the session
            price_country_data = await self.redis_client.json().get('main_data:price-country') or {}
            
            for app_name, data in search_results["results"].items():
                try:
                    app_id = str(data.get('app_id', app_name))
                    clean_app_name = self.input_validator.sanitize_text(app_name)
                    total_stock = int(data.get("total_stock", 0))
                    lowest_price = float(data.get("lowest_price", 0.0)) * float(COMMISSION)
                    app_code = data.get("app_code", "")
                    first_code = app_code.split(",")[0].strip().lower() if "," in app_code else app_code.lower()
                    
                    # Fast price data lookup with default
                    app_price_data = price_country_data.get(app_id, {})
                    
                    # Ultra-fast country processing - single pass O(n)
                    countries = []
                    country_prices = {}  # {country: lowest_price}
                    
                    if app_price_data:
                        try:
                            # Single pass to get lowest price per country
                            for price_str, country in app_price_data.items():
                                try:
                                    price = float(price_str)
                                    if price <= 0:
                                        continue
                                        
                                    curr_price = country_prices.get(country)
                                    if curr_price is None or price < curr_price:
                                        country_prices[country] = price
                                except (ValueError, TypeError):
                                    continue

                            # Get top 3 countries by price - using sorted for cleaner code
                            if country_prices:
                                countries = [
                                    country for country, _ in sorted(
                                        country_prices.items(), 
                                        key=lambda x: x[1]
                                    )[:3]
                                ]

                        except Exception as e:
                            logging.error(f"Error processing price data for app {app_id}: {e}")
                    
                    # Format country display - only show ... if we have more unique countries
                    has_more = len(country_prices) > 3  # More efficient than using set
                    country_list = countries + (["..."] if has_more else [])
                    top_country_display = f"[{', '.join(country_list) or '🌍'}]"

                    # Build description with optimized string concatenation
                    description = "".join([
                        f"❯ Tʜᴇ Sᴛᴀʀᴛɪɴɢ Pʀɪᴄᴇ Is Oɴʟʏ {lowest_price:.2f} Pᴏɪɴᴛ's.\n",
                        f"• Aᴠᴀɪʟᴀʙʟᴇ Aᴄʀᴏss » {top_country_display}\n",
                        f"• Tᴏᴛᴀʟ Sᴛᴏᴄᴋ » {total_stock}"
                    ])

                    # Result ID generation without string interpolation
                    result_id = "_".join([app_name, str(total_stock), f"{lowest_price:.2f}"])
                    if result_id not in used_result_ids:
                        used_result_ids.add(result_id)
                        results.append(
                            InlineQueryResultArticle(
                                id=result_id,
                                title=clean_app_name.title().translate(await small_caps()),
                                description=description,
                                thumbnail_url=f"https://udayscripts.in/image/service/{first_code}.png" if first_code else "https://img.icons8.com/color/48/000000/shop.png",
                                input_message_content=InputTextMessageContent(f"#Sᴇʀᴠɪᴄᴇ|{app_id}"),
                                reply_markup=InlineKeyboardMarkup().add(
                                    InlineKeyboardButton("🛒 Sᴇʀᴠɪᴄᴇs", switch_inline_query_current_chat="#Sᴇʀᴠɪᴄᴇ ")
                                )
                            )
                        )
                except Exception as e:
                    logging.error(f"Error processing app {app_name}: {e}")
                    continue

            end_time = time.time()
            logging.info(f"Total execution time: {end_time - start_time:.3f}s")

            await self.bot.answer_inline_query(
                inline_query.id,
                results[:50],
                cache_time=30,
                next_offset=str(offset + CACHE_RESULTS_PER_PAGE) if len(results) >= CACHE_RESULTS_PER_PAGE else ""
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
 
    async def show_service_manager(self, inline_query) -> None:
        return await self.query_apps_admin(inline_query)
# Initialize the search manager instance for inline queries.
search_manager = UserSearchManagement()

async def init_managers(user_manager, order_manager=None, bot: Optional[AsyncTeleBot] = None) -> bool:
    """Initialize the search manager with the required components."""
    return await search_manager.init_managers(user_manager, bot)

async def register_handlers(bot: AsyncTeleBot) -> None:
    """Register inline query handlers with the provided bot instance."""
    await search_manager.register_handlers(bot)
