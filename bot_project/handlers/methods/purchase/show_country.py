from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
from utils.functions import setup_logger, small_caps
from utils.redis_manager import redis_manager, RedisManager
from utils.cache_manager import cache_manager, CachePrefix
from utils.config import SERVICE_INDEX, COMMISSION
from redis.commands.search.query import Query
from handlers.security import RateLimiter, InputValidator, TransactionGuard
from typing import Dict, Any, Optional, List, Tuple
import asyncio
from handlers.manager.operation import OrderManagement, UserManagement
from redis import Redis
from functools import partial
from utils.redis_keys import RedisKeys
#import logging

class UserCountryManagement:
    def __init__(self) -> None:
        self.bot: Optional[AsyncTeleBot] = None
        self.input_validator: Optional[InputValidator] = None
        self.transaction_guard: Optional[TransactionGuard] = None
        self.user_manager: Optional[UserManagement] = None  # Added missing attribute
        self._initialized: bool = False
        self._buttons_cache: Dict[str, Tuple[InlineKeyboardMarkup, List[str]]] = {}
        self.redis_client: Optional[RedisManager] = None

    async def init_managers(self, user_mgr: UserManagement, bot: Optional[AsyncTeleBot] = None) -> bool:
        try:
            if not user_mgr or not bot:
                #logging.error("User manager and bot instance are required")
                return False

            self.user_manager = user_mgr
            self.bot = bot
            self.input_validator = getattr(bot, 'input_validator', None)
            self.transaction_guard = getattr(bot, 'transaction_guard', None)
            self.redis_client = await redis_manager.get_client()

            if not all([self.user_manager, self.input_validator, self.transaction_guard]):
                missing = [name for name, comp in [
                    ('user_manager', self.user_manager),
                    ('input_validator', self.input_validator),
                    ('transaction_guard', self.transaction_guard)
                ] if not comp]
                #logging.error(f"Missing required components: {', '.join(missing)}")
                return False

            self._initialized = True
            #logging.info("|| show_countries handler managers initialized successfully")
            return True

        except Exception as e:
            #logging.error(f"Error initializing managers: {e}")
            return False

    async def validate_country_request(self, user_id: str, app_id: str, server_id: str, page: int = 1) -> Dict[str, Any]:
        try:
            if not (self.input_validator.validate_user_id(user_id) and app_id.isdigit() and server_id.isdigit() and page >= 1):
                return {"valid": False, "error": "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Pᴀʀᴀᴍᴇᴛᴇʀs"}

            return {"valid": True}

        except Exception as e:
            #logging.error(f"Validation error: {e}")
            return {"valid": False, "error": "🔒 Iɴᴛᴇʀɴᴀʟ Vᴀʟɪᴅᴀᴛɪᴏɴ Eʀʀᴏʀ"}

    async def country_search(self, app_id: str, country_id: Optional[str] = None, server_id: Optional[str] = None, is_admin: bool = False) -> Optional[Dict[str, Any]]:
        """
        Aggregates service data by country for the given app_id.
        Combines all servers per country and returns only the lowest price for each country.
        """
        try:
            redis_client = self.redis_client
            if not redis_client:
                return None

            # Build query string with provided filters.
            query_str = f'@app_id:{app_id}'
            if server_id:
                query_str += f' @server_id:{server_id}'

            if country_id:
                query_str += f' @country_id:{country_id}'
                
            query_str += " @app_price:[0.01 +inf] @app_count:[1 +inf]"
            if not is_admin:
                query_str += " @is_show_server:(True) @is_show_country:(True) @is_show_app:(True)"

            groupby_fields = ["@country_id", "@country_name", "@server_id", "@app_name"]
            if is_admin:
                groupby_fields.append("@is_show_country")
                groupby_num = "5"
            else:
                groupby_num = "4"

            aggregation_query = [
                "FT.AGGREGATE", SERVICE_INDEX, query_str,
                "GROUPBY", groupby_num, *groupby_fields,
                "REDUCE", "MIN", "1", "@app_price", "AS", "MIN_PRICE",
                "REDUCE", "SUM", "1", "@app_count", "AS", "TOTAL_STOCK"
            ]
            #print(f"Executing aggregation query: {' '.join(aggregation_query)}")
            result = await redis_client.execute_command(*aggregation_query)
            if not result or len(result) < 2:
                return None

            # Retrieve full country details for decoding country information.
            whole_country_data = await redis_client.json().get('main_data:details:country_data') or {}
            docs = []
            for row in result[1:]:
                # Create a dictionary for the row, decoding bytes if necessary.
                row_dict = {
                    row[i].decode('utf-8') if isinstance(row[i], bytes) else row[i]:
                    row[i+1].decode('utf-8') if isinstance(row[i+1], bytes) else row[i+1]
                    for i in range(0, len(row), 2)
                }
                try:
                    price = float(row_dict.get("MIN_PRICE", 0))
                    count = int(row_dict.get("TOTAL_STOCK", 0))
                except ValueError:
                    continue

                country_id_val = row_dict.get("country_id", "")
                country_info = whole_country_data.get(country_id_val, {})
                country_code = country_info.get('country_code', '')
                country_name = country_info.get('country_name', '')
                docs.append({
                    'country_id': country_id_val,
                    'country_name': country_name,
                    'country_code': country_code,
                    'app_name': row_dict.get("app_name", "Unknown"),
                    'app_price': price,
                    'app_count': count,
                    'app_id': app_id,
                    'is_show_country': row_dict.get("is_show_country", False)
                })

            if not docs:
                return None

            # Group results by country code and keep the record with the lowest price.
            grouped = {}
            for doc in docs:
                key = doc['country_code']
                if key in grouped:
                    if doc['app_price'] < grouped[key]['app_price']:
                        grouped[key] = doc
                else:
                    grouped[key] = doc

            aggregated_docs = list(grouped.values())
            # Sort the aggregated results by price and country code.
            sorted_docs = sorted(aggregated_docs, key=lambda x: (x['app_price'], x['country_code']))
            return {'total': len(sorted_docs), 'docs': sorted_docs}

        except Exception as e:
            print(f"Aggregation query error in country_search: {e}")
            return None

    async def generate_buttons(self, search_result: Dict[str, Any], page: int = 1, per_page_items: int = 6, country_id: Optional[str] = None, is_admin: bool = False) -> Optional[Tuple[InlineKeyboardMarkup, List[str]]]:
        """
        Generates inline buttons for each unique country (one button per country)
        using the lowest price data. The button text includes the country code,
        a truncated country name, and the price (without server id).
        """
        try:
            docs = search_result.get('docs', [])
            if not docs:
                return None, None

            # Use the app_id and app_name from the first document.
            app_id = docs[0]['app_id']
            app_name = docs[0].get('app_name', 'Unknown Service')

            markup = InlineKeyboardMarkup()
            total_items = len(docs)
            start_index = (page - 1) * per_page_items
            end_index = min(page * per_page_items, total_items)
            
             # Add navigation buttons if needed.
            app_code = str(app_id.translate(await small_caps()))
            prev_buttons = []
            next_buttons = []
            select_buttons = []
            search_buttons = []

            for doc in docs[start_index:end_index]:
                country_code = doc['country_code']
                country_name = doc['country_name'][:12]  # Limit length if needed.
                price = float(doc['app_price']) * float(COMMISSION)
                int_country_id = doc['country_id']

                if is_admin:
                    callback_data = f"admin_servers:{app_id}:{int_country_id}:{page}"
                    if len(callback_data) > 64:
                        print(f"Callback data too long for {country_name}: {len(callback_data)} chars")
                        continue
                    country_name_short = country_name[:5] + ('.' if len(country_name) > 5 else '')
                    button_label = f"〔{country_code}〕 » {country_name_short}".translate(await small_caps())
                    is_show = str(doc['is_show_country'])
                    if is_show == 'True':
                        line = f"☰ {price:.2f}    ⃝🟢".translate(await small_caps())
                    elif is_show == 'False':
                        line = f"☰ {price:.2f} 🔴 ⃝ ".translate(await small_caps())
                    markup.add(
                        InlineKeyboardButton(button_label, callback_data=callback_data), # #
                        InlineKeyboardButton(line, callback_data=f"admin_is_country:{page}:{app_id}:{int_country_id}:{is_show}") # page, app_id, country_id, is_show
                    )
                else:
                    callback_data = f"servers:{app_id}:{int_country_id}:{page}"
                    if len(callback_data) > 64:
                        print(f"Callback data too long for {country_name}: {len(callback_data)} chars")
                        continue
                    button_label = f"{country_code} {country_name} ↝ 💎 {price:.2f}".translate(await small_caps())
                    markup.add(InlineKeyboardButton(button_label, callback_data=callback_data))
                    
            
            if is_admin:
                if page > 1:
                    prev_buttons.append(InlineKeyboardButton("« Pʀᴇᴠɪᴏᴜs", callback_data=f"admin_country:{page - 1}:{app_id}"))
                if end_index < total_items:
                    next_buttons.append(InlineKeyboardButton("Nᴇxᴛ »", callback_data=f"admin_country:{page + 1}:{app_id}"))
                search_buttons.append(InlineKeyboardButton(text="⋮ Eᴅɪᴛ", callback_data=f"edit:{app_id}"))
                search_buttons.append(InlineKeyboardButton(text="⌕ Cᴏᴜɴᴛʀɪᴇs", switch_inline_query_current_chat=f"#AᴅᴍɪɴAᴘᴘIᴅ:{app_code}"))

                if (not country_id and page == 1) or (end_index >= total_items):
                    select_buttons.append(InlineKeyboardButton(text="• Sᴇʟᴇᴄᴛ [🇮🇳]", callback_data=f"admin_servers:{app_id}:{'22'}:{page} "))
                elif country_id:
                    select_buttons.append(InlineKeyboardButton(text=f"• Dᴇsᴇʟᴇᴄᴛ [{country_code}]", callback_data=f"admin_servers:{app_id}:{int_country_id}:{page} "))
                is_admin = 'Aᴅᴍɪɴ'
            
            else:
                if page > 1:
                    prev_buttons.append(InlineKeyboardButton("« Pʀᴇᴠɪᴏᴜs", callback_data=f"country:{page - 1}:{app_id}"))
                if end_index < total_items:
                    next_buttons.append(InlineKeyboardButton("Nᴇxᴛ »", callback_data=f"country:{page + 1}:{app_id}"))
                search_buttons.append(InlineKeyboardButton(text="⌕ Sᴇᴀʀᴄʜ Cᴏᴜɴᴛʀɪᴇs", switch_inline_query_current_chat=f"#AᴘᴘIᴅ:{app_code}"))

                if (not country_id and page == 1) or (end_index >= total_items):
                    select_buttons.append(InlineKeyboardButton(text="• Sᴇʟᴇᴄᴛ [🇮🇳]", callback_data=f"servers:{app_id}:{'22'}:{page} "))
                elif country_id:
                    select_buttons.append(InlineKeyboardButton(text=f"• Dᴇsᴇʟᴇᴄᴛ [{country_code}]", callback_data=f"servers:{app_id}:{int_country_id}:{page} "))
                is_admin = ''
            
            if (prev_buttons and not next_buttons) and select_buttons:
                markup.add(*prev_buttons, *select_buttons)
                markup.add(*search_buttons)
            elif (next_buttons and not prev_buttons) and select_buttons:
                markup.add(*select_buttons, *next_buttons)
                markup.add(*search_buttons)
            elif next_buttons and prev_buttons:
                markup.add(*prev_buttons, *next_buttons)
                markup.add(*search_buttons)
            elif not (prev_buttons and next_buttons) and select_buttons:
                markup.add(*select_buttons, InlineKeyboardButton(text="⌕ Cᴏᴜɴᴛʀɪᴇs", switch_inline_query_current_chat=f"#{is_admin}AᴘᴘIᴅ:{app_code}"))
            return markup, [app_id, app_name]

        except Exception as e:
            print(f"Error generating buttons: {e}")
            return None, None

    async def get_country_data(self, country_id: str = None) -> dict:
        """Get country data from Redis."""
        try:
            whole_country_data = await self.redis_client.json().get('main_data:details:country_data') or {}
            if country_id:
                return whole_country_data.get(country_id, {})
            return whole_country_data
        except Exception as e:
            print(f"Error fetching country data: {e}")
            return {}

    async def process_buy_command(self, message: Message) -> None:
        """
        Process a buy command from a user message.
        """
        try:
            parts = message.text.replace(' ', '').split('_')
            user_id = message.from_user.id
            if len(parts) < 2:
                await self.bot.reply_to(message, "⚠️ Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ", show_alert=True)
                return

            app_id = parts[1]
            country_id = parts[2] if len(parts) > 2 else None
            page = 1
            transaction_key = RedisKeys.transaction_lock_key(user_id, f"show_country:{app_id}:{country_id}")
            async with TransactionGuard(self.redis_client) as guard:
                if not await self._acquire_transaction_lock(guard, transaction_key, message):
                    return
                try:
                    if not app_id.isdigit():
                        await self.bot.reply_to(message, "🚫 Iɴᴠᴀʟɪᴅ Aᴘᴘ ID")
                        return
                except Exception as e:
                    print(f"Error processing buy command: {e}")
                    await self.bot.reply_to(message, "🚫 Eʀʀᴏʀ Gᴇɴᴇʀᴀᴛɪɴɢ Rᴇǫᴜᴇsᴛ.")
                    return
                finally:
                    await guard.release_lock(transaction_key)
            
            print(f"Country ID: {country_id}\nPage: {page}\nApp ID: {app_id}")
            try:
                page = int(page)
            except ValueError:
                await self.bot.reply_to(message, "⚠️ Iɴᴠᴀʟɪᴅ Pᴀɢᴇ Nᴜᴍʙᴇʀ")
                return

            search_result = await self.country_search(app_id=app_id, country_id=country_id)
            if not search_result:
                await self.bot.reply_to(message, "🌎 Nᴏ Cᴏᴜɴᴛʀɪᴇs Aᴠᴀɪʟᴀʙʟᴇ")
                return

            markup, server_info = await self.generate_buttons(search_result=search_result, page=page, country_id=country_id)
            if not markup or not server_info:
                await self.bot.reply_to(message, "🚫 Eʀʀᴏʀ Gᴇɴᴇʀᴀᴛɪɴɢ Mᴇɴᴜ")
                return

            text = (
                "<b>⦿ Sᴇʀᴠɪᴄᴇ ❯ </b>"
                f"<b>{server_info[1].translate(await small_caps())}\n\n"
                "↓ Sᴇʟᴇᴄᴛ Tʜᴇ Cᴏᴜɴᴛʀʏ.</b>.."
            )

            await self.bot.send_message(
                chat_id=message.chat.id,
                reply_to_message_id=message.message_id,
                text=text,
                reply_markup=markup,
                parse_mode='HTML'
            )

        except Exception as e:
            print(f"Error in process_buy_command: {e}")
            await self.bot.reply_to(message, "Error processing request.")



    async def process_admin_command(self, message: Message) -> None:
        """
        Process a buy command from a user message.
        """
        try:
            parts = message.text.split('|')
            user_id = message.from_user.id
            if len(parts) < 2 or parts[0] != '#Sᴇʀᴠɪᴄᴇ':
                await self.bot.reply_to(message, "⚠️ Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ", show_alert=True)
                return

            app_id = parts[1]
            country_id = parts[2] if len(parts) > 2 else None
            page = 1
            transaction_key = RedisKeys.transaction_lock_key(user_id, f"show_country:{app_id}:{country_id}")
            async with TransactionGuard(self.redis_client) as guard:
                if not await self._acquire_transaction_lock(guard, transaction_key, message):
                    return
                try:
                    if not app_id.isdigit():
                        await self.bot.reply_to(message, "🚫 Iɴᴠᴀʟɪᴅ Aᴘᴘ ID")
                        return
                except Exception as e:
                    print(f"Error processing buy command: {e}")
                    await self.bot.reply_to(message, "🚫 Eʀʀᴏʀ Gᴇɴᴇʀᴀᴛɪɴɢ Rᴇǫᴜᴇsᴛ.")
                    return
                finally:
                    await guard.release_lock(transaction_key)
            
            print(f"Country ID: {country_id}\nPage: {page}\nApp ID: {app_id}")
            try:
                page = int(page)
            except ValueError:
                await self.bot.reply_to(message, "⚠️ Iɴᴠᴀʟɪᴅ Pᴀɢᴇ Nᴜᴍʙᴇʀ")
                return

            search_result = await self.country_search(app_id=app_id, country_id=country_id, is_admin=True)
            if not search_result:
                await self.bot.reply_to(message, "🌎 Nᴏ Cᴏᴜɴᴛʀɪᴇs Aᴠᴀɪʟᴀʙʟᴇ")
                return

            markup, server_info = await self.generate_buttons(search_result=search_result, page=page, country_id=country_id, is_admin=True)
            if not markup or not server_info:
                await self.bot.reply_to(message, "🚫 Eʀʀᴏʀ Gᴇɴᴇʀᴀᴛɪɴɢ Mᴇɴᴜ")
                return

            text = (
                "<b>⦿ Sᴇʀᴠɪᴄᴇ ❯ </b>"
                f"<b>{server_info[1].translate(await small_caps())}\n\n"
                "↓ Sᴇʟᴇᴄᴛ Tʜᴇ Cᴏᴜɴᴛʀʏ.</b>.."
            )

            await self.bot.send_message(
                chat_id=message.chat.id,
                reply_to_message_id=message.message_id,
                text=text,
                reply_markup=markup,
                parse_mode='HTML'
            )

        except Exception as e:
            print(f"Error in process_buy_command: {e}")
            await self.bot.reply_to(message, "Error processing request.")







    async def handle_show_countries(self, call: CallbackQuery, is_admin: bool = False) -> None:
        try:
            parts = call.data.split(":")
            user_id = call.message.chat.id
            if len(parts) not in (3, 4):
                await self.bot.answer_callback_query(call.id, "⚠️ Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ", show_alert=True)
                return
            if len(parts) == 3:
                _, page, app_id = parts
                country_id = None
            else:
                _, page, app_id, country_id = parts
            transaction_key = RedisKeys.transaction_lock_key(user_id, f"show_country:{app_id}:{country_id}")
            async with TransactionGuard(self.redis_client) as guard:
                if not await self._acquire_transaction_lock(guard, transaction_key, call):
                    return
                try:
                    print(f"Country ID: {country_id}\nPage: {page}\nApp ID: {app_id}")
                    try:
                        page = int(page)
                    except ValueError:
                        await self.bot.answer_callback_query(call.id, "⚠️ Iɴᴠᴀʟɪᴅ Pᴀɢᴇ Nᴜᴍʙᴇʀ", show_alert=True)
                        return
                    search_result = await self.country_search(app_id=app_id, country_id=country_id, is_admin=is_admin)
                    if not search_result:
                        await self.bot.answer_callback_query(call.id, "🌎 Nᴏ Cᴏᴜɴᴛʀɪᴇs Aᴠᴀɪʟᴀʙʟᴇ", show_alert=True)
                        return
                    markup, server_info = await self.generate_buttons(search_result=search_result, page=page, country_id=country_id, is_admin=is_admin)
                    if not markup or not server_info:
                        await self.bot.answer_callback_query(call.id, "🚫 Eʀʀᴏʀ Gᴇɴᴇʀᴀᴛɪɴɢ Mᴇɴᴜ", show_alert=True)
                        return
                    text = (
                        "<b>⦿ Sᴇʀᴠɪᴄᴇ ❯ </b>"
                        f"<b>{server_info[1].translate(await small_caps())}\n\n"
                        "↓ Sᴇʟᴇᴄᴛ Tʜᴇ Cᴏᴜɴᴛʀʏ.</b>.."
                    )
                    await self.bot.edit_message_text(
                        chat_id=call.message.chat.id,
                        message_id=call.message.message_id,
                        text=text,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                    
                except Exception as e:
                    error_message = "<blockquote><b>👨🏻‍💻 Bᴀᴅ Aᴄᴛɪᴏɴ Pᴇʀғᴏʀᴍᴇᴅ, Yᴏᴜ Nᴇᴇᴅ Tᴏ Cᴏɴᴛᴀᴄᴛ Cᴜsᴛᴏᴍᴇʀ Sᴜᴘᴘᴏʀᴛ Fʀᴏᴍ Hᴇʟᴘ Dᴇsᴋ...</b></blockquote>"    
                    await self.bot.send_message(user_id, error_message, parse_mode='html')
                finally:
                    await guard.release_lock(transaction_key)
        except Exception as e:
            error_message = "<blockquote><b>👨🏻‍💻 Bᴀᴅ Aᴄᴛɪᴏɴ Pᴇʀғᴏʀᴍᴇᴅ, Yᴏᴜ Nᴇᴇᴅ Tᴏ Cᴏɴᴛᴀᴄᴛ Cᴜsᴛᴏᴍᴇʀ Sᴜᴘᴘᴏʀᴛ Fʀᴏᴍ Hᴇʟᴘ Dᴇsᴋ...</b></blockquote>"
            await self.bot.send_message(user_id, error_message, parse_mode='html')
    

    async def is_country_save(self, app_id: str, country_id: str, is_show: bool):
        """
        Searches Redis for keys matching the pattern 'service_data:{country_id}:*:{app_id}'
        and updates each hash field ('is_show_app', 'is_show_server', 'is_show_country') to "True"
        if is_admin is True; otherwise "False".
        Returns a list of keys if found, or None.
        """
        pattern = f"service_data:{country_id}:*:{app_id}"
        # If your Redis client is async, use await here; otherwise adjust accordingly.
        keys = await self.redis_client.keys(pattern)
        if not keys:
            return None
        if str(is_show) == 'True':
            new_status = 'False'
        elif str(is_show) == 'False':
            new_status = 'True'

        for key in keys:
            #await self.redis_client.hset(key, 'is_show_app', new_status)
            await self.redis_client.hset(key, 'is_show_server', new_status)
            await self.redis_client.hset(key, 'is_show_country', new_status)
        return keys

    async def handle_is_admin_countries(self, call: CallbackQuery, is_admin: bool = False) -> None:
        try:
            parts = call.data.split(":")
            user_id = call.message.chat.id
            if len(parts) not in (3, 4, 5):
                await self.bot.answer_callback_query(call.id, "⚠️ Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ", show_alert=True)
                return
            if len(parts) == 3:
                _, page, app_id = parts
                country_id = None
            elif len(parts) == 4:
                _, page, app_id, country_id = parts
                #is_show = 'False'
            else:
                _, page, app_id, country_id, is_show = parts
                
            transaction_key = RedisKeys.transaction_lock_key(user_id, f"show_country:{app_id}:{country_id}")
            async with TransactionGuard(self.redis_client) as guard:
                if not await self._acquire_transaction_lock(guard, transaction_key, call):
                    return
                try:
                    print(f"Country ID: {country_id}\nPage: {page}\nApp ID: {app_id}")
                    try:
                        page = int(page)
                    except ValueError:
                        await self.bot.answer_callback_query(call.id, "⚠️ Iɴᴠᴀʟɪᴅ Pᴀɢᴇ Nᴜᴍʙᴇʀ", show_alert=True)
                        return
                    #empliment save function
                    t = await self.is_country_save(app_id=app_id, country_id=country_id, is_show=is_show)
                    #print(t)
                    search_result = await self.country_search(app_id=app_id, is_admin=is_admin)
                    if not search_result:
                        await self.bot.answer_callback_query(call.id, "🌎 Nᴏ Cᴏᴜɴᴛʀɪᴇs Aᴠᴀɪʟᴀʙʟᴇ", show_alert=True)
                        return
                    markup, server_info = await self.generate_buttons(search_result=search_result, page=page, is_admin=is_admin)
                    if not markup or not server_info:
                        await self.bot.answer_callback_query(call.id, "🚫 Eʀʀᴏʀ Gᴇɴᴇʀᴀᴛɪɴɢ Mᴇɴᴜ", show_alert=True)
                        return
                    text = (
                        "<b>⦿ Sᴇʀᴠɪᴄᴇ ❯ </b>"
                        f"<b>{server_info[1].translate(await small_caps())}\n\n"
                        "↓ Sᴇʟᴇᴄᴛ Tʜᴇ Cᴏᴜɴᴛʀʏ.</b>.."
                    )
                    await self.bot.edit_message_text(
                        chat_id=call.message.chat.id,
                        message_id=call.message.message_id,
                        text=text,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                    
                except Exception as e:
                    error_message = "<blockquote><b>👨🏻‍💻 Bᴀᴅ Aᴄᴛɪᴏɴ Pᴇʀғᴏʀᴍᴇᴅ, Yᴏᴜ Nᴇᴇᴅ Tᴏ Cᴏɴᴛᴀᴄᴛ Cᴜsᴛᴏᴍᴇʀ Sᴜᴘᴘᴏʀᴛ Fʀᴏᴍ Hᴇʟᴘ Dᴇsᴋ...</b></blockquote>"    
                    await self.bot.send_message(user_id, error_message, parse_mode='html')
                finally:
                    await guard.release_lock(transaction_key)
        except Exception as e:
            error_message = "<blockquote><b>👨🏻‍💻 Bᴀᴅ Aᴄᴛɪᴏɴ Pᴇʀғᴏʀᴍᴇᴅ, Yᴏᴜ Nᴇᴇᴅ Tᴏ Cᴏɴᴛᴀᴄᴛ Cᴜsᴛᴏᴍᴇʀ Sᴜᴘᴘᴏʀᴛ Fʀᴏᴍ Hᴇʟᴘ Dᴇsᴋ...</b></blockquote>"
            await self.bot.send_message(user_id, error_message, parse_mode='html')
    





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

    async def register_handlers(self, bot: AsyncTeleBot) -> None:
        @bot.message_handler(regexp=r'^/Buy_\d+')
        async def handle_buy_command(message: Message):
            try:
                process_task = partial(self.process_buy_command, message)
                asyncio.create_task(process_task())
            except ValueError:
                asyncio.create_task(bot.send_message(message.chat.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", parse_mode='html'))
            except Exception as e:
                #logging.error(f"Callback error: {e}")
                asyncio.create_task(bot.send_message(message.chat.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ...", parse_mode='html'))
        
        @bot.message_handler(regexp=r'^#Sᴇʀᴠɪᴄᴇ\|(\d+)$')
        async def handle_admin_command(message: Message):
            try:
                process_task = partial(self.process_admin_command, message)
                asyncio.create_task(process_task())
            except ValueError:
                asyncio.create_task(bot.send_message(message.chat.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", parse_mode='html'))
            except Exception as e:
                #logging.error(f"Callback error: {e}")
                asyncio.create_task(bot.send_message(message.chat.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ...", parse_mode='html'))

        @bot.callback_query_handler(func=lambda call: call.data.startswith("country:"))
        async def handle_country_callback(call: CallbackQuery):
            try:
                process_task = partial(self.handle_show_countries, call)
                asyncio.create_task(process_task())
            except ValueError:
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", show_alert=True))
            except Exception as e:
                #logging.error(f"Callback error: {e}")
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ", show_alert=True))
            
        @bot.callback_query_handler(func=lambda call: call.data.startswith("admin_country:"))
        async def handle_country_callback(call: CallbackQuery):
            try:
                process_task = partial(self.handle_show_countries, call, is_admin=True)
                asyncio.create_task(process_task())
            except ValueError:
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", show_alert=True))
            except Exception as e:
                #logging.error(f"Callback error: {e}")
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ", show_alert=True))

        @bot.callback_query_handler(func=lambda call: call.data.startswith("admin_is_country:"))
        async def handle_country_callback(call: CallbackQuery):
            try:
                process_task = partial(self.handle_is_admin_countries, call, is_admin=True)
                asyncio.create_task(process_task())
            except ValueError:
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", show_alert=True))
            except Exception as e:
                #logging.error(f"Callback error: {e}")
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ", show_alert=True))
            
country_management = UserCountryManagement()

async def init_managers(user_manager: UserManagement, bot: Optional[AsyncTeleBot] = None, order_manager: Optional[OrderManagement] = None) -> bool:
    return await country_management.init_managers(user_manager, bot)

async def register_handlers(bot: AsyncTeleBot) -> None:
    await country_management.register_handlers(bot)

__all__ = ['register_handlers', 'init_managers']
