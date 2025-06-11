from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message, ForceReply
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
SERVICE_PREFIX = "service_data"

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

    async def country_search(self, app_id: str, country_id: Optional[str] = None, server_id: Optional[str] = None, is_admin: bool = False, app_count: Optional[str] = "[1 +inf]", app_price: Optional[str] = "[0.01 +inf]", sort_by: Optional[str] = "ASC", limit: Optional[int] = 500) -> Optional[Dict[str, Any]]:
        """
        Aggregates service data by country for the given app_id.
        Combines all servers per country and returns only the lowest price for each country.
        Also returns a list of all server_ids found.
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

            query_str += f" @app_price:{app_price}"
            if not is_admin:
                query_str += f"@is_show_server:(True) @is_show_country:(True) @is_show_app:(True)" #@app_count:{app_count} 

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
                "REDUCE", "SUM", "1", "@app_count", "AS", "TOTAL_STOCK",
                "SORTBY", "2", "@MIN_PRICE", sort_by,   # or DESC for highest
                "LIMIT", "0", limit                     # returns only the top result

            ]

            result = await redis_client.execute_command(*aggregation_query)
            if not result or len(result) < 2:
                return None

            whole_country_data = await redis_client.json().get('main_data:details:country_data') or {}
            docs = []
            server_ids_set = set()

            for row in result[1:]:
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

                # Parse server_id and store in doc
                raw_sid = row_dict.get("server_id", "")
                parsed_sid = raw_sid
                try:
                    parsed_sid = int(raw_sid)
                except (ValueError, TypeError):
                    pass
                server_ids_set.add(parsed_sid)

                docs.append({
                    'country_id': country_id_val,
                    'country_name': country_name,
                    'country_code': country_code,
                    'app_name': row_dict.get("app_name", "Unknown"),
                    'app_price': price,
                    'app_count': count,
                    'app_id': app_id,
                    'is_show_country': row_dict.get("is_show_country", False),
                    'server_id': parsed_sid
                })

            if not docs:
                return None

            grouped = {}
            for doc in docs:
                key = doc['country_code']
                if key in grouped:
                    if doc['app_price'] < grouped[key]['app_price']:
                        grouped[key] = doc
                else:
                    grouped[key] = doc

            sorted_docs = sorted(grouped.values(), key=lambda x: (x['app_price'], x['country_code']))
            sorted_server_ids = sorted(server_ids_set, key=lambda x: (isinstance(x, str), x))

            return {
                'total': len(sorted_docs),
                'docs': sorted_docs,
                'server_ids': sorted_server_ids
            }

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
                search_buttons.append(InlineKeyboardButton(text="⋮ Mᴏᴅɪғʏ", callback_data=f"#modify_data:{app_id}"))
                search_buttons.append(InlineKeyboardButton(text="⌕ Cᴏᴜɴᴛʀɪᴇs", switch_inline_query_current_chat=f"#AᴅᴍɪɴAᴘᴘIᴅ:{app_code} "))

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
                search_buttons.append(InlineKeyboardButton(text="⌕ Sᴇᴀʀᴄʜ Cᴏᴜɴᴛʀɪᴇs", switch_inline_query_current_chat=f"#AᴘᴘIᴅ:{app_code} "))

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
                markup.add(*search_buttons)
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
                print(f"1 Invalid callback data: {call.data}")
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
    
    async def is_country_save(self, app_id: str=None, country_id: str=None, is_show: bool=False, server_id: str=None, field: str=None, new_status: str=None):
        """
        Searches Redis for keys matching the pattern 'service_data:{country_id}:*:{app_id}'
        and updates each hash field ('is_show_app', 'is_show_server', 'is_show_country') to "True"
        if is_admin is True; otherwise "False".
        Returns a list of keys if found, or None.
        """
        if not server_id:
            server_id = '*'
        if not country_id:
            country_id = '*'
        if not app_id:
            app_id = '*'

        pattern = f"service_data:{country_id}:{server_id}:{app_id}"
        # If your Redis client is async, use await here; otherwise adjust accordingly.
        keys = await self.redis_client.keys(pattern)
        if not new_status:
            if not keys:
                return None
            if str(is_show) == 'True':
                new_status = 'False'
            elif str(is_show) == 'False':
                new_status = 'True'


        for key in keys:
            if not field:
                await self.redis_client.hset(key, 'is_show_app', new_status)
                await self.redis_client.hset(key, 'is_show_server', new_status)
                await self.redis_client.hset(key, 'is_show_country', new_status)
            elif field:
                if str(field) == 'is_adjustable' and await self.redis_client.hexists(key, field):
                    await self.redis_client.hdel(key, field)
                elif str(field) == 'app_name':
                    await self.redis_client.hset(key, field, new_status)
                    await self.redis_client.hset(key, 'search_tags', new_status.replace(" ", "").lower())
                else:
                    await self.redis_client.hset(key, field, new_status)
        return keys

    async def handle_is_admin_countries(self, call: CallbackQuery, is_admin: bool = False) -> None:
        try:
            parts = call.data.split(":")
            user_id = call.message.chat.id
            if len(parts) not in (3, 4, 5):
                print(f"2 Invalid callback data: {call.data}")
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



    async def update_app_data(self, data, field, app_name, new_value):
        if str(field) == 'app_name':
            """Update app name."""
            if app_name in data:
                data[new_value] = data.pop(app_name)
                print(f"App name changed from '{app_name}' to '{new_value}'")
            else:
                print(f"App '{app_name}' not found!")
        elif str(field) == 'app_code':
            """Update app code."""
            app_code = new_value.replace(" ", "").split(',') if ',' in new_value else new_value
            if app_name in data:
                data[app_name]["code"] = app_code
                print(f"Code for '{app_name}' updated to {new_value}")
            else:
                print(f"App '{app_name}' not found!")
        else:
            print(f"field Not Found: {field}")
        return data


    async def handle_modify_data(self, call: CallbackQuery, is_server: bool = False, is_update: bool = False, is_reply: bool = False, is_adjustable: bool = False) -> None:
        try:
            text = ''
            country_id = None
            app_id = None
            server_id = None
            if is_reply:
                # When processing a reply, "call" is actually the message from the user.
                message = call
                user_id = message.chat.id
                app_data = message.text.strip()
                # Default to "0" if empty response.
                if not app_data:
                    app_data = "0"
                try:
                    if message.reply_to_message:
                        await self.bot.delete_message(user_id, message.reply_to_message.message_id)
                    await self.bot.delete_message(user_id, message.message_id)
                except Exception as e:
                    print("Error deleting messages:", e)

                service_data = await self.redis_client.json().get('cache_data:app-edit') or {}
                print(service_data)
                key = f'{message.chat.id}:{message.reply_to_message.message_id}'
                if key in service_data:
                    stored_data = service_data[key]
                    message_id = stored_data.get("message_id")
                    app_id = stored_data.get("app_id")
                    country_id = stored_data.get("country_id", None)
                    server_id = stored_data.get("server_id", None)
                    if country_id:
                        text += f" @country_id:{country_id}"
                    if server_id:
                        text += f" @server_id:{server_id}"
                    field = stored_data.get("field")
                    del service_data[key]
                    service_code = await self.redis_client.json().get('main_data:service:app_data') or {}
                    server_query = [
                        "FT.SEARCH", "service_index", f"@app_id:{app_id}", 
                        "RETURN", "1", "app_name", 
                        "LIMIT", "0", "1"
                    ]
                    results = await self.redis_client.execute_command(*server_query)
                    print(results)
                    # Check if results exist and have at least one valid entry
                    if isinstance(results, list) and len(results) > 2 and isinstance(results[2], list):
                        data = dict(zip(results[2][::2], results[2][1::2]))  # Convert list to dictionary
                        app_name = data.get('app_name')  # Get app_name safely
                    else:
                        app_name = None  # Handle missing data case

                    print(f"App Name: {app_name}")
                    if app_name:
                        updated = await self.update_app_data(service_code, field, app_name, app_data)
                        if updated:
                            await self.redis_client.json().set('main_data:service:app_data', '$', updated)
                    await self.is_country_save(app_id=app_id, field=field, new_status=app_data, country_id=country_id, server_id=server_id)
                    await self.redis_client.json().set('cache_data:app-edit', '$', service_data)
                else:
                    print("Stored service data not found for key:", key)

            else:
                parts = call.data.split(":")
                user_id = call.message.chat.id

            if is_update:
                await self.bot.answer_callback_query(call.id)
                if str(len(parts)) not in ['3', '5']:
                    print(f"3 Invalid callback data: {call.data}")
                    await self.bot.answer_callback_query(call.id, "⚠️ Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ", show_alert=True)
                    return
                if len(parts) == 3:
                    _, field, app_id = parts
                elif len(parts) == 5:
                    _, field, app_id, country_id, server_id  = parts
                    if country_id:
                        text += f" @country_id:{country_id}"
                    if server_id:
                        text += f" @server_id:{server_id}"
                force_reply_markup = ForceReply(selective=True)
                human_field = field.replace('_', ' ').title().translate(await small_caps())
                msg = await self.bot.send_message(
                    call.message.chat.id,
                    f"<b>❯ Pʟᴇᴀsᴇ Eɴᴛᴇʀ {human_field} Fᴏʀ AᴘᴘIᴅ »</b> <code>{app_id}</code>",
                    reply_markup=force_reply_markup,
                    parse_mode='HTML'
                )
                service_data = await self.redis_client.json().get('cache_data:app-edit') or {}
                key = f'{call.message.chat.id}:{msg.message_id}'
                service_data[key] = {"field": field, "app_id": app_id, "message_id": call.message.message_id}
                if country_id:
                    service_data[key].update({"country_id": country_id})
                if server_id:
                    service_data[key].update({"server_id": server_id})
                await self.redis_client.json().set('cache_data:app-edit', '$', service_data)
                return

            elif is_server:
                if len(parts) != 4:
                    print(f"4 Invalid callback data: {call.data}")
                    await self.bot.answer_callback_query(call.id, "⚠️ Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ", show_alert=True)
                    return
                _, app_id, server_id, is_show = parts
                t = await self.is_country_save(app_id=app_id, is_show=is_show, server_id=server_id)

            elif is_adjustable:
                if len(parts) != 4:
                    print(f"56 Invalid callback data: {call.data}")
                    await self.bot.answer_callback_query(call.id, "⚠️ Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ", show_alert=True)
                    return
                _, app_id, country_id, server_id = parts
                if country_id:
                    text += f" @country_id:{country_id}"
                if server_id:
                    text += f" @server_id:{server_id}"
                t = await self.is_country_save(app_id=app_id, field='is_adjustable', country_id=country_id, server_id=server_id, new_status='True')

            elif not (is_server or is_reply or is_update):
                if str(len(parts)) not in ['2', '4']:
                    print(f"5 Invalid callback data: {call.data}")
                    await self.bot.answer_callback_query(call.id, "⚠️ Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ", show_alert=True)
                    return
                if len(parts) == 2:
                    _, app_id = parts
                elif len(parts) == 4:
                    _, app_id, country_id, server_id = parts
                    if country_id:
                        text += f" @country_id:{country_id}"
                    if server_id:
                        text += f" @server_id:{server_id}"

                
            total_query = [
                "FT.AGGREGATE", "service_index", f"@app_id:{app_id}{text}",
                "GROUPBY", "1", "@app_id",
                "REDUCE", "FIRST_VALUE", "1", "@app_name", "AS", "app_name",
                "REDUCE", "FIRST_VALUE", "1", "@app_code", "AS", "app_code",
                "REDUCE", "FIRST_VALUE", "1", "@app_price", "AS", "app_price",
                "REDUCE", "COUNT_DISTINCT", "1", "@server_id", "AS", "total_servers",
                "REDUCE", "COUNT_DISTINCT", "1", "@country_id", "AS", "total_countries"
            ]
            order_query = [
                "FT.AGGREGATE", "order_index", f"@order_status:(COMPLETED|PROCESSING) @app_id:{app_id}{text}",
                "GROUPBY", "0",
                "REDUCE", "SUM", "1", "@order_amount", "AS", "total_order_amount",
                "REDUCE", "COUNT", "0", "AS", "total_orders"
            ] 
            cancel_query = [
                "FT.AGGREGATE", "order_index", f"@order_status:(CANCELLED|TIMEOUT) @app_id:{app_id}{text}",
                "GROUPBY", "0",
                "REDUCE", "COUNT", "0", "AS", "total_cancelled_orders"
            ]
            server_query = None
            if not country_id:
                server_query = [
                    "FT.AGGREGATE", "service_index", f"@app_id:{app_id}{text}",
                    "LOAD", "1", "@is_show_server",
                    "GROUPBY", "1", "@server_id",
                    "REDUCE", "FIRST_VALUE", "1", "@is_show_server", "AS", "is_show_server"
                ]

            tasks = [
                self.redis_client.execute_command(*total_query),
                self.redis_client.execute_command(*order_query),
                self.redis_client.execute_command(*cancel_query)
            ]
            if server_query:
                tasks.append(self.redis_client.execute_command(*server_query))
            results = await asyncio.gather(*tasks, return_exceptions=True)
            total_country_res, order_res, cancel_res = results[:3]
            if server_query:
                server_res = results[3]
            
            # Check for expected structure in total_country_res
            if not isinstance(total_country_res, list) or len(total_country_res) < 2:
                raise ValueError("Unexpected response structure for total_query")
            flat_list = total_country_res[1] if isinstance(total_country_res[1], list) else []
            result_dict = {flat_list[i*2]: flat_list[i*2 + 1] for i in range(len(flat_list) // 2)}

            # Extract values with defaults if missing
            app_name = result_dict.get("app_name", "Unknown").translate(await small_caps())
            app_code = result_dict.get("app_code", "Unknown").translate(await small_caps())
            app_price = result_dict.get("app_price", "").translate(await small_caps())
            country_data = await redis_manager.redis_client.json().get('main_data:details:country_data') or {}
            country_name = country_data.get(country_id, {}).get('country_name', '').translate(await small_caps())
            country_code = country_data.get(country_id, {}).get('country_code', '')
            total_servers = result_dict.get("total_servers", "0").translate(await small_caps())
            total_countries = result_dict.get("total_countries", "0").translate(await small_caps())

            # Process order results with safety checks.
            # Use defaults (0) if order_res doesn't have the expected indices.
            try:
                sell_price = float(order_res[1][1])
            except (IndexError, ValueError, TypeError):
                sell_price = 0.0

            try:
                total_success_orders = int(order_res[1][3])
            except (IndexError, ValueError, TypeError):
                total_success_orders = 0

            try:
                total_cancelled = int(cancel_res[1][1])
            except (IndexError, ValueError, TypeError):
                total_cancelled = 0

            try:
                total_orders = int(order_res[1][-1]) + total_cancelled  # If total_orders is the last element
            except (IndexError, ValueError, TypeError):
                total_orders = total_success_orders + total_cancelled  # Fallback


            # Calculate product price and earned commission. If sell_price is 0, defaults remain 0.
            product_price = sell_price / float(COMMISSION) if float(COMMISSION) != 0 else 0.0
            earned = sell_price - product_price

            # If there are no orders, default success ratio to 0.
            success_ratio = (total_success_orders / total_orders * 100) if total_orders > 0 else 0
            success_rate = f"{success_ratio:.2f}".replace(".00", "")

            # Create Server Buttons
            keyboard = InlineKeyboardMarkup()
            if server_query:
                server_buttons = []
                if isinstance(server_res, list) and len(server_res) > 1:
                    sorted_servers = sorted(server_res[1:], key=lambda x: int(x[1]))  # Sort by server_id
                    for row in sorted_servers:
                        server_id = row[1]
                        is_show_server = row[3]
                        text = f"{server_id}" if str(is_show_server) == 'True' else f"{server_id}⃠"
                        server_buttons.append(
                            InlineKeyboardButton(text.translate(await small_caps()), callback_data=f"is_server_off:{app_id}:{server_id}:{is_show_server}")
                        )

                if server_buttons:
                    keyboard.row(*server_buttons)
                keyboard.add(
                    InlineKeyboardButton("Mᴏᴅɪғʏ Nᴀᴍᴇ", callback_data=f"update_data:app_name:{app_id}"),
                    InlineKeyboardButton("Uᴘᴅᴀᴛᴇ Cᴏᴅᴇ", callback_data=f"update_data:app_code:{app_id}")
                )
                keyboard.add(
                    InlineKeyboardButton("⬅️ Bᴀᴄᴋ", callback_data=f"admin_country:1:{app_id}"),
                    InlineKeyboardButton("Sᴇᴛ Mᴏᴄᴋ", callback_data="show_country")
                )

                caption = (
                    "<b>🛒 Sᴇʀᴠɪᴄᴇ Iɴsɪɢʜᴛs ❯</b>\n\n"
                    "<blockquote expandable>"
                    "🌐 Aᴘᴘ Nᴀᴍᴇ  »  <code>{}</code>\n"
                    "📜 Aᴘᴘ Cᴏᴅᴇ   »  <code>{}</code>\n\n"
                    "🔔 Mᴏᴄᴋ Nᴜᴍʙᴇʀ   »  <code>{}</code> <b>Pᴇʀcᴇɴᴛ</b>\n"
                    "✅ Sᴜᴄᴄᴇss Rᴀᴛᴇ    »  <code>{}</code> <b>Pᴇʀcᴇɴᴛ</b>"
                    "</blockquote>\n\n<blockquote expandable>"
                    "📨 Tᴏᴛᴀʟ Sᴇʀᴠᴇʀs   »  <code>{}</code>\n"
                    "🌎 Tᴏᴛᴀʟ Cᴏᴜɴᴛʀʏ  »  <code>{}</code>\n\n"
                    "🛍️ Tᴏᴛᴀʟ Pᴜʀᴄʜᴀsᴇ  »  <code>{}</code> <b>Oʀᴅᴇʀs</b>\n"
                    "💸 Tᴏᴛᴀʟ Rᴇᴠᴇɴᴜᴇ    »  <code>{}</code> <b>Rs</b>"
                    "</blockquote>\n\n"
                    "Sᴇʟᴇᴄᴛ A Sᴇʀᴠɪᴄᴇ Oᴘᴛɪᴏɴ Bᴇʟᴏᴡ."
                ).format(
                    app_name,
                    app_code,
                    "10".translate(await small_caps()),
                    str(success_rate).translate(await small_caps()),
                    total_servers,
                    total_countries,
                    str(total_success_orders).translate(await small_caps()),
                    "{:.2f}".format(earned).translate(await small_caps()), 
                )
            else:
                redis_key = f"{SERVICE_PREFIX}:{country_id}:{server_id}:{app_id}"
                is_adjustable = await self.redis_client.hget(redis_key, "is_adjustable")
                tick = "🔴" if is_adjustable else "🟢"
                keyboard.add(
                    InlineKeyboardButton(f"Aᴅᴊᴜsᴛᴀʙʟᴇ [{tick}]", callback_data=f"is_adjustable:{app_id}:{country_id}:{server_id}"),
                    InlineKeyboardButton("Uᴘᴅᴀᴛᴇ Pʀɪᴄᴇ", callback_data=f"update_data:app_price:{app_id}:{country_id}:{server_id}")
                )
                callback_data = f"admin_servers:{app_id}:{country_id}:1"
                keyboard.add(
                    InlineKeyboardButton("⬅️ Bᴀᴄᴋ", callback_data=callback_data),
                    InlineKeyboardButton("Sᴇᴛ Mᴏᴄᴋ", callback_data="show_country")
                )
                caption = (
                    "<b>🛒 Sᴇʀᴠɪᴄᴇ Iɴsɪɢʜᴛs ❯</b>\n\n"
                    "<blockquote expandable>"
                    "🌐 Aᴘᴘ Nᴀᴍᴇ  »  <code>{}</code>\n"
                    "💰 Aᴘᴘ Pʀɪᴄᴇ  »  <code>{}</code> <b>Pᴏɪɴᴛs</b>\n\n"
                    "🔔 Mᴏᴄᴋ Nᴜᴍʙᴇʀ   »  <code>{}</code> <b>Pᴇʀcᴇɴᴛ</b>\n"
                    "✅ Sᴜᴄᴄᴇss Rᴀᴛᴇ    »  <code>{}</code> <b>Pᴇʀcᴇɴᴛ</b>"
                    "</blockquote>\n\n<blockquote expandable>"
                    "🌎 Cᴏᴜɴᴛʀʏ      »  <code>{}</code> <b>[ <code>{}</code> ]</b>\n"
                    "💡 Sᴇʀᴠᴇʀ Nᴀᴍᴇ  »  <code>#Sᴇʀᴠᴇʀ{}</code>\n\n"
                    "🛍️ Tᴏᴛᴀʟ Pᴜʀᴄʜᴀsᴇ  »  <code>{}</code> <b>Oʀᴅᴇʀs</b>\n"
                    "💸 Tᴏᴛᴀʟ Rᴇᴠᴇɴᴜᴇ    »  <code>{}</code> <b>Rs</b>"
                    "</blockquote>\n\n"
                    "Sᴇʟᴇᴄᴛ A Sᴇʀᴠɪᴄᴇ Oᴘᴛɪᴏɴ Bᴇʟᴏᴡ."
                ).format(
                    app_name,
                    app_price,
                    "0".translate(await small_caps()),
                    str(success_rate).translate(await small_caps()),
                    country_name,
                    country_code,
                    server_id,
                    str(total_success_orders).translate(await small_caps()),
                    "{:.2f}".format(earned).translate(await small_caps()), 
                )
            
            if is_reply:
                await self.bot.edit_message_text(
                    chat_id=user_id,
                    message_id=message_id,
                    text=caption,
                    parse_mode='HTML',
                    reply_markup=keyboard
                )
            else:
                await self.bot.edit_message_text(
                    chat_id=user_id,
                    message_id=call.message.message_id,
                    text=caption,
                    parse_mode='HTML',
                    reply_markup=keyboard
                )
                await self.bot.answer_callback_query(call.id, "✅ Sᴜᴄᴄᴇssғᴜʟ Lᴏᴀᴅ", show_alert=False)

        except Exception as e:
            print(f"Error in handle_modify_data: {e}")
            if is_reply:
                await self.bot.send_message(user_id, f"🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ...\n\n{str(e)}", parse_mode='html')
            else:
                await self.bot.answer_callback_query(call.id, "⚠️ Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ", show_alert=True)


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
            
        @bot.callback_query_handler(func=lambda call: call.data.startswith("#modify_data:"))
        async def handle_modify_data_callback(call: CallbackQuery):
            try:
                process_task = partial(self.handle_modify_data, call)
                asyncio.create_task(process_task())
            except ValueError:
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", show_alert=True))
            except Exception as e:
                #logging.error(f"Callback error: {e}")
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ", show_alert=True))

        @bot.callback_query_handler(func=lambda call: call.data.startswith("is_adjustable:"))
        async def handle_is_adjustable_callback(call: CallbackQuery):
            try:
                process_task = partial(self.handle_modify_data, call, is_adjustable=True)
                asyncio.create_task(process_task())
            except ValueError:
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", show_alert=True))
            except Exception as e:
                #logging.error(f"Callback error: {e}")
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ", show_alert=True))

        @bot.callback_query_handler(func=lambda call: call.data.startswith("is_server_off:"))
        async def handle_is_server_off_callback(call: CallbackQuery):
            try:
                process_task = partial(self.handle_modify_data, call, is_server=True)
                asyncio.create_task(process_task())
            except ValueError:
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", show_alert=True))
            except Exception as e:
                #logging.error(f"Callback error: {e}")
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ", show_alert=True))

        @bot.callback_query_handler(func=lambda call: call.data.startswith("update_data:"))
        async def handle_update_data_callback(call: CallbackQuery):
            try:
                process_task = partial(self.handle_modify_data, call, is_update=True)
                asyncio.create_task(process_task())
            except ValueError:
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", show_alert=True))
            except Exception as e:
                #logging.error(f"Callback error: {e}")
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ", show_alert=True))
            
        @bot.message_handler(func=lambda message: message.reply_to_message and message.reply_to_message.text.startswith("❯ Pʟᴇᴀsᴇ Eɴᴛᴇʀ"))
        async def handle_modify_data(message: Message):
            try:
                process_task = partial(self.handle_modify_data, message, is_reply=True)
                asyncio.create_task(process_task())
            except ValueError:
                asyncio.create_task(bot.send_message(message.chat.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", parse_mode='html'))
            except Exception as e:
                #logging.error(f"Callback error: {e}")
                asyncio.create_task(bot.send_message(message.chat.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ...", parse_mode='html'))



country_management = UserCountryManagement()

async def init_managers(user_manager: UserManagement, bot: Optional[AsyncTeleBot] = None, order_manager: Optional[OrderManagement] = None) -> bool:
    return await country_management.init_managers(user_manager, bot)

async def register_handlers(bot: AsyncTeleBot) -> None:
    await country_management.register_handlers(bot)

__all__ = ['register_handlers', 'init_managers']
