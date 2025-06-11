from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message, InlineQuery, InlineQueryResultArticle, InputTextMessageContent
from redis.commands.search.query import Query
from utils.functions import setup_logger, small_caps, large_caps, country_flag_link
from utils.cache_manager import cache_manager, CachePrefix
from utils.redis_manager import redis_manager, RedisManager
from utils.config import APP_COUNT, SERVICE_INDEX, COMMISSION
from handlers.security import RateLimiter, InputValidator, TransactionGuard
from handlers.manager.operation import OrderManagement, UserManagement
from datetime import datetime, timedelta
from termcolor import colored
from pydantic import BaseModel, Field, validator
from utils.redis_keys import RedisKeys

#import await logging
import json
import asyncio
from functools import lru_cache, partial
from typing import Dict, Any, Optional, List
from redis import Redis

class UserServerManagement:
    def __init__(self) -> None:
        self.user_manager: Optional[OrderManagement] = None
        self.input_validator: Optional[InputValidator] = None
        self.transaction_guard: Optional[TransactionGuard] = None
        self._initialized: bool = False
        self.bot: Optional[AsyncTeleBot] = None
        self.redis_client: Optional[RedisManager] = None

    async def init_managers(self, user_mgr: UserManagement, bot: Optional[AsyncTeleBot] = None) -> bool:
        try:
            if not user_mgr or not bot:
                #await logging.error("User manager and bot instance are required")
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
                #await logging.error(f"Missing required components: {', '.join(missing)}")
                return False

            self._initialized = True
            #await logging.info("||show_servers handler managers initialized successfully")
            return True

        except Exception as e:
            #await logging.error(f"Error initializing managers: {e}")
            return False

    async def ensure_managers_initialized(self) -> bool:
        if not self._initialized:
            #await logging.error("Security components not properly initialized")
            return False
        return True
    
    async def stock_formatter(self, n: float) -> str: 
        if n < 2:
            return "🌑"  # Dark Moon for < 5
        if n < 10:
            return "🔴"  # Red for 5 - 99
        if n < 30:
            return "🟠"  # Orange for 100 - 499
        if n < 50:
            return "🟡"  # Yellow for 500 - 999
        if n < 1000:
            return "🟢"  # Green for 1,000 - 9,999
        if n < 100000:
            return "🟢" #"🔵"  # Blue for 10,000 - 99,999
        return "🟢"  # White for 100,000+

    async def validate_server_request(self, user_id: str, app_id: str, server_id: Optional[str] = None) -> Dict[str, Any]:
        try:
            if not await self.ensure_managers_initialized():
                return {"valid": False, "error": "🛠️ Sᴇʀᴠɪᴄᴇ Tᴇᴍᴘᴏʀᴀʀɪʟʏ Uɴᴀᴠᴀɪʟᴀʙʟᴇ..."}
                return {"valid": False, "error": f"⚠️ Tᴏᴏ Mᴀɴʏ Sᴇʀᴠᴇʀ Rᴇǫᴜᴇsᴛs. Pʟᴇᴀsᴇ Wᴀɪᴛ {reset_time:.0f}s. (🔄 {remaining} ʟᴇғᴛ)"}
            if not self.input_validator.validate_user_id(user_id):
                return {"valid": False, "error": "🆔 Iɴᴠᴀʟɪᴅ Usᴇʀ ID Fᴏʀᴍᴀᴛ"}
            if not app_id.isdigit():
                return {"valid": False, "error": "🔢 Iɴᴠᴀʟɪᴅ Aᴘᴘ ID Fᴏʀᴍᴀᴛ"}
            if server_id and not server_id.isdigit():
                return {"valid": False, "error": "🔢 Iɴᴠᴀʟɪᴅ Sᴇʀᴠᴇʀ ID Fᴏʀᴍᴀᴛ"}
            return {"valid": True}
        except Exception as e:
            #await logging.error(f"Error validating server request: {e}")
            return {"valid": False, "error": "🔒 Iɴᴛᴇʀɴᴀʟ Vᴀʟɪᴅᴀᴛɪᴏɴ Eʀʀᴏʀ"}

    async def build_query(self, filters: dict) -> str:
        async def process_filter(field: str, value: Any) -> Optional[str]:
            if value is None:
                return None
            if isinstance(value, list) and value:
                options = ' | '.join(map(str, value))
                return f"@{field}:({options})"
            elif isinstance(value, tuple) and len(value) == 2:
                start, end = value
                return f"@{field}:[{start} {end}]"
            else:
                return f"@{field}:{value}"

        tasks = [process_filter(field, value) for field, value in filters.items()]
        query_parts = await asyncio.gather(*tasks)
        query_parts = [part for part in query_parts if part is not None]
        return ' '.join(query_parts) if query_parts else '*'

    async def fetch_server_data(
        self,
        redis_client: RedisManager,
        app_id: str,
        country_id: Optional[str] = None,
        country_name: Optional[str] = None,
        is_inline: Optional[bool] = False,
        is_admin: Optional[bool] = False,
        app_count: Optional[str] = "[1 +inf]",
        app_price: Optional[str] = "[0.01 +inf]",
        limit: Optional[int] = None,
        sort_by: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        try:
            query_str = f"@app_id:{app_id}"
            if country_id:
                query_str += f" @country_id:{country_id}"
            if country_name:
                query_str += f" @country_name:(%%{country_name}%%|{country_name}*|{country_name})"
            query_str += f" @app_price:{app_price} "
            if not is_admin:
                query_str += f"@is_show_server:(True) @is_show_country:(True) @is_show_app:(True)" #@app_count:{app_count} 

            groupby_fields = ["@server_id", "@app_name", "@country_id"]
            if is_admin:
                groupby_fields.append("@is_show_server")
                groupby_num = "4"
            else:
                groupby_num = "3"

            aggregation_query = [
                "FT.AGGREGATE", SERVICE_INDEX, query_str,
                "GROUPBY", groupby_num, *groupby_fields,
                "REDUCE", "MIN", "1", "@app_price", "AS", "MIN_PRICE",
                "REDUCE", "SUM", "1", "@app_count", "AS", "TOTAL_STOCK"
            ]

            if sort_by:
                aggregation_query.extend([
                    "SORTBY", "2", "@MIN_PRICE", sort_by.upper()
                ])

            if limit:
                aggregation_query.extend([
                    "LIMIT", "0", str(limit)
                ])
            print(colored(f"Executing aggregation query: {' '.join(aggregation_query)}", "blue"))
            result = await redis_client.execute_command(*aggregation_query)
            print(colored(f"\n\nAggregation result: {result}\n\n", "blue"))
            if not result or len(result) < 2:
                print(colored(f"No results found for query: {query_str}", "red"))
                return None

            whole_country_data = await redis_client.json().get('main_data:details:country_data') or {}
            servers_data = {}
            all_countries = {}
            global_app_name = None

            for row in result[1:]:
                flat_row = []
                for item in row:
                    if isinstance(item, bytes):
                        flat_row.append(item.decode('utf-8'))
                    else:
                        flat_row.append(str(item))
                row_dict = {}
                for i in range(0, len(flat_row), 2):
                    if i + 1 < len(flat_row):
                        row_dict[flat_row[i]] = flat_row[i + 1]

                server = row_dict.get("server_id")
                app_name_val = row_dict.get("app_name", "Unknown Service")
                cid = row_dict.get("country_id", "0")  # Use country_id directly as unique key
                # Get the country code from the country data using cid
                country = whole_country_data.get(cid, {}).get('country_code', '')
                if is_admin:
                    show_server = row_dict.get("is_show_server", False)
                if not server or not country:
                    #print(f"Skipping row due to missing server or country: {row_dict}")
                    continue

                try:
                    price = float(row_dict.get("MIN_PRICE", 0))
                    stock = int(float(row_dict.get("TOTAL_STOCK", 0)))
                except ValueError as e:
                    #print(f"Error parsing aggregated fields for row {row_dict}: {e}")
                    continue

                global_app_name = app_name_val
                all_countries[cid] = min(all_countries.get(cid, float('inf')), price)
    
                if server not in servers_data:
                    server_data = {
                        "countries": {cid: price},
                        "min_price": price,
                        "total_stock": stock,
                    }
                    if is_admin:
                        server_data["is_show_server"] = show_server
                    if is_inline:
                        server_data["prices"] = {}
                        # Save the inline price data with country_id as the key
                        server_data["prices"][cid] = price
                    servers_data[server] = server_data
                else:
                    srv = servers_data[server]
                    srv["countries"][cid] = min(srv["countries"].get(cid, float('inf')), price)
                    srv["min_price"] = min(srv["min_price"], price)
                    srv["total_stock"] += stock
                    if is_admin:
                        srv["is_show_server"] = show_server
                    if is_inline:
                        srv["prices"][cid] = min(srv["prices"].get(cid, float('inf')), price)

            if not servers_data:
                print(colored("No valid server data found after aggregation", "red"))
                return None

            top_countries = sorted(all_countries.items(), key=lambda x: (x[1], x[0]))[:3]
            top_country_ids = [cid for cid, _ in top_countries]

            # For each server, sort the country ids by price
            for server in servers_data:
                srv_countries = servers_data[server]["countries"]
                sorted_srv_countries = sorted(srv_countries.items(), key=lambda x: x[1])
                servers_data[server]["countries"] = [cid for cid, _ in sorted_srv_countries]

            data = {
                "servers": servers_data,
                "app_name": global_app_name,
                "all_countries": top_country_ids,
            }
            return data

        except Exception as e:
            #print(f"Error fetching server data: {e}")
            return None

    async def show_server(self, message: Message, app_id: str, country_id: Optional[str] = None, country_code: Optional[str] = None, page: Optional[int] = 1, is_admin: bool = False):
        try:
            data = await self.fetch_server_data(redis_client=self.redis_client, app_id=app_id, country_id=country_id, is_admin=is_admin)
            if not data or not data.get("servers"):
                # No available servers found
                return None, None, None

            keyboard = InlineKeyboardMarkup()
            # Retrieve full country data mapping (cid -> info)
            full_country_data = await self.get_country_data()
    
            sorted_servers = sorted(data["servers"].items(), key=lambda x: float(x[1]["min_price"]))
            for server_id, info in sorted_servers:
                # 'countries' now holds a list of country IDs (cids)
                countries = info["countries"]
                is_show = info.get("is_show_server")
                # Build a display list by mapping each cid to its country_code (flag)
                country_display = []
                for i, cid in enumerate(countries):
                    if i < 3:
                        code = full_country_data.get(cid, {}).get("country_code", "")
                        if code:
                            country_display.append(code)
                price = float(info["min_price"]) * float(COMMISSION)
                if len(countries) > 3:
                    country_display.append("...")
                stock = await self.stock_formatter(info["total_stock"])

                if is_admin:
                    callback_data = f"#modify_data:{app_id}:{country_id}:{server_id}"
                    if len(callback_data) > 64:
                        #print(f"Callback data too long for {country_id}: {len(callback_data)} chars")
                        continue
                    button_label = f"〔{', '.join(country_display)}〕 » Sᴇʀᴠᴇʀ{server_id}".translate(await small_caps())
                    if is_show == 'True':
                        line = f"☰ {price:.2f}    ⃝🟢".translate(await small_caps())
                    elif is_show == 'False':
                        line = f"☰ {price:.2f} 🔴 ⃝ ".translate(await small_caps())
                    keyboard.add(
                        InlineKeyboardButton(button_label, callback_data=callback_data),
                        InlineKeyboardButton(line, callback_data=f"admin_is_server:{page}:{app_id}:{country_id}:{server_id}:{is_show}")
                    )
                    is_admin = 'Admin_'
                else:
                    button_text = (
                        f"Sᴇʀᴠᴇʀ{server_id} ➨ "
                        f"[{', '.join(country_display)}] » 💎 {price:.2f} 〔{stock}〕"
                    )
                    callback_data = f"purchase:{app_id}:{price:.2f}:{server_id}:{country_id}:{country_display[0]}"
                    keyboard.add(
                        InlineKeyboardButton(text=button_text.translate(await small_caps()), callback_data=callback_data)
                    )
                    is_admin = ''

            if not keyboard.keyboard:
                await self.bot.reply_to(message, "❌ Nᴏ Aᴠᴀɪʟᴀʙʟᴇ Sᴇʀᴠᴇʀs Wɪᴛʜ Sᴛᴏᴄᴋ.")
                return None, None, None

            app_code = str(app_id.translate(await small_caps()))
            if country_id:
                keyboard.add(
                    InlineKeyboardButton(text=f"• Dᴇsᴇʟᴇᴄᴛ [{country_code}]", callback_data=f"{is_admin.lower()}country:{page}:{app_id}"),
                    InlineKeyboardButton(text=f"⌕ Cᴏᴜɴᴛʀɪᴇs", switch_inline_query_current_chat=f"#{is_admin.replace('_', '').translate(await small_caps())}AᴘᴘIᴅ:{app_code} ")
                )

            text = (
                f"<b>⦿ Sᴇʀᴠɪᴄᴇ ❯</b> {data['app_name'].translate(await small_caps())}\n\n"
                f"<b>↓ Cʜᴏᴏsᴇ Sᴇʀᴠᴇʀ Bᴇʟᴏw</b>"
            )
            return message, text, keyboard

        except Exception as e:
            #print(f"Error showing servers: {e}")
            await self.bot.reply_to(message, "❌ Aɴ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ Wʜɪʟᴇ Fᴇᴛᴄʜɪɴɢ Sᴇʀᴠᴇʀs.")
            return None, None, None

    async def process_show_servers(self, call: CallbackQuery, is_admin: bool = False) -> None:
        """
        Process a callback query to show servers.
        """
        try:
            parts = call.data.replace(' ', '').split(':')
            user_id = call.message.chat.id
            #print(f"Parts: {parts}")
            #print(f"Parts length: {call.data}")
            if len(parts) < 2 or len(parts) > 4:
                await self.bot.answer_callback_query(call.id, "⚠️ Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ", show_alert=True)
                return
            app_id = parts[1]
            country_id = parts[2] if len(parts) > 2 else None
            page = parts[3] if len(parts) > 3 else 1
            if not app_id.isdigit():
                await self.bot.answer_callback_query(call.id, "🚫 Iɴᴠᴀʟɪᴅ Aᴘᴘ ID", show_alert=True)
                return
            transaction_key = RedisKeys.transaction_lock_key(user_id, f"show_servers:{app_id}:{country_id}")
            async with TransactionGuard(self.redis_client) as guard:
                if not await self._acquire_transaction_lock(guard, transaction_key, call):
                    return
                try:
                    country_data = await self.get_country_data(country_id)
                    country_code = country_data.get('country_code', None)
                    msg, text, keyboard = await self.show_server(call.message, app_id, country_id, country_code, page, is_admin)
                    if msg and text and keyboard:
                         await self.bot.edit_message_text(
                            chat_id=call.message.chat.id,
                            message_id=call.message.message_id,
                            text=text,
                            reply_markup=keyboard,
                            parse_mode='HTML'
                        )
                    else:
                        await self.bot.answer_callback_query(call.id, "🚫 Nᴏ Sᴇʀᴠᴇʀs Aᴠᴀɪʟᴀʙʟᴇ.", show_alert=False)
                except Exception as e:
                    await self.bot.answer_callback_query(call.id, "🚫 Nᴏ Sᴇʀᴠᴇʀs Aᴠᴀɪʟᴀʙʟᴇ.", show_alert=False)
                finally:
                    await guard.release_lock(transaction_key)
        except Exception as e:
            await self.bot.answer_callback_query(call.id, "🚫 Nᴏ Sᴇʀᴠᴇʀs Aᴠᴀɪʟᴀʙʟᴇ.", show_alert=False)

    async def is_server_save(self, app_id: str, server_id: str, country_id: str, is_show: bool):
        """
        Searches Redis for keys matching the pattern 'service_data:{country_id}:*:{app_id}'
        and updates each hash field ('is_show_app', 'is_show_server', 'is_show_country') to "True"
        if is_admin is True; otherwise "False".
        Returns a list of keys if found, or None.
        """
        pattern = f"service_data:{country_id}:{server_id}:{app_id}"
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

    async def handle_is_admin_servers(self, call: CallbackQuery, is_admin: bool = False) -> None:
        """
        Process a callback query to show servers.
        """
        try:
            parts = call.data.replace(' ', '').split(':')
            user_id = call.message.chat.id
            #print(f"Parts: {parts}")
            #print(f"Parts length: {call.data}")
            if len(parts) < 6 or len(parts) > 6:
                await self.bot.answer_callback_query(call.id, "⚠️ Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ", show_alert=True)
                return
            page = parts[1]
            app_id = parts[2]
            country_id = parts[3]
            server_id = parts[4]
            is_show = parts[5] if len(parts) > 5 else False
            if not app_id.isdigit() or not server_id.isdigit():
                await self.bot.answer_callback_query(call.id, "🚫 Iɴᴠᴀʟɪᴅ Aᴘᴘ ID or Sᴇʀᴠᴇʀ ID", show_alert=True)
                return
            transaction_key = RedisKeys.transaction_lock_key(user_id, f"show_servers:{app_id}:{country_id}")
            async with TransactionGuard(self.redis_client) as guard:
                if not await self._acquire_transaction_lock(guard, transaction_key, call):
                    return
                try:
                    #empliment save function
                    t = await self.is_server_save(app_id=app_id, server_id=server_id, country_id=country_id, is_show=is_show)
                    ##print(t)
                    country_data = await self.get_country_data(country_id)
                    country_code = country_data.get('country_code', None)
                    msg, text, keyboard = await self.show_server(call.message, app_id, country_id, country_code, page, is_admin)
                    if msg and text and keyboard:
                         await self.bot.edit_message_text(
                            chat_id=call.message.chat.id,
                            message_id=call.message.message_id,
                            text=text,
                            reply_markup=keyboard,
                            parse_mode='HTML'
                        )
                    else:
                        await self.bot.answer_callback_query(call.id, "🚫 Nᴏ Sᴇʀᴠᴇʀs Aᴠᴀɪʟᴀʙʟᴇ.", show_alert=False)
                except Exception as e:
                    await self.bot.answer_callback_query(call.id, "🚫 Nᴏ Sᴇʀᴠᴇʀs Aᴠᴀɪʟᴀʙʟᴇ.", show_alert=False)
                finally:
                    await guard.release_lock(transaction_key)
        except Exception as e:
            await self.bot.answer_callback_query(call.id, "🚫 Nᴏ Sᴇʀᴠᴇʀs Aᴠᴀɪʟᴀʙʟᴇ.", show_alert=False)

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

    async def process_buy_command(self, message: Message, is_admin: bool = False) -> None:
        """
        Process a callback query to show servers.
        """
        try:
            if not is_admin:
                parts = message.text.split('_')
            else:
                parts = message.text.split('|')
            
            print(f"Parts: {parts}")
            user_id = message.from_user.id
            app_id = parts[1]
            country_id = parts[2] if len(parts) > 2 else None
            if not app_id.isdigit():
                await self.bot.reply_to(message, "🚫 Iɴᴠᴀʟɪᴅ Aᴘᴘ ID")
                return
            if len(parts) < 2:
                await self.bot.reply_to(message, "⚠️ Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ", show_alert=True)
                return
            app_id = parts[1]
            country_id = parts[2] if len(parts) > 2 else None
            if not app_id.isdigit():
                await self.bot.reply_to(message, "🚫 Iɴᴠᴀʟɪᴅ Aᴘᴘ ID", show_alert=True)
                return
            transaction_key = RedisKeys.transaction_lock_key(user_id, f"show_servers:{app_id}:{country_id}")
            async with TransactionGuard(self.redis_client) as guard:
                if not await self._acquire_transaction_lock(guard, transaction_key, message):
                    return
                try:
                    country_data = await self.get_country_data(country_id)
                    country_code = country_data.get('country_code', None)
                    msg, text, keyboard = await self.show_server(message, app_id, country_id, country_code, "1", is_admin)
                    if msg and text and keyboard:
                        await self.bot.send_message(
                            chat_id=message.chat.id,
                            text=text,
                            reply_markup=keyboard,
                            parse_mode='HTML'
                        )
                    else:
                        await self.bot.reply_to(message, "🚫 Nᴏ Sᴇʀᴠᴇʀs Aᴠᴀɪʟᴀʙʟᴇ.", show_alert=False)
                except Exception as e:
                    error_message = "<blockquote><b>👨🏻‍💻Nᴏ Sᴇʀᴠᴇʀs Aᴠᴀɪʟᴀʙʟᴇ.</b>..</blockquote>"
                    await self.bot.send_message(user_id, error_message, parse_mode='html')
                finally:
                    await guard.release_lock(transaction_key)
        except Exception as e:
            error_message = "<blockquote><b>👨🏻‍💻Nᴏ Sᴇʀᴠᴇʀs Aᴠᴀɪʟᴀʙʟᴇ.</b>..</blockquote>"
            await self.bot.send_message(user_id, error_message, parse_mode='html')

    async def _handle_app_id_inline(
        self, 
        inline_query, 
        is_admin: bool = False, 
        app_count: str = "[1 +inf]", 
        app_price: str = "[0.01 +inf]", 
        limit: int = None,
        sort_by: Optional[str] = None
    ):
        try:
            if not is_admin:
                query_parts = inline_query.query.split('#AᴘᴘIᴅ:')[1].split()
            else:
                query_parts = inline_query.query.split('#AᴅᴍɪɴAᴘᴘIᴅ:')[1].split()
            app_id = query_parts[0].translate(await large_caps())
            country_filter = query_parts[1].lower() if len(query_parts) > 1 else None

            country_data = await self.get_country_data()

            # Build mapping dictionaries keyed by unique country_id (cid)
            country_names = {}  # cid -> full name
            country_codes = {}  # lower case full name or parts -> country code
            for cid, info in country_data.items():
                code, name = info.get("country_code"), info.get("country_name")
                if code and name:
                    country_names[cid] = name
                    country_codes[name.lower()] = code
                    for part in name.lower().split():
                        if len(part) > 2:
                            country_codes[part] = code

            data = await self.fetch_server_data(
                redis_client=self.redis_client, 
                app_id=app_id, 
                country_name=country_filter,
                is_inline=True,
                app_count=app_count,
                app_price=app_price,
                limit=limit,
                sort_by=sort_by
            )
            print(data)

            if not data or not data.get("servers"):
                if inline_query.id != "tool":
                    await self.bot.answer_inline_query(inline_query.id, '[]')
                else:
                    print(colored(f"No servers found for app_id: {app_id} and country_filter: {country_filter}", "red"))
                    return []
                return

            # Aggregate data using country_id (cid) as key
            country_stats = {}
            for server_id, server_info in data["servers"].items():
                try:
                    server_num = server_id.rsplit("_", 1)[-1] if "_" in server_id else server_id
                    if not server_num.isdigit():
                        continue

                    # Now iterate over the inline prices using cid keys
                    for cid, price in server_info.get("prices", {}).items():
                        stats = country_stats.get(cid)
                        if stats is None:
                            # country_names is keyed by cid
                            cname = country_names.get(cid, "Unknown")
                            country_stats[cid] = [
                                price,                         # Minimum price
                                server_info["total_stock"],    # Total stock
                                {server_num},                  # Set of server numbers
                                cname                          # Country name
                            ]
                        else:
                            stats[0] = min(stats[0], price)
                            stats[1] += server_info["total_stock"]
                            stats[2].add(server_num)
                except (ValueError, KeyError, IndexError) as e:
                    continue

            # Filter results based on the country filter (if provided)
            filtered_countries = (
                [country_filter] if country_filter in country_stats
                else [cid for cid, stats in country_stats.items() 
                      if country_filter.lower() in stats[3].lower() or country_filter.lower() in country_codes]
            ) if country_filter else list(country_stats.keys())

            filtered_countries.sort(key=lambda cid: (country_stats[cid][0], country_stats[cid][3]))
            
            

            offset = int(inline_query.offset or 0)
            limit = 50
            results = []
         
            for cid in filtered_countries[offset:offset + limit]:
                min_price, total_stock, server_nums, country_name = country_stats[cid]

                server_list = sorted(server_nums, key=int)
                if inline_query.id != "tool":
                    server_display = (
                        f"[{', '.join(server_list)}]" if len(server_list) <= 3 
                        else f"[{', '.join(server_list[:3])}, ...]"
                    )
                else:
                    server_display = f"[{', '.join(server_list)}]"
                price = float(min_price) * float(COMMISSION)

                description = "".join([
                    f"❯ Tʜᴇ Sᴛᴀʀᴛɪɴɢ Pʀɪᴄᴇ Is Oɴʟʏ {price:.2f} Pᴏɪɴᴛ's.\n",
                    f"• Sᴇʀᴠᴇʀs » {server_display}\n",
                    f"• Tᴏᴛᴀʟ Sᴛᴏᴄᴋ » {total_stock}"
                ]).translate(await small_caps())
                if not is_admin:
                    input_message_content = f"/Buy_{app_id}_{cid}"
                else:
                    input_message_content = f"#Sᴇʀᴠɪᴄᴇ|{app_id}|{cid}"
                if inline_query.id == "tool":
                    results.append({
                        'country_id': f"{cid}",
                        'country_name': f"{country_name}",
                        'country_code': f"{country_data[cid]['country_code']}"
                    })
                else:
                    results.append(
                        InlineQueryResultArticle(
                            id=f"{cid}_{min_price}",
                            title=f"{country_name} [{country_data[cid]['country_code']}]".translate(await small_caps()),
                            description=description,
                            input_message_content=InputTextMessageContent(input_message_content),
                            thumbnail_url=country_data[cid]["flag_url"]
                        )
                    )   

            next_offset = str(offset + limit) if len(filtered_countries) > offset + limit else ""
            if inline_query.id == "tool":
                print(colored(f"Inline query id: {inline_query.id}", "red"))
                return results
            await self.bot.answer_inline_query(
                inline_query.id, 
                results,
                cache_time=1,
                next_offset=next_offset
            )
        except Exception as e:
            await self.bot.answer_inline_query(inline_query.id, [])

    async def get_country_data(self, country_id: str=None) -> dict:
        """Get country data from Redis."""
        try:
            whole_country_data = await self.redis_client.json().get('main_data:details:country_data') or {}
            if country_id:
                return whole_country_data.get(country_id, {})
            ###print('country data')
            ###print(whole_country_data)
            return whole_country_data
        except Exception as e:
            ###print(f"Error fetching country data: {e}")
            return {}


    async def register_handlers(self, bot: AsyncTeleBot) -> None:
        @bot.inline_handler(func=lambda query: query.query.startswith('#AᴘᴘIᴅ'))
        async def handle_app_id_inline(inline_query):
            try:
                await self._handle_app_id_inline(inline_query)
            except Exception as e:
                ###print(f"Error processing inline query: {e}")
                await self.bot.answer_inline_query(inline_query.id, [])
        
        @bot.inline_handler(func=lambda query: query.query.startswith('#AᴅᴍɪɴAᴘᴘIᴅ'))
        async def handle_admin_app_id_inline(inline_query):
            try:
                await self._handle_app_id_inline(inline_query, is_admin=True)
            except Exception as e:
                ###print(f"Error processing inline query: {e}")
                await self.bot.answer_inline_query(inline_query.id, [])
        
                    
        @bot.callback_query_handler(func=lambda call: call.data.startswith("servers:"))
        async def handle_show_servers(call: CallbackQuery):
            try:
                process_task = partial(self.process_show_servers, call)
                asyncio.create_task(process_task())
            except ValueError:
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", show_alert=True))
            except Exception as e:
                #logging.error(f"Callback error: {e}")
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ", show_alert=True))

        @bot.callback_query_handler(func=lambda call: call.data.startswith("admin_servers:"))
        async def handle_show_servers(call: CallbackQuery):
            try:
                process_task = partial(self.process_show_servers, call, is_admin=True)
                asyncio.create_task(process_task())
            except ValueError:
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", show_alert=True))
            except Exception as e:
                #logging.error(f"Callback error: {e}")
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ", show_alert=True))

        @bot.callback_query_handler(func=lambda call: call.data.startswith("admin_is_server:"))
        async def handle_country_callback(call: CallbackQuery):
            try:
                process_task = partial(self.handle_is_admin_servers, call, is_admin=True)
                asyncio.create_task(process_task())
            except ValueError:
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", show_alert=True))
            except Exception as e:
                #logging.error(f"Callback error: {e}")
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ", show_alert=True))

        @bot.message_handler(regexp=r'^/Buy_\d+_\d+$')
        async def handle_buy_command(message: Message):
            try:
                process_task = partial(self.process_buy_command, message)
                asyncio.create_task(process_task())
            except ValueError:
                asyncio.create_task(bot.send_message(message.chat.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", parse_mode='html'))
            except Exception as e:
                #logging.error(f"Callback error: {e}")
                asyncio.create_task(bot.send_message(message.chat.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ...", parse_mode='html'))

        @bot.message_handler(regexp=r'^#Sᴇʀᴠɪᴄᴇ\|\d+\|\d+$')
        async def handle_admin_buy_command(message: Message):
            try:
                process_task = partial(self.process_buy_command, message, is_admin=True)
                asyncio.create_task(process_task())
            except ValueError:
                asyncio.create_task(bot.send_message(message.chat.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", parse_mode='html'))
            except Exception as e:
                #logging.error(f"Callback error: {e}")
                asyncio.create_task(bot.send_message(message.chat.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ...", parse_mode='html'))

# ---------------------------------------------------------------------------
# Global instance and wrapper functions for backward compatibility
# ---------------------------------------------------------------------------
server_management = UserServerManagement()

async def init_managers(user_manager: OrderManagement, order_manager: Optional[OrderManagement]=None, bot: Optional[AsyncTeleBot] = None) -> bool:
    """Initialize the server manager with required components."""
    return await server_management.init_managers(user_manager, bot)

async def register_handlers(bot: AsyncTeleBot) -> None:
    """Register handlers for showing servers."""
    await server_management.register_handlers(bot)

__all__ = ['init_managers', 'register_handlers']
