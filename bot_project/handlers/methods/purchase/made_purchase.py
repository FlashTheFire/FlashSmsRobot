from typing import Tuple, Dict, Optional, Any, List
from datetime import datetime
import aiohttp
#import logging
import re
import json
import time
import phonenumbers
import asyncio
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery, InputMediaPhoto, InputMediaVideo
from phonenumbers import parse, format_number, PhoneNumberFormat, NumberParseException
from redis.commands.search.query import Query
from redis import WatchError
from redis.asyncio import Redis
from telebot.types import CallbackQuery, User, Chat, Message
from redis.commands.search.field import NumericField
import asyncio

import requests
import uuid
from termcolor import colored
from utils.config import COMMISSION
from requests import RequestException
import os
import sys
import asyncio
import aiohttp
import base64
from io import BytesIO
from PIL import Image, ImageDraw, ImageFont
from pilmoji import Pilmoji
from urllib.parse import quote

# Local imports
from utils.redis_keys import RedisKeys 
from utils.functions import convert_rub_to_usd, get_api_info, AfterMin, small_caps, convert_usd_to_rub, convert_rub_to_usd
from handlers.manager.operation import FinancialManagement, UserManagement, OrderManagement
from utils.cache_manager import cache_manager, CachePrefix
from utils.config import SERVICE_INDEX
from handlers.security import RateLimiter, InputValidator, TransactionGuard
from utils.redis_manager import RedisManager, redis_manager
from utils.config import BASE_TIMEOUT
from handlers.main.top_services import top_service_manager, TopServiceManager
from functools import partial
from utils.config import ADMIN_ID

IMGBB_API_KEY = "530530e324408b15858555c78a657a96"  # Replace with your actual API key if needed



target_chat_id   = 7990629353    # the private group/chat you’re listening to
DESTINATION_CHAT_ID = 5716978793       # Where to send the parsed OTP info

#logger = logging.getLogger(__name__)

class UserPurchaseManagement:
    def __init__(self) -> None:
        self._initialized = False
        self.order_manager: Optional[OrderManagement] = None
        self.user_manager: Optional[UserManagement] = None
        self.aggregator: Optional[FinancialManagement] = None
        self.redis_client: Optional[RedisManager] = None
        self.rate_limiter: Optional[RateLimiter] = None
        self.top_service_manager: Optional[TopServiceManager] = None
        self.input_validator: Optional[InputValidator] = None
        self.transaction_guard: Optional[TransactionGuard] = None
        self.bot: Optional[AsyncTeleBot] = None
        self.ADMIN_ID = ADMIN_ID

    async def init_managers(self, order_mgr: OrderManagement, user_mgr: UserManagement, 
                            bot: AsyncTeleBot) -> bool:
        """Initialize required components for purchase handling asynchronously"""
        try:
            if not all([order_mgr, user_mgr, bot]):
                #logger.error("Missing required components for initialization")
                return False

            self.order_manager = order_mgr
            self.user_manager = user_mgr
            self.aggregator = bot.aggregator
            self.bot = bot
            self.input_validator = bot.input_validator
            self.transaction_guard = bot.transaction_guard
            self.top_service_manager = top_service_manager
            redis_client = await redis_manager.get_client()
            self.rate_limiter = RateLimiter(
                redis_client=redis_client,
                duration=60,
                max_requests=100
            )
            self.redis_client = redis_client
            self._initialized = True
            self._running_schedules: set[str] = set()

            asyncio.create_task(self._listen_for_schedule_events())

            #logging.infoawait asyncio.to_thread(, '-' * 70)
            #await asyncio.to_thread(logger.info, "|| Purchase managers initialized successfully")
            return True
        except Exception as e:
            print(f"Initialization error: {e}")
            return False

    async def fetch_app_data(
        self,
        app_id: str,
        country_id: str,
        server_id: Optional[str] = None,
        price: Optional[float] = None,
    ) -> Optional[Dict]:
        """
        Retrieve a document matching app_id and country_id.
        If server_id is given, filter by it without price logic.
        If server_id is None and price is given, filter by documents with app_price ≤ price,
        returning the highest possible price under the limit. If that yields no results, fallback
        to any price. If both server_id and price are None, return the first match by app_id and country_id.
        """
        def fld(val, id_field, name_field):
            if not val:
                return None
            return (
                f"@{id_field}:{val}"
                if val.isnumeric()
                else f"@{name_field}:(%%{val}%%|{val}*|{val})"
            )

        try:
            redis_client = self.redis_client
            # Build cache key parts
            key_parts = [app_id]
            if server_id is not None:
                key_parts.append(server_id)
            key_parts.append(country_id)
            if price is not None and server_id is None:
                key_parts.append(str(price))
            cache_key = "app_data:" + ":".join(str(part) for part in key_parts)

            # Base RedisSearch tag query (no braces)
            parts = filter(
                None,
                [
                    fld(country_id, "country_id", "country_name"),
                    fld(app_id, "app_id", "app_name"),
                    f"@server_id:{server_id}" if server_id else None,
                    "@app_price:[0.01 +inf]",
                ],
            )
            q = " ".join(parts) or "*"

            # Initial price-filtered search when applicable
            # Build full RedisSearch tag query
            query_str_full = q
            if server_id is None and price is not None:
                # append numeric range filter in tag syntax
                query_str_full += f" @app_price:[0 {price}]"
            q = Query(query_str_full).sort_by("app_price", asc=True)
            q = q.paging(0, 1)
            result = await redis_client.ft(SERVICE_INDEX).search(q)

            # Fallback: if price filtering yielded nothing, retry without price filter
            if not result.docs and server_id is None and price is not None:
                q2 = Query(q).sort_by("app_price", asc=True).paging(0, 1)
                result = await redis_client.ft(SERVICE_INDEX).search(q2)

            if not result.docs:
                return None

            # Process and cache
            app_data = await self._process_app_documents([result.docs[0]])
            await cache_manager.set(redis_client, cache_key, app_data, 300, CachePrefix.SEARCH)
            if price is None:
                return app_data
            return app_data if float(app_data['app_price']) <= price else {}
        except Exception as e:
            print(f"App data fetch error: {e}")
            return None

    async def _process_app_documents(self, docs) -> Dict:
        """
        Process Redis documents into a dict containing all available fields.
        """
        doc = docs[0]
        # Dynamically extract all public fields
        data = {
            field: getattr(doc, field)
            for field in dir(doc)
            if not field.startswith("_") and not callable(getattr(doc, field))
        }
        return data

    async def unmask_number(self, masked: str, candidates: list[str]) -> str:
        """
        Given something like '7707503*897', build a regex '^7707503\d897$'
        and return the one candidate that matches, or return masked if none.
        """
        # Escape then turn '*' → '\d'
        pattern = "^" + re.escape(masked).replace(r"\*", r"\d") + "$"
        for num in candidates:
            if re.match(pattern, num):
                return num
        return masked


    async def reconstruct_fake_call(self, full_data) -> CallbackQuery:
        if not isinstance(full_data, dict):
            raise ValueError("Invalid input for reconstructing fake call")

        user = User(
            id=full_data.get("user_id", 0),
            is_bot=False,
            first_name=full_data.get("first_name", "User")
        )

        chat = Chat(
            id=full_data.get("call_chat_id", 0),
            type=full_data.get("chat_type", "private")
        )

        message = Message(
            message_id=full_data.get("message_id", 0),
            from_user=user,
            chat=chat,
            date=int(time.time()),
            content_type="text",
            options={},
            json_string="{}"
        )

        call = CallbackQuery(
            id=str(uuid.uuid4()),  # or reuse a stored ID
            from_user=user,
            chat_instance="fake-instance",  # dummy data
            message=message,
            data=full_data.get("call_data", ""),
            json_string="{}"  # dummy data
        )

        return call
    
    async def get_stylized_time_ago(self, score: int) -> str:
        """
        Get stylized time ago from score (past time difference)
        """
        now = int(time.time())
        diff = now - score  # Use past time difference

        if diff <= 0:
            return "Jᴜsᴛ ɴᴏᴡ"

        if diff < 60:
            value = diff
            unit = "Sᴇᴄᴏɴᴅ"
        elif diff < 3600:
            value = diff // 60
            unit = "Mɪɴᴜᴛᴇ"
        elif diff < 86400:
            value = diff // 3600
            unit = "Hᴏᴜʀ"
        else:
            value = diff // 86400
            unit = "Dᴀʏ"

        return f"{value} {unit}{'s' if value != 1 else ''}"

    '''async def _process_app_documents(self, docs) -> Dict:
        """Process Redis documents into app data structure asynchronously"""
        app_data = {
            'app_name': docs[0].app_name,
            'app_code': docs[0].app_code,
            'app_price': float(docs[0].app_price),
            'app_count': int(docs[0].app_count),
            'operator': docs[0].server_name,
            'server_id': docs[0].server_id,
        }

        #for doc in docs:
        #    try:
        #        price = float(doc.app_price)
        #        count = int(doc.app_count)
        #        if float(count) > 0:
        #            app_data['min_price'] = min(app_data['min_price'], price)
        #            app_data['total_stock'] += count
        #            if server := getattr(doc, 'server_id', None):
        #                app_data['servers'].add(server)
        #    except (ValueError, AttributeError):
        #        continue

        #app_data['min_price'] = app_data['min_price'] if app_data['min_price'] != float('inf') else 0
        #app_data['servers'] = sorted(app_data['servers'])
        return app_data'''

    async def process_purchase_flow(self, call, user_id: str, app_id: str, price: float,
                                  server_id: int, country_id: str, country_code: str, country_name: str) -> bool:
        """Handle complete purchase transaction flow"""
        start_time = time.time() 
        progress_msg = await self.bot.send_message(user_id, 
                                                 "<b>⏳ Pʀᴏᴄᴇssɪɴɢ Yᴏᴜʀ Oʀᴅᴇʀ..</b>.", 
                                                 parse_mode="HTML")
        transaction_key = RedisKeys.transaction_lock_key(user_id, f"purchase:{user_id}")
        redis_client = self.redis_client

        async with TransactionGuard(redis_client) as guard:
            if not await self._acquire_transaction_lock(guard, transaction_key, call, progress_msg.message_id):
                return False
            end_time = time.time()
            print(f"Transaction lock acquired in {end_time - start_time:.8f} seconds")

            try:
                return await self._execute_purchase_steps(call, user_id, app_id, price, 
                                                        server_id, country_id, country_code, country_name, progress_msg, transaction_key, guard)
            except Exception as e:
                print(f"Purchase processing error: {e}")
                try:
                    await self.bot.answer_callback_query(call.id, "🚫 Pᴜʀᴄʜᴀsᴇ Fᴀɪʟᴇᴅ. Pʟᴇᴀsᴇ Tʀʏ Aɢᴀɪɴ.", show_alert=True)
                except:
                    pass
                return False
            finally:
                await guard.release_lock(transaction_key)

    async def _acquire_transaction_lock(self, guard, transaction_key, call, message_id) -> bool:
        """Acquire transaction lock with error handling"""
        lock = await guard.acquire_lock(transaction_key)
        if not lock:
            try:
                await self.bot.edit_message_text(chat_id=call.message.chat.id, message_id=message_id, 
                                                text="<b>🔒 Aɴᴏᴛʜᴇʀ Tʀᴀɴsᴀᴄᴛɪᴏɴ Iɴ Pʀᴏɢʀᴇss.</b>", parse_mode="HTML")
                await self.bot.answer_callback_query(call.id, 
                    "🔒 Aɴᴏᴛʜᴇʀ Tʀᴀɴsᴀᴄᴛɪᴏɴ Iɴ Pʀᴏɢʀᴇss, Pʟᴇᴀsᴇ Wᴀɪᴛ...", show_alert=False)
            except:
                pass
            return False
        
        return True

    async def _execute_purchase_steps(self, call, user_id, app_id, price, 
                                    server_id, country_id, country_code, country_name, progress_msg, transaction_key, guard) -> bool:
        """Execute all steps in purchase process"""
        callback_user_id = call.from_user.id if call.from_user else None
        chat_id = call.message.chat.id if call.message and call.message.chat else callback_user_id

        redis_key = f"service_data:{country_id}:{server_id}:{app_id}"
        current_price = await redis_manager.redis_client.hget(redis_key, 'app_price')
        
        if current_price is not None:
            current_price = float(current_price.decode() if isinstance(current_price, bytes) else current_price)
            price = round(float(current_price) * float(COMMISSION), 2)
        else:
            price = 1000

        if not await self._handle_user_balance(user_id, price, chat_id, progress_msg):
            return False
        
        app_data = await self.fetch_app_data(app_id,  country_id, server_id)
        print(app_data)
        if not app_data:
            raise ValueError("🚫 Iɴᴠᴀʟɪᴅ Aᴘᴘʟɪᴄᴀᴛɪᴏɴ Cᴏɴғɪɢᴜʀᴀᴛɪᴏɴ")
        #logging.info(app_data)
 
        await self.bot.edit_message_text(
            chat_id=chat_id,
            message_id=progress_msg.message_id,
            text="<b>⌛ Fᴇᴛᴄʜɪɴɢ Fʀᴏᴍ Sᴇʀᴠᴇʀ..</b>.", 
            parse_mode="HTML"
        )
        phone_result = await self.fetch_phone_number(server_id, app_data['app_code'], country_id, price=price, operator=app_data['server_name'], app_name=app_data['app_name'], chat_id=chat_id, app_id=app_id)
        print(json.dumps(phone_result, indent=4))
        if not phone_result.get("status"):
            # Release lock & notify error
            if phone_result.get("message"):
                try:
                    try:
                        await self.bot.answer_callback_query(
                            call.id,
                            phone_result.get('message', '❌ Uɴᴋɴᴏᴡɴ Eʀʀᴏʀ'),
                            show_alert=False
                        )
                    except:
                        pass
                    # Offer inline buttons
                    markup = InlineKeyboardMarkup()

                    redis_key = f"schedule:service_data:{country_id}:{server_id}:{app_id}"
        
                    is_user_registered = await redis_manager.redis_client.zscore(redis_key, chat_id)

                    callback_id = f"{user_id}:{country_id}:{server_id}:{app_id}"
                    full_data = {
                        "server_id": server_id,
                        "app_code": app_data['app_code'],
                        "country_id": country_id,
                        "price": price,
                        "operator": app_data['server_name'],
                        "app_name": app_data['app_name'],
                        "guard": None,
                        "message_id": progress_msg.message_id,
                        "chat_id": chat_id,
                        "transaction_key": transaction_key,
                        "app_data": app_data,
                        "country_code": country_code,
                        "country_name": country_name,
                        "app_id": app_id,
                        "call_data": call.data,
                        "user_id": call.from_user.id,
                        "first_name": call.from_user.first_name,
                        "chat_type": call.message.chat.type if call.message else "private",
                        "call_chat_id": call.message.chat.id if call.message else chat_id,
                    }
                    await redis_manager.redis_client.set(f"cache-data:callback_data:{callback_id}", json.dumps(full_data))
                    if is_user_registered is None:
                        btn = InlineKeyboardButton(
                            "🔔 Qᴜᴇᴜᴇ Bᴜʏ", callback_data=f"notify_on:{callback_id}"
                        )
                    else:
                        btn = InlineKeyboardButton(
                            "🔕 Lᴇᴀᴠᴇ Qᴜᴇᴜᴇ", callback_data=f"notify_off:{callback_id}"
                        )
                    search = InlineKeyboardButton(
                        text="⌕ Cᴏᴜɴᴛʀɪᴇs",
                        switch_inline_query_current_chat=f"#AᴘᴘIᴅ:{str(full_data['app_id']).translate(await small_caps())} "
                    )
                    markup.add(btn, search)
                    await self.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=progress_msg.message_id,
                        text=(
                            "<b>📵 Nᴏ Nᴜᴍʙᴇʀꜱ Iɴ Sᴛᴏᴄᴋ Rɪɢʜᴛ Nᴏᴡ.</b>\n\n"
                            "<blockquote expandable>"
                            "<b>Wᴏᴜʟᴅ Yᴏᴜ Lɪᴋᴇ Mᴇ Tᴏ “</b><code>Nᴏᴛɪғʏ</code><b>”</b>\n"
                            "<b>Yᴏᴜ Wʜᴇɴ Tʜᴇ Sᴇʀᴠɪᴄᴇ Bᴇᴄᴏᴍᴇꜱ Aᴠᴀɪʟᴀʙʟᴇ?</b>\n\n"
                            f"<b>• Sᴇʀᴠɪᴄᴇ »</b> <code>{full_data['app_name'].translate(await small_caps())}</code>\n"
                            f"<b>• Cᴏᴜɴᴛʀʏ »</b> <code>{full_data['country_name'].translate(await small_caps())}</code> "
                            f"[<code>{full_data['country_code']}</code>]\n"
                            f"<b>• Aᴍᴏᴜɴᴛ »</b> 💎 <code>{str(full_data['price']).translate(await small_caps())}</code> "
                            f"[<code>{str(full_data['server_id']).translate(await small_caps())}</code>]"
                            "</blockquote>"
                        ),
                        reply_markup=markup,
                        parse_mode="HTML"
                    )
                    return False
                except Exception as e:
                    print(f"Error sending notification: {e}")
                    return False
            else:
                raise False

        try:
            await guard.release_lock(transaction_key)
            await self.bot.answer_callback_query(call.id, "🛍️ Nᴜᴍʙᴇʀ Pᴜʀᴄʜᴀsᴇᴅ Sᴜᴄᴄᴇssғᴜʟʟʏ...", show_alert=False)
        except:
            pass
        await self._finalize_purchase(call, phone_result, app_data, price, country_id, country_code, country_name, phone_result['service'], progress_msg)
        return True

    async def _handle_user_balance(self, user_id, price, chat_id, progress_msg=None) -> bool:
        """Handle balance check and deduction"""
        try:
            user_data = await self.aggregator.get_user(user_id)
            if not user_data or not user_data.get('response'):
                #logger.error("Failed to retrieve user data.")
                return False

            current_balance = user_data["metrics"]["current_balance"]
            
            if current_balance <= price:
                if progress_msg is not None:
                    await self._handle_insufficient_balance(chat_id, progress_msg, price, current_balance)
                return False
            
            return True
        except Exception as e:
            #logger.error(f"Error handling user balance: {str(e)}")
            return False

    async def _validate_purchase_request(self, user_id: str, price: float) -> Dict:
        """Validate purchase request parameters"""
        try:
            if not await self.rate_limiter.limit(key="made_purchase", user_id=user_id):
                remaining, reset_time = await self.rate_limiter.remaining_limit(key="made_purchase", user_id=user_id)
                return {"valid": False, "error": "🚫 Tᴏᴏ Mᴀɴʏ Rᴇǫᴜᴇsᴛs. Pʟᴇᴀsᴇ Wᴀɪᴛ A Mɪɴᴜᴛᴇ..."}
            if not self.input_validator.validate_user_id(user_id):
                return {"valid": False, "error": "🔒 Iɴᴠᴀʟɪᴅ Usᴇʀ Cʀᴇᴅᴇɴᴛɪᴀʟs..."}
            if not self.input_validator.validate_amount(price):
                return {"valid": False, "error": "💰 Iɴᴠᴀʟɪᴅ Pʀɪᴄᴇ Aᴍᴏᴜɴᴛ..."}

            return {"valid": True}
        except Exception as e:
            #logger.error(f"Validation error: {e}")
            return {"valid": False, "error": "⚠️ Sʏsᴛᴇᴍ Vᴀʟɪᴅᴀᴛɪᴏɴ Fᴀɪʟᴇᴅ"}

    async def format_phone_number(self, phone_number: str) -> Tuple[str, str]:
        """
        Formats a phone number into country code and national number.
        Works for all countries and ensures compatibility with international apps.
        """
        try:
            # Ensure the number starts with "+"
            if phone_number.isdigit():
                phone_number = f"+{phone_number}"

            # Parse the phone number
            parsed = parse(phone_number)

            # Extract country code
            country_code = f"+{parsed.country_code}"

            # Format as national number (without trunk prefix)
            national_number = format_number(parsed, PhoneNumberFormat.NATIONAL)

            # Remove unnecessary characters ((), spaces)
            national_number = national_number.replace("(", "").replace(")", "").replace("-", "").replace(" ", "")

            # Remove leading trunk prefix if it exists
            if phonenumbers.country_code_for_region(phonenumbers.region_code_for_number(parsed)) == parsed.country_code:
                example_number = phonenumbers.example_number_for_type(
                    phonenumbers.region_code_for_number(parsed), phonenumbers.PhoneNumberType.MOBILE
                )
                if example_number:
                    formatted_example = format_number(example_number, PhoneNumberFormat.NATIONAL)
                    trunk_prefix = formatted_example[0] if formatted_example[0].isdigit() else ""
                    if trunk_prefix and national_number.startswith(trunk_prefix):
                        national_number = national_number[len(trunk_prefix):].lstrip("-")

            return country_code, national_number

        except NumberParseException:
            return '', phone_number  # Return as-is if parsing fails
            
    async def fetch_phone_number(self, server: int, service: str, country: str, price: float, operator: str = None, app_name: str = None, chat_id: int = None, app_id: int = None) -> dict:
        server_name, api_key = await get_api_info(server)
        service_parts = service.split(',')
        attempt = 3 if server == 1 and len(service_parts) > 1 else 2 if len(service_parts) > 1 else 1
        attempts = attempt
        if str(operator) == "free":
            reserve_result = await self.order_manager.manage_number_order(
                redis_client=self.redis_client,
                country_id=country,
                server_id=server,
                app_id=app_id,
                operator=operator,
                order_id=None,          # let function generate f"987654321{num}"
                action="reserve",
                user_id=chat_id
            )
            print("RESERVE →", json.dumps(reserve_result, indent=2))
            if reserve_result["status"] == False:
                response = reserve_result['message']
            elif reserve_result["status"] == True:
                number = reserve_result["number"]
                order_id = reserve_result["order_id"]
                response = f"ACCESS_NUMBER:{order_id}:{number}"
            else:
                response = "NO_NUMBERS"
            return await self._process_api_response(service_parts, response)
        for attempt in range(attempts):
            if attempt == 0:
                api_name = service_parts[1] if len(service_parts) > 1 else service_parts[0]
            elif attempt == 1:
                api_name = app_name if server == 1 else service_parts[0]
            else:
                api_name = service_parts[0]


            url = await self._build_api_url(server_name, api_key, api_name, country, price, operator)
            print(f"🔢 Attempt {attempt + 1}: Fetching Number From Server {server} For {api_name}")
            print(f"API URL: {url}")
            try:
                timeout = aiohttp.ClientTimeout(total=5)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url) as response:
                        if response.status != 200:
                            result = {"status": False, "message": f"HTTP {response.status}"}
                            break
                        response_text = await response.text()
                        print(f"API Response: {response_text}")
                        if response_text.startswith("ACCESS_NUMBER:"):
                            result = await self._process_api_response(service_parts, response_text)
                            break
                        elif response_text in ["WRONG_SERVICE", "BAD_SERVICE", "NO_NUMBERS"]:
                            result = await self._process_api_response(service_parts, response_text)
                        elif response_text == "NO_BALANCE":
                            await self.bot.send_message(chat_id='5716978793', text=f"<b>💸 Iɴsᴜғғɪᴄɪᴇɴᴛ Bᴀʟᴀɴᴄᴇ...</b>\n\n- Sᴇʀᴠᴇʀ Nᴀᴍᴇ : <code>{server_name}</code>")
                            result = {"status": False, "message": " Iɴsᴜғғɪᴄɪᴇɴᴛ Bᴀʟᴀɴᴄᴇ..."}
                        else:
                            result = {"status": False, "message": f"Unknown response from API: {response_text}"}
                            
            except asyncio.TimeoutError:
                result = await self._process_api_response(service_parts, "NO_NUMBERS")
                break
            except aiohttp.ClientError as e:
                result = {"status": False, "message": f"Network error: {e}"}
                break
        return result

    async def _build_api_url(self, server_name: str, api_key: str, service: str, country: str, price: float, operator: str) -> str:
        price = round(float(price) / float(COMMISSION), 8)
        if str(server_name) in ["api.sms-activate.org", "smshub.org"]:
            price = round(convert_rub_to_usd(price), 4)
        else:
            price = round(price, 2)
        params = {
            "api_key": api_key, "action": "getNumber", "service": service.replace(' ', '').lower(),
            "country": country, "maxPrice": price, "operator": operator, "ref_id": "harsh123"
        }
        query_string = "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
        return await asyncio.to_thread(lambda: f"https://{server_name}/stubs/handler_api.php?{query_string}")

    async def _determine_api_name(self, server: int, service_parts: List[str], app_name: str = None) -> str:
        if server == 1:
            return (service_parts[1] if len(service_parts) > 1 else service_parts[0]) or app_name 
        elif server in [3, 4, 5]:
            return service_parts[1] if len(service_parts) > 1 else service_parts[0]
        else:
            return service_parts[0]

    async def _process_api_response(self, service_parts: List[str], response_text: str) -> Dict:
        if response_text.startswith("ACCESS_NUMBER"):
            return await self._process_success_response(response_text, service_parts[0])
        else:
            return await self._handle_api_errors(response_text)

    async def _process_success_response(self, response: str, service: str) -> Dict:
        match = re.match(r"ACCESS_NUMBER:(\d+):(\d+)", response)
        if not match:
            return {"status": False, "message": "🚫 Iɴᴠᴀʟɪᴅ Rᴇsᴘᴏɴsᴇ Fᴏʀᴍᴀᴛ"}
        order_id, full_phone = match.groups()
        code, number = await self.format_phone_number(f"+{full_phone}")
        return {'status': True, 'order_id': order_id, 'number': number, 'code': code, 'service': service}

    async def _handle_api_errors(self, response: str) -> Dict:
        error_map = {
            "WRONG_SERVICE": "🚫 Wʀᴏɴɢ Sᴇʀᴠɪᴄᴇ Sᴘᴇᴄɪғɪᴇᴅ...",
            "NO_NUMBERS": "📵 Nᴏ Nᴜᴍʙᴇʀs Aᴠᴀɪʟᴀʙʟᴇ...",
            "NO_BALANCE": "💸 Iɴsᴜғғɪᴄɪᴇɴᴛ Bᴀʟᴀɴᴄᴇ...",
            "API_KEY_NOT_VALID": "🔑 Iɴᴠᴀʟɪᴅ API Kᴇʏ...",
            "BAD_SERVICE": "🚫 Wʀᴏɴɢ Sᴇʀᴠɪᴄᴇ Sᴘᴇᴄɪғɪᴇᴅ..."
        }
        error_msg = error_map.get(response, f"Unknown error: {response}")
        #logger.error(f"API error: {error_msg}")
        return {"status": False, "message": error_msg}

    async def _handle_insufficient_balance(self, chat_id, progress_msg, price, balance):
        """Handle insufficient balance scenario"""
        keyboard = InlineKeyboardMarkup().row(
            InlineKeyboardButton("🔥 Dᴇᴘᴏsɪᴛ Nᴏᴡ Tᴏ Pᴜʀᴄʜᴀsᴇ", callback_data="USER:DEPOSIT")
        )
        await self.bot.edit_message_text(
            chat_id=chat_id,
            message_id=progress_msg.message_id,
            text=await self._balance_alert_content(price, balance),
            reply_markup=keyboard,
            parse_mode="HTML",
        )

    async def _balance_alert_content(self, price: float, balance: float) -> str:
        """Generate insufficient balance message content asynchronously"""
        return (
            f"<b>🏛️ Iɴsᴜғғɪᴄɪᴇɴᴛ Bᴀʟᴀɴᴄᴇ!</b>\n\n"
            f"💰 <b>Yᴏᴜʀ Bᴀʟᴀɴᴄᴇ »</b> <code>{balance:.2f}</code> 💎\n"
            f"🫴🏻 <b>Rᴇǫᴜɪʀᴇᴅ Bᴀʟᴀɴᴄᴇ »</b> <code>{price:.2f}</code> 💎\n\n"
            f"⚡ <b>Rᴇᴄʜᴀʀɢᴇ Yᴏᴜʀ Wᴀʟʟᴇᴛ \nTᴏ Cᴏɴᴛɪɴᴜᴇ Pᴜʀᴄʜᴀsᴇs.</b>"
        )

    async def _finalize_purchase(self, call, result: Dict, app_data: Dict, price: float,
                               country_id: str, country_code: str, country_name: str, service: str, progress_msg: Message, is_new: bool = False, is_api: bool = False, app_id: str = None, server_id: str = None) -> None:
        """Complete purchase transaction and update systems"""
        purchase_data = await self._build_purchase_data(call, result, app_data, price, country_id, country_code, country_name, service, progress_msg, is_api, app_id, server_id)
        
        try:
            order_id = await self._send_purchase_confirmation(call, purchase_data, is_new, is_api)
        except Exception as e:
            print(f"Finalization error: {e}")
            raise e
        return order_id
        

    async def _build_purchase_data(self, call, result, app_data, price, country_id, country_code, country_name, service, progress_msg, is_api: bool = False, app_id: str = None, server_id: str = None) -> Dict:
        """Build unified purchase data structure asynchronously"""
        callback_user_id = call.from_user.id if call.from_user else None
        chat_id = call.message.chat.id if call.message and call.message.chat else callback_user_id
        
        # Use asyncio.to_thread for potentially blocking operations
        if not app_id or not server_id:
            app_id, server_id = await asyncio.gather(
                asyncio.to_thread(lambda: call.data.split(':')[1]),
                asyncio.to_thread(lambda: call.data.split(':')[3])
            )
        
        valid_until = await AfterMin(10)
        return {
            **result,
            'app_id': app_id,
            'app_name': app_data['app_name'],
            'server_id': server_id,
            'app_price': price,
            'service': service,
            'app_code': app_data['app_code'],
            'country_id': country_id,
            'country_code': country_code,
            'country_name': country_name,
            'chat_id': chat_id,
            'user_id': callback_user_id,
            'message_id': progress_msg,
            'valid_until': valid_until,
            'is_api': is_api
        }

    async def _create_order_record(self, order_id: str, data: Dict) -> str:
        """Create order record in database"""
        order_data = await self._build_order_data(data)
        response = await self.order_manager.add_order_data(order_id['result'], data['user_id'], order_data)
        
        if not response.get('response'):
            raise Exception("⚠️ Oʀᴅᴇʀ Dᴀᴛᴀ Sᴛᴏʀᴀɢᴇ Fᴀɪʟᴇᴅ")

        return order_id['result']

    async def _build_order_data(self, data: Dict) -> Dict:
        """Build order data structure asynchronously"""
        current_time = str(time.time())
        utc_now = str(datetime.utcnow())
        
        order_data = {
            "order_id": str(data['order_id']),
            "message_id": str(data['message_id'].message_id),
            "user_id": str(data['user_id']),
            "server_id": str(data['server_id']),
            "country_id": str(data['country_id']),
            "country_code": str(data['country_code']),
            "country_name": str(data['country_name']),
            "valid_until": str(data['valid_until']),
            "app_id": str(data['app_id']),
            "app_code": str(data['app_code']),
            "app_name": str(data['app_name']),
            "order_amount": str(data['app_price']),
            "order_number": json.dumps([data['code'], data['number']]),
            "order_status": "PENDING",
            "refund_status": "false",
            "sms_list": json.dumps([]),
            "sms_count": 0,
            "order_history": json.dumps([{"timestamp": current_time, "action": "ORDER_CREATED"}]),
            "created_at": utc_now,
            "last_updated":  f"{int(BASE_TIMEOUT) - 1:02}"
        }
        
        return order_data

    async def _send_purchase_confirmation(self, call, data: Dict, is_new: bool = False, is_api: bool = False) -> None:
        """Send purchase confirmation to user"""
        print("_send_purchase_confirmation")
        print(data)
        order_id = await self.order_manager.create_order_id(user_id=data['user_id'])
        if not order_id.get('response'):
            raise Exception("⚠️ Oʀᴅᴇʀ ID Cʀᴇᴀᴛɪᴏɴ Fᴀɪʟᴇᴅ")
        if str(data['order_id']).startswith("987654321"):
            order_id['result'] = data['order_id']
        keyboard = InlineKeyboardMarkup().row(
            InlineKeyboardButton("✘ Cᴀɴᴄᴇʟ", callback_data=f"status_cancel:{order_id['result']}:{data['user_id']}"),
            InlineKeyboardButton("↻ Bᴜʏ Aɢᴀɪɴ", callback_data=call.data)
        )
        if not is_new:
            await self.bot.edit_message_text(
                chat_id=data['chat_id'],
                message_id=data['message_id'].message_id,
                text=await self._confirmation_message_content(data, minute=str(BASE_TIMEOUT)),
                parse_mode="HTML",
                reply_markup=keyboard
            )
        elif is_new and not is_api:
            message = await self.bot.send_message(
                chat_id=data['chat_id'],
                text=await self._confirmation_message_content(data, minute=str(BASE_TIMEOUT)),
                parse_mode="HTML",
                #reply_to_message_id=data['message_id'].message_id,
                reply_markup=keyboard
            )
            data['message_id'] = message



        
        # Combine tasks into a single coroutine
        order_id = await self._create_order_record(order_id, data)
        if data['app_name'].lower() == "telegram" and not is_api:
            await self.bot.send_message(
                chat_id=data['chat_id'],
                text=f"<b>🔗 Uʀʟ:</b> t.me/{data['code']}{data['number']}",
                parse_mode="HTML"
            )


        

        data['msg_id'] = data['message_id'].message_id
        tasks = [
            self._process_and_save_image(data, data['service']),
            self.user_manager.send_order_report(self.bot, "send_message", order_id, data['user_id'], '-1002203139746', data, is_api),
            self.add_service_to_leaderboard(data['app_id'], data['country_id'], data['server_id'], data['app_name'], data['service'])
        ]
        if not is_api:
            tasks.append(self._delayed_message_edit(data, keyboard))
            tasks.append(self.user_manager.user_metrics_report(self.bot, 'edit_message_text', data['user_id'], '-1002203139746'))
        await asyncio.gather(*tasks)
        return order_id

    async def add_service_to_leaderboard(self, app_id: str, country_id: str, server_id: str, service_name: str, service_code: str) -> None:
        """Add service to leaderboard asynchronously"""
        await self.top_service_manager.update_service_purchase(app_id, country_id, server_id, service_name, service_code)

    async def _delayed_message_edit(self, data, keyboard):
        await asyncio.sleep(1)
        try:
            await self.bot.edit_message_text(
                chat_id=data['chat_id'],
                message_id=data['message_id'].message_id,
                text=await self._confirmation_message_content(data, minute = f"{int(BASE_TIMEOUT) - 1:02}"),
                parse_mode="HTML",
                reply_markup=keyboard
            )
        except Exception as e:
            pass
            #logger.error(f"Failed to edit message after delay: {e}")

    async def _confirmation_message_content(self, data: Dict[str, Any], minute: str = '10') -> str:
        """Generate purchase confirmation message asynchronously"""
        try:
            app_name = data['app_name'].translate(await small_caps())
            message = (
                f"<blockquote><b>📦 {app_name} [</b> 💎 "
                f"<code>{data['app_price']}</code> <b>][</b> <code>{data['country_code']}</code> "
                f"<b>][</b> <code>{data['server_id']}</code> <b>]</b></blockquote>\n\n"
                f"<b>📞 Nᴜᴍʙᴇʀ »</b> <code>{data['code']}</code> <code>{data['number']}</code>\n\n"
                f"⏱ <b>Vᴀʟɪᴅ Uɴᴛɪʟ »</b> {data['valid_until']} <b>[</b><code>{minute}</code> <code>Mɪɴ</code><b>]</b>"
            )
            return message
        except Exception as e:
            #logger.error(f"Error generating confirmation message: {e}")
            return "Error generating confirmation message."

    async def _process_and_save_image(self, data: Dict, service: str) -> None:
        app_id = data['app_id']
        country_id = data['country_id']
        country_code = data['country_code']
        key = f"image_data:country-service"
        redis_client = self.redis_client
        existing_link = await redis_client.hget(key, f"{country_id}-{app_id}")
        if existing_link:
            return

        bg_url = f"https://smsactivate.s3.eu-central-1.amazonaws.com/assets/ico/{service}0.webp"
        async with aiohttp.ClientSession() as session:
            try:
                # Get country details from Redis
                country_data = await redis_client.json().get('main_data:details:country_data') or {}
                flag_url = country_data.get(str(country_id), {}).get('flag_url')
                if not flag_url:
                    return
                
                direct_link = await self._process_image_with_flag(bg_url, flag_url, session)
                await redis_client.hset(key, f"{country_id}-{app_id}", direct_link)
            except Exception as e:
                pass

    async def _process_image_with_flag(self, bg_url: str, flag_url: str, session: aiohttp.ClientSession) -> str:
        # Download both background and flag images
        bg = await self._load_image_from_url(bg_url, session)
        flag = await self._load_image_from_url(flag_url, session)
        
        bg_width, bg_height = bg.size

        # Determine flag size and margins dynamically
        scale_fraction = 0.37
        smaller_dim = min(bg_width, bg_height)
        flag_size = int(smaller_dim * scale_fraction)
        if flag_size < 10:
            flag_size = 10

        margin_x = int(bg_width * 0.0435)
        margin_y = int(bg_height * 0.035)

        # Resize flag to desired size
        flag = flag.resize((flag_size, flag_size), Image.Resampling.LANCZOS)

        # Paste the flag on the background image in the top-right corner
        pos_x = max(bg_width - flag_size - margin_x, 0)
        pos_y = max(margin_y, 0)
        bg.paste(flag, (pos_x, pos_y), flag)

        # Upload the composited image to imgbb and return the direct link
        direct_link = await self._upload_image_to_imgbb(bg, IMGBB_API_KEY, session)
        return direct_link

    async def _load_image_from_url(self, url: str, session: aiohttp.ClientSession) -> Image.Image:
        async with session.get(url) as response:
            if response.status != 200:
                raise Exception(f"Error fetching image from URL {url}: status {response.status}")
            data = await response.read()
        def _open_image() -> Image.Image:
            from PIL import Image
            return Image.open(BytesIO(data)).convert("RGBA")
        return await asyncio.to_thread(_open_image)
    
    async def _upload_image_to_imgbb(self, img: Image.Image, api_key: str, session: aiohttp.ClientSession) -> str:
        buffer = BytesIO()
        img.save(buffer, format="PNG", optimize=True, compress_level=1)
        buffer.seek(0)
        encoded_image = base64.b64encode(buffer.getvalue()).decode("utf-8")
        url = "https://api.imgbb.com/1/upload"
        payload = {
            "key": api_key,
            "image": encoded_image
        }
        async with session.post(url, data=payload) as response:
            if response.status != 200:
                raise Exception(f"Error uploading to imgbb: status {response.status}")
            json_data = await response.json()
            return json_data["data"]["url"]
    
    async def schedule_number_check(self, **kwargs) -> None:
        """
        Called on user request; stores callback and schedule entry.
        """
        full_data = kwargs
        timeout_seconds = full_data.get('timeout_seconds', 24 * 3600)
        poll_interval = full_data.get('poll_interval', 10)

        # Build redis keys
        key_suffix = f"service_data:{full_data['country_id']}:{full_data['server_id']}:{full_data['app_id']}"
        redis_key = f"schedule:{key_suffix}"
        full_data['key'] = key_suffix
        full_data['timeout_seconds'] = timeout_seconds
        full_data['poll_interval'] = poll_interval

        # Persist callback data
        callback_id = full_data['callback_id']
        await self.redis_client.set(f"cache-data:callback_data:{callback_id}", json.dumps(full_data))
        # Add user to sorted set with expiry score
        await self.redis_client.zadd(redis_key, {callback_id: int(time.time()) + timeout_seconds})
        key = redis_key
        await self._start_schedule_loop(key)

    async def _listen_for_schedule_events(self) -> None:
        """
        Bootstrap existing schedules once, then subscribe to keyspace events for zadd.
        Launch _background_check_loop only if there's not already a loop running for that key.
        """

        # 1) Enable keyspace notifications for sorted-set events
        await self.redis_client.config_set('notify-keyspace-events', 'Kz')

        # 2) Bootstrap existing schedules *once* at startup
        cursor = '0'
        while True:
            cursor, keys = await self.redis_client.scan(
                cursor=cursor,
                match='schedule:service_data:*',
                count=100
            )
            for raw_key in keys:
                key = raw_key.decode() if isinstance(raw_key, bytes) else raw_key
                if key not in self._running_schedules:
                    print(colored(f"Bootstrapping existing schedule: {key}", 'magenta'))
                    self._start_schedule_loop(key)
            if cursor == '0':
                break

        # 3) Subscribe to keyspace pattern for real-time adds
        pubsub = self.redis_client.pubsub()
        await pubsub.psubscribe('__keyspace@0__:schedule:service_data:*')
        print(colored("Listening for schedule events...", "green"))

        async for message in pubsub.listen():
            if message['type'] != 'pmessage':
                continue

            event = message['data']
            if isinstance(event, bytes):
                event = event.decode()
            if event != 'zadd':
                continue

            channel = message['channel']
            if isinstance(channel, bytes):
                channel = channel.decode()
            # Extract the actual Redis key
            _, key = channel.split('__keyspace@0__:', 1)

            print(colored(f"New schedule event for key: {key}", "green"))
            self._start_schedule_loop(key)

    def _start_schedule_loop(self, key: str):
        """
        Kick off _background_check_loop for `key` if not already running.
        """
        async def runner():
            try:
                await self._background_check_loop(key)
            finally:
                # Ensure we clear the flag when the loop exits
                self._running_schedules.discard(key)
                print(colored(f"Schedule loop ended for {key}", "yellow"))

        # Mark as running
        if key not in self._running_schedules:
            self._running_schedules.add(key)

            # Fire-and-forget
            asyncio.create_task(runner())

    async def _background_check_loop(self, redis_key: str) -> None:
        """
        Periodically checks availability and notifies users, with batch balance check every 30 polls.
        """
        print(colored(f"Starting background check loop for {redis_key}", "green"))

        # Retrieve full_data from the first member's cache-data:callback_data
        uids = await self.redis_client.zrange(redis_key, 0, 0)
        if not uids:
            return
        first_id = uids[0]
        full_data = json.loads(await self.redis_client.get(f"cache-data:callback_data:{first_id}"))

        # Counter for batch balance checks
        check_count = 0

        async def notify_and_remove(uids, message_fn, message_notify, keyboard=None):
            """Helper to send notifications and remove from sorted set."""
            for uid in uids:
                raw_data = await self.redis_client.get(f"cache-data:callback_data:{uid}")
                user_full_data = json.loads(raw_data)
                user_id = user_full_data['user_id']
                message_id = user_full_data['message_id']

                try:
                    await self.bot.send_message(int(user_id), message_fn(user_id), reply_markup=keyboard, parse_mode="HTML")
                except Exception as e:
                    try:
                        await self.bot.send_message(int(user_id), message_fn, reply_markup=keyboard, parse_mode="HTML")
                    except Exception as e:
                        print(colored(f"Failed notifying uid {user_id}: {e}", "red"))
                        print(f"Failed notifying  uid {user_id}: {e}")
                try:
                    callback_id = user_full_data['callback_id']
                    markup = InlineKeyboardMarkup(
                        [
                            [
                                InlineKeyboardButton(
                                    "🔔 Qᴜᴇᴜᴇ Bᴜʏ",
                                    callback_data=f"notify_on:{callback_id}"
                                ),
                                InlineKeyboardButton(
                                    text="⌕ Cᴏᴜɴᴛʀɪᴇs",
                                    switch_inline_query_current_chat=f"#AᴘᴘIᴅ:{str(full_data['app_id']).translate(await small_caps())} "
                                )
                            ]
                        ]
                    )
                    text = (
                        f"<b>{message_notify}</b>\n\n"
                        "<blockquote expandable>"
                        "<b>Wᴏᴜʟᴅ Yᴏᴜ Lɪᴋᴇ Mᴇ Tᴏ “</b><code>Nᴏᴛɪғʏ</code><b>”</b>\n"
                        "<b>Yᴏᴜ Wʜᴇɴ Tʜᴇ Sᴇʀᴠɪᴄᴇ Bᴇᴄᴏᴍᴇs Aᴠᴀɪʟᴀʙʟᴇ.!?</b>\n\n"
                        f"<b>• Sᴇʀᴠɪᴄᴇ »</b> <code>{str(full_data['app_name']).translate(await small_caps())}</code>\n"
                        f"<b>• Cᴏᴜɴᴛʀʏ »</b> <code>{str(full_data['country_name']).translate(await small_caps())}</code> "
                        f"[<code>{full_data['country_code']}</code>]\n"
                        f"<b>• Aᴍᴏᴜɴᴛ »</b> 💎 <code>{str(full_data['price']).translate(await small_caps())}</code> "
                        f"[<code>{str(full_data['server_id']).translate(await small_caps())}</code>]"
                        "</blockquote>"
                    )
                    await self.bot.edit_message_text(
                        chat_id=user_id,
                        message_id=message_id,
                        text=text,
                        reply_markup=markup,
                        parse_mode="HTML"
                    )
                except Exception as e:
                    print(f"Failed notifying user {user_id}: {e}")
                await self.redis_client.zrem(redis_key, uid)

        while True:
            now = int(time.time())
            print(colored(f"Polling for {full_data['app_name']} (server {full_data['server_id']})…", "green"))

            # Increment our counter and perform batch balance check every 30 iterations
            check_count += 1
            if check_count % 30 == 0:
                remaining = await self.redis_client.zrange(redis_key, 0, -1)
                # Check each user's balance asynchronously in batch
                insufficient = []
                for uid in remaining:
                    raw = await self.redis_client.get(f"cache-data:callback_data:{uid}")
                    user = json.loads(raw)
                    has_balance = await self._handle_user_balance(user['user_id'], user['price'], user['user_id'], progress_msg=None)
                    if not has_balance:
                        insufficient.append(uid)
                if insufficient:
                    # Notify all insufficient and remove them
                    for uid in insufficient:
                        raw_data = await self.redis_client.get(f"cache-data:callback_data:{uid}")
                        user_full_data = json.loads(raw_data)
                        score = await self.redis_client.zscore(redis_key, uid)
                        score = int(score) if score is not None else None
                        time_ago = await self.get_stylized_time_ago(score) if score is not None else None
                        await notify_and_remove(
                            [uid],
                            lambda uid: (
                                f"<blockquote expandable><b>🔔 Nᴜᴍʙᴇʀꜱ Aʀᴇ Bᴀᴄᴋ Iɴ Sᴛᴏᴄᴋ!</b></blockquote>\n\n"
                                f"<b>✨ Gʀᴇᴀᴛ Nᴇᴡꜱ:</b> Nᴜᴍʙᴇʀꜱ Fᴏʀ “<code>{str(user_full_data['app_name'])}</code>” Hᴀᴠᴇ Jᴜꜱᴛ Bᴇᴇɴ Rᴇꜱᴛᴏᴄᴋᴇᴅ <b>{time_ago}</b>.\n\n"
                                f"<b>⚠️ Hᴏᴡᴇᴠᴇʀ, Yᴏᴜʀ Cᴜʀʀᴇɴᴛ Bᴀʟᴀɴᴄᴇ Iꜱ Nᴏᴛ Sᴜꜰꜰɪᴄɪᴇɴᴛ Tᴏ Mᴀᴋᴇ A Pᴜʀᴄʜᴀꜱᴇ.</b>\n"
                                f"<b>💳 Pʟᴇᴀꜱᴇ Dᴇᴘᴏꜱɪᴛ Fᴜɴᴅꜱ Nᴏᴡ Tᴏ Cʟᴀɪᴍ Yᴏᴜʀ Nᴜᴍʙᴇʀ Bᴇꜰᴏʀᴇ Sᴛᴏᴄᴋ Rᴜɴꜱ Oᴜᴛ.</b>"
                            ),
                            message_notify="🟢 Sᴛᴏᴄᴋ Aᴠᴀɪʟᴀʙʟᴇ – Pʀᴏᴄᴇᴇᴅ Tᴏ Bᴜʏ Nᴏᴡ!",
                        )
                continue

            # 1) Notify & remove expired
            expired = await self.redis_client.zrangebyscore(redis_key, 0, now)
            if expired:
                await notify_and_remove(
                    expired,
                    lambda uid: (
                        f"<i>⏳ Yᴏᴜʀ Pʟᴀᴄᴇ Iɴ Tʜᴇ Ǫᴜᴇᴜᴇ Hᴀꜱ Bᴇᴇɴ Rᴇʟᴇᴀꜱᴇᴅ.</i>"
                        f"⏰ <i>Uɴꜰᴏʀᴛᴜɴᴀᴛᴇʟʏ, “</i><code>{full_data['app_name']}</code><i>” Wᴀꜱ Nᴏᴛ Aᴠᴀɪʟᴀʙʟᴇ Wɪᴛʜɪɴ Lᴀꜱᴛ</i> <code>{full_data['timeout_seconds'] // 3600}</code> <i>Hᴏᴜʀꜱ.</i>\n\n"
                    ),
                    message_notify="💡 Tʀʏ Aɴᴏᴛʜᴇʀ Sᴇʀᴠᴇʀ Fᴏʀ Fᴀsᴛᴇʀ Rᴇsᴜʟᴛs.."
                )

            # 2) Exit if none
            remaining = await self.redis_client.zrange(redis_key, 0, -1)
            if not remaining:
                print("No more waiting users; exiting loop.")
                return

            # 3) Check availability
            print(colored(f"Checking availability for {full_data['app_name']}…", "yellow"))
            try:
                phone_result = await self.fetch_phone_number(
                    full_data['server_id'],
                    full_data['app_code'],
                    full_data['country_id'],
                    price=full_data['price'],
                    operator=full_data['operator'],
                    app_name=full_data['app_name'],
                    chat_id=int(first_id.split(':')[0]),
                    app_id=full_data['app_id']
                )
                print(colored(f"Fetch result: {phone_result}", "cyan"))

                if phone_result.get('status'):
                    # 4) Process first able user
                    small_cap = await small_caps()
                    for uid in remaining:
                        raw_data = await self.redis_client.get(f"cache-data:callback_data:{uid}")
                        user_full_data = json.loads(raw_data)
                        user_id = user_full_data['user_id']
                        if await self._handle_user_balance(user_id, user_full_data['price'], user_id, progress_msg=None):
                            user_full_data.update(chat_id=user_id)
                            call = await self.reconstruct_fake_call(user_full_data)
                            await self._finalize_purchase(
                                call,
                                phone_result,
                                user_full_data,
                                user_full_data['price'],
                                user_full_data['country_id'],
                                user_full_data['country_code'],
                                user_full_data['country_name'],
                                phone_result['service'],
                                call.message,
                                is_new=True
                            )
                            score = await self.redis_client.zscore(redis_key, uid)
                            score = int(score) if score is not None else None
                            time_ago = await self.get_stylized_time_ago(score) if score is not None else None

                            await notify_and_remove(
                                [uid],
                                lambda uid: (
                                    f"<blockquote expandable><b>✅ Yᴏᴜʀ Oʀᴅᴇʀ Fᴏʀ “</b><code>{str(user_full_data['app_name']).translate(small_cap)}</code>"
                                    f"<b>” Hᴀꜱ Bᴇᴇɴ Pᴜʀᴄʜᴀꜱᴇᴅ Sᴜᴄᴄᴇꜰᴜʟʟʏ!</b>\n\n"
                                    f" <b>• Gᴏᴏᴅ Nᴇᴡs! Nᴜᴍʙᴇʀꜱ Aʀᴇ Bᴀᴄᴋ Iɴ Sᴛᴏᴄᴋ, Wɪᴛʜɪɴ Lᴀsᴛ {time_ago}.</b>\n\n</blockquote>"
                                ),
                                message_notify="🟢 Wᴇ Pᴜʀᴄʜᴀꜱᴇᴅ A Nᴜᴍʙᴇʀ Fᴏʀ Yᴏᴜ!",
                            )
                            break
                        else:
                            raw_data = await self.redis_client.get(f"cache-data:callback_data:{uid}")
                            user_full_data = json.loads(raw_data)
                            score = await self.redis_client.zscore(redis_key, uid)
                            score = int(score) if score is not None else None
                            time_ago = await self.get_stylized_time_ago(score) if score is not None else None
                            await notify_and_remove(
                                [uid],
                                (
                                    f"<blockquote expandable><b>🔔 Nᴜᴍʙᴇʀꜱ Aʀᴇ Bᴀᴄᴋ Iɴ Sᴛᴏᴄᴋ!</b></blockquote>\n\n"
                                    f"<b>✨ Gʀᴇᴀᴛ Nᴇᴡꜱ:</b> Nᴜᴍʙᴇʀꜱ Fᴏʀ “<code>{str(user_full_data['app_name']).translate(small_cap)}</code>” Hᴀᴠᴇ Jᴜꜱᴛ Bᴇᴇɴ Rᴇꜱᴛᴏᴄᴋᴇᴅ <b>{time_ago}</b>.\n\n"
                                    f"<b>⚠️ Hᴏᴡᴇᴠᴇʀ, Yᴏᴜʀ Cᴜʀʀᴇɴᴛ Bᴀʟᴀɴᴄᴇ Iꜱ Nᴏᴛ Sᴜꜰꜰɪᴄɪᴇɴᴛ Tᴏ Mᴀᴋᴇ A Pᴜʀᴄʜᴀꜱᴇ.</b>\n"
                                    f"<b>💳 Pʟᴇᴀꜱᴇ Dᴇᴘᴏꜱɪᴛ Fᴜɴᴅꜱ Nᴏᴡ Tᴏ Cʟᴀɪᴍ Yᴏᴜʀ Nᴜᴍʙᴇʀ Bᴇꜰᴏʀᴇ Sᴛᴏᴄᴋ Rᴜɴꜱ Oᴜᴛ.</b>"
                                ),
                                message_notify="🟢 Sᴛᴏᴄᴋ Aᴠᴀɪʟᴀʙʟᴇ – Pʀᴏᴄᴇᴇᴅ Tᴏ Bᴜʏ Nᴏᴡ!",
                            )
                            print(colored(f"User {user_id} has insufficient balance, skipping.", "red"))
                    
                    # 5) Notify all remaining
                    post_remaining = await self.redis_client.zrange(redis_key, 0, -1)
                    if post_remaining:
                        text = (
                            f"<blockquote expandable><b> “{str(full_data['app_name']).translate(await small_caps())}” Is Aᴠᴀɪʟᴀʙʟᴇ Fᴏʀ Pᴜʀᴄʜᴀsᴇ.</b></blockquote>\n\n"
                            f"<b>• Cᴏᴜɴᴛʀʏ »</b> <code>{str(full_data['country_name']).translate(await small_caps())}</code> [<code>{full_data['country_code']}</code>]\n"
                            f"<b>• Aᴍᴏᴜɴᴛ »</b> 💎 <code>{str(full_data['price']).translate(await small_caps())}</code> [<code>{str(full_data['server_id']).translate(await small_caps())}</code>]"
                        )
                        keyboard = InlineKeyboardMarkup()
                        keyboard.add(
                            InlineKeyboardButton(
                                "🛒 Pᴜʀᴄʜᴀsᴇ Tʜɪs Sᴇʀᴠɪᴄᴇ",
                                callback_data=f"purchase:{full_data.get('app_id','')}:{full_data.get('price','')}:{full_data.get('server_id','')}:{full_data.get('country_id','')}:{full_data.get('country_code','')}"
                            )
                        )
                        await notify_and_remove(
                            post_remaining,
                            lambda uid: text,
                            message_notify="🟢 Sᴛᴏᴄᴋ Aᴠᴀɪʟᴀʙʟᴇ – Pʀᴏᴄᴇᴇᴅ Tᴏ Bᴜʏ Nᴏᴡ!",
                            keyboard=keyboard
                        )
                    return
            except (ValueError, TypeError) as e:
                print(colored(f"Error during availability check: {e}", "red"))

            # 6) Sleep
            print(colored(f"Sleeping for {full_data.get('poll_interval', 10)} seconds", "blue"))
            await asyncio.sleep(full_data.get('poll_interval', 10))



purchase_manager = UserPurchaseManagement()

async def init_managers(
    order_manager: OrderManagement,
    user_manager: UserManagement,
    bot: AsyncTeleBot
) -> bool:
    return await purchase_manager.init_managers(order_manager, user_manager, bot)

async def register_handlers(bot: AsyncTeleBot) -> None:
    """Register purchase-related bot handlers."""
    @bot.callback_query_handler(func=lambda call: call.data.startswith("purchase:"))
    async def handle_purchase_callback(call):
        try:
            _, app_id, price, server_id, country_id, country_code = call.data.replace(' ', '').split(':')
            #logging.info(app_id, price, server_id, country_id, country_code)
            text = f'service_data:{country_id}:{server_id}:{app_id}'
            country_name = await redis_manager.redis_client.hget(text, 'country_name')
            
            process_purchase = partial(
                purchase_manager.process_purchase_flow,
                call,
                str(call.from_user.id),
                app_id,
                round(float(price), 2),
                int(server_id),
                country_id,
                country_code,
                country_name
            )
            asyncio.create_task(process_purchase())
        except ValueError:
            asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", show_alert=True))
        except Exception as e:
            print(f"Callback error: {e}")
            asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ", show_alert=True))



    @bot.callback_query_handler(func=lambda c: c.data.startswith("notify_on:"))
    async def handle_notify_on(call: CallbackQuery):
        callback_id = call.data.split(":", 1)[1]
        raw_data = await redis_manager.redis_client.get(f"cache-data:callback_data:{callback_id}")
        if not raw_data:
            await bot.answer_callback_query(call.id, "⛔ Expired or invalid data.")
            return
        full_data = json.loads(raw_data)
        await bot.answer_callback_query(call.id, "✅ 𝗡ᴏᴛɪғɪᴄᴀᴛɪᴏɴꜱ Eɴᴀʙʟᴇᴅ – Yᴏᴜ’ʟʟ Bᴇ Aʟᴇʀᴛᴇᴅ Wʜᴇɴ Sᴛᴏᴄᴋ Aʀʀɪᴠᴇs!")
        try:
            '''redis_key = f"schedule:service_data:{full_data['country_id']}:{full_data['server_id']}:{full_data['app_id']}"    
            is_user_registered = await redis_manager.redis_client.zscore(redis_key, full_data['chat_id'])
            markup = InlineKeyboardMarkup()
            if is_user_registered is None:
                btn = InlineKeyboardButton(
                    "🔔 Qᴜᴇᴜᴇ Bᴜʏ", callback_data=f"notify_on:{callback_id}"
                )
            else:
                btn = InlineKeyboardButton(
                    "🔕 Lᴇᴀᴠᴇ Qᴜᴇᴜᴇ", callback_data=f"notify_off:{callback_id}"
                )
            search = InlineKeyboardButton(
                text="⌕ Cᴏᴜɴᴛʀɪᴇs",
                switch_inline_query_current_chat=f"#AᴘᴘIᴅ:{str(full_data['app_id']).translate(await small_caps())} "
            )
            markup.add(btn, search)'''
            markup = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🔕 Lᴇᴀᴠᴇ Qᴜᴇᴜᴇ",
                            callback_data=f"notify_off:{callback_id}"
                        ),   
                        InlineKeyboardButton(
                            text="⌕ Cᴏᴜɴᴛʀɪᴇs",
                            switch_inline_query_current_chat=f"#AᴘᴘIᴅ:{str(full_data['app_id']).translate(await small_caps())} "
                        )
                    ]
                ]
            )
            text = (
                "<b>🔄 Cʜᴇᴄᴋɪɴɢ Tʜᴇ Sᴛᴏᴄᴋ Eᴠᴇʀʏ Sᴇᴄᴏɴᴅ...</b>\n\n"
                "<blockquote expandable>"
                f"<b>Wᴏᴜʟᴅ Yᴏᴜ Lɪᴋᴇ Mᴇ Tᴏ “</b><code>Sᴛᴏᴘ Nᴏᴛɪғʏɪɴɢ</code><b>”\n"
                f"Yᴏᴜ Wʜᴇɴ Tʜᴇ Sᴇʀᴠɪᴄᴇ Bᴇᴄᴏᴍᴇs Aᴠᴀɪʟᴀʙʟᴇ.!?</b>\n\n"
                f"<b>• Sᴇʀᴠɪᴄᴇ »</b> <code>{str(full_data['app_name']).translate(await small_caps())}</code>\n"
                f"<b>• Cᴏᴜɴᴛʀʏ »</b> <code>{str(full_data['country_name']).translate(await small_caps())}</code> "
                f"[<code>{full_data['country_code']}</code>]\n"
                f"<b>• Aᴍᴏᴜɴᴛ »</b> 💎 <code>{str(full_data['price']).translate(await small_caps())}</code> "
                f"[<code>{str(full_data['server_id']).translate(await small_caps())}</code>]"
                "</blockquote>"
            )
            await bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=text,
                reply_markup=markup,
                parse_mode="HTML"
            )
            await redis_manager.redis_client.set(f"cache-data:callback_data:{callback_id}", json.dumps(full_data), ex=86400)
        except Exception as e:
            print(f"Error editing message: {e}")
        full_data['callback_id'] = callback_id        
        await purchase_manager.schedule_number_check(**full_data)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("notify_off:"))
    async def handle_notify_off(call: CallbackQuery):
        callback_id = call.data.split(":", 1)[1]
        raw_data = await redis_manager.redis_client.get(f"cache-data:callback_data:{callback_id}")
        if not raw_data:
            await bot.answer_callback_query(call.id, "⛔ Expired data.")
            return
        full_data = json.loads(raw_data)
        redis_key = f"service_data:{full_data['country_id']}:{full_data['server_id']}:{full_data['app_id']}"
        await  redis_manager.redis_client.zrem(f"schedule:{redis_key}", callback_id)
        await bot.answer_callback_query(call.id, "🔕 𝗡ᴏᴛɪғɪᴄᴀᴛɪᴏɴꜱ Dɪsᴀʙʟᴇᴅ – Aʟᴇʀᴛs Sɪʟᴇɴᴄᴇᴅ. Yᴏᴜ'ʀᴇ Oғғ ᴛʜᴇ Gʀɪᴅ...")
        try:
            '''redis_key = f"schedule:service_data:{full_data['country_id']}:{full_data['server_id']}:{full_data['app_id']}"    
            is_user_registered = await redis_manager.redis_client.zscore(redis_key, full_data['chat_id'])
            markup = InlineKeyboardMarkup()
            if is_user_registered is None:
                btn = InlineKeyboardButton(
                    "🔔 Qᴜᴇᴜᴇ Bᴜʏ", callback_data=f"notify_on:{callback_id}"
                )
            else:
                btn = InlineKeyboardButton(
                    "🔕 Lᴇᴀᴠᴇ Qᴜᴇᴜᴇ", callback_data=f"notify_off:{callback_id}"
                )
            search = InlineKeyboardButton(
                text="⌕ Cᴏᴜɴᴛʀɪᴇs",
                switch_inline_query_current_chat=f"#AᴘᴘIᴅ:{str(full_data['app_id']).translate(await small_caps())} "
            )
            markup.add(btn, search)'''
            markup = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🔔 Qᴜᴇᴜᴇ Bᴜʏ",
                            callback_data=f"notify_on:{callback_id}"
                        ),   
                        InlineKeyboardButton(
                            text="⌕ Cᴏᴜɴᴛʀɪᴇs",
                            switch_inline_query_current_chat=f"#AᴘᴘIᴅ:{str(full_data['app_id']).translate(await small_caps())} "
                        )
                    ]
                ]
            )
            text = (
                "<b>💡 Tʀʏ Aɴᴏᴛʜᴇʀ Sᴇʀᴠᴇʀ Fᴏʀ Fᴀsᴛᴇʀ Rᴇsᴜʟᴛs.</b>\n\n"
                "<blockquote expandable>"
                "<b>Wᴏᴜʟᴅ Yᴏᴜ Lɪᴋᴇ Mᴇ Tᴏ “</b><code>Nᴏᴛɪғʏ</code><b>”</b>\n"
                "<b>Yᴏᴜ Wʜᴇɴ Tʜᴇ Sᴇʀᴠɪᴄᴇ Bᴇᴄᴏᴍᴇs Aᴠᴀɪʟᴀʙʟᴇ.!?</b>\n\n"
                f"<b>• Sᴇʀᴠɪᴄᴇ »</b> <code>{str(full_data['app_name']).translate(await small_caps())}</code>\n"
                f"<b>• Cᴏᴜɴᴛʀʏ »</b> <code>{str(full_data['country_name']).translate(await small_caps())}</code> "
                f"[<code>{full_data['country_code']}</code>]\n"
                f"<b>• Aᴍᴏᴜɴᴛ »</b> 💎 <code>{str(full_data['price']).translate(await small_caps())}</code> "
                f"[<code>{str(full_data['server_id']).translate(await small_caps())}</code>]"
                "</blockquote>"
            )
            await bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=text,
                reply_markup=markup,
                parse_mode="HTML"
            )
        except Exception as e:
            print(f"Error editing message: {e}")
    
    
    @bot.channel_post_handler()
    async def otp_handler(msg: Message) -> None:
        # … your existing docstring …

        print(msg.text)
        pattern = re.compile(
            r"""
            🔥\s*TG\s*TECH\s*RECEIVER\s*✨\s*\n+        # Flexible header line
            ⏰\s*Time:\s*(?P<time>[^\n\r]+)\s*\n+       # Time line
            ⚙️\s*Service:\s*(?P<service>[^\n\r]+)\s*\n+ # Service line
            ☎️\s*Number:\s*(?P<number>[^\n\r]+)\s*\n+   # Number line
            🔑\s*OTP:\s*(?P<otp>[^\n\r]+)               # OTP line
            """,
            re.VERBOSE | re.IGNORECASE | re.MULTILINE
        )

        def parse_fields(text: str) -> Optional[Dict[str, Any]]:
            match = pattern.search(text)
            if not match:
                return None

            raw_time = match["time"].strip()
            try:
                parsed_time = datetime.strptime(raw_time, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                parsed_time = raw_time

            return {
                "time": parsed_time,
                "service": match["service"].strip(),
                "number": match["number"].strip(),
                "otp": match["otp"].strip(),
            }

        def build_message(data: Dict[str, Any]) -> str:
            return (
                "<blockquote expandable><b>🔥 NEW OTP PARSED ✅</b>\n"
                f"🕒 <b>Time:</b> {data['time']}\n"
                f"🔧 <b>Service:</b> {data['service']}\n"
                f"📞 <b>Number:</b> {data['number']}\n"
                f"🔑 <b>OTP:</b> <code>{data['otp']}</code></blockquote>"
            )

        try:
            text = msg.text or ""
            parsed = parse_fields(text)
            if not parsed:
                print("Forwarded message didn’t match OTP format, skipping.")
                return

            # try to unmask the number if it has a '*'
            if "*" in parsed["number"]:
                CANDIDATES = await purchase_manager.order_manager.get_candidates()
                full = await purchase_manager.unmask_number(parsed["number"], CANDIDATES)
                print(f"Unmasked {parsed['number']} → {full}")
                parsed["number"] = full

            order_id = f'987654321{parsed["number"]}'
            order_data = await purchase_manager.order_manager.get_order_data(order_id)
            if not order_data['response']:
                print("Order not found.")
                return
            order_data = order_data['result']

            if parsed['otp'].isnumeric() and parsed['number'].isnumeric():
                add_result = await purchase_manager.order_manager.manage_number_order(
                    redis_client=purchase_manager.redis_client,
                    country_id=order_data['country_id'],
                    server_id=order_data['server_id'],
                    app_id=order_data['app_id'],
                    operator="free",
                    order_id=order_data['order_id'],
                    action="add",
                    sms_code=parsed['otp']
                )
                print(colored(f"Add Code: {add_result}", "yellow"))
                await bot.send_message(
                    chat_id=order_data['user_id'],
                    text=f"✅ <b>Sᴍs Rᴇᴄɪᴇᴠᴇᴅ »</b> <code>{parsed['otp']}</code> <b>[</b><code>{parsed['number']}</code><b>]</b>\n\n",
                    parse_mode="HTML"
                )

            formatted = build_message(parsed)
            await bot.send_message(DESTINATION_CHAT_ID, formatted, parse_mode="HTML")
            print("OTP forwarded successfully:", parsed)

        except Exception as exc:
            print("Unexpected error in otp_handler:", exc)


    

__all__ = ['init_managers', 'register_handlers']


















'''
# Global instance and interface
purchase_manager = UserPurchaseManagement()

async def init_managers(order_manager: OrderManagement, user_manager: UserManagement, bot: AsyncTeleBot) -> bool:
    """Initialize purchase management system asynchronously"""
    return await purchase_manager.init_managers(order_manager, user_manager, bot)

async def register_handlers(bot: AsyncTeleBot) -> None:
    """Register purchase-related bot handlers."""
    @bot.callback_query_handler(func=lambda call: call.data.startswith("purchase:"))
    async def handle_purchase_callback(call):
        try:
            _, app_id, price, server_id, country_id, country_code = call.data.replace(' ', '').split(':')
            #logging.info(app_id, price, server_id, country_id, country_code)
            text = f'service_data:{country_id}:{server_id}:{app_id}'
            country_name = await redis_manager.redis_client.hget(text, 'country_name')
            
            process_purchase = partial(
                purchase_manager.process_purchase_flow,
                call,
                str(call.from_user.id),
                app_id,
                round(float(price), 2),
                int(server_id),
                country_id,
                country_code,
                country_name
            )
            asyncio.create_task(process_purchase())
        except ValueError:
            asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", show_alert=True))
        except Exception as e:
            #logger.error(f"Callback error: {e}")
            asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ", show_alert=True))
'''        # Define all valid matching rules in a dictionary
'''rules = {
            (672, 6, 22): {"type": "adi", "ranges": '200'},
            (572, 6, 22): {"type": "jx", "ranges": '200'},
            (672, 2, 22): {"type": "adi", "ranges": '200'},
            (672, 5, 22): {"type": "adi", "ranges": '200'},
            (579, 5, 22): {"type": "ace", "ranges": '100'},
            (579, 6, 22): {"type": "ace", "ranges": '100'},
            (579, 5, 22): {"type": "ace", "ranges": '100'},
            (572, 2, 22): {"type": "jx", "ranges": '200'},
            (572, 1, 22): {"type": "jx", "ranges": '200'},
            (572, 5, 22): {"type": "jx", "ranges": '200'},
            (572, 4, 22): {"type": "jx", "ranges": '200'},
            (86, 1, 22): {"type": "bgb", "ranges": '150'},
            (86, 2, 22): {"type": "bgb", "ranges": '150'},
            (56, 2, 22): {"type": "ot", "ranges": '250'},
            (56, 6, 22): {"type": "ot", "ranges": '250'},
            (56, 3, 22): {"type": "ot", "ranges": '250'},
        }

        rule = rules.get((int(app_id), int(server_id), int(country_id)), None)
        if rule:
            must_api_code = rule["type"]
            must_return = True
            ranges = int(rule["ranges"])
        else:
            must_api_code = None
            must_return = False
            ranges = 50'''

'''elif response_text == "NO_NUMBERS":
                            for i in range(ranges):
                                async with session.get(url) as response:
                                    response_text = await response.text()
                                    print(f"{1 + i}. API Response: {response_text}")
                                    if response_text != "NO_NUMBERS":
                                        result = await self._process_api_response(server, service_parts, country, price, operator, response_text, app_name)
                                        break
                                    if response_text.startswith("ACCESS_NUMBER:"):
                                        result = await self._process_api_response(server, service_parts, country, price, operator, response_text, app_name)
                                        break
                                    if i % 15 == 0:
                                        emoji = "⏳" if i % 10 == 5 else "⌛"
                                        try:
                                            await self.bot.edit_message_text(
                                                chat_id=chat_id,
                                                message_id=message_id,
                                                text=f"<b>{emoji} Fᴇᴛᴄʜɪɴɢ Fʀᴏᴍ Sᴇʀᴠᴇʀ,</b> <code>{str(i + 1).translate(await small_caps())}</code> <b>Aᴛᴛᴇᴍᴘᴛs.</b>..",
                                                parse_mode="HTML"
                                            )
                                        except Exception:
                                            pass
                            print(f"{1 + ranges}. API Response: NO_NUMBERS")
                            result = {"status": False, "message": "📵 Nᴏ Nᴜᴍʙᴇʀs Aᴠᴀɪʟᴀʙʟᴇ..."}
                            break'''
"""    async def _add_to_redis(self, key: str, chat_id: int, timeout_seconds: int) -> None:
        print(f"Adding to Redis for {key}")
        expire_at = int(time.time()) + timeout_seconds
        await self.redis_client.zadd(f"schedule:{key}", {chat_id: expire_at})

    async def schedule_number_check(
        self,
        **kwargs
    ) -> None:
        print(f"Scheduling number check for {kwargs['app_name']} from {kwargs['server_id']}")
        full_data = kwargs

        timeout_seconds = full_data.get('timeout_seconds', 24 * 3600)  # default 24h
        poll_interval = full_data.get('poll_interval', 10)

        redis_key = f"service_data:{full_data['country_id']}:{full_data['server_id']}:{full_data['app_id']}"
        exists = await self.redis_client.exists(f"schedule:{redis_key}")

        await self._add_to_redis(redis_key, full_data['callback_id'], timeout_seconds)
        if exists:
            return

        full_data['key'] = redis_key
        full_data['timeout_seconds'] = timeout_seconds
        full_data['poll_interval'] = poll_interval

        asyncio.create_task(
            self._background_check_loop(**dict(full_data))
        )

    async def _background_check_loop(self, **full_data) -> None:
        '''
        Periodically checks availability of a number for all users waiting on this schedule key.
        Expires entries older than `timeout_seconds`, notifies them, then exits when either
        someone successfully purchases or the timeout is reached.
        '''
        redis_key = f"schedule:{full_data['key']}"
        async def notify_and_remove(uids, message_fn, message_notify, keyboard=None):
            '''Helper to send notifications and remove from sorted set.'''
            for uid in uids:
                raw_data = await redis_manager.redis_client.get(f"cache-data:callback_data:{user_id}")
                user_full_data = json.loads(raw_data)
                user_id = user_full_data['user_id']
                message_id = user_full_data['message_id']

                try:
                    await self.bot.send_message(int(user_id), message_fn(user_id), reply_markup=keyboard, parse_mode="HTML")
                except Exception as e:
                    print(f"Failed notifying {user_id}: {e}")
                try:
                    markup = InlineKeyboardMarkup(
                        [
                            [
                                InlineKeyboardButton(
                                    "🔔 Qᴜᴇᴜᴇ Bᴜʏ",
                                    callback_data=f"notify_on:{callback_id}"
                                ),   
                                InlineKeyboardButton(
                                    text="⌕ Cᴏᴜɴᴛʀɪᴇs",
                                    switch_inline_query_current_chat=f"#AᴘᴘIᴅ:{str(full_data['app_id']).translate(await small_caps())} "
                                )
                            ]
                        ]
                    )
                    text = (
                        f"<b>{message_notify}</b>\n\n"
                        "<blockquote expandable>"
                        "<b>Wᴏᴜʟᴅ Yᴏᴜ Lɪᴋᴇ Mᴇ Tᴏ “</b><code>Nᴏᴛɪғʏ</code><b>”</b>\n"
                        "<b>Yᴏᴜ Wʜᴇɴ Tʜᴇ Sᴇʀᴠɪᴄᴇ Bᴇᴄᴏᴍᴇs Aᴠᴀɪʟᴀʙʟᴇ.!?</b>\n\n"
                        f"<b>• Sᴇʀᴠɪᴄᴇ »</b> <code>{str(full_data['app_name']).translate(await small_caps())}</code>\n"
                        f"<b>• Cᴏᴜɴᴛʀʏ »</b> <code>{str(full_data['country_name']).translate(await small_caps())}</code> "
                        f"[<code>{full_data['country_code']}</code>]\n"
                        f"<b>• Aᴍᴏᴜɴᴛ »</b> 💎 <code>{str(full_data['price']).translate(await small_caps())}</code> "
                        f"[<code>{str(full_data['server_id']).translate(await small_caps())}</code>]"
                        "</blockquote>"
                    )
                    await self.bot.edit_message_text(
                        chat_id=user_id,
                        message_id=message_id,
                        text=text,
                        reply_markup=markup,
                        parse_mode="HTML"
                    )
                except Exception as e:
                    print(f"Failed notifying {user_id}: {e}")
                await self.redis_client.zrem(redis_key, uid)

        while True:
            now = int(time.time())
            print(colored(f"Polling for {full_data['app_name']} (server {full_data['server_id']})…", "green"))

            # 1) Notify & remove any entries that have timed out
            expired = await self.redis_client.zrangebyscore(redis_key, 0, now)
            if expired:
                await notify_and_remove(
                    expired,
                    lambda uid: (
                        f"<i>⏳ Yᴏᴜʀ Pʟᴀᴄᴇ Iɴ Tʜᴇ Ǫᴜᴇᴜᴇ Hᴀꜱ Bᴇᴇɴ Rᴇʟᴇᴀꜱᴇᴅ.</i>",
                        f"⏰ <i>Uɴꜰᴏʀᴛᴜɴᴀᴛᴇʟʏ, “</i><b>{full_data['app_name']}</b><i>” Wᴀꜱ Nᴏᴛ Aᴠᴀɪʟᴀʙʟᴇ Wɪᴛʜɪɴ Lᴀꜱᴛ</b> <code>{full_data['timeout_seconds'] // 3600}</code> <i>Hᴏᴜʀꜱ.</b>\n\n"
                    ),
                    message_notify="💡 Tʀʏ Aɴᴏᴛʜᴇʀ Sᴇʀᴠᴇʀ Fᴏʀ Fᴀsᴛᴇʀ Rᴇsᴜʟᴛs.."
                )

            # 2) If nobody's left waiting, we're done
            remaining = await self.redis_client.zrange(redis_key, 0, -1)
            if not remaining:
                print("No more waiting users; exiting loop.")
                return

            # 3) Single availability check per interval
            #primary_uid = int(remaining[0])
            print(colored(f"Checking availability for {full_data['app_name']}…", "yellow"))

            try:
                phone_result = await self.fetch_phone_number(
                    full_data['server_id'],
                    full_data['app_code'],
                    full_data['country_id'],
                    price=full_data['price'],
                    operator=full_data['operator'],
                    app_name=full_data['app_name']
                )
                print(colored(f"Fetch result: {phone_result}", "cyan"))

                if phone_result.get('status'):
                    # 4) Loop through all waiting users and find the first with sufficient balance
                    for uid_bytes in remaining:
                        raw_data = await redis_manager.redis_client.get(f"cache-data:callback_data:{uid_bytes}")
                        user_full_data = json.loads(raw_data)
                        user_id = user_full_data['user_id']
                        if await self._handle_user_balance(user_id, user_full_data['price'], user_id, progress_msg=None):
                            # reconstruct call in that user's context
                            user_full_data.update(chat_id=user_id)
                            call = await self.reconstruct_fake_call(user_full_data)

                            # finalize purchase for this user
                            await self._finalize_purchase(
                                call,
                                phone_result,
                                user_full_data,
                                user_full_data["price"],
                                user_full_data["country_id"],
                                user_full_data["country_code"],
                                user_full_data["country_name"],
                                phone_result["service"],
                                call.message,
                                is_new=True
                            )
                            text = (
                                f"<blockquote expandable><b>✅ Yᴏᴜʀ Oʀᴅᴇʀ Fᴏʀ “</b><code>{str(user_full_data['app_name']).translate(await small_caps())}</code><b>” Hᴀꜱ Bᴇᴇɴ Pᴜʀᴄʜᴀꜱᴇᴅ Sᴜᴄᴄᴇꜱꜰᴜʟʟʏ!</b>\n\n"
                                f"⏰ <b>Fᴏʀᴛᴜɴᴀᴛᴇʟʏ, Nᴜᴍʙᴇʀ Aʀᴇ Bᴀᴄᴋ Iɴ Sᴛᴏᴄᴋ, Wɪᴛʜɪɴ Lᴀꜱᴛ</b> <code>{full_data['timeout_seconds'] // 3600}</code> <i>Hᴏᴜʀꜱ.</b>\n\n</blockquote>"
                            )
                            await self.bot.send_message(user_id, text, parse_mode="HTML")
                            # remove *that* user and break
                            await self.redis_client.zrem(redis_key, uid_bytes)
                            break
                        else:
                            print(colored(f"User {user_id} has insufficient balance, skipping.", "red"))
                    else:
                        # nobody could pay: wait for next tick
                        print(colored("No user had sufficient balance; retrying…", "magenta"))
                        break

                    # 5) If phone_result succeeded, notify *all* remaining users that it's available now
                    post_remaining = await self.redis_client.zrange(redis_key, 0, -1)
                    if post_remaining:
                        text = (
                            f"<blockquote expandable><b> “{str(full_data['app_name']).translate(await small_caps())}” Is Aᴠᴀɪʟᴀʙʟᴇ Fᴏʀ Pᴜʀᴄʜᴀsᴇ.</b></blockquote>\n\n"
                            f"<b>• Cᴏᴜɴᴛʀʏ »</b> <code>{str(full_data['country_name']).translate(await small_caps())}</code> [<code>{full_data['country_code']}</code>]\n"
                            f"<b>• Aᴍᴏᴜɴᴛ »</b> 💎 <code>{str(full_data['price']).translate(await small_caps())}</code> [<code>{str(full_data['server_id']).translate(await small_caps())}</code>]",
                        )
                        keyboard = InlineKeyboardMarkup()
                        keyboard.add(
                            InlineKeyboardButton(
                                "🛒 Pᴜʀᴄʜᴀsᴇ Tʜɪs Sᴇʀᴠɪᴄᴇ",
                                callback_data=f"purchase:{full_data.get('app_id', '')}:{full_data.get('price', '')}:{full_data.get('server_id', '')}:{full_data.get('country_id', '')}:{full_data.get('country_code', '')}"
                            )
                        )
                        await notify_and_remove(
                            post_remaining,
                            lambda uid: text,
                            message_notify="🟢 Sᴛᴏᴄᴋ Aᴠᴀɪʟᴀʙʟᴇ – Pʀᴏᴄᴇᴇᴅ Tᴏ Bᴜʏ Nᴏᴡ!",
                            keyboard=keyboard
                        )
                    return

            except Exception as e:
                print(colored(f"Error during availability check: {e}", "red"))

            # 6) Safety sleep and timeout guard
            await asyncio.sleep(full_data['poll_interval'])
"""

'''    async def _handle_bad_service(self, server: int, service_parts: List[str], country: str, 
                                  price: float, operator: str, app_name: str = None) -> Dict:
        services_to_try = []
        if server == 1:
            services_to_try = [app_name] if app_name else []
        elif server in [3, 4, 5]:
            services_to_try = service_parts

        services_to_try = [s for s in services_to_try if s]  # Remove None values
        
        for service in services_to_try:
            result = await self.fetch_phone_number(server, service, country, price, operator)
            if result['status'] or result['message'] != "🚫 Wʀᴏɴɢ Sᴇʀᴠɪᴄᴇ Sᴘᴇᴄɪғɪᴇᴅ...":
                return result
        
        return await self._handle_api_errors("BAD_SERVICE")

'''