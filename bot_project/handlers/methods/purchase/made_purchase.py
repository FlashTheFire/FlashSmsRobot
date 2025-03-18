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
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from phonenumbers import parse, format_number, PhoneNumberFormat, NumberParseException
from redis.commands.search.query import Query
import requests
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

IMGBB_API_KEY = "530530e324408b15858555c78a657a96"  # Replace with your actual API key if needed

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
            #logging.infoawait asyncio.to_thread(, '-' * 70)
            #await asyncio.to_thread(logger.info, "|| Purchase managers initialized successfully")
            return True
        except Exception as e:
            #await asyncio.to_thread(logger.error, f"Initialization error: {e}")
            return False

    async def fetch_app_data(self, app_id: str, server_id: str, country_id: str) -> Optional[Dict]:
        """Retrieve and cache application data from Redis"""
        try:
            redis_client = self.redis_client
            cache_key = f"app_data:{app_id}:{server_id}:{country_id}"
            
            #if cached := await cache_manager.get(redis_client, cache_key, prefix=CachePrefix.SEARCH):
            #    return cached["data"]

            query_str = f'@app_id:{app_id} @server_id:{server_id} @country_id:{country_id}'
            result = await redis_client.ft(SERVICE_INDEX).search(
                Query(query_str)
                .return_fields("app_name", "app_code", "app_price", "app_count", "server_name")
            )

            if not result.docs:
                return None

            app_data = await self._process_app_documents(result.docs)
            await cache_manager.set(redis_client, cache_key, app_data, 300, CachePrefix.SEARCH)
            return app_data
            
        except Exception as e:
            #logger.error(f"App data fetch error: {e}")
            return None

    async def _process_app_documents(self, docs) -> Dict:
        """Process Redis documents into app data structure asynchronously"""
        app_data = {
            'app_name': docs[0].app_name,
            'app_code': docs[0].app_code,
            'app_price': float(docs[0].app_price),
            'app_count': int(docs[0].app_count),
            'operator': docs[0].server_name,
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
        return app_data

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
            if not await self._acquire_transaction_lock(guard, transaction_key, call, price):
                return False
            end_time = time.time()
            print(f"Transaction lock acquired in {end_time - start_time:.8f} seconds")

            try:
                return await self._execute_purchase_steps(call, user_id, app_id, price, 
                                                        server_id, country_id, country_code, country_name, progress_msg, transaction_key, guard)
            except Exception as e:
                #logger.error(f"Purchase processing error: {e}")
                try:
                    await self.bot.answer_callback_query(call.id, "🚫 Pᴜʀᴄʜᴀsᴇ Fᴀɪʟᴇᴅ. Pʟᴇᴀsᴇ Tʀʏ Aɢᴀɪɴ.", show_alert=True)
                except:
                    pass
                return False
            finally:
                await guard.release_lock(transaction_key)

    async def _acquire_transaction_lock(self, guard, transaction_key, call, price) -> bool:
        """Acquire transaction lock with error handling"""
        lock = await guard.acquire_lock(transaction_key)
        if not lock:
            try:
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


        if not await self._handle_user_balance(user_id, price, chat_id, progress_msg):
            return False
        
        app_data = await self.fetch_app_data(app_id, server_id, country_id)
        if not app_data:
            raise ValueError("🚫 Iɴᴠᴀʟɪᴅ Aᴘᴘʟɪᴄᴀᴛɪᴏɴ Cᴏɴғɪɢᴜʀᴀᴛɪᴏɴ")
        #logging.info(app_data)

        
        await self.bot.edit_message_text(
            chat_id=chat_id,
            message_id=progress_msg.message_id,
            text="<b>⌛ Fᴇᴛᴄʜɪɴɢ Fʀᴏᴍ Sᴇʀᴠᴇʀ..</b>.", 
            parse_mode="HTML"
        )
        phone_result = await self.fetch_phone_number(server_id, app_data['app_code'], country_id, price=price, operator=app_data['operator'], app_name=app_data['app_name'])
        if not phone_result.get("status"):
            if phone_result.get("message"):
                try:
                    await self.bot.answer_callback_query(call.id, phone_result.get('message', '❌ Uɴᴋɴᴏᴡɴ Eʀʀᴏʀ'), show_alert=False)
                    await self.bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, 
                                                     text=f"<b>{phone_result.get('message', '❌ Uɴᴋɴᴏᴡɴ Eʀʀᴏʀ')}</b>", parse_mode="HTML")
                except:
                    return False
            raise Exception(f"🔢 Pʜᴏɴᴇ Nᴜᴍʙᴇʀ Aᴄǫᴜɪsɪᴛɪᴏɴ Fᴀɪʟᴇᴅ: {phone_result.get('message', '❌ Uɴᴋɴᴏᴡɴ Eʀʀᴏʀ')}")
        try:
            await guard.release_lock(transaction_key)
            await self.bot.answer_callback_query(call.id, "🛍️ Nᴜᴍʙᴇʀ Pᴜʀᴄʜᴀsᴇᴅ Sᴜᴄᴄᴇssғᴜʟʟʏ...", show_alert=False)
        except:
            pass
        await self._finalize_purchase(call, phone_result, app_data, price, country_id, country_code, country_name, phone_result['service'], progress_msg)
        return True

    async def _handle_user_balance(self, user_id, price, chat_id, progress_msg) -> bool:
        """Handle balance check and deduction"""
        try:
            user_data = await self.aggregator.get_user(user_id)
            if not user_data or not user_data.get('response'):
                #logger.error("Failed to retrieve user data.")
                return False

            current_balance = user_data["metrics"]["current_balance"]
            
            if current_balance < price:
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
            
    async def fetch_phone_number(self, server: int, service: str, country: str, price: float, operator: str = None, app_name: str = None) -> dict:
        server_name, api_key = await get_api_info(server)
        service_parts = service.split(',')
        attempts = 3 if server == 1 and len(service_parts) > 1 else 2 if len(service_parts) > 1 else 1

        async def attempt_fetch(attempt: int) -> dict:
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
                            return {"status": False, "message": f"HTTP {response.status}"}
                        response_text = await response.text()
                        print(f"API Response: {response_text}")
                        if response_text in ["WRONG_SERVICE", "BAD_SERVICE"]:
                            return None  # Indicates this attempt should be ignored
                        return await self._process_api_response(server, service_parts, country, price, operator, response_text, app_name)
            except asyncio.TimeoutError:
                return await self._process_api_response(server, service_parts, country, price, operator, 'NO_NUMBERS', app_name)
            except aiohttp.ClientError as e:
                return {"status": False, "message": f"Network error: {e}"}

        tasks = [attempt_fetch(i) for i in range(attempts)]
        results = await asyncio.gather(*tasks)
        # Return the first valid result with a positive status
        for result in results:
            if result is not None and result.get("status") is True:
                return result
        # If none succeeded, return the first non-None error result (if any)
        for result in results:
            if result is not None:
                return result
        return {"status": False, "message": "🚫 Fᴀɪʟᴇᴅ Aғᴛᴇʀ Mᴜʟᴛɪᴘʟᴇ Aᴛᴛᴇᴍᴘᴛs"}

    async def _build_api_url(self, server_name: str, api_key: str, service: str, country: str, price: float, operator: str) -> str:
        price = round(float(price) / float(COMMISSION), 8)
        if str(server_name) in ["api.sms-activate.org", "smshub.org"]:
            price = round(convert_rub_to_usd(price), 4)
        else:
            price = round(price, 2)
        params = {
            "api_key": api_key, "action": "getNumber", "service": service.replace(' ', '').lower(),
            "country": country, "maxPrice": price, "operator": operator
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

    async def _process_api_response(self, server: int, service_parts: List[str], country: str, 
                                    price: float, operator: str, response_text: str, app_name: str = None) -> Dict:
        if response_text == "BAD_SERVICE":
            return await self._handle_bad_service(server, service_parts, country, price, operator, app_name)
        elif response_text.startswith("ACCESS_NUMBER"):
            return await self._process_success_response(response_text, service_parts[0])
        else:
            return await self._handle_api_errors(response_text)

    async def _handle_bad_service(self, server: int, service_parts: List[str], country: str, 
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
                               country_id: str, country_code: str, country_name: str, service: str, progress_msg: Message) -> None:
        """Complete purchase transaction and update systems"""
        purchase_data = await self._build_purchase_data(call, result, app_data, price, country_id, country_code, country_name, service, progress_msg)
        
        try:
            order_id = await self._create_order_record(purchase_data)
            await self._send_purchase_confirmation(call, purchase_data, order_id)
        except Exception as e:
            #logger.error(f"Finalization error: {e}")
            raise

    async def _build_purchase_data(self, call, result, app_data, price, country_id, country_code, country_name, service, progress_msg) -> Dict:
        """Build unified purchase data structure asynchronously"""
        callback_user_id = call.from_user.id if call.from_user else None
        chat_id = call.message.chat.id if call.message and call.message.chat else callback_user_id
        
        # Use asyncio.to_thread for potentially blocking operations
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
            'valid_until': valid_until
        }

    async def _create_order_record(self, data: Dict) -> str:
        """Create order record in database"""
        order_id = await self.order_manager.create_order_id(user_id=data['user_id'])
        if not order_id.get('response'):
            raise Exception("⚠️ Oʀᴅᴇʀ ID Cʀᴇᴀᴛɪᴏɴ Fᴀɪʟᴇᴅ")

        order_data = await self._build_order_data(data, order_id['result'])
        response = await self.order_manager.add_order_data(order_id['result'], data['user_id'], order_data)
        
        if not response.get('response'):
            raise Exception("⚠️ Oʀᴅᴇʀ Dᴀᴛᴀ Sᴛᴏʀᴀɢᴇ Fᴀɪʟᴇᴅ")

        return order_id['result']

    async def _build_order_data(self, data: Dict, order_id: str) -> Dict:
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

    async def _send_purchase_confirmation(self, call, data: Dict, order_id: str) -> None:
        """Send purchase confirmation to user"""
        keyboard = InlineKeyboardMarkup().row(
            InlineKeyboardButton("✘ Cᴀɴᴄᴇʟ", callback_data=f"status_cancel:{order_id}:{data['user_id']}"),
            InlineKeyboardButton("↻ Bᴜʏ Aɢᴀɪɴ", callback_data=call.data)
        )
        await self.bot.edit_message_text(
            chat_id=data['chat_id'],
            message_id=data['message_id'].message_id,
            text=await self._confirmation_message_content(data, minute=str(BASE_TIMEOUT)),
            parse_mode="HTML",
            reply_markup=keyboard
        )
        
        # Combine tasks into a single coroutine
        
        from handlers.main.show_wallet import wallet_manager
        await asyncio.gather(
            self._delayed_message_edit(data, keyboard),
            self._process_and_save_image(data, data['service']),
            self.user_manager.send_order_report(self.bot, "send_message", order_id, data['user_id'], '-1002203139746', data),
            self.user_manager.user_metrics_report(self.bot, 'edit_message_text', data['user_id'], '-1002203139746'),
            self.add_service_to_leaderboard(data['app_id'], data['country_id'], data['server_id'], data['app_name'], data['service'])
        )

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

        bg_url = f"https://udayscripts.in/image/service/{service}.png"
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

__all__ = ['init_managers', 'register_handlers']




'''validation = await self._validate_purchase_request(user_id, price)
        if not validation["valid"]:
            try:
                await self.bot.answer_callback_query(call.id, validation["error"], show_alert=True)
            except:
                pass
            return False'''
        
        