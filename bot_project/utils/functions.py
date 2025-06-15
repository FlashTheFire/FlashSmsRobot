#!/usr/bin/env python3
import os
import sys
import asyncio
import functools
import contextlib
import base64
from PIL import Image
from io import BytesIO
import aiohttp

from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import logging
from utils.redis_manager import RedisManager, redis_manager
import inspect
from colorlog import ColoredFormatter
from datetime import datetime, timedelta
from typing import Optional, Union, Dict, Any, List, Tuple
from PIL import Image, ImageDraw, ImageFont, ImageOps
import json
import aiohttp
from io import BytesIO
from telebot.async_telebot import AsyncTeleBot
from telebot.types import Message
from utils.config import BOT_TOKEN as BotToken, USER_IMAGE_HASH, DEPOSIT_INR_QR_CODE
import redis.asyncio as redis
from redis.commands.search.query import Query
import pytz
import string
import numpy as np
import qrcode
from utils.config import COMMISSION

# Constants and providers
DEPOSIT_PROVIDERS = {
    1: ('paytm.udayscriptsx.workers.dev/', 'SzFThC49898719386494'),
}
QR_BASE_URL = (
    "https://qr.udayscriptsx.workers.dev/"
    "?data=upi%3A%2F%2Fpay%3Fpa%3Dpaytmqr281005050101nbxw0hx35cpo%40paytm"
    "%26pn%3DPaytm%2520Merchant%26tr%3D{order_id}"
    "%26tn%3DAdding%2520Fund&body=dot&eye=frame13&eyeball=ball14"
    "&col1=121f28&col2=121f28&logo=https://i.postimg.cc/cCrHr3TQ/1000011838-removebg.png"
)
ALPHABET = "𝄃𝄂𝄀𝄁"
BASE = len(ALPHABET)

# --------------------------------------------------------------------------
# All functions are now asynchronous

async def encode_order_id(order_id: int) -> str:
    """Asynchronously encodes a non-negative integer order_id into a barcode string."""
    def _encode():
        order_id_int = int(order_id)
        if order_id_int < 0:
            raise ValueError("Order ID must be non-negative")
        if order_id_int == 0:
            return ALPHABET[0]
        encoded = ""
        while order_id_int > 0:
            order_id_int, remainder = divmod(order_id_int, BASE)
            encoded = ALPHABET[remainder] + encoded
        return encoded
    return await asyncio.to_thread(_encode)

async def decode_barcode_id(barcode: str) -> int:
    """Asynchronously decodes a barcode string back into the original order_id integer."""
    def _decode():
        order_id = 0
        for symbol in str(barcode):
            value = ALPHABET.find(symbol)
            if value == -1:
                raise ValueError(f"Invalid symbol '{symbol}' in barcode.")
            order_id = order_id * BASE + value
        return order_id
    return await asyncio.to_thread(_decode)

async def country_flag_link(flag_emoji: str, size: int = 320) -> str:
    """
    Convert a country flag emoji into its corresponding flag image URL.

    Parameters:
    - flag_emoji: A string containing the country flag emoji (e.g., '🇮🇳').
    - size: The width of the flag image (commonly 80, 160, 320, etc.).

    Returns:
    - A URL string pointing to the flag image.
    """
    if len(flag_emoji) != 2:
        raise ValueError("Invalid flag emoji. Please provide a valid country flag emoji.")

    # Convert each regional indicator symbol into the corresponding letter.
    country_code = ''.join(chr(ord(c) - 127397) for c in flag_emoji).lower()

    # Construct the URL using FlagCDN's URL pattern.
    url = f"https://hatscripts.github.io/circle-flags/flags/{country_code}.svg"
    return "https://res.cloudinary.com/djfsvvzto/image/upload/zfanvluzouhuys0qn7ou.png"
    return url

async def fetch_qr(order_id: str) -> BytesIO:
    """Asynchronously fetches the QR code image and returns it as a BytesIO."""
    qr_url = QR_BASE_URL.format(order_id=order_id)
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(qr_url) as response:
                if response.status != 200:
                    raise Exception(f"Failed to fetch QR code JSON, status code: {response.status}")
                response_json = await response.json()
                image_url = response_json.get("image")
                if not image_url:
                    raise Exception("Image URL not found in the JSON response")
            async with session.get(image_url) as image_response:
                if image_response.status != 200:
                    raise Exception(f"Failed to fetch QR code image, status code: {image_response.status}")
                print('image_response')
                print(image_response)
                return BytesIO(await image_response.read())
        except Exception as e:
            print(f"Error fetching QR code: {e}")

async def qr_code(
    deposit_id: str,
    size: int,
    position: Tuple[int, int],
    radius: int,
) -> BytesIO:
    """Asynchronously generates and overlays a QR code on an image, returning it as BytesIO."""
    qr_img_bytes, rect_img = await asyncio.gather(
        fetch_qr(deposit_id),
        fetch_image_from_url(DEPOSIT_INR_QR_CODE)
    )
    print('qr_img_bytes')
    print(qr_img_bytes)
    print('rect_img')
    print(rect_img)

    square_img = Image.open(qr_img_bytes).convert("RGBA")
    square_img = ImageOps.fit(square_img, (size, size), Image.LANCZOS)
    mask = Image.new("L", (size, size), 0)
    draw = ImageDraw.Draw(mask)
    draw.rounded_rectangle((0, 0, size, size), radius, fill=255)
    square_img.putalpha(mask)
    rect_img = rect_img.convert("RGBA")
    rect_img.paste(square_img, position, square_img)
    img_byte_arr = BytesIO()
    await asyncio.to_thread(rect_img.save, img_byte_arr, "PNG")
    img_byte_arr.seek(0)
    print('img_byte_arr')
    print(img_byte_arr)
    return img_byte_arr

async def format_currency(amount: Union[float, int], currency: str = "INR") -> str:
    """Asynchronously formats currency with proper symbols."""
    # This is a quick computation; no need to offload
    if currency == "INR":
        return f"₹{amount:,.2f}"
    elif currency == "USD":
        return f"${amount:,.2f}"
    else:
        return f"{amount:,.2f} {currency}"
def convert_usd_to_rub(amount_usd, exchange_rate=100, tax_rate=COMMISSION):
    """Converts USD to RUB, applying the given exchange rate and tax rate."""
    from handlers.manager.operation import SMS_ACTIVATE_TAX
    return round(float(amount_usd) * exchange_rate * float(tax_rate) * float(SMS_ACTIVATE_TAX), 8)


def convert_rub_to_usd(amount_rub, exchange_rate=100, tax_rate=COMMISSION):
    """Converts RUB to USD, applying the given exchange rate and tax rate."""
    from handlers.manager.operation import SMS_ACTIVATE_TAX
    return round(float(amount_rub) / exchange_rate, 8)






async def create_keyboard(buttons: List[Dict[str, Any]], row_width: int = 2) -> InlineKeyboardMarkup:
    """Asynchronously creates an InlineKeyboardMarkup for TeleBot."""
    def _create():
        keyboard = InlineKeyboardMarkup()
        row = []
        for button in buttons:
            if 'url' in button:
                inline_button = InlineKeyboardButton(text=button['text'], url=button['url'])
            elif 'switch_inline_query' in button:
                inline_button = InlineKeyboardButton(text=button['text'], switch_inline_query=button['switch_inline_query'])
            elif 'switch_inline_query_current_chat' in button:
                inline_button = InlineKeyboardButton(
                    text=button['text'],
                    switch_inline_query_current_chat=button['switch_inline_query_current_chat']
                )
            elif 'pay' in button and button['pay']:
                inline_button = InlineKeyboardButton(text=button['text'], pay=True)
            else:
                inline_button = InlineKeyboardButton(text=button['text'], callback_data=button['callback_data'])
            row.append(inline_button)
            if len(row) >= row_width:
                keyboard.row(*row)
                row = []
        if row:
            keyboard.row(*row)
        return keyboard
    return await asyncio.to_thread(_create)

async def serialize_data(data: Any) -> str:
    """Asynchronously serializes data to JSON format."""
    return await asyncio.to_thread(json.dumps, data)

async def deserialize_data(data: str) -> Any:
    """Asynchronously deserializes JSON data."""
    try:
        return await asyncio.to_thread(json.loads, data) if data else {}
    except json.JSONDecodeError:
        logging.error("Error decoding JSON data from Redis.")
        return {}

# --------------------------------------------------------------------------
# Advanced Asynchronous Logger

class AdvancedLogger:
    def __init__(self, log_file: Optional[str] = None, where_logger: Optional[str] = None):
        self.logger = logging.getLogger("advanced_logger")
        self.logger.setLevel(logging.DEBUG)
        self.where_logger = where_logger or "N/A"
        if not self.logger.handlers:
            formatter = ColoredFormatter(
                '%(asctime)s » 〔 %(custom_file)s » %(custom_func)s 〕\n'
                '%(log_color)s%(levelname)-8s ❯ %(message)s',
                log_colors={
                    'DEBUG': 'cyan',
                    'INFO': 'green',
                    'WARNING': 'yellow',
                    'ERROR': 'red',
                    'CRITICAL': 'bold_red',
                },
                datefmt='%Y-%m-%d »%H:%M:%S'
            )
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    async def _log(self, level: int, message: str, function_name: Optional[str] = None, **kwargs):
        extra = {
            'custom_file': self.where_logger,
            'custom_func': function_name or "N/A"
        }
        if kwargs.get('extra'):
            extra.update(kwargs['extra'])
        adapter = logging.LoggerAdapter(self.logger, extra)
        await asyncio.to_thread(adapter.log, level, message)

    async def info(self, message: str, function_name: Optional[str] = None, **kwargs):
        await self._log(logging.INFO, message, function_name, **kwargs)

    async def debug(self, message: str, function_name: Optional[str] = None, **kwargs):
        await self._log(logging.DEBUG, message, function_name, **kwargs)

    async def warning(self, message: str, function_name: Optional[str] = None, **kwargs):
        await self._log(logging.WARNING, message, function_name, **kwargs)

    async def error(self, message: str, function_name: Optional[str] = None, **kwargs):
        await self._log(logging.ERROR, message, function_name, **kwargs)

    async def critical(self, message: str, function_name: Optional[str] = None, **kwargs):
        await self._log(logging.CRITICAL, message, function_name, **kwargs)

async def setup_logger(log_file: Optional[str] = None, where_logger: Optional[str] = None) -> AdvancedLogger:
    """Asynchronously initializes the AdvancedLogger."""
    return AdvancedLogger(log_file=log_file, where_logger=where_logger)

# --------------------------------------------------------------------------
#  Asynchronous Profile Photo & Image Functions



# Redis key for storing user profile images


async def get_tg_profile_photo(user_id: int) -> Dict[str, Any]:
    """Asynchronously retrieves the Telegram profile photo for a user, storing and fetching from Redis."""
    redis_client = await redis_manager.get_client()

    # Fetch base64-encoded image from Redis
    img_base64 = await redis_client.hget(USER_IMAGE_HASH, str(user_id))
    if img_base64:
        try:
            img_data = base64.b64decode(img_base64)
            logging.debug(f"Profile image for user {user_id} loaded from Redis hash.")
            return {'response': True, 'result': img_data}
        except Exception as e:
            logging.error(f"Error decoding base64 image for user {user_id}: {e}")

    # If not found, fetch from Telegram
    try:
        url = f'https://api.telegram.org/bot{BotToken}/getUserProfilePhotos'
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params={'user_id': user_id, 'limit': 1}) as response:
                data = await response.json()
                if data.get('ok') and data['result']['total_count'] > 0:
                    file_id = data['result']['photos'][0][0]['file_id']
                    file_url = f'https://api.telegram.org/bot{BotToken}/getFile'
                    async with session.get(file_url, params={'file_id': file_id}) as file_response:
                        file_data = await file_response.json()
                        if file_data.get('ok'):
                            image_url = f"https://api.telegram.org/file/bot{BotToken}/{file_data['result']['file_path']}"
                            # Download and save to Redis
                            save_result = await save_image_to_redis(user_id, image_url, redis_client)
                            if save_result.get('response'):
                                logging.info(f"Profile image stored in Redis for user {user_id}.")
                                return {'response': True, 'result': save_result.get('result')}
        return {'response': False, 'error': "No profile photo found."}
    except Exception as e:
        logging.error(f"Error getting profile image: {e}")
        return {'response': False, 'error': str(e)}


async def save_image_to_redis(user_id: int, image_url: str, redis_client: RedisManager) -> Dict[str, Any]:
    """Downloads image from URL, encodes in base64, and saves to Redis hash."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(image_url) as response:
                response.raise_for_status()
                img_data = await response.read()

                # Encode image to base64 string for safe Redis storage
                img_base64 = base64.b64encode(img_data).decode('utf-8')

                await redis_client.hset(USER_IMAGE_HASH, str(user_id), img_base64)
                return {'response': True, 'result': img_data}
    except Exception as e:
        logging.error(f"Error saving image to Redis: {e}")
        return {'response': False, 'error': str(e)}


async def convert_points(balance: Union[float, int], currency: str = "INR") -> int:
    """Asynchronously converts points (stub implementation)."""
    return 0

async def get_api_info(server_number: int, type: str = 'sms') -> Tuple[Optional[str], Optional[str]]:
    """Asynchronously retrieves API info for a given server number."""
    if type not in ['sms', 'recharge']:
        raise ValueError("type must be 'sms' or 'recharge'.")
    if not isinstance(server_number, int):
        raise ValueError("server_number must be an integer.")
    if type == 'sms':
        from utils.api import SMS_PROVIDERS  # import here if not globally available
        api_items = list(SMS_PROVIDERS.items())
    elif type == 'recharge':
        api_items = list(DEPOSIT_PROVIDERS.items())
    if 1 <= server_number <= len(api_items):
        server_name, api_key = api_items[server_number - 1]
        return server_name, api_key
    else:
        return None, None

async def small_caps() -> dict:
    """Asynchronously returns a translation table for small caps conversion."""
    return str.maketrans(
        'abcdefghijklmnopqrstuvwxyz1234567890',
        'ᴀʙᴄᴅᴇғɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢ𝟷𝟸𝟹𝟺𝟻𝟼𝟽𝟾𝟿𝟶'
    )
async def large_caps() -> dict:
    """Asynchronously returns a translation table for large caps conversion."""
    return str.maketrans(
        'ᴀʙᴄᴅᴇғɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢ𝟷𝟸𝟹𝟺𝟻𝟼𝟽𝟾𝟿𝟶',
        'abcdefghijklmnopqrstuvwxyz1234567890'
    )


async def get_sms_text_by_code(order_id: str, sms: str, server_id: int) -> Optional[str]:
    """
    Retrieves SMS text matching the given code using different providers based on server_id.
    
    :param order_id: The order/activation ID.
    :param sms: The expected SMS code to match.
    :param server_id: Provider ID (1 = 5SIM, 6 = SMS-Activate.ae).
    :return: Matched SMS text or None if not found or on error.
    """
    try:
        async with aiohttp.ClientSession() as session:
            if server_id == 1:
                from utils.api import FIVESIM
                url = f"https://5sim.net/v1/user/check/{order_id}"
                headers = {
                    "Authorization": f"Bearer {FIVESIM}",
                    "Accept": "application/json"
                }
                async with session.get(url, headers=headers, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for item in data.get("sms", []):
                            if item.get("code") == sms:
                                return item.get("text")
            
            elif server_id == 6:
                from utils.api import SMS_ACTIVATE
                url = (
                    f"https://api.sms-activate.ae/stubs/handler_api.php"
                    f"?api_key={SMS_ACTIVATE}&action=getStatusV2&id={order_id}"
                )
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        content_type = resp.headers.get("Content-Type", "")
                        if "application/json" in content_type:
                            data = await resp.json()
                            sms_data = data.get("sms")
                            if sms_data and sms_data.get("code") == sms:
                                return sms_data.get("text")
                        else:
                            # handle possible plain text errors
                            text = await resp.text()
                            print(f"[SMS-ACTIVATE ERROR] {text}")
                            return None

    except asyncio.TimeoutError:
        print("Request timed out.")
    except aiohttp.ClientError as e:
        print(f"AIOHTTP client error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

    return None

async def fetch_url_str(url: str) -> str:
    """Asynchronously fetches text content from a URL."""
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()
async def fetch_image_from_url(url: str) -> Image.Image:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            image_bytes = await resp.read()
            return Image.open(BytesIO(image_bytes))

async def AfterMin(minutes: int) -> str:
    """Asynchronously calculates a time string after a given number of minutes."""
    def _calc():
        utc_now = datetime.utcnow()
        ist = pytz.timezone('Asia/Kolkata')
        ist_now = utc_now.replace(tzinfo=pytz.utc).astimezone(ist)
        ist_future = ist_now + timedelta(minutes=minutes)
        hour = ist_future.hour % 12 or 12
        am_pm = "Aᴍ" if ist_future.hour < 12 else "Pᴍ"
        return f"<code>{hour:02}</code><b>:</b><code>{ist_future.minute:02}</code> <code>{am_pm}</code>"
    return await asyncio.to_thread(_calc)

async def handle_redis_exceptions(func):
    """Decorator: Asynchronously catches and logs exceptions from Redis operations."""
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Error in {func.__name__}: {e}")
            return {'response': False, 'error': str(e)}
    return wrapper

async def encode_base62(num: int) -> str:
    """Asynchronously performs Base62 encoding using NumPy."""
    def _encode():
        if num == 0:
            return BASE62_ALPHABET[0]
        encoded_chars = []
        n = num
        while n:
            n, rem = divmod(n, 62)
            encoded_chars.append(BASE62_ALPHABET[rem])
        return ''.join(encoded_chars[::-1])
    return await asyncio.to_thread(_encode)

async def decode_base62(encoded: str) -> int:
    """Asynchronously performs Base62 decoding using a dictionary lookup."""
    def _decode():
        num = 0
        for char in encoded:
            num = num * 62 + BASE62_LOOKUP[char]
        return num
    return await asyncio.to_thread(_decode)

# Base62 characters and lookup dictionary
BASE62_ALPHABET = np.array(list(string.digits + string.ascii_letters), dtype='<U1')
BASE62_LOOKUP = {char: idx for idx, char in enumerate(BASE62_ALPHABET)}
