#!/usr/bin/env python3
import os
import sys
import asyncio
import functools
import contextlib

from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import logging
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
from utils.config import BOT_TOKEN as BotToken
import redis.asyncio as redis
from redis.commands.search.query import Query
import pytz
import string
import numpy as np
import qrcode

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
            return BytesIO(await image_response.read())

async def qr_code(
    deposit_id: str,
    size: int,
    position: Tuple[int, int],
    radius: int,
    rect_img_path: str = r"C:\Users\LOQ\OneDrive\Desktop\Coding-Flash\flash_sms\bot_project\images\general\deposit-inr-qr_code.jpeg"
) -> BytesIO:
    """Asynchronously generates and overlays a QR code on an image, returning it as BytesIO."""
    qr_img_bytes, rect_img = await asyncio.gather(
        fetch_qr(deposit_id),
        asyncio.to_thread(Image.open, rect_img_path)
    )
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
def convert_usd_to_rub(amount_usd, exchange_rate=100, tax_rate=0.10):
    return round((amount_usd * exchange_rate) * (1 + tax_rate) + 0.005, 8)
def convert_rub_to_usd(amount_rub, exchange_rate=100, tax_rate=0.10):
    return round((amount_rub / (exchange_rate * (1 + tax_rate))) + 0.005, 8)







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

async def get_tg_profile_photo(user_id: int) -> Dict[str, Any]:
    """Asynchronously retrieves the Telegram profile photo for a user."""
    def _exists(path: str) -> bool:
        return os.path.exists(path)
    file_name = f"bot_project/images/profile/{user_id}.png"
    if await asyncio.to_thread(_exists, file_name):
        logging.debug(f"Profile image already exists for user {user_id}: {file_name}")
        return {'response': True, 'result': file_name}
    
    try:
        url = f'https://api.telegram.org/bot{BotToken}/getUserProfilePhotos'
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params={'user_id': user_id, 'limit': 1}) as response:
                data = await response.json()
                if data['ok'] and data['result']['total_count'] > 0:
                    file_id = data['result']['photos'][0][0]['file_id']
                    file_url = f'https://api.telegram.org/bot{BotToken}/getFile'
                    async with session.get(file_url, params={'file_id': file_id}) as file_response:
                        file_data = await file_response.json()
                        if file_data['ok']:
                            image_url = f"https://api.telegram.org/file/bot{BotToken}/{file_data['result']['file_path']}"
                            result = await save_image(f'{user_id}.png', image_url, "bot_project/images/profile")
                            if result.get('response'):
                                logging.info(f"Profile image saved for user {user_id}: {result.get('result')}")
                                return {'response': True, 'result': result.get('result')}
                            return {'response': True, 'result': result.get("result")}
        return {'response': False, 'error': "No profile photo found."}
    except Exception as e:
        logging.error(f"Error getting profile image: {e}")
        return {'response': False, 'error': str(e)}

async def save_image(file_name: str, image_url: str, save_directory: str) -> Dict[str, Any]:
    """Asynchronously downloads and saves an image from a URL."""
    try:
        logging.debug(f"Saving image to directory: {save_directory}, file: {file_name}")
        if image_url:
            async with aiohttp.ClientSession() as session:
                async with session.get(image_url) as response:
                    response.raise_for_status()
                    img_data = await response.read()
                    img = await asyncio.to_thread(Image.open, BytesIO(img_data))
                    if not os.path.exists(save_directory):
                        await asyncio.to_thread(os.makedirs, save_directory)
                    save_path = os.path.join(save_directory, file_name)
                    await asyncio.to_thread(img.save, save_path, 'PNG')
                    logging.info(f"Image saved at: {save_path}")
                    return {'response': True, 'result': save_path}
        else:
            return {'response': False}
    except Exception as e:
        logging.error(f"Error saving profile image: {e}")
        return {'response': False}

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


async def get_sms_text_by_code(order_id: str, sms: str) -> Optional[str]:
    """Asynchronously retrieves SMS text by code from an API."""
    from utils.api import FIVESIM  # Import FIVESIM key if necessary
    url = f"https://5sim.net/v1/user/check/{order_id}"
    headers = {
        "Authorization": f"Bearer {FIVESIM}",
        "Accept": "application/json"
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                for code in data.get('sms', []):
                    if code.get('code') == sms:
                        return code.get('text')
    return None

async def fetch_url_str(url: str) -> str:
    """Asynchronously fetches text content from a URL."""
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()

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
