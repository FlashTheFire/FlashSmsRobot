import asyncio
import json
import logging
import os
import aiofiles
import aiohttp
import io

import redis.asyncio as redis
from telebot.async_telebot import AsyncTeleBot

# === Configuration ===
ELASTICACHE_REDIS_URL = "redis://localhost:6379/0"  # Change if needed (AWS or local)
TELEGRAM_BOT_TOKEN = "7128013478:AAGwmYGSGSbyEAYnySG8nh6TAAC6fxHHJho"
ADMIN_CHAT_ID = "5716978793"  # Admin chat identifier (int or str)
DUMP_FILE = "redis_dump.json"
DUMP_INTERVAL = 600  # 10 minutes in seconds

# Set this flag to True to skip keys that start with "service_data:"
FILTER_SERVICE_DATA = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


async def load_old_data(filename: str) -> dict:
    """Load previous dump data from file, if it exists."""
    if not os.path.exists(filename):
        return {}
    try:
        async with aiofiles.open(filename, "r") as f:
            content = await f.read()
            return json.loads(content)
    except Exception as e:
        logging.error(f"Error loading old data from {filename}: {e}")
        return {}


async def dump_redis_data(redis_url: str) -> dict:
    """
    Uses an asynchronous SCAN loop to dump all keys and their values.
    Skips keys that are already present in the old dump.
    If FILTER_SERVICE_DATA is True, skips keys that start with "service_data:".
    Supports types: string, list, set, hash, zset, and ReJSON (ReJSON-RL).
    """
    old_data = await load_old_data(DUMP_FILE)
    r = redis.from_url(redis_url, decode_responses=True)
    data = dict(old_data)  # start with cached data
    cursor = 0
    try:
        while True:
            cursor, keys = await r.scan(cursor=cursor, count=100)
            if keys:
                for key in keys:
                    if key in old_data:
                        continue
                    if FILTER_SERVICE_DATA and key.startswith("service_data:"):
                        continue
                    try:
                        key_type = await r.type(key)
                        if key_type == "string":
                            value = await r.get(key)
                        elif key_type == "list":
                            value = await r.lrange(key, 0, -1)
                        elif key_type == "set":
                            value = list(await r.smembers(key))
                        elif key_type == "hash":
                            value = await r.hgetall(key)
                        elif key_type == "zset":
                            value = await r.zrange(key, 0, -1, withscores=True)
                        elif key_type == "ReJSON-RL":
                            # For ReJSON keys, use JSON.GET command.
                            value = await r.execute_command("JSON.GET", key)
                        else:
                            logging.warning(f"Unsupported key type for key {key}: {key_type}. Skipping.")
                            continue
                        data[key] = {"type": key_type, "value": value}
                    except Exception as ex:
                        logging.error(f"Error reading key {key}: {ex}")
            if cursor == 0:
                break
    except Exception as e:
        logging.error(f"Error during dump: {e}")
    await r.aclose()  # Use aclose() to avoid deprecation warnings.
    logging.info(f"Dumped {len(data)} keys from Redis.")
    return data


async def save_data_to_file(data: dict, filename: str) -> None:
    """Asynchronously writes the data dictionary to a JSON file."""
    try:
        async with aiofiles.open(filename, "w") as f:
            await f.write(json.dumps(data, indent=2))
        logging.info(f"Saved data to file {filename}")
    except Exception as e:
        logging.error(f"Error writing file {filename}: {e}")


async def upload_file(filename: str) -> str:
    """
    Uploads the file to 0x0.st (a free file transfer service) and returns the URL.
    Uses an asynchronous HTTP POST with multipart/form-data.
    """
    upload_url = "https://0x0.st"
    try:
        # Read the file data asynchronously
        async with aiofiles.open(filename, "rb") as f:
            file_data = await f.read()
        # Prepare form data with an in-memory file
        data = aiohttp.FormData()
        data.add_field("file", io.BytesIO(file_data), filename=os.path.basename(filename))
        async with aiohttp.ClientSession() as session:
            async with session.post(upload_url, data=data) as response:
                if response.status == 200:
                    file_url = (await response.text()).strip()
                    logging.info(f"Uploaded file. URL: {file_url}")
                    return file_url
                else:
                    logging.error(f"Failed to upload file. Status code: {response.status}")
                    return ""
    except Exception as e:
        logging.error(f"Error uploading file: {e}")
        return ""


async def send_dump_link(bot: AsyncTeleBot, chat_id, file_url: str) -> None:
    """Sends the file URL via Telegram to the specified admin chat."""
    try:
        message = f"Redis dump available at:\n{file_url}"
        await bot.send_message(chat_id, message)
        logging.info("Sent file URL to Telegram admin.")
    except Exception as e:
        logging.error(f"Error sending message via Telegram: {e}")


async def main_loop():
    """
    Main loop: Dumps Redis data (skipping already dumped keys and service_data:* keys if enabled),
    saves to file, uploads the file to 0x0.st, sends the download URL via Telegram,
    and then sleeps for DUMP_INTERVAL seconds before repeating.
    """
    bot = AsyncTeleBot(TELEGRAM_BOT_TOKEN)
    while True:
        logging.info("Starting dump process from Redis...")
        data = await dump_redis_data(ELASTICACHE_REDIS_URL)
        if data:
            await save_data_to_file(data, DUMP_FILE)
            file_url = await upload_file(DUMP_FILE)
            if file_url:
                await send_dump_link(bot, ADMIN_CHAT_ID, file_url)
            else:
                logging.error("Upload failed; no URL to send.")
        else:
            logging.error("No data dumped; skipping file upload and send.")
        logging.info(f"Sleeping for {DUMP_INTERVAL} seconds until next dump.")
        await asyncio.sleep(DUMP_INTERVAL)

if __name__ == '__main__':
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logging.info("Process interrupted by user. Exiting...")























'''import asyncio
import cloudinary
import cloudinary.uploader
import cloudinary.api

# Configure Cloudinary with your credentials.
cloudinary.config(
    cloud_name="djfsvvzto",
    api_key="291392939686751",
    api_secret="t5YvGkbk7ez71mzMS-3ZZoBFlFQ"
)

def convert_svg_to_png_upload(svg_url: str) -> str:
    """
    Converts an SVG URL into a PNG URL using Cloudinary's upload API.
    """
    # Upload the SVG from the provided URL to Cloudinary.
    result = cloudinary.uploader.upload(svg_url, resource_type="image", overwrite=True)
    # Build the PNG URL from the uploaded image using a transformation.
    png_url = cloudinary.CloudinaryImage(result["public_id"]).build_url(format="png")
    return png_url

def emoji_to_country_code(flag_emoji: str) -> str:
    """
    Converts a flag emoji into its corresponding two-letter country code.
    Example: '🇷🇺' -> 'ru'
    """
    return ''.join(chr(ord(c) - 127397) for c in flag_emoji).lower()

class CountryFlagUpdater:
    def __init__(self, redis_client):
        self.redis_client = redis_client

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

    async def update_flag_urls(self):
        """Convert each country's flag SVG to a PNG URL and update Redis with the new key 'flag_url'."""
        country_data = await self.get_country_data()
        for key, val in country_data.items():
            flag_emoji = val.get("country_code")
            if not flag_emoji:
                continue
            # Convert flag emoji to country code (e.g. 🇷🇺 -> 'ru')
            country_code = emoji_to_country_code(flag_emoji)
            # Construct the SVG URL based on the country code.
            svg_url = f"https://hatscripts.github.io/circle-flags/flags/{country_code}.svg"
            try:
                png_url = convert_svg_to_png_upload(svg_url)
                # Update the in-memory dictionary.
                val["flag_url"] = png_url
                # Update the Redis JSON document.
                # This sets the "flag_url" field for the specific country record.
                await self.redis_client.json().set('main_data:details:country_data', f'.{key}.flag_url', png_url)
                print(f"Updated country {key} with flag URL: {png_url}")
            except Exception as e:
                print(f"Error converting flag for country {key}: {e}")

# --------------------------
# Example usage:
# Assuming you have an async Redis client instance (e.g., using redis.asyncio)
#
from redis.asyncio import Redis
#
async def main():
    redis_client = Redis(host='localhost', port=6379, decode_responses=True)
    updater = CountryFlagUpdater(redis_client)
    await updater.update_flag_urls()
#
asyncio.run(main())
# --------------------------
'''


'''"<b>🔐 Sᴇᴄᴜʀɪᴛʏ Sᴛᴀᴛᴜs:</b>\n"
                f"<code>├</code> 🛡 Sʏsᴛᴇᴍ Sᴛᴀᴛᴜs: <code>{stats.get('system_status')}</code>\n"
                f"<code>├</code> 📈 Rᴀᴛᴇ Lɪᴍɪᴛᴇʀ: <code>{stats.get('rate_limit')}</code>\n"
                f"<code>└</code> 📡 Aᴘɪ Sᴛᴀᴛᴜs: <code>{stats.get('api_status')}</code>\n\n"'''



'''from datetime import datetime, timedelta

async def _calculate_remaining_time(self, created_at: str, timeout: int) -> str:
    try:
        timeout = int(timeout)
        created_at_dt = datetime.fromisoformat(created_at).replace(tzinfo=None)
        elapsed = (datetime.utcnow() - created_at_dt).total_seconds()
        remaining = max(0, timeout * 60 - elapsed)
        mins, secs = divmod(int(remaining), 60)
        r = f"<code>{mins:02}</code><code>:</code><code>{secs:02}</code>"
        print(r)
        return r
    except Exception:
        return "<code>--</code><code>:</code><code>--</code>"

if __name__ == '__main__':
    import asyncio
    
    async def main():
        remaining = await _calculate_remaining_time(None, "2025-03-06T15:06:32.095489", "10")
        print(f"Remaining time: {remaining}")

    asyncio.run(main())

def get_country_flag_link(flag_emoji: str, size: int = 80) -> str:
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
    url = f"https://flagcdn.com/w{size}/{country_code}.png"
    return url

# Example usage:
flag_url = get_country_flag_link("🇮🇳", size=320)
print(flag_url)  # Outputs: https://flagcdn.com/w80/in.png



async def _handle_app_id_inline(self, inline_query):
        try:
            app_id = inline_query.query.split('#AᴘᴘIᴅ:')[1].translate(await large_caps())
            data = await self.fetch_server_data(self.redis_client, app_id)
            if not data or not data.get("servers"):
                await self.bot.answer_inline_query(inline_query.id, [])
                return
            
            servers = data["servers"]
            print(json.dumps(data, indent=4))
            sorted_servers = sorted(servers.items(), key=lambda x: float(x[1]["min_price"]))

            results = []
            for srv_id, info in sorted_servers:
                countries = info["countries"]
                country_display = [code for i, code in enumerate(countries) if i < 3]
                if len(countries) > 3:
                    country_display.append("...")
                button_text = (
                    f"Sᴇʀᴠᴇʀ{str(srv_id)} ➨ "
                    f"[{', '.join(country_display)}] » 💎 {info['min_price']:.2f}"
                )
                results.append(InlineQueryResultArticle(
                    id=str(srv_id),
                    title=button_text,
                    input_message_content=InputTextMessageContent(
                        f"<b>⦿ Sᴇʀᴠɪᴄᴇ ❯</b> {data['app_name'].translate(await small_caps())}\n\n"
                        f"<b>↓ Cʜᴏᴏsᴇ Sᴇʀᴠᴇʀ Bᴇʟᴏᴡ</b>"
                    ),
                    description=country_display[0],
                    thumbnail_url="https://i.postimg.cc/cCrHr3TQ/1000011838-removebg.png"
                ))
            await self.bot.answer_inline_query(inline_query.id, results, cache_time=1)
        except Exception as e:
            print(f"Error in _handle_app_id_inline: {e}")
            await self.bot.answer_inline_query(inline_query.id, [])

'''































































































"""import os
import sys
import asyncio
import aiohttp
import base64
from io import BytesIO
from PIL import Image, ImageDraw, ImageFont
from pilmoji import Pilmoji

IMGBB_API_KEY = "530530e324408b15858555c78a657a96"  # Replace with your actual API key if needed

async def load_image_from_url(url: str, session: aiohttp.ClientSession) -> Image.Image:
    '''"""'''
    Fetch the image from a URL asynchronously and return a PIL Image in RGBA mode.
    '''"""'''
    async with session.get(url) as response:
        if response.status != 200:
            raise Exception(f"Error fetching image from URL {url}: status {response.status}")
        data = await response.read()
    def _open_image() -> Image.Image:
        return Image.open(BytesIO(data)).convert("RGBA")
    return await asyncio.to_thread(_open_image)

async def upload_image_to_imgbb(img: Image.Image, api_key: str, session: aiohttp.ClientSession) -> str:
    '''"""'''
    Upload the given PIL Image to imgbb and return the direct link.
    '''"""'''
    buffer = BytesIO()
    # Save image in PNG format with optimization for speed/size
    img.save(buffer, format="PNG", optimize=True, compress_level=1)
    buffer.seek(0)
    encoded_image = base64.b64encode(buffer.getvalue()).decode("utf-8")
    url = "https://api.imgbb.com/1/upload"
    data = {
        "key": api_key,
        "image": encoded_image
    }
    async with session.post(url, data=data) as response:
        if response.status != 200:
            raise Exception(f"Error uploading to imgbb: status {response.status}")
        json_data = await response.json()
        return json_data["data"]["url"]

async def render_emoji(emoji_text: str, font: ImageFont.FreeTypeFont, size: int) -> Image.Image:
    '''"""'''
    Create a transparent image and render the emoji onto it using Pilmoji.
    '''"""'''
    emoji_img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    def draw_emoji():
        with Pilmoji(emoji_img) as pilmoji:
            pilmoji.text((0, 0), emoji_text, font=font, fill=(255, 255, 255, 255))
        return emoji_img
    return await asyncio.to_thread(draw_emoji)

async def process_image(bg_url: str, session: aiohttp.ClientSession) -> str:
    '''"""'''
    Process one background image:
    - Download the background image,
    - Dynamically compute the emoji size and margins,
    - Render the emoji,
    - Paste the emoji on the background,
    - Upload the composited image to imgbb,
    - Return the direct link.
    '''"""'''
    bg = await load_image_from_url(bg_url, session)
    bg_width, bg_height = bg.size

    # Dynamically scale the emoji size (40% of the smaller background dimension)
    scale_fraction = 0.35
    smaller_dim = min(bg_width, bg_height)
    emoji_size = int(smaller_dim * scale_fraction)
    if emoji_size < 10:
        emoji_size = 10

    # Dynamic margins: 4% of width and 5% of height
    margin_x = int(bg_width * 0.04)
    margin_y = int(bg_height * 0.05)

    emoji_text = "🇮🇳"  # Indian flag (two Unicode code points)

    # Select appropriate emoji font (Windows: Segoe UI Emoji; otherwise, use Noto Color Emoji)
    if os.name == "nt":
        windir = os.environ.get("WINDIR", "C:\\Windows")
        font_candidate = os.path.join(windir, "Fonts", "seguiemj.ttf")
        if os.path.exists(font_candidate):
            font_path = font_candidate
        else:
            raise Exception("Error: Segoe UI Emoji font not found in Windows Fonts directory.")
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        font_path = os.path.join(script_dir, "NotoColorEmoji.ttf")
        if not os.path.exists(font_path):
            raise Exception(f"Error: Noto Color Emoji font not found at {font_path}")

    try:
        font = ImageFont.truetype(font_path, emoji_size)
    except Exception as e:
        raise Exception(f"Error loading font: {e}")

    emoji_img = await render_emoji(emoji_text, font, emoji_size)

    # Position emoji in the top-right corner with dynamic margins
    pos_x = bg_width - emoji_size - margin_x
    pos_y = margin_y
    pos_x = max(pos_x, 0)
    pos_y = max(pos_y, 0)

    bg.paste(emoji_img, (pos_x, pos_y), emoji_img)

    direct_link = await upload_image_to_imgbb(bg, IMGBB_API_KEY, session)
    return direct_link

async def main():
    # List of background image URLs to process concurrently
    bg_urls = [
        "https://udayscripts.in/image/service/tg.png",
        # You can add more URLs here to process in parallel.
    ]
    async with aiohttp.ClientSession() as session:
        tasks = [process_image(url, session) for url in bg_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                print("Error processing image:", result)
            else:
                print("Direct link to composited image:", result)

if __name__ == "__main__":
    asyncio.run(main())



"""
















































































































































































































'''import asyncio
import logging
import json
import base64
import time
from pathlib import Path
import aiohttp
from redis.asyncio import Redis

# Constants
IMG_DIR = Path("bot_project/images/service")  # Directory containing images
UPLOAD_URL = "https://api.imgbb.com/1/upload"  # Image upload endpoint
APP_DATA_PREFIX = "app:image"  # Redis key prefix
API_KEY = "530530e324408b15858555c78a657a96"  # Replace with your actual API key

# Redis connection details
REDIS_HOST = "redis-16106.c305.ap-south-1-1.ec2.redns.redis-cloud.com"
REDIS_PORT = 16106
REDIS_PASSWORD = "dW6AGa56NCFa5c4CnTwkStIfv126TsYA"

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("upload_images.log")
    ]
)

def encode_image(image_path: Path) -> str:
    """Encodes the image to a base64 string."""
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode("utf-8")

async def upload_image(redis_manager: Redis, session: aiohttp.ClientSession, image_path: Path, custom_name: str):
    """Uploads an image to the server and saves its data to Redis."""
    retries = 5  # Retry on failure
    backoff = 2  # Initial backoff time in seconds
    for attempt in range(1, retries + 1):
        try:
            logging.debug(f"Attempt {attempt} - Uploading image: {image_path} with custom name: {custom_name}")
            encoded_image = encode_image(image_path)
            payload = {
                "key": API_KEY,
                "image": encoded_image,
                "name": custom_name
            }

            async with session.post(UPLOAD_URL, data=payload) as response:
                logging.debug(f"Received response status: {response.status} for image: {image_path}")
                if response.status == 200:
                    data = await response.json()
                    if "data" in data and "url" in data["data"]:
                        record = {
                            "name": custom_name,
                            "url": data["data"]["url"],
                            "delete_url": data["data"].get("delete_url")
                        }
                        await redis_manager.hset(APP_DATA_PREFIX, custom_name, json.dumps(record))
                        logging.info(f"Successfully uploaded and saved image '{custom_name}' to Redis.")
                        return data
                    else:
                        logging.error(f"Unexpected response format for image {image_path}: {data}")
                        return {"error": "Unexpected response format"}
                else:
                    error_message = await response.text()
                    logging.error(f"Failed to upload {image_path}. Status: {response.status}, Error: {error_message}")
                    return {"error": error_message}
        except Exception as e:
            logging.error(f"Error during upload of {image_path}: {e}")
            if attempt < retries:
                logging.info(f"Retrying upload for {image_path}. Attempt {attempt + 1}/{retries}.")
                await asyncio.sleep(backoff)
                backoff *= 2
            else:
                logging.error(f"Failed to upload {image_path} after {retries} attempts.")
                return {"error": str(e)}

async def upload_images_from_directory(redis_manager: Redis, directory: Path):
    """Uploads all images from the specified directory and saves URLs to Redis."""
    if not directory.exists() or not directory.is_dir():
        logging.error(f"Invalid directory: {directory}")
        return

    tasks = []
    async with aiohttp.ClientSession() as session:
        for file_name in directory.iterdir():
            if file_name.suffix.lower() in (".png", ".jpg", ".jpeg", ".gif", ".bmp"):
                custom_name = file_name.stem
                logging.debug(f"Adding task for image: {file_name}")
                tasks.append(upload_image(redis_manager, session, file_name, custom_name))
                
                # Add a small delay between each upload to avoid hitting rate limit
                await asyncio.sleep(0.2)  # Increase delay if necessary

        logging.info(f"Starting upload for {len(tasks)} images.")
        await asyncio.gather(*tasks, return_exceptions=True)

        # Add a longer delay after all tasks to ensure compliance with rate limits
        await asyncio.sleep(5)  # Adjust as necessary

async def get_redis_client():
    """Returns a Redis client instance."""
    return Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True,
    )

async def main():
    """Main function to execute the image upload from directory to Redis."""
    redis_manager = await get_redis_client()  # Get Redis client
    directory = IMG_DIR  # Set the directory path where your images are stored
    
    # Log start time
    start_time = time.time()
    logging.info("Starting image upload process.")
    
    # Start uploading images
    await upload_images_from_directory(redis_manager, directory)
    
    # Log total time taken
    end_time = time.time()
    logging.info(f"Completed image upload process in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.critical(f"Critical error in image upload process: {e}")



'''


































































'''import random

# Predefined list of countries
countries = ["india", "germany", "usa", "uk", "france", "brazil", "canada", "china", "australia", "japan"]
# Predefined list of sample keywords
keywords = ["tech", "sports", "clothing", "rummy", "games", "electronics", "books", "furniture", "shoes", "food", "health"]

# Generate random sample data
sample_data = []
for i in range(3, 53):  # IDs from 3 to 52
    sample_data.append({
        "id": str(i),
        "name": f"Item{i}",
        "value": random.sample(keywords, k=random.randint(1, 3)),  # Randomly select 1 to 3 keywords
        "total_purchased": random.randint(100, 10000),
        "total_available": random.randint(50, 1000),
        "starting_price": round(random.uniform(10, 100), 2),
        "country": random.choice(countries)
    })

print(len(sample_data), sample_data[:50])  # Display the number of entries and the first 5 entries




'''












































































































































































































'''import redis
import logging
from datetime import datetime
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Initialize Redis client
redis_client = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)

# Utility functions
def add_or_update_user(user_id, data):
    """Add a new user or update existing user data."""
    user_key = f"user:{user_id}"
    
    # Update user fields
    for key, value in data.items():
        if key in {"balance", "total_numbers_purchased", "total_spend", "total_deposit_amount"}:
            redis_client.hincrbyfloat(user_key, key, float(value))
        else:
            redis_client.hset(user_key, key, value)

    logging.info(f"User {user_id} added/updated with data: {data}")

def add_order(order_id, data):
    """Add a new order."""
    order_key = f"order:{order_id}"
    history_key = f"{order_key}:history"
    user_orders_key = f"user:{data.get('user_id')}:orders"
    datetime_orders_key = "orders_by_datetime"

    # Store order details
    for key, value in data.items():
        if key != "history":
            redis_client.hset(order_key, key, value)
    
    # Add history
    if "history" in data:
        for event in data["history"]:
            redis_client.rpush(history_key, json.dumps(event))
    
    # Add to user-specific and datetime-specific indexes
    if "user_id" in data:
        redis_client.sadd(user_orders_key, order_id)
    if "datetime" in data:
        redis_client.zadd(datetime_orders_key, {order_id: datetime.strptime(data["datetime"], '%Y-%m-%d %H:%M:%S.%f').timestamp()})

    logging.info(f"Order {order_id} added with data: {data}")

def update_order_status(order_id, status, history_event):
    """Update order status and add a history event."""
    order_key = f"order:{order_id}"
    history_key = f"{order_key}:history"
    
    redis_client.hset(order_key, "status", status)
    redis_client.rpush(history_key, json.dumps(history_event))

    logging.info(f"Order {order_id} status updated to '{status}' with history event: {history_event}")

def add_deposit(deposit_id, data):
    """Add a new deposit."""
    deposit_key = f"deposit:{deposit_id}"
    history_key = f"{deposit_key}:history"

    # Store deposit details
    for key, value in data.items():
        if key != "history":
            redis_client.hset(deposit_key, key, value)
    
    # Add history
    if "history" in data:
        for event in data["history"]:
            redis_client.rpush(history_key, json.dumps(event))

    logging.info(f"Deposit {deposit_id} added with data: {data}")

# Retrieval functions
def get_orders_by_user(user_id):
    """Retrieve all order IDs for a specific user."""
    user_orders_key = f"user:{user_id}:orders"
    order_ids = redis_client.smembers(user_orders_key)
    logging.info(f"Orders for user {user_id}: {order_ids}")
    return list(order_ids)

def get_orders_by_datetime(start_datetime, end_datetime):
    """Retrieve all order IDs within a datetime range."""
    datetime_orders_key = "orders_by_datetime"
    start_timestamp = datetime.strptime(start_datetime, '%Y-%m-%d %H:%M:%S').timestamp()
    end_timestamp = datetime.strptime(end_datetime, '%Y-%m-%d %H:%M:%S').timestamp()
    order_ids = redis_client.zrangebyscore(datetime_orders_key, start_timestamp, end_timestamp)
    logging.info(f"Orders from {start_datetime} to {end_datetime}: {order_ids}")
    return order_ids

# Example usage
sample_user = {
    "user_id": 123456,
    "username": "John Doe",
    "balance": 50.0,
    "total_numbers_purchased": 5,
    "total_spend": 20.0,
    "total_deposit_amount": 70.0,
    "user_forum_id": 7890,
    "last_purchase_time": str(datetime.utcnow())
}
add_or_update_user(sample_user["user_id"], sample_user)

sample_order = {
    "user_id": 123456,  # Include user ID for indexing
    "number": "+91 9363931970",
    "message_id": 6960,
    "sms": "WAITING, 415256",
    "button_text": "buy_2 ttn india 4.20 tataneu",
    "status": "WAITING",
    "datetime": str(datetime.utcnow()),
    "amount": 4.20,
    "server": "2",
    "history": [
        {"datetime": str(datetime.utcnow()), "action": "ORDER_CREATED"},
        {"datetime": str(datetime.utcnow()), "action": "SMS_RECEIVED: 415256"}
    ]
}
add_order(1, sample_order)

# Update order status
update_order_status(1, "CONFIRMED", {"datetime": str(datetime.utcnow()), "action": "ORDER_CONFIRMED"})

# Retrieve orders by user
get_orders_by_user(123456)

# Retrieve orders by datetime range
start_time = "2024-12-01 00:00:00"
end_time = "2024-12-02 23:59:59"
get_orders_by_datetime(start_time, end_time)
'''