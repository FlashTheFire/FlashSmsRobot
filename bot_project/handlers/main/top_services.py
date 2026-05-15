from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto, CallbackQuery
from handlers.security import RateLimiter
from utils.redis_manager import redis_manager
from handlers.manager.operation import UserManagement
from utils.functions import small_caps
from utils.config import ADMIN_ID, APP_IMAGE_LIST, CHANNEL_ID
import asyncio
import time
import os
import random
from PIL import Image, ImageDraw, ImageFont
import logging
import aiohttp
from io import BytesIO
from typing import Optional, Dict, List, Tuple
import traceback
from functools import partial
from utils.redis_keys import RedisKeys
from handlers.security import RateLimiter, InputValidator, TransactionGuard


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('TopServiceManager')

class TopServiceManager:
    """Manager class for handling Top Service leaderboard functionality."""
    
    def __init__(self):
        self.user_manager: Optional[UserManagement] = None
        self._initialized = False
        self.base_path = "bot_project/images/"
        self.font_path = "bot_project/fonts/NewtonHowardFont.ttf"
        self.bot: Optional[AsyncTeleBot] = None
        self.default_logo = os.path.join(self.base_path, "general/default_service.png")
        self.redis_client = None
        self._update_task = None
        self.LEADERBOARD_KEY = "image_data:leaderboard-file_id"
        self.UPDATE_INTERVAL = 60   # 10 minutes in seconds
        self.ADMIN_CHAT_ID = CHANNEL_ID  # Replace with your admin channel/group ID
        #logger.info("TopServiceManager initialized")
    
    @staticmethod
    async def create_rounded_icon(image: Image.Image, radius: int) -> Image.Image:
        """Create rounded icon with specified radius asynchronously."""
        mask = Image.new("L", image.size, 0)
        draw = ImageDraw.Draw(mask)
        draw.rounded_rectangle([0, 0, *image.size], radius=radius, fill=255)
        rounded = Image.new("RGBA", image.size)
        rounded.paste(image, mask=mask)
        return rounded
    async def fetch_service_data(self) -> List[Tuple[str, int, str, str]]:
        """Fetch service data from Redis and sort by purchase count."""
        try:
            service_key = 'main_data:details:service_data'
            # Initialize key if it doesn't exist
            if not await self.redis_client.exists(service_key):
                await self.redis_client.json().set(service_key, '$', {})
                #logger.info(f"Initialized empty {service_key}")

            service_data = await self.redis_client.json().get(service_key)
            
            if not service_data:
                # logger.debug("No service data found in Redis (leaderboard is empty)")
                return []
                
            services = []
            for service_id, data in service_data.items():
                try:
                    service_name = data.get('service_name', 'Unknown')
                    purchased = int(data.get('purchased', 0))  # Ensure integer
                    logo_url = str(data.get('logo_url', ''))  # Ensure string
                    server_id = str(data.get('server_id', ''))  # Ensure string
                    country_url = str(data.get('country_url', ''))  # Ensure string
                    service_code = str(data.get('service_code', ''))  # Ensure string
                    country_id = str(data.get('country_id', ''))  # Ensure string
                    app_id = str(data.get('app_id', ''))  # Ensure string
                    services.append((service_name, purchased, logo_url, country_url, service_code, server_id, country_id, app_id))
                    #logger.debug(f"Processed service {service_id}: {service_name}")
                except (ValueError, TypeError) as e:
                    logger.warning("Error processing service %s: %s", service_id, e)
                    continue
            
            sorted_services = sorted(services, key=lambda x: x[1], reverse=True)
            #logger.info(f"Successfully fetched and sorted {len(sorted_services)} services")
            return sorted_services
        except Exception as e:
            #logger.error(f"Error fetching service data: {e}\n{traceback.format_exc()}")
            return []

    async def download_image(self, url: str) -> Optional[Image.Image]:
        """Download image from URL and return as PIL Image."""
        try:
            #logger.debug(f"Downloading image from {url}")
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.read()
                        return Image.open(BytesIO(data))
                    else:
                        logger.warning("Failed to download image from %s – HTTP %s", url, response.status)
                        return None

        except asyncio.TimeoutError:
            logger.warning("Timeout downloading image from %s", url)
            return None
        except Exception as e:
            logger.warning("Error downloading image from %s: %s", url, e)
            return None
    async def get_service_icon(self, logo_url: str) -> Image.Image:
        """Get service icon from URL or return default icon."""
        try:
            if logo_url:
                img = await self.download_image(logo_url)
                if img:
                    #logger.debug(f"Successfully loaded image from {logo_url}")
                    return img
                logger.warning("Failed to load image from %s, using default", logo_url)
            
            if not os.path.exists(self.default_logo):
                #logger.error(f"Default logo not found at {self.default_logo}")
                # Create a blank image as last resort
                return Image.new('RGB', (60, 60), color='gray')
                
            return Image.open(self.default_logo)
        except Exception as e:
            logger.error("Error in get_service_icon: %s", e, exc_info=True)
            return Image.new('RGB', (60, 60), color='gray')

    async def _create_enhanced_leaderboard(self, leaderboard_data: list, output_file: str) -> None:
        """Generate leaderboard image with dynamic data asynchronously."""
        try:
            img_width, img_height = 1920, 1080
            background_image = os.path.join(self.base_path, "general/topservice-background.jpg")
            
            if not os.path.exists(background_image):
                raise FileNotFoundError(f"Background image not found: {background_image}")

            bg_img = await self._load_and_resize_image(background_image, (img_width, img_height))
            overlay = await self._create_gradient_overlay(img_width, img_height)
            bg_img = Image.alpha_composite(bg_img.convert("RGBA"), overlay)
            draw = ImageDraw.Draw(bg_img)
            
            font_title, font_labels = await self._load_fonts()
            await self._draw_title(draw, img_width, font_title)
            
            bar_start_x, bar_start_y = 490, 290
            bar_height, max_bar_width, spacing = 50, 1100, 20
            max_score = max(entry[1] for entry in leaderboard_data) if leaderboard_data else 1

            tasks = []
            for i, (service_name, score, logo_url, country_url, service_code, server_id, country_id, app_id) in enumerate(leaderboard_data):
                if bar_start_y + i * (bar_height + spacing) + bar_height <= img_height - 50:
                    tasks.append(
                        self._draw_leaderboard_entry(
                            draw, bg_img, i, service_name, score, logo_url, country_url, server_id,
                            bar_start_x, bar_start_y + i * (bar_height + spacing),
                            bar_height, max_bar_width, max_score, font_labels
                        )
                    )
            
            await asyncio.gather(*tasks)
            final_image = bg_img.convert("RGB")
            final_image.save(output_file, "JPEG", quality=100)

        except Exception as e:
            #logger.error(f"Error creating leaderboard: {e}\n{traceback.format_exc()}")
            raise
    async def _draw_leaderboard_entry(self, draw: ImageDraw.Draw, bg_img: Image.Image, index: int, 
                                    label: str, score: int, logo_url: str, country_url: str, server_id: int, bar_start_x: int, 
                                    current_y: int, bar_height: int, max_bar_width: int, 
                                    max_score: int, font_labels: ImageFont.ImageFont) -> None:
        """Draw a single leaderboard entry with icon from URL."""
        bar_width = int((score / max_score) * max_bar_width) if max_score != 0 else 0
        
        # Draw shadow and bar
        shadow_rect = [bar_start_x + 8, current_y + 8, bar_start_x + bar_width + 8, current_y + bar_height + 8]
        draw.rounded_rectangle(shadow_rect, radius=20, fill="#000000")
        bar_rect = [bar_start_x, current_y, bar_start_x + bar_width, current_y + bar_height]
        draw.rounded_rectangle(bar_rect, radius=20, fill="#ff6363")
        
        # Get and draw icon
        icon = await self.get_service_icon(logo_url)
        icon = icon.resize((60, 60), Image.Resampling.LANCZOS)
        rounded_icon = await self.create_rounded_icon(icon, 15)
        bg_img.paste(rounded_icon, (bar_start_x - 280, current_y - 2), rounded_icon)

        icon = await self.get_service_icon(country_url)
        icon = icon.resize((48, 48), Image.Resampling.LANCZOS)
        rounded_icon = await self.create_rounded_icon(icon, 15)
        bg_img.paste(rounded_icon, (bar_start_x - 5, current_y + 1), rounded_icon)

        # Draw text
        label = label[:7] + '.' if len(label) > 7 else label

        draw.text((bar_start_x - 210, current_y), label, font=font_labels, fill=(255, 255, 255))
        draw.text((bar_start_x + bar_width + 20, current_y + 2), str(f's-{server_id}'), font=font_labels, fill=(255, 99, 99))

    async def _create_service_keyboard(self, leaderboard_data: list, page: int = 0, items_per_page: int = 12) -> InlineKeyboardMarkup:
        """
        Create an inline keyboard with service buttons for the given page.
        Each page shows up to 12 items (4 rows of 3 buttons).
        Pagination buttons (Previous/Next) are added if needed.
        """
        try:
            keyboard = InlineKeyboardMarkup()
            # Assume small_caps() returns a translation table for str.translate()
            small_caps_table = await small_caps()  # your function to get small caps mapping

            # Calculate slice indexes for the current page
            start_index = page * items_per_page
            end_index = start_index + items_per_page
            page_data = leaderboard_data[start_index:end_index]

            # Build grid of service buttons (3 per row)
            for i in range(0, len(page_data), 3):
                row = []
                for service_name, score, logo_url, country_url, service_code, server_id, country_id, app_id in page_data[i:i+3]:
                    try:
                        # Example transformation: take the first word, remove periods, then translate to small caps.
                        button_text = service_name.split()[0].replace('.', '')
                        if len(button_text) > 9:
                            button_text = button_text[:9] + '.'
                        button_text = button_text.translate(small_caps_table)
                        row.append(
                            InlineKeyboardButton(
                                text=button_text,
                                callback_data= f"leaderboard:{app_id}:{country_id}:{server_id}"
                            )
                        )
                    except Exception:
                        continue
                if row:
                    keyboard.row(*row)

            # Add pagination row if there are multiple pages
            pagination_buttons = []
            total_items = len(leaderboard_data)
            if page > 0:
                pagination_buttons.append(
                    InlineKeyboardButton("⬅️ Previous", callback_data=f"paginate:{page-1}")
                )
            if end_index < total_items:
                pagination_buttons.append(
                    InlineKeyboardButton("Next ➡️", callback_data=f"paginate:{page+1}")
                )
            if pagination_buttons:
                keyboard.row(*pagination_buttons)

            # Always add a final "Back to Home" button
            keyboard.row(
                InlineKeyboardButton("🔙 Bᴀᴄᴋ Tᴏ Hᴏᴍᴇ Pᴀɢᴇ [ Mᴀɪɴ-Mᴇɴᴜ ]", callback_data='start')
            )
            return keyboard

        except Exception:
            # Fallback: return keyboard with only the back button.
            fallback_keyboard = InlineKeyboardMarkup()
            fallback_keyboard.row(
                InlineKeyboardButton("🔙 Bᴀᴄᴋ Tᴏ Hᴏᴍᴇ Pᴀɢᴇ [ Mᴀɪɴ-Mᴇɴᴜ ]", callback_data='start')
            )
            return fallback_keyboard
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
                logger.warning("Error sending transaction-lock message: %s", e)
            return False
        return True

    async def _handle_callback(self, bot: AsyncTeleBot, call: CallbackQuery) -> None:
        """Handle Top Service callback query."""
        try:
            if not self._initialized:
                await bot.answer_callback_query(call.id, "Service unavailable")
                return

            chat_id = call.message.chat.id
            message_id = call.message.message_id
            transaction_key = RedisKeys.transaction_lock_key(chat_id, f"top_service:main")
            async with TransactionGuard(self.redis_client) as guard:
                if not await self._acquire_transaction_lock(guard, transaction_key, call):
                    return
                try:
                    # Try to get cached data first
                    cache_data = await self._get_cached_leaderboard(must_return=True)
                    if cache_data:
                        file_id = cache_data.get("file_id")
                        try:
                            if file_id:
                                await bot.edit_message_media(
                                    media=InputMediaPhoto(media=file_id),
                                    chat_id=chat_id,
                                    message_id=message_id,
                                    reply_markup=cache_data["keyboard"]
                                )
                                return
                        except Exception as e:
                            logger.warning("Error using cached leaderboard data: %s", e)
                except Exception as e:
                    logger.error("Error processing top-service callback: %s", e, exc_info=True)
                    await self.bot.send_message(chat_id, "🚫 Eʀʀᴏʀ Gᴇɴᴇʀᴀᴛɪɴɢ Rᴇǫᴜᴇsᴛ.")
                    return
                finally:
                    await guard.release_lock(transaction_key)

        except Exception as e:
            logger.error("Unhandled error in _handle_callback: %s", e, exc_info=True)
            try:
                await bot.answer_callback_query(call.id, "An error occurred")
            except:
                pass
    async def _handle_callback_page(self, bot: AsyncTeleBot, call: CallbackQuery, page: int) -> None:
        # Retrieve your full service list. Implement get_leaderboard_data() as needed.
        try:
            chat_id = call.message.chat.id
            transaction_key = RedisKeys.transaction_lock_key(chat_id, f"top_service:page{page}")
            async with TransactionGuard(self.redis_client) as guard:
                if not await self._acquire_transaction_lock(guard, transaction_key, call):
                    return
                try:
                    leaderboard_data = await self.fetch_service_data()
                    keyboard = await self._create_service_keyboard(leaderboard_data, page=page)
                    # Edit the message's inline keyboard with the new pagination keyboard.
                    await bot.edit_message_reply_markup(
                            chat_id=chat_id,
                            message_id=call.message.message_id,
                            reply_markup=keyboard
                    )
                except Exception as e:
                    logger.error("Error processing paginate callback: %s", e, exc_info=True)
                    await self.bot.send_message(chat_id, "🚫 Eʀʀᴏʀ Gᴇɴᴇʀᴀᴛɪɴɢ Rᴇǫᴜᴇsᴛ.")
                    return
                finally:
                    await guard.release_lock(transaction_key)
        except Exception as e:
            logger.error("Unhandled error in _handle_callback_page: %s", e, exc_info=True)
            try:
                await bot.answer_callback_query(call.id, "An error occurred")
            except:
                pass

    async def _serialize_keyboard(self, keyboard: InlineKeyboardMarkup) -> list:
        """Serialize InlineKeyboardMarkup to JSON-compatible format."""
        try:
            serialized = []
            for row in keyboard.keyboard:
                serialized_row = []
                for button in row:
                    serialized_row.append({
                        'text': button.text,
                        'callback_data': button.callback_data
                    })
                serialized.append(serialized_row)
            return serialized
        except Exception as e:
            #logger.error(f"Error serializing keyboard: {e}")
            return []
    async def _deserialize_keyboard(self, data: list) -> InlineKeyboardMarkup:
        """Deserialize JSON data back to InlineKeyboardMarkup."""
        try:
            keyboard = InlineKeyboardMarkup()
            for row in data:
                buttons = []
                for button in row:
                    buttons.append(
                        InlineKeyboardButton(
                            text=button['text'],
                            callback_data=button['callback_data']
                        )
                    )
                keyboard.row(*buttons)
            return keyboard
        except Exception as e:
            #logger.error(f"Error deserializing keyboard: {e}")
            # Return a basic keyboard with just the back button as fallback
            keyboard = InlineKeyboardMarkup()
            keyboard.row(InlineKeyboardButton("🔙 Bᴀᴄᴋ Tᴏ Hᴏᴍᴇ Pᴀɢᴇ [ Mᴀɪɴ-Mᴇɴᴜ ]", callback_data='start'))
            return keyboard

    async def _auto_update_leaderboard(self) -> None:
        """Background task to update leaderboard image periodically."""
        last_leaderboard_data = None
        while True:
            try:
                output_file = os.path.join(self.base_path, "current/topservice-current.jpg")
                os.makedirs(os.path.dirname(output_file), exist_ok=True)

                leaderboard_data = await self.fetch_service_data()
                if leaderboard_data:
                    if last_leaderboard_data == leaderboard_data:
                        await asyncio.sleep(self.UPDATE_INTERVAL)
                        continue

                    last_leaderboard_data = leaderboard_data
                    await self._create_enhanced_leaderboard(leaderboard_data, output_file)
                    keyboard = await self._create_service_keyboard(leaderboard_data)
                    
                    # Update file_id with fallback mechanism
                    new_file_id = await self._update_file_id_with_fallback(output_file)
                    
                    # Store file info in Redis with serialized keyboard and file_id
                    cache_data = {
                        "file_path": output_file,
                        "timestamp": int(time.time()),  # wall-clock; survives restarts
                        "keyboard_data": await self._serialize_keyboard(keyboard),
                        "file_id": new_file_id if new_file_id else None,
                        "leaderboard_data": leaderboard_data
                    }
                    await self.redis_client.json().set(self.LEADERBOARD_KEY, '$', cache_data)
                    
                    if new_file_id:
                        pass
                    else:
                        pass
                await asyncio.sleep(self.UPDATE_INTERVAL)
            except asyncio.CancelledError:
                logger.warning("Leaderboard update task cancelled.")
                raise  # allow task cancellation to propagate
            except Exception as e:
                logger.error("Error in automatic leaderboard update: %s", e, exc_info=True)
                await asyncio.sleep(60)
    async def _update_file_id_with_fallback(self, output_file: str) -> Optional[str]:
        """Update file_id using admin channel."""
        if not self.bot:
            return None

        try:
            with open(output_file, 'rb') as media_file:
                result = await self.bot.send_photo(
                    chat_id=self.ADMIN_CHAT_ID,
                    photo=media_file,
                    caption="🔄 Lᴇᴀᴅᴇʀʙᴏᴀʀᴅ Uᴘᴅᴀᴛᴇ..."
                )
                
                if result and result.photo:
                    new_file_id = result.photo[-1].file_id
                    try:
                        await self.bot.delete_message(
                            chat_id=self.ADMIN_CHAT_ID,
                            message_id=result.message_id
                        )
                    except Exception as e:
                        logger.warning("Could not delete leaderboard update message: %s", e)
                        pass
                    
                    return new_file_id
                return None
                
        except Exception as e:
            logger.error("Error updating leaderboard file_id: %s", e, exc_info=True)
            return None

    async def _get_cached_leaderboard(self, must_return: bool = False) -> Optional[Dict]:
        """Get cached leaderboard data if not expired."""
        try:
            cache_data = await self.redis_client.json().get(self.LEADERBOARD_KEY)
            if not cache_data:
                return None
                
            current_time = int(time.time())  # wall-clock; consistent with stored timestamp
            cached_time = cache_data.get("timestamp", 0)
            
            if must_return:
                keyboard_data = cache_data.get("keyboard_data", [])
                keyboard = await self._deserialize_keyboard(keyboard_data)
                return {
                    "timestamp": cached_time,
                    "keyboard": keyboard,
                    "file_id": cache_data.get("file_id")
                }
            if current_time - cached_time <= self.UPDATE_INTERVAL:
                # Deserialize keyboard data
                keyboard_data = cache_data.get("keyboard_data", [])
                keyboard = await self._deserialize_keyboard(keyboard_data)
                return {
                    "timestamp": cached_time,
                    "keyboard": keyboard,
                    "file_id": cache_data.get("file_id")
                }
            return None
        except Exception as e:
            logger.error("Error getting cached leaderboard: %s", e, exc_info=True)
            return None

    async def _load_and_resize_image(self, image_path: str, size: tuple) -> Image.Image:
        return Image.open(image_path).resize(size, Image.Resampling.LANCZOS)
    async def _create_gradient_overlay(self, width: int, height: int) -> Image.Image:
        overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
        draw_overlay = ImageDraw.Draw(overlay)
        for i in range(height):
            draw_overlay.line((0, i, width, i), fill=(0, 0, 0, int(100 * (1 - i / height))))
        return overlay

    async def _load_fonts(self) -> tuple:
        font_title = ImageFont.truetype(self.font_path, 120) if os.path.exists(self.font_path) else ImageFont.load_default()
        font_labels = ImageFont.truetype(self.font_path, 45) if os.path.exists(self.font_path) else ImageFont.load_default()
        return font_title, font_labels
    async def _draw_title(self, draw: ImageDraw.Draw, img_width: int, font_title: ImageFont.ImageFont) -> None:
        title_text = "Top Service"
        title_bbox = draw.textbbox((0, 0), title_text, font=font_title)
        title_w = title_bbox[2] - title_bbox[0]
        draw.text(((img_width - title_w) // 2 + 5, 50 + 5), title_text, font=font_title, fill=(0, 0, 0))
        draw.text(((img_width - title_w) // 2, 50), title_text, font=font_title, fill=(255, 255, 255))
    
    
    async def init_managers(self, user_mgr: UserManagement=None, bot: Optional[AsyncTeleBot] = None) -> bool:
        """Initialize required components asynchronously."""
        try:
            if not user_mgr:
                #logger.error("User manager is required")
                return False

            self.user_manager = user_mgr
            self.bot = bot
            self.redis_client = await redis_manager.get_client()
            
            # Verify Redis connection
            if not await self.redis_client.ping():
                #logger.error("Failed to connect to Redis")
                return False
                
            # Verify admin channel access
            if self.bot:
                try:
                    await self.bot.get_chat(self.ADMIN_CHAT_ID)
                except Exception as e:
                    #logger.error(f"Cannot access admin channel: {e}")
                    return False
                
            self._initialized = True
            #logger.info("TopServiceManager managers initialized successfully")
            
            # Start background update task
            try:
                self._update_task = asyncio.create_task(self._auto_update_leaderboard())
            except asyncio.CancelledError:
                print("Automatic leaderboard update task cancelled")
            except Exception as e:
                print(f"Error starting automatic leaderboard update task: {e}")
            #logger.info("Started automatic leaderboard update task")
            
            return True
        except Exception as e:
            #logger.error(f"Error initializing TopServiceManager: {e}\n{traceback.format_exc()}")
            return False
    async def register_handlers(self, bot: AsyncTeleBot) -> None:
        """Register callback handlers with TeleBot."""
        @bot.callback_query_handler(func=lambda call: call.data.startswith('USER:TOPSERVICE'))
        async def topservice_callback_wrapper(call: CallbackQuery):
            try:
                process_task = partial(self._handle_callback, bot, call)
                asyncio.create_task(process_task())
            except ValueError:
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", show_alert=True))
            except Exception as e:
                #logging.error(f"Callback error: {e}")
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ...", show_alert=True))

        @bot.callback_query_handler(func=lambda call: call.data.startswith('paginate:'))
        async def paginate_callback_handler(call: CallbackQuery):
            try:
                page = int(call.data.split(':')[1])
            except (IndexError, ValueError):
                page = 0
            try:
                process_task = partial(self._handle_callback_page, bot, call, page)
                asyncio.create_task(process_task())
            except ValueError:
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", show_alert=True))
            except Exception as e:
                #logging.error(f"Callback error: {e}")
                asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ...", show_alert=True))

    async def update_service_purchase(self, app_id: str, country_id: str, server_id: str, service_name: str, service_code: str) -> bool:
        """Update service purchase count or create new service entry."""
        try:
            if not self._initialized or not self.redis_client:
                #logger.error("TopServiceManager not initialized")
                return False

            service_key = 'main_data:details:service_data'
            entry_key = f"{app_id}:{country_id}:{server_id}"
            entry_path = f'$["{entry_key}"]'
            purchased_path = f'$["{entry_key}"].purchased'

            # --- Atomic increment (fast path for existing entries) ---
            # numincrby is a single Redis command; no read-modify-write TOCTOU.
            incr_result = await self.redis_client.json().numincrby(
                service_key, purchased_path, 1
            )

            if not incr_result or incr_result[0] is None:
                # Entry does not yet exist – build and insert it.
                country_data = await self.redis_client.json().get('main_data:details:country_data') or {}
                flag_url = (
                    country_data.get(str(country_id), {}).get('flag_url')
                    or 'https://upload.wikimedia.org/wikipedia/commons/thumb/2/25/'
                       'Icon-round-Question_mark.jpg/800px-Icon-round-Question_mark.jpg'
                )
                bg_url = f"https://smsactivate.s3.eu-central-1.amazonaws.com/assets/ico/{service_code}0.webp"
                if str(app_id) in APP_IMAGE_LIST:
                    bg_url = APP_IMAGE_LIST[str(app_id)]

                new_entry = {
                    "service_name": service_name,
                    "service_code": service_code,
                    "server_id": server_id,
                    "app_id": app_id,
                    "country_id": country_id,
                    "logo_url": bg_url,
                    "country_url": flag_url,
                    "purchased": 1,
                }
                # NX=True: only write if path is absent – prevents overwriting a
                # concurrent insert that may have arrived between our numincrby
                # returning None and this set.
                set_ok = await self.redis_client.json().set(
                    service_key, entry_path, new_entry, nx=True
                )
                if set_ok is None:
                    # A concurrent request already created the entry; increment instead.
                    await self.redis_client.json().numincrby(
                        service_key, purchased_path, 1
                    )
                logger.info("Created new service entry for %s", entry_key)
            else:
                logger.debug("Incremented purchase count for %s to %s", entry_key, incr_result[0])
            return True

        except Exception as e:
            #logger.error(f"Error updating service purchase: {e}\n{traceback.format_exc()}")
            return False



# Initialize the manager
top_service_manager = TopServiceManager()

async def init_managers(user_manager: UserManagement, order_manager=None, bot: Optional[AsyncTeleBot] = None) -> bool:
    """Initialize the top service manager asynchronously."""
    return await top_service_manager.init_managers(user_manager, bot)

async def register_handlers(bot: AsyncTeleBot) -> None:
    """Register top service handlers asynchronously."""
    await top_service_manager.register_handlers(bot)

__all__ = ['top_service_manager', 'init_managers', 'register_handlers']
