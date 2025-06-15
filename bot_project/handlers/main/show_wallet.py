import hashlib
import os
import logging
import asyncio
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
from telebot.types import InputMediaPhoto, InputMediaVideo, Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from utils.functions import create_keyboard, convert_points, get_tg_profile_photo
from utils.redis_manager import redis_manager, RedisManager
from handlers.manager.operation import FinancialManagement, UserManagement, OrderManagement, FinancialSummaryAggregator
from utils.config import LOADING_GIF
from telebot.async_telebot import AsyncTeleBot
from typing import Optional, Dict, Any
import json
import time
import aiofiles
from redis import Redis
from functools import partial
from utils.redis_keys import RedisKeys
from handlers.security import RateLimiter, InputValidator, TransactionGuard


#loggging = logging.get#loggging(__name__)

class UserWalletManagement:
    """Enhanced wallet management with complete image change detection"""
    
    def __init__(self):
        self.user_manager: Optional[UserManagement] = None
        self.bot: Optional[AsyncTeleBot] = None
        self.redis_client: Optional[Redis] = None
        self.aggregator: Optional[FinancialManagement] = None
        self._font_cache: Dict[str, Any] = {}
        self._image_templates: Dict[str, Image.Image] = {}

    async def init_managers(self, user_mgr: UserManagement, bot: AsyncTeleBot) -> bool:
        """Initialize required components with dependency injection"""
        try:
            self.user_manager = user_mgr
            self.bot = bot
            self.aggregator = getattr(bot, 'aggregator', None)
            self.redis_client = await redis_manager.get_client()
            await self._preload_assets()
            #loggging.info("Wallet management initialized successfully.")
            return True
        except Exception as e:
            #loggging.error(f"Initialization error: {e}")
            return False

    async def _preload_assets(self) -> None:
        """Preload fonts and image templates asynchronously"""
        try:
            font_task = asyncio.create_task(self._load_font("bot_project/fonts/NewtonHowardFont.ttf", 40))
            image_task = asyncio.create_task(self._load_image("bot_project/images/general/profile-main_page.png"))
            
            self._font_cache['primary'], self._image_templates['landscape'] = await asyncio.gather(font_task, image_task)
            
            print("Assets preloaded successfully")
        except Exception as e:
            print(f"Error preloading assets: {e}")

    async def _load_font(self, path: str, size: int) -> ImageFont.FreeTypeFont:
        return await asyncio.to_thread(ImageFont.truetype, path, size)

    async def _load_image(self, image_path: str) -> Image.Image:
        """Load image asynchronously using aiofiles"""
        try:
            async with aiofiles.open(image_path, mode='rb') as f:
                img_data = await f.read()
            return Image.open(BytesIO(img_data))
        except Exception as e:
            #loggging.error(f"Error loading image: {e}")
            raise
    
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

    async def process_wallet_callback(self, call: CallbackQuery) -> None:
        """Process wallet callback with full caching logic using asyncio for ultra-fast processing"""
        try:
            user_id, chat_id, message_id = str(call.from_user.id), call.message.chat.id, call.message.message_id
            transaction_key = RedisKeys.transaction_lock_key(chat_id, f"show_wallet:main")
            async with TransactionGuard(self.redis_client) as guard:
                if not await self._acquire_transaction_lock(guard, transaction_key, call):
                    return
                try:
                    keyboard = self._create_wallet_keyboard(call)
            
                    loading_animation_task = self._show_loading_animation(chat_id, message_id, keyboard)
                    user_data_task = self._get_user_wallet_data(user_id)
                    profile_image_task = get_tg_profile_photo(call.from_user.id)

                    _, user_data, profile_image = await asyncio.gather(
                        loading_animation_task, user_data_task, profile_image_task
                    )

                    if not user_data:
                        print(f"Failed to retrieve user data for user {user_id}")
                        return

                    current_info = await self._generate_wallet_info_hash(user_data)
                    cached_image = await self._check_image_cache(user_id, current_info)

                    caption = await self._build_wallet_caption(user_data)
                    if cached_image:
                        await self._update_with_cached_image(chat_id, message_id, keyboard, cached_image, caption)
                    else:
                        image_path = profile_image.get("result") if profile_image.get("response") else None
                        await self._generate_and_update_image(
                            call, user_data, image_path, chat_id, message_id, keyboard, caption, current_info
                        )
                except Exception as e:
                    import traceback
                    trace = traceback.format_exc()
                    print(f"[Wallet Error] full traceback:\n{trace}")
                    await self.bot.send_message(
                        call.message.chat.id,
                        "🚫 An internal error occurred. Check logs."
                    )
                    return
                finally:
                    await guard.release_lock(transaction_key)
        except Exception as e:
            import traceback
            trace = traceback.format_exc()
            print(f"[Wallet Error] full traceback:\n{trace}")
            await self.bot.send_message(
                call.message.chat.id,
                "🚫 An internal error occurred. Check logs."
            )

    async def process_wallet_update(self, user_id) -> None:
        """Process wallet update with full caching logic using asyncio for ultra-fast processing"""
        try:
            user_data, profile_image = await asyncio.gather(
                self._get_user_wallet_data(user_id),
                get_tg_profile_photo(user_id)
            )
            
            current_info = await self._generate_wallet_info_hash(user_data)
            cached_image = await self._check_image_cache(user_id, current_info)

            if not cached_image:
                image_path = profile_image.get("result") if profile_image.get("response") else None
                await self._generate_and_update_image(
                    call=None,
                    user_data=user_data,
                    image_path=image_path,
                    chat_id=-1002203139746,
                    message_id=350,
                    keyboard=None,
                    caption=str(time.time()),
                    current_info=current_info
                )
                #loggging.info(f"Generated new image for user {user_id}")
            
            #loggging.debug(f"Wallet update processed for user {user_id}")
        except Exception as e:
            pass
            #loggging.error(f"Wallet update error for user {user_id}: {e}")

    async def _compute_image_hash(self, image_path: Optional[str]) -> str:
        """Generate MD5 hash for image content asynchronously"""
        if not image_path or not os.path.exists(image_path):
            return "no_image"
        
        try:
            img = await asyncio.to_thread(Image.open, image_path)
            img_bytes = BytesIO()
            await asyncio.to_thread(img.save, img_bytes, format='PNG')
            return hashlib.md5(img_bytes.getvalue()).hexdigest()
        except Exception as e:
            #loggging.error(f"Image hash computation failed: {e}")
            return "error"

    async def _generate_wallet_info_hash(self, user_data: dict) -> str:
        """Generate unique cache key incorporating all visual elements"""
        return (
            f"BALANCE:{user_data['current_balance']:.2f}:"
            f"DEPOSIT:{user_data['total_deposits']:.2f}:"
            f"SPEND:{user_data['spend_balance']:.2f}"
        )

    async def _generate_and_update_image(
        self,
        call: Optional[CallbackQuery],
        user_data: Dict[str, Any],
        image_path: Optional[str],
        chat_id: int,
        message_id: int,
        keyboard: InlineKeyboardMarkup,
        caption: str,
        current_info: str
    ) -> None:
        """Generate new wallet image and update cache asynchronously"""
        wallet_image = await self.create_wallet_image(
            balance=user_data['current_balance'],
            deposit=user_data['total_deposits'],
            spend=user_data['spend_balance'],
            user_image_path=image_path
        )
        print(wallet_image)
        user_id = str(call.from_user.id) if call else user_data['user_id']
        if wallet_image:
            await self._update_wallet_image(
                chat_id, message_id, keyboard, wallet_image,
                caption, user_id, current_info
            )
        elif call:
            try:
                await self.bot.answer_callback_query(call.id, 
                    "Failed to generate wallet image.", show_alert=True)
            except Exception as e:
                pass
                #loggging.error(f"Failed to answer callback query: {e}")

    async def create_wallet_image(self, balance: float, deposit: float, spend: float,
                                  user_image_path: Optional[str] = None) -> Optional[BytesIO]:
        """Generate wallet image with integrated caching checks asynchronously"""
        try:
            # Use a copy of the preloaded template instead of reloading from file
            landscape_img = self._image_templates['landscape'].copy()
            landscape_img = landscape_img.convert("RGBA")
            
            if user_image_path and os.path.exists(user_image_path):
                await self._add_profile_image(landscape_img, user_image_path)

            draw = ImageDraw.Draw(landscape_img)
            text = f"Balance: {balance:.0f}\nDeposit: {deposit:.0f}\nSpend: {spend:.0f}"
            text_position = (247, 430)
            text_size = 40
            text_font_path = "bot_project/fonts/NewtonHowardFont.ttf"
            
            await self._draw_text(draw, text, text_position, text_size, text_font_path)

            output = BytesIO()
            await asyncio.to_thread(landscape_img.save, output, format='PNG')
            output.seek(0)
            print("Wallet image generated successfully.")
            return output
        except Exception as e:
            print(f"Unexpected error during image creation: {e}")
            return None

    async def _add_profile_image(self, base_image: Image.Image, profile_path: str):
        """Add circular profile image to base image asynchronously"""
        try:
            circular_img = await self._process_profile_image(profile_path)
            base_image.paste(circular_img, (275, 168), circular_img)
        except Exception as e:
            pass
            #loggging.error(f"Profile image processing error: {e}")

    async def _process_profile_image(self, image_path: str) -> Image.Image:
        """Process user profile image into circular mask asynchronously"""
        try:
            size = 165
            user_img = await asyncio.to_thread(Image.open, image_path)
            user_img = user_img.convert("RGBA").resize((size, size), Image.LANCZOS)
            mask = Image.new('L', (size, size), 0)
            draw = ImageDraw.Draw(mask)
            draw.ellipse((0, 0, size, size), fill=255)
            circular_img = Image.new("RGBA", (size, size))
            circular_img.paste(user_img, (0, 0), mask)
            return circular_img
        except Exception as e:
            #loggging.error(f"Profile image processing error: {e}")
            raise

    async def _draw_text(self, draw: ImageDraw.Draw, text: str, text_position: tuple, text_size: int, text_font_path: str) -> None:
        """Draw text at a specific position with given size asynchronously."""
        try:
            font = await asyncio.to_thread(ImageFont.truetype, text_font_path, text_size)
            draw.text(text_position, text, font=font, fill="white")
        except Exception as e:
            pass
            #loggging.error(f"Error drawing text: {e}")

    async def _check_image_cache(self, user_id: str, current_info: str) -> Optional[str]:
        """Check Redis cache for existing valid image asynchronously"""
        try:
            key = "image_data:user-wallet"
            cached_data = await self.redis_client.hget(key, user_id)
            if not cached_data:
                return None

            cached_info = json.loads(cached_data)
            return cached_info.get("file_id") if cached_info.get("info") == current_info else None
        except json.JSONDecodeError:
            #loggging.error(f"Invalid JSON in cache for user {user_id}")
            return None
        except Exception as e:
            #loggging.error(f"Cache check error for user {user_id}: {e}")
            return None

    async def _update_wallet_image(self, chat_id: int, message_id: int, keyboard: InlineKeyboardMarkup,
                                   image: BytesIO, caption: str, user_id: int, current_info: str):
        """Update display and cache new image asynchronously"""
        try:
            msg = await self.bot.edit_message_media(
                media=InputMediaPhoto(media=image, caption=caption, parse_mode="HTML"),
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=keyboard
            )
            #loggging.debug(f"Updated message id: {msg.message_id}")
            await self.redis_client.hset(
                "image_data:user-wallet",
                user_id,
                json.dumps({
                    "file_id": msg.photo[-1].file_id,
                    "info": current_info
                })
            )
        except Exception as e:
            pass
            #loggging.error(f"Image update error: {e}")

    def _create_wallet_keyboard(self, call: CallbackQuery):
        """Generate standardized wallet keyboard"""
        keyboard = InlineKeyboardMarkup()
        keyboard.row(
            InlineKeyboardButton("💰 Dᴇᴘᴏsɪᴛ", callback_data="USER:DEPOSIT"),
            InlineKeyboardButton("💁🏻 Rᴇғᴇʀ", callback_data="USER:REFFERAL"),
            InlineKeyboardButton("📑 Hɪsᴛᴏʀʏ", callback_data="USER:HISTORY")
        )
        keyboard.row(
            InlineKeyboardButton("🔙 Bᴀᴄᴋ Tᴏ Hᴏᴍᴇ", callback_data='start'),
            InlineKeyboardButton("↻ Rᴇғʀᴇsʜ Pᴀɢᴇ", callback_data=call.data)
        )
        return keyboard

    async def _show_loading_animation(self, chat_id: int, message_id: int, keyboard) -> None:
        """Display loading animation during data processing asynchronously"""
        try:
            await self.bot.edit_message_media(
                media=InputMediaVideo(
                    media=LOADING_GIF, 
                    caption=self._get_loading_caption_template(), 
                    parse_mode="HTML"
                ),
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=keyboard
            )
            #loggging.debug("Loading animation displayed.")
        except Exception as e:
            pass
            #loggging.error(f"Loading animation error: {e}")

    async def _get_user_wallet_data(self, user_id: str) -> Optional[dict]:
        """Retrieve and process user wallet data asynchronously"""
        try:
            data = await self.aggregator.get_user(user_id)
            if not data or not data.get('response'):
                #loggging.error("User data response indicated failure.")
                return None
            #loggging.debug(f"Raw user data: {data}")
            user_profile = data.get("user_profile")
            current_balance = data["metrics"]["current_balance"]
            spend_balance = data["metrics"]["spend_balance"]
            total_deposits = data["metrics"]["deposits"]["total_amount"]
            target_currency = 'USD'
            timestamp = data["timestamp"]

            processed_data = {
                'user_id': user_id,
                'current_balance': current_balance,
                'total_deposits': total_deposits,
                'spend_balance': spend_balance,
                'target_currency': target_currency,
                'user_profile': user_profile,
                'timestamp': timestamp
            }
            #loggging.debug(f"Processed wallet data: {processed_data}")
            return processed_data
        except Exception as e:
            #loggging.error(f"Error retrieving user wallet data: {e}")
            return None

    async def _build_wallet_caption(self, user_data: dict) -> str:
        """Construct wallet caption text asynchronously"""
        try:
            conversion_tasks = [
                convert_points(user_data['current_balance'], user_data['target_currency']),
                convert_points(user_data['spend_balance'], user_data['target_currency']),
                convert_points(user_data['total_deposits'], user_data['target_currency'])
            ]
            converted_balance, converted_spend, converted_deposits = await asyncio.gather(*conversion_tasks)

            caption = (
                f"<b>🔥 Yᴏᴜʀ Fʟᴀsʜ-Wᴀʟʟᴇᴛ 》</b>\n\n"
                f"💰 <b>Bᴀʟᴀɴᴄᴇ:</b> <code>{user_data['current_balance']:.2f}</code> 💎 "
                f"<code>〚{user_data['target_currency']} {converted_balance:.2f}〛</code>\n"
                f"📊 <b>Sᴘᴇɴᴅ:</b> <code>{user_data['spend_balance']:.2f}</code> 💎 "
                f"<code>〚{user_data['target_currency']} {converted_spend:.2f}〛</code>\n"
                f"📈 <b>Dᴇᴘᴏsɪᴛ:</b> <code>{user_data['total_deposits']:.2f}</code> 💎 "
                f"<code>〚{user_data['target_currency']} {converted_deposits:.2f}〛</code>\n\n"
                f"📌 <b>Mᴀɴᴀɢᴇ Yᴏᴜʀ Aᴄᴄᴏᴜɴᴛ Hᴇʀᴇ</b>"
            )
            return caption
        except Exception as e:
            #loggging.error(f"Error building wallet caption: {e}")
            return "Error building wallet caption."

    def _get_loading_caption_template(self) -> str:
        """Return loading screen caption template"""
        user_data = {'current_balance': 0.0, 'spend_balance': 0.0, 'total_deposits': 0.0, 'target_currency': 'USD', 'converted': [0.0, 0.0, 0.0]}
        caption = (
            f"<b>🔥 Yᴏᴜʀ Fʟᴀsʜ-Wᴀʟʟᴇᴛ 》</b>\n\n"
            f"💰 <b>Bᴀʟᴀɴᴄᴇ:</b> <code>{user_data['current_balance']:.2f}</code> 💎 "
            f"<code>〚{user_data['target_currency']} {user_data['converted'][0]:.2f}〛</code>\n"
            f"📊 <b>Sᴘᴇɴᴅ:</b> <code>{user_data['spend_balance']:.2f}</code> 💎 "
            f"<code>〚{user_data['target_currency']} {user_data['converted'][1]:.2f}〛</code>\n"
            f"📈 <b>Dᴇᴘᴏsɪᴛ:</b> <code>{user_data['total_deposits']:.2f}</code> 💎 "
            f"<code>〚{user_data['target_currency']} {user_data['converted'][2]:.2f}〛</code>\n\n"
            f"📌 <b>Mᴀɴᴀɢᴇ Yᴏᴜʀ Aᴄᴄᴏᴜɴᴛ Hᴇʀᴇ</b>"
        )
        return caption
        
    async def _update_with_cached_image(self, chat_id: int, message_id: int, keyboard, file_id: str, caption: str) -> None:
        """Update message with cached wallet image asynchronously"""
        try:
            await self.bot.edit_message_media(
                media=InputMediaPhoto(media=file_id, caption=caption, parse_mode="HTML"),
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=keyboard
            )
            #loggging.info("Wallet display updated with cached image.")
        except Exception as e:
            pass
            #loggging.error(f"Cached image update error: {e}")

# Global instance and interface
wallet_manager = UserWalletManagement()

async def init_managers(user_manager: UserManagement, order_manager: OrderManagement, bot: AsyncTeleBot) -> bool:
    return await wallet_manager.init_managers(user_manager, bot)

async def register_handlers(bot: AsyncTeleBot) -> bool:
    @bot.callback_query_handler(func=lambda call: call.data == "USER:WALLET")
    async def wallet_callback_handler(call: CallbackQuery):
        try:
            process_task = partial(wallet_manager.process_wallet_callback, call)
            asyncio.create_task(process_task())
        except ValueError:
            asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", show_alert=True))
        except Exception as e:
            #logging.error(f"Callback error: {e}")
            asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ...", show_alert=True))

__all__ = ['init_managers', 'register_handlers']
