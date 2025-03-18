from telebot.async_telebot import AsyncTeleBot
from telebot.types import Message, InlineKeyboardMarkup, ReplyKeyboardMarkup

from utils.config import BOT_TOKEN
from typing import Optional, Union
import logging

logger = logging.getLogger(__name__)

bot = AsyncTeleBot(BOT_TOKEN)

async def send_reply(chat_id, text, reply_to_message, reply_markup=None, parse_mode="HTML"):
    """Send a reply message."""
    try:
        return await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_to_message_id=reply_to_message,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )
    except Exception as e:
        logger.error(f"Error sending reply: {e}")
        return None

async def edit_keyboard(chat_id, message_id, reply_markup):
    """Edit message keyboard."""
    try:
        return await bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.error(f"Error editing keyboard: {e}")
        return None

async def edit_message(chat_id, text, message_id, reply_markup=None, parse_mode="HTML"):
    """Edit message text."""
    try:
        return await bot.edit_message_text(
            text=text,
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )
    except Exception as e:
        logger.error(f"Error editing message: {e}")
        return None

async def handle_message(message: Message):
    """Handle incoming text messages."""
    try:
        text = message.text
        chat_id = message.chat.id
        
        from utils.functions import decode_barcode_id, encode_order_id
        
        if all(char in "𝄃𝄂𝄀𝄁" for char in text):
            decoded_text = await decode_barcode_id(text)
            await send_reply(
                chat_id=chat_id,
                text=f"Decoded message: {decoded_text}",
                reply_to_message=message.message_id
            )
        elif any(char.isdigit() for char in text):
            try:
                encoded_text = await encode_order_id(int(text))
                await send_reply(
                    chat_id=chat_id,
                    text=f"Encoded message: {encoded_text}",
                    reply_to_message=message.message_id
                )
            except ValueError:
                pass
                #await send_reply(
                #    chat_id=chat_id,
                #    text="Please enter a valid integer.",
                #    reply_to_message=message.message_id
                #)
    except Exception as e:
        pass
        #logger.error(f"Error handling message: {e}")

async def handle_files(message: Message):
    """Handle incoming files and media."""
    try:
        # Handle photos
        if message.content_type == "photo":
            file_id = message.photo[-1].file_id  # Get the highest resolution photo
            file_type = "Photo"

        # Handle videos
        elif message.content_type == "video":
            file_id = message.video.file_id
            file_type = "Video"

        # Handle documents
        elif message.content_type == "document":
            file_id = message.document.file_id
            file_type = "Document"

        # Handle animations (GIFs)
        elif message.content_type == "animation":
            file_id = message.animation.file_id
            file_type = "Animation"

        # Handle audio
        elif message.content_type == "audio":
            file_id = message.audio.file_id
            file_type = "Audio"

        # Handle voice messages
        elif message.content_type == "voice":
            file_id = message.voice.file_id
            file_type = "Voice"

        # Handle stickers
        elif message.content_type == "sticker":
            file_id = message.sticker.file_id
            file_type = "Sticker"

        await send_reply(
            chat_id=message.chat.id,
            text=f"{file_type} received!\nFile ID: <code>{file_id}</code>",
            reply_to_message=message.message_id
        )
    except Exception as e:
        logger.error(f"Error handling file: {e}")

async def register_handlers(bot: AsyncTeleBot):
    """Register message handlers."""
    try:
        # Register text message handler
        bot.register_message_handler(
            handle_message,
            content_types=["text"],
            pass_bot=True
        )
        
        # Register file handlers
        bot.register_message_handler(
            handle_files,
            content_types=["photo", "video", "document", "animation", "audio", "voice", "sticker"],
            pass_bot=True
        )
        
        return True
    except Exception as e:
        logger.error(f"Error registering message handlers: {e}")
        return False

__all__ = ['handle_message', 'register_handlers']
