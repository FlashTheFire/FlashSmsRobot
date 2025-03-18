from typing import Optional
import logging
from asyncio import gather

from telebot.async_telebot import AsyncTeleBot
from telebot.types import InputMediaVideo, Message
from utils.functions import create_keyboard  # assumed async
from utils.redis_manager import redis_manager
from handlers.manager.operation import UserManagement, OrderManagement
from utils.config import LOADING_GIF


class ReferManagement:
    def __init__(self):
        self.user_manager: Optional[UserManagement] = None
        self.order_manager: Optional[OrderManagement] = None
        self.bot: Optional[AsyncTeleBot] = None

    async def init_managers(
        self,
        user_mgr: Optional[UserManagement] = None,
        order_mgr: Optional[OrderManagement] = None,
        bot: Optional[AsyncTeleBot] = None
    ) -> bool:
        try:
            self.user_manager = user_mgr
            self.order_manager = order_mgr
            if bot:
                self.bot = bot
            return True
        except Exception as e:
            logging.exception("Error initializing managers: %s", e)
            return False

    async def handle_referral(self, message: Message) -> None:
        if not self.user_manager:
            await message.reply("Service temporarily unavailable. Please try again later.")
            return

        user_id = str(message.from_user.id)
        user_data = await self.user_manager.get_user_data(user_id)
        if not user_data:
            await message.reply("Please start the bot first using /start")
            return
        # You can add more referral handling logic here

    async def _show_loading_animation(
        self, bot: AsyncTeleBot, chat_id: int, message_id: int, caption: str, keyboard
    ) -> None:
        try:
            await bot.edit_message_media(
                media=InputMediaVideo(media=LOADING_GIF, caption=caption, parse_mode="HTML"),
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=keyboard
            )
        except Exception as e:
            logging.exception("Error showing loading animation: %s", e)

    async def _show_loading_animation(self, chat_id: int, message_id: int, keyboard, caption: str) -> None:
        """Display loading animation during data processing asynchronously"""
        try:
            await self.bot.edit_message_media(
                media=InputMediaVideo(
                    media=LOADING_GIF, 
                    caption=caption,
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

    async def referral_handle(self, bot: AsyncTeleBot, call, request_type="new"):
        chat_id = call.message.chat.id
        message_id = call.message.message_id
        video_url = "https://t.me/public_file_only/493"

        user_id = str(call.from_user.id)
        buttons = [
            {"text": "💰 Wɪᴛʜᴅʀᴀᴡ", "callback_data": "USER:REFFERAL:WITHDRAW", "position": (0, 0)},
            {"text": "🔍 Rᴇꜰᴇʀʀᴀʟ", "switch_inline_query_current_chat": "#Yᴏᴜʀ-Rᴇꜰᴇʀʀᴀʟ", "position": (0, 1)},
            {"text": "👋 Rᴇғᴇʀ Lɪɴᴋ", "callback_data": "USER:REFFERAL:LINK", "position": (0, 2)},
            {"text": "🔙 Rᴇᴛᴜʀɴ Bᴀᴄᴋ", "callback_data": "start", "position": (1, 0)},
            {"text": "↻ Rᴇғʀᴇsʜ Pᴀɢᴇ", "callback_data": call.data, "position": (1, 1)},
        ]
        caption = (
            '<u><b>🎁 Rᴇꜰᴇʀ ᴀɴᴅ Eᴀʀɴ</b></u> <b>❯</b>\n'
            '<b>  • Iɴᴠɪᴛᴇ Yᴏᴜʀ Fʀɪᴇɴᴅs Aɴᴅ Eᴀʀɴ Rᴇᴡᴀʀᴅs!</b>\n\n'
            '<b>📊 Yᴏᴜʀ Rᴇꜰᴇʀʀᴀʟ Sᴛᴀᴛᴜs :</b>\n'
            '<b>  • Iɴᴠɪᴛᴇᴅ Fʀɪᴇɴᴅs »</b> <code>{invited_friends}</code>\n'
            '<b>  • Eᴀʀɴᴇᴅ Pᴏɪɴᴛs »</b> <code>{earned_points:.2f}</code> 💰\n\n'
            '<b>💵 Eᴀʀɴɪɴɢ Rᴀᴛᴇ :</b>\n'
            '<i>  • Eᴀʀɴ</i> <code>{low_rate:02.0f}%</code> <i>Rᴇᴄʜᴀʀɢᴇs Oᴠᴇʀ</i> <code>{low_threshold:02.0f}</code> <i>Pᴏɪɴᴛs Bʏ Yᴏᴜʀ Fʀɪᴇɴᴅs, Aɴᴅ </i> <code>{high_rate:02.0f}%</code> <i>Oɴ Rᴇᴄʜᴀʀɢᴇs Oᴠᴇʀ </i><code>{high_threshold:02.0f}</code> <i>Pᴏɪᴛs.</i>'
        )
        row_width = 3
        keyboard = await create_keyboard(buttons, row_width=row_width)
        await self._show_loading_animation(chat_id, message_id, keyboard, caption.format(
                        invited_friends=0,
                        earned_points=0.00,
                        low_rate=5,
                        low_threshold=10,
                        high_rate=10,
                        high_threshold=50
                    ))
        me = await bot.get_me()
        bot_username = me.username
        referral_link = f"https://t.me/{bot_username}?start={user_id}"
        referral_text = f"Enjoy free meals with Infoy! Use my referral code to claim your reward: {referral_link}"
        encoded_referral_text = referral_text.replace(" ", "%20").replace(":", "%3A")

        if request_type == 'link':
            row_width = 4
            buttons = [
                {
                    "text": "✆ Wʜᴀᴛs..",
                    "url": f"https://wa.me/?text={encoded_referral_text}",
                    "position": (0, 0)
                },
                {
                    "text": "𝕏 Tᴡɪᴛᴛ..",
                    "url": f"https://twitter.com/intent/tweet?text={encoded_referral_text}",
                    "position": (0, 1)
                },
                {
                    "text": "ⓕ Fᴀᴄᴇʙ..",
                    "url": f"https://www.facebook.com/sharer/sharer.php?u={referral_link}&quote={encoded_referral_text}",
                    "position": (0, 2)
                },
                {
                    "text": "➥ Tᴇʟᴇɢ..",
                    "switch_inline_query": referral_text,
                    "position": (0, 3)
                },
                {
                    "text": "🔙 Rᴇᴛᴜʀɴ Bᴀᴄᴋ Tᴏ Rᴇꜰᴇʀ Pᴀɢᴇ [ Rᴇꜰᴇʀ-Mᴇɴᴜ ]",
                    "callback_data": "USER:REFFERAL",
                    "position": (1, 0)
                },
            ]
            caption = (
                "<u><b>🎁 Rᴇꜰᴇʀ A Nᴇᴡ Fʀɪᴇɴᴅ</b> ❯</u>\n"
                "<b>  • Sʜᴀʀᴇ Yᴏᴜʀ Rᴇꜰᴇʀʀᴀʟ Lɪɴᴋ Aɴᴅ Eᴀʀɴ!</b>\n\n"
                f"<b>🔗 Yᴏᴜʀ Rᴇꜰᴇʀʀᴀʟ Lɪɴᴋ:</b>\n"
                f"<b>  •</b> <code>{referral_link}</code>\n\n"
                "<b>🔍 Hᴏᴡ Iᴛ Wᴏʀᴋs:</b>\n"
                "<b>Sʜᴀʀᴇ Yᴏᴜʀ Rᴇꜰᴇʀʀᴀʟ Lɪɴᴋ.</b> <i>Wʜᴇɴ A Nᴇᴡ Usᴇʀ Mᴀᴋᴇs A Dᴇᴘᴏsɪᴛ Usɪɴɢ Yᴏᴜʀ Lɪɴᴋ, "
                "Yᴏᴜ'ʟʟ Eᴀʀɴ Vᴀʟᴜᴀʙʟᴇ Pᴏɪɴᴛs Aᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ.</i>\n\n"
                "<b>  • Fɪʀsᴛ Rᴇᴄʜᴀʀɢᴇ Bᴏɴᴜs :</b>\n"
                "   <i>- Rᴇᴄᴇɪᴠᴇ </i><code>10%</code><i> Oғ Yᴏᴜʀ Fʀɪᴇɴᴅ's Fɪʀsᴛ Rᴇᴄʜᴀʀɢᴇ Aᴍᴏᴜɴᴛ "
                "Iғ Iᴛ Exᴄᴇᴇᴅs </i><code>100</code><i> Pᴏɪɴᴛs.</i>"
            )
        elif request_type == 'withdraw':
            row_width = 1
            buttons = [
                {"text": "🔙 Rᴇᴛᴜʀɴ Bᴀᴄᴋ Tᴏ Rᴇꜰᴇʀ Pᴀɢᴇ [ Rᴇғᴇʀ-Mᴇɴᴜ ]", "callback_data": "USER:REFFERAL", "position": (0, 0)},
            ]
            caption = (
                '<b>💵 <u>Wɪᴛʜᴅʀᴀᴡ Yᴏᴜʀ Eᴀʀɴɪɴɢs</u> ❯</b>  \n'
                '<b>  • Eᴀsɪʟʏ Wɪᴛʜᴅʀᴀᴡ Yᴏᴜʀ Rᴇꜰᴇʀʀᴀʟ Eᴀʀɴɪɴɢs Iɴ Jᴜsᴛ A Fᴇᴡ Sᴛᴇᴘs!</b>\n\n'
                '<b>💰 Cᴜʀʀᴇɴᴛ Rᴇꜰᴇʀʀᴀʟ Bᴀʟᴀɴᴄᴇ:</b>  \n'
                '<b>  • Cᴏᴍᴍɪssɪᴏɴ Eᴀʀɴɪɴɢs : ₹</b> <code>00.00</code> \n\n'
                '<b>🚀 Wɪᴛʜᴅʀᴀᴡᴀʟ Pʀᴏᴄᴇss & Iᴍᴘᴏʀᴛᴀɴᴛ 》</b>  \n'
                '<blockquote expandable><b>💰 Wɪᴛʜᴅʀᴀᴡᴀʟ Pʀᴏᴄᴇss</b>\n'
                '       • <i>Sᴇʟᴇᴄᴛ Tʜᴇ Aᴍᴏᴜɴᴛ</i>.\n'
                '       • <i>Eɴᴛᴇʀ Pᴀʏᴍᴇɴᴛ Dᴇᴛᴀɪʟs</i>.  \n'
                '       • <i>Sᴜʙᴍɪᴛ Yᴏᴜʀ Rᴇǫᴜᴇsᴛ.</i>\n\n'
                '<b> ❗Iᴍᴘᴏʀᴛᴀɴᴛ »</b>\n'
                '       • <i>Pʟᴇᴀsᴇ Wᴀɪᴛ Fᴏʀ Pʀᴏᴄᴇssɪɴɢ (</i> <code>6</code> <i>Hᴏᴜᴍs).</i>\n'
                '       • <i>Mᴏɴᴛʜʟʏ Lɪᴍɪᴛ »</i> <code>10</code> <i>Wɪᴛʜᴅʀᴀᴡs.</i>\n'
                '       • <i>Oɴʟʏ</i> <b>Vᴇʀɪғɪᴇᴅ Usᴇʀs</b> <i>Cᴀɴ Wɪᴛʜᴅʀᴀᴡ (Mᴏʙɪʟᴇ/Eᴍᴀɪʟ Vᴇʀɪғɪᴄᴀᴛɪᴏɴ Rᴇǫᴜɪʀᴇᴅ).</i></blockquote>\n\n'
                '<b>📝 Mɪɴɪᴍᴜᴍ Wɪᴛʜᴅʀᴀᴡ »</b>\n'
                '  • <i>Aᴠᴀɪʟᴀʙʟᴇ Fᴏʀ Aᴍᴏᴜɴᴛs Oᴠᴇʀ ₹</i> <code>50</code>.'
            )


        keyboard = await create_keyboard(buttons, row_width=row_width)


        try:
            await bot.edit_message_media(
                media=InputMediaVideo(
                    media=video_url,
                    caption=caption.format(
                        invited_friends=0,
                        earned_points=0.00,
                        low_rate=5,
                        low_threshold=10,
                        high_rate=10,
                        high_threshold=50
                    ),
                    parse_mode="HTML"
                ),
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=keyboard
            )
        except Exception as e:
            logging.exception("Error editing message media: %s", e)

    async def register_handlers(self, bot: AsyncTeleBot):
        """Register all referral handlers with the provided bot."""
        try:
            # Ensure managers are initialized and the bot is stored
            #await self.init_managers(user_mgr=self.user_manager, order_mgr=self.order_manager, bot=bot)
            self.bot = bot

            bot.register_callback_query_handler(
                self.handle_referral_callback,
                func=lambda call: call.data == "USER:REFFERAL"
            )
            bot.register_callback_query_handler(
                self.handle_referral_withdraw_callback,
                func=lambda call: call.data == "USER:REFFERAL:WITHDRAW"
            )
            bot.register_callback_query_handler(
                self.handle_referral_link_callback,
                func=lambda call: call.data == "USER:REFFERAL:LINK"
            )
        except Exception as e:
            logging.exception("Error registering handlers: %s", e)

    async def handle_referral_callback(self, call):
        try:
            await self.referral_handle(self.bot, call, request_type="new")
        except Exception as e:
            logging.exception("Error in referral callback: %s", e)
            await self.bot.answer_callback_query(
                call.id,
                "An error occurred. Please try again later.",
                show_alert=True
            )

    async def handle_referral_withdraw_callback(self, call):
        try:
            await self.referral_handle(self.bot, call, request_type="withdraw")
        except Exception as e:
            logging.exception("Error in referral withdraw callback: %s", e)
            await self.bot.answer_callback_query(
                call.id,
                "An error occurred. Please try again later.",
                show_alert=True
            )

    async def handle_referral_link_callback(self, call):
        try:
            await self.referral_handle(self.bot, call, request_type="link")
        except Exception as e:
            logging.exception("Error in referral link callback: %s", e)
            await self.bot.answer_callback_query(
                call.id,
                "An error occurred. Please try again later.",
                show_alert=True
            )

    async def register_referral_command(self, bot: AsyncTeleBot) -> bool:
        """Register referral command handler."""
        try:
            bot.register_message_handler(
                self.handle_referral,
                commands=['referral'],
                pass_bot=True
            )
            return True
        except Exception as e:
            logging.exception("Error registering referral command: %s", e)
            return False


# Create a global instance
refer_management = ReferManagement()


async def init_managers(user_manager=None, order_manager=None, bot: Optional[AsyncTeleBot] = None) -> bool:
    return await refer_management.init_managers(user_manager, order_manager, bot)


async def register_handlers(bot: AsyncTeleBot) -> None:
    await refer_management.register_handlers(bot)


__all__ = ['register_handlers', 'init_managers', 'refer_management']
