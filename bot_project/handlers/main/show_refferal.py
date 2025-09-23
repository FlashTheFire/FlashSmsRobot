from typing import Optional
import logging
from asyncio import gather

from telebot.async_telebot import AsyncTeleBot
from telebot.types import (
    InlineQueryResultArticle,
    InputTextMessageContent,
)
from datetime import datetime
import time
import html
from telebot.types import InputMediaVideo, Message, InlineQuery
from utils.functions import create_keyboard, encode_base62, time_ago  # assumed async
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
            {"text": "⌕ Rᴇꜰᴇʀʀᴀʟ", "switch_inline_query_current_chat": "#Yᴏᴜʀ-Rᴇꜰᴇʀʀᴀʟ", "position": (0, 1)},
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
        referral_link = f"https://t.me/{bot_username}?start={str(await encode_base62(int(user_id)))}"
        top_services = (
            "🎯 Tᴏᴘ-Rᴀᴛᴇᴅ Sᴇʀᴠɪᴄᴇs:\n"
            "    •  Tᴇʟᴇɢʀᴀᴍ     • Wʜᴀᴛsᴀᴘᴘ [✆]\n" 
            "    •  Gᴍᴀɪʟ        • Fᴀᴄᴇʙᴏᴏᴋ [ⓕ]\n" 
            "    •  Iɴsᴛᴀɢʀᴀᴍ    • Tᴡɪᴛᴛᴇʀ [𝕏]\n" 
            "    • Wɪɴᴢᴏ, Rᴜᴍᴍʏ, Sᴡɪɢɢʏ & Mᴀɴʏ-Mᴏʀᴇ...\n\n"
            )
        first_text = (
            "⚡ Fʟᴀsʜ Sᴍs Oᴛᴘ Bᴏᴛ:\n\n"
            "👉 Wᴀɴᴛ Tᴏ Rᴇᴄᴇɪᴠᴇ Oᴛᴘs Fʀᴏᴍ Aɴʏ Aᴘᴘ Oʀ Wᴇʙsɪᴛᴇ Oɴ Uɴʟɪᴍɪᴛᴇᴅ Nᴜᴍʙᴇʀs Wᴏʀʟᴅᴡɪᴅᴇ?\n"
            f"🔗 Gᴇᴛ Sᴛᴀʀᴛᴇᴅ Wɪᴛʜ FʟᴀsʜSᴍs » {referral_link}\n\n"
        )
        last_text = (
            "💼 Aᴠᴀɪʟᴀʙʟᴇ Iɴ 170+ Cᴏᴜɴᴛʀɪᴇs, Sᴜᴘᴘᴏʀᴛɪɴɢ 1500+ Aᴘᴘs"
        )
        second_text = (
            " Wɪᴛʜ Pʀᴇᴍɪᴜᴍ Oᴘᴇʀᴀᴛᴏʀs\n"  
            "🚀 Fᴀsᴛ • Sᴇᴄᴜʀᴇ • 24/7 Aᴄᴄᴇss"
        )
        import urllib
        if request_type == 'link':
            row_width = 4
            buttons = [
                {
                    "text": "✆ Wʜᴀᴛs..",
                    "url": f"https://wa.me/?text={urllib.parse.quote(first_text + top_services + last_text + second_text)}",
                    "position": (0, 0)
                },
                {
                    "text": "𝕏 Tᴡɪᴛᴛ..",
                    "url": f"https://twitter.com/intent/tweet?text={urllib.parse.quote(first_text + last_text + '.')}",
                    "position": (0, 1)
                },
                {
                    "text": "ⓕ Fᴀᴄᴇʙ..",
                    "url": f"https://www.facebook.com/sharer/sharer.php?u={referral_link}&quote={urllib.parse.quote(first_text + top_services + last_text + second_text)}",
                    "position": (0, 2)
                },
                {
                    "text": "➥ Tᴇʟᴇɢ..",
                    "switch_inline_query": " ",
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
            bot.register_inline_handler(
                self.handle_referrals_inline,
                func=lambda inline_query: inline_query.query == "#Yᴏᴜʀ-Rᴇꜰᴇʀʀᴀʟ"
            )

        except Exception as e:
            logging.exception("Error registering handlers: %s", e)

    async def handle_referrals_inline(self, inline_query: InlineQuery):
        """
        Inline results for referrals.
        Trigger: "#Yᴏᴜʀ-Rᴇꜰᴇʀʀᴀʟ"
        Shows summary + list of referred users from zset:
            key = user_data:{referrer_id}:profile:reffer
            member = referred_user_id, score = epoch seconds
        """
        try:
            
            referrer_id = str(inline_query.from_user.id)
            offset = int(inline_query.offset or 0)
            start = offset
            end = offset + 10 - 1

            zkey = f"user_data:{referrer_id}:profile:reffer"

            # fetch referrals newest first
            raw = await redis_manager.redis_client.zrevrange(zkey, start, end, withscores=True)
            # total count
            total_referrals = int(await redis_manager.redis_client.zcard(zkey) or 0)

            inline_results = []

            # Summary top article (demo total_earned calculation — replace as needed)
            demo_per_referral = 5.0  # demo points per referral
            total_earned_demo = total_referrals * demo_per_referral
            summary_text = (
                f"📊 <b>Your Referral Summary</b>\n\n"
                f"🔢 Total referrals » <b>{total_referrals}</b>\n"
                f"💎 Total earned (demo) » <b>{total_earned_demo:.2f} Points</b>\n\n"
                f"ℹ️ This is demo data — replace `demo_per_referral` with real logic."
            )
            inline_results.append(
                InlineQueryResultArticle(
                    id="ref_summary",
                    title="🧾 Your Referrals",
                    description=f"Total: {total_referrals}  •  Earned (demo): {total_earned_demo:.2f} pts",
                    input_message_content=InputTextMessageContent(
                        message_text=summary_text,
                        parse_mode="HTML",
                    )
                )
            )

            # For each referred user, build an article
            for idx, (member, score) in enumerate(raw, start=1 + offset):
                # decode bytes if necessary
                if isinstance(member, (bytes, bytearray)):
                    member_id = member.decode()
                else:
                    member_id = str(member)

                # score from zset is int epoch seconds (or may be float depending on how you stored it)
                ts = float(score)

                # try to get user profile (small hgetall)
                uname = None
                try:
                    profile_key = f"user_data:{member_id}:profile:main"
                    pdata = await redis_manager.redis_client.hgetall(profile_key)
                    if pdata:
                        # redis-py may return dict of bytes
                        if isinstance(pdata, dict):
                            # try common fields
                            fn = pdata.get("first_name") or pdata.get(b"first_name") or b""
                            un = pdata.get("username") or pdata.get(b"username") or b""
                            # decode
                            fn = fn.decode() if isinstance(fn, (bytes, bytearray)) else str(fn)
                            un = un.decode() if isinstance(un, (bytes, bytearray)) else str(un)
                            uname = fn or (('@' + un) if un and un != "N/A" else None)
                except Exception:
                    uname = None

                display_name = html.escape(uname) if uname else f"User {member_id}"
                time_str = time_ago(ts)
                iso_ts = datetime.fromtimestamp(ts).isoformat()

                message_text = (
                    f"👤 <b>Referred User</b>\n"
                    f"🔹 <b>ID:</b> <code>{member_id}</code>\n"
                    f"🔹 <b>Name:</b> {display_name}\n"
                    f"⏱️ <b>Referred:</b> <code>{time_str}</code> (<code>{iso_ts}</code>)\n"
                )

                inline_results.append(
                    InlineQueryResultArticle(
                        id=f"ref_{member_id}",
                        title=f"{display_name} — {time_str}",
                        description=f"Referred {time_str} — ID {member_id}",
                            input_message_content=InputTextMessageContent(
                            message_text=message_text,
                            parse_mode="HTML",
                        )
                    )
                )

            # pagination: next_offset if more entries remain
            next_offset = ""
            if offset + len(raw) < total_referrals:
                next_offset = str(offset + len(raw))

            await self.bot.answer_inline_query(
                inline_query.id,
                results=inline_results,
                cache_time=0,
                next_offset=next_offset
            )
    
        except Exception as e:
            # best-effort logging
            try:
                print(f"handle_referrals_inline error: {e}")
            except Exception:
                print(f"handle_referrals_inline error: {e}")
            return

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
