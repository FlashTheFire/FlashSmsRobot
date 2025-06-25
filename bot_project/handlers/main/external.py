import asyncio
import logging
from typing import Optional, Dict, Any, List

from telethon import TelegramClient, events
from telethon.errors import PeerIdInvalidError
from telethon.tl.custom import Button
from telethon.tl.types import InputPeerChannel
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError

from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

class ForwardManager:
    CALLBACK_START = "fm_start"
    CALLBACK_STOP = "fm_stop"
    CALLBACK_SHOW_LOGS = "fm_show_logs"

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        source_chats: list,
        dest_chat: str,
    ):
        # telethon client
        self.client = TelegramClient(StringSession(), api_id, api_hash)
        self.SOURCE_CHATS = source_chats
        self.DEST_CHAT = dest_chat

        # telegram control bot
        self.bot: Optional[AsyncTeleBot] = None

        # forwarding enabled flag
        self.enabled: bool = False

        # in-memory log buffer
        self.log_buffer: List[str] = []

        # setup Python logging to capture into buffer
        self._setup_logging()

        # attach Telethon handler
        @self.client.on(events.NewMessage(chats=self.SOURCE_CHATS))
        async def _on_new_message(event):
            if not self.enabled:
                return  # ignore if off
            await self._forward_event(event)

    def _setup_logging(self):
        self.logger = logging.getLogger("ForwardManager")
        self.logger.setLevel(logging.INFO)
        # in-memory list handler
        class BufferHandler(logging.Handler):
            def __init__(self, buf: List[str]):
                super().__init__()
                self.buf = buf
            def emit(self, record):
                msg = self.format(record)
                self.buf.append(msg)
        handler = BufferHandler(self.log_buffer)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
        self.logger.addHandler(handler)

    async def init_managers(
        self,
        user_mgr: Optional[Any] = None,
        order_mgr: Optional[Any] = None,
        bot: Optional[AsyncTeleBot] = None
    ) -> bool:
        try:
            self.user_manager = user_mgr
            self.order_manager = order_mgr
            if bot:
                self.bot = bot
            return True
        except Exception as e:
            self.logger.exception("Error initializing managers: %s", e)
            return False

    async def register_handlers(self, bot: AsyncTeleBot):
        self.bot = bot

        # 1) Command to log in
        @bot.message_handler(commands=['login'])
        async def ask_login(message):
            await bot.send_message(
                message.chat.id,
                "🔑 Please send *your phone number* (e.g. +919876543210) _or_ *your Telegram Bot token_@\n"
                "_If you’re a user account, send your phone; if you're using a Bot API token, send that instead_",
                parse_mode="Markdown"
            )
            # mark this chat as expecting credentials
            self.expecting_login = message.chat.id

        # 2) Handler for the credentials message
        @bot.message_handler(func=lambda m: getattr(self, 'expecting_login', None) == m.chat.id)
        async def receive_credentials(message):
            text = message.text.strip()
            try:
                if ":" in text:
                    # Treat as Bot token
                    await bot.send_message(message.chat.id, "🔄 Logging in as Bot…")
                    await self.client.start(bot_token=text)
                else:
                    # Treat as user account phone number
                    self.phone = text
                    await bot.send_message(message.chat.id, f"📤 Sending code to {text}…")
                    await self.client.send_code_request(text)
                    await bot.send_message(
                        message.chat.id,
                        "✉️ Code sent! Please reply with the code you received via SMS."
                    )
                    # mark to expect SMS code next
                    self.expecting_code = message.chat.id
                del self.expecting_login
            except Exception as e:
                await bot.send_message(message.chat.id, f"❌ Login error: {e}")

        # 3) Handler for the SMS code
        @bot.message_handler(func=lambda m: getattr(self, 'expecting_code', None) == m.chat.id)
        async def receive_code(message):
            code = message.text.strip()
            try:
                await self.client.sign_in(self.phone, code)
            except SessionPasswordNeededError:
                await bot.send_message(message.chat.id, "🔐 Two-step enabled. Please send your 2FA password.")
                self.expecting_2fa = message.chat.id
                return

            await bot.send_message(message.chat.id, "✅ Logged in successfully!")
            del self.expecting_code

        # 4) Handler for 2FA password, if needed
        @bot.message_handler(func=lambda m: getattr(self, 'expecting_2fa', None) == m.chat.id)
        async def receive_2fa(message):
            pw = message.text.strip()
            try:
                await self.client.sign_in(password=pw)
                await bot.send_message(message.chat.id, "✅ Two-step auth passed, fully logged in!")
            except Exception as e:
                await bot.send_message(message.chat.id, f"❌ 2FA error: {e}")
            finally:
                del self.expecting_2fa

        """Register the Telegram‐bot handlers for control buttons."""
        if not self.bot:
            self.bot = bot

        # command to show the control panel
        @bot.message_handler(commands=['forward_control'])
        async def _(message):
            await bot.send_message(
                message.chat.id,
                "📡 Forwarder Control Panel:",
                reply_markup=self._control_keyboard()
            )

        # callback for start
        @bot.callback_query_handler(func=lambda c: c.data == self.CALLBACK_START)
        async def _(call: CallbackQuery):
            self.enabled = True
            self.logger.info("Forwarding ENABLED by user")
            await bot.answer_callback_query(call.id, "Forwarding ENABLED")
            await bot.edit_message_reply_markup(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=self._control_keyboard()
            )

        # callback for stop
        @bot.callback_query_handler(func=lambda c: c.data == self.CALLBACK_STOP)
        async def _(call: CallbackQuery):
            self.enabled = False
            self.logger.info("Forwarding DISABLED by user")
            await bot.answer_callback_query(call.id, "Forwarding DISABLED")
            await bot.edit_message_reply_markup(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=self._control_keyboard()
            )

        # callback for show logs
        @bot.callback_query_handler(func=lambda c: c.data == self.CALLBACK_SHOW_LOGS)
        async def _(call: CallbackQuery):
            log_text = "\n".join(self.log_buffer[-20:]) or "(no logs yet)"
            # Telegram maximum message length ~4096 chars
            for chunk in [log_text[i:i+4000] for i in range(0, len(log_text), 4000)]:
                await bot.send_message(call.message.chat.id, f"📝 Logs:\n<pre>{chunk}</pre>", parse_mode="HTML")
            await bot.answer_callback_query(call.id)

    def _control_keyboard(self) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        kb.row(
            InlineKeyboardButton("▶️ Start", callback_data=self.CALLBACK_START),
            InlineKeyboardButton("⏸ Stop", callback_data=self.CALLBACK_STOP),
        )
        kb.row(
            InlineKeyboardButton("📄 Show Logs", callback_data=self.CALLBACK_SHOW_LOGS),
        )
        return kb

    async def _forward_event(self, event):
        try:
            src = await event.get_chat()
            msg = event.message
            self.logger.info(f"Received {msg.id} in {src.id!r} → forwarding")
            await self.client.forward_messages(
                entity=self.DEST_CHAT,
                messages=msg.id,
                from_peer=src
            )
            self.logger.info("Forward successful")
        except PeerIdInvalidError:
            self.logger.warning("Peer invalid on forward; re-caching peers")
            await self._cache_peers()
            await self.client.forward_messages(
                entity=self.DEST_CHAT,
                messages=msg.id,
                from_peer=src
            )
            self.logger.info("Forward after re-cache successful")
        except Exception as e:
            self.logger.exception("Unexpected error in forwarding: %s", e)

    async def _cache_peers(self) -> Dict[str, Any]:
        resolved = {}
        for chat in (*self.SOURCE_CHATS, self.DEST_CHAT):
            try:
                ent = await self.client.get_entity(chat)
                resolved[chat] = ent
                self.logger.info(f"Cached peer {chat} → {getattr(ent, 'id', ent)}")
            except Exception as e:
                self.logger.error(f"Failed caching {chat}: {e}")
        return resolved

    async def start(self):
        # Don’t call client.start() here! Wait until the user logs in via /login.
        await self.client.connect()
        if not await self.client.is_user_authorized():
            self.logger.info("Awaiting user to /login")
        else:
            # already authorized (e.g. via StringSession), proceed to cache peers
            await self._cache_peers()
            self.logger.info("Telethon client ready")

# ───── Usage ─────

# create your global instance
forward_manager = ForwardManager(
    api_id=26383754,
    api_hash="f743596f09f383e7bbcc62ce62367f06",
    source_chats=["TGTECHOTP", "tg_tech_receiver_bot"],
    dest_chat="flashthefiresms"
)

async def init_managers(
    user_manager=None,
    order_manager=None,
    bot: Optional[AsyncTeleBot] = None
) -> bool:
    return await forward_manager.init_managers(user_manager, order_manager, bot)

async def register_handlers(bot: AsyncTeleBot) -> None:
    await forward_manager.register_handlers(bot)

__all__ = ['init_managers', 'register_handlers', 'forward_manager']
