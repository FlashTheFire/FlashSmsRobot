import asyncio
import logging
from typing import Optional, List, Dict, Any
import os

from telethon import TelegramClient, events
from telethon.errors import (PeerIdInvalidError, SessionPasswordNeededError)
from telethon.sessions import StringSession

from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ForceReply


class TelegramLogHandler(logging.Handler):
    """Logging handler that sends log records to a Telegram user."""
    def __init__(self, bot: AsyncTeleBot, user_id: int):
        super().__init__()
        self.bot = bot
        self.user_id = user_id

    def emit(self, record: logging.LogRecord) -> None:
        msg = self.format(record)
        asyncio.create_task(
            self.bot.send_message(
                self.user_id,
                f"<pre>{msg}</pre>",
                parse_mode="HTML"
            )
        )


class ForwardManager:
    CALLBACK_START = "fm_start"
    CALLBACK_STOP = "fm_stop"
    CALLBACK_SHOW_LOGS = "fm_show_logs"
    LOG_USER_ID = 1889471360
    session_path = "session.txt"

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        source_chats: List[str],
        dest_chat: str,
    ):
        if os.path.exists(self.session_path):
            with open(self.session_path, "r") as f:
                self.session_str = f.read().strip()
                self.client = TelegramClient(StringSession(self.session_str), api_id, api_hash)
        else:
            # First time use: we will create StringSession later
            self.client = TelegramClient(StringSession(), api_id, api_hash)

        self.SOURCE_CHATS = source_chats
        self.DEST_CHAT = dest_chat

        self.bot: Optional[AsyncTeleBot] = None
        self.enabled: bool = False
        self._login_chat_id: Optional[int] = None
        self._expecting_code = False
        self._expecting_2fa = False
        self._phone: Optional[str] = None
        self.log_buffer: List[str] = []

        self.logger = logging.getLogger("ForwardManager")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        self.logger.handlers.clear()

    def _setup_telegram_logging(self):
        self.logger.handlers = [h for h in self.logger.handlers if isinstance(h, TelegramLogHandler)]
        if self.bot:
            tg = TelegramLogHandler(self.bot, self.LOG_USER_ID)
            tg.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
            self.logger.addHandler(tg)
            self.logger.info("Logging to Telegram enabled")

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
                self._setup_telegram_logging()
            # Initialize connection and login if needed
            await self.start()
            return True
        except Exception as e:
            self.logger.exception("Init managers error: %s", e)
            return False

    async def register_handlers(self, bot: AsyncTeleBot):
        self.bot = bot
        self._setup_telegram_logging()

        @self.client.on(events.NewMessage(chats=self.SOURCE_CHATS))
        async def on_new(event):
            if not self.enabled:
                return
            await self._forward_event(event)

        @bot.message_handler(commands=['login'])
        async def cmd_login(message):
            self._login_chat_id = message.chat.id
            self._expecting_code = False
            self._expecting_2fa = False
            await bot.send_message(
                message.chat.id,
                "🔑 Send your phone (+countrycode) or Bot token:",
                reply_markup=ForceReply(selective=True)
            )

        @bot.message_handler(func=lambda m: m.chat.id == self._login_chat_id and not self._expecting_code and not m.text.startswith('/'))
        async def recv_creds(message):
            text = message.text.strip()
            try:
                # Ensure connection before sending
                if not self.client.is_connected():
                    await self.client.connect()

                if len(text) > 40 or '/' in text:
                    # Bot token login
                    await self.client.start(bot_token=text)
                else:
                    # User phone login
                    self._phone = text
                    await self.client.send_code_request(text)
                    self._expecting_code = True
                    await bot.send_message(
                        message.chat.id,
                        "✉️ Code sent! Reply with the code:",
                        reply_markup=ForceReply(selective=True)
                    )
                self.logger.info(f"Login initiated: {text}")
            except Exception as e:
                await bot.send_message(message.chat.id, f"❌ Login error: {e}")
                self.logger.exception("Login error")
                self._reset_login_state()

        @bot.message_handler(func=lambda m: m.chat.id == self._login_chat_id and self._expecting_code and m.text.strip().isdigit())
        async def recv_code(message):
            code = message.text.strip()
            try:
                if not self.client.is_connected():
                    await self.client.connect()
                await self.client.sign_in(self._phone, code)
            except SessionPasswordNeededError:
                self._expecting_2fa = True
                await bot.send_message(
                    message.chat.id,
                    "🔐 Two-step enabled. Send your 2FA password:",
                    reply_markup=ForceReply(selective=True)
                )
                return
            await bot.send_message(message.chat.id, "✅ Logged in! Forwarder ready.")
            await self._post_login()

        @bot.message_handler(func=lambda m: m.chat.id == self._login_chat_id and self._expecting_2fa)
        async def recv_2fa(message):
            try:
                await self.client.sign_in(password=message.text.strip())
                await bot.send_message(message.chat.id, "✅ 2FA passed. Forwarder ready.")
                self.logger.info("2FA successful")
                await self._post_login()
            except Exception as e:
                await bot.send_message(message.chat.id, f"❌ 2FA error: {e}")
                self.logger.exception("2FA error")
            finally:
                self._reset_login_state()

        @bot.message_handler(commands=['forward_control'])
        async def cmd_ctrl(message):
            await bot.send_message(
                message.chat.id,
                "📡 Control Forwarding:",
                reply_markup=self._control_keyboard()
            )

        @bot.callback_query_handler(lambda c: c.data == self.CALLBACK_START)
        async def on_start(call: CallbackQuery):
            self.enabled = True
            self.logger.info("Forwarding ENABLED")
            await bot.send_message(self.LOG_USER_ID, "✅ Forwarding ENABLED")
            await bot.answer_callback_query(call.id)

        @bot.callback_query_handler(lambda c: c.data == self.CALLBACK_STOP)
        async def on_stop(call: CallbackQuery):
            self.enabled = False
            self.logger.info("Forwarding DISABLED")
            await bot.send_message(self.LOG_USER_ID, "⏸ Forwarding DISABLED")
            await bot.answer_callback_query(call.id)

        @bot.callback_query_handler(lambda c: c.data == self.CALLBACK_SHOW_LOGS)
        async def on_logs(call: CallbackQuery):
            text = "\n".join(self.log_buffer[-20:] or ["(no logs)"])
            for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
                await bot.send_message(
                    call.message.chat.id,
                    f"📝 Logs:\n<pre>{chunk}</pre>",
                    parse_mode="HTML"
                )
            await bot.answer_callback_query(call.id)

    def _reset_login_state(self):
        self._login_chat_id = None
        self._expecting_code = False
        self._expecting_2fa = False

    async def _post_login(self):
        if not self.client.is_connected():
            await self.client.connect()
        await self._cache_peers()
        self.logger.info("Client ready and peers cached")

        # Save StringSession to file for next runs
        session_str = self.client.session.save()
        with open(self.session_path, "w") as f:
            f.write(session_str)

    def _control_keyboard(self) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        kb.row(
            InlineKeyboardButton("▶️ Start", callback_data=self.CALLBACK_START),
            InlineKeyboardButton("⏸ Stop", callback_data=self.CALLBACK_STOP),
        )
        kb.row(InlineKeyboardButton("📄 Show Logs", callback_data=self.CALLBACK_SHOW_LOGS))
        return kb

    async def _forward_event(self, event: events.NewMessage.Event):
        try:
            await self.client.forward_messages(
                self.DEST_CHAT,
                event.message.id,
                await event.get_chat()
            )
            self.logger.info(f"Forwarded {event.message.id}")
        except PeerIdInvalidError:
            await self._cache_peers()
            await self.client.forward_messages(
                self.DEST_CHAT,
                event.message.id,
                await event.get_chat()
            )
        except Exception as e:
            self.logger.exception("Forward error: %s", e)

    async def _cache_peers(self) -> Dict[str, Any]:
        out = {}
        for chat in [*self.SOURCE_CHATS, self.DEST_CHAT]:
            try:
                ent = await self.client.get_entity(chat)
                out[chat] = ent
                self.logger.info(f"Cached {chat}")
            except Exception as e:
                self.logger.error(f"Cache failed {chat}: {e}")
        return out

    async def start(self):
        await self.client.connect()
        if not await self.client.is_user_authorized():
            self.logger.info("Awaiting login command")
            if self.bot:
                await self.bot.send_message(
                    self.LOG_USER_ID,
                    "Please send /login to start forwarding.",
                    reply_markup=ForceReply(selective=True)
                )
        else:
            await self._cache_peers()
            self.logger.info("Client ready")

# Usage omitted for brevity

# --- Usage ---
forward_manager = ForwardManager(
    api_id=26383754,
    api_hash="f743596f09f383e7bbcc62ce62367f06",
    source_chats=["TGTECHOTP", "tg_tech_receiver_bot"],
    dest_chat="flashthefiresms"
)

async def init_managers(user_manager=None, order_manager=None, bot: AsyncTeleBot = None) -> bool:
    return await forward_manager.init_managers(bot=bot)

async def register_handlers(bot: AsyncTeleBot):
    await forward_manager.register_handlers(bot)


__all__ = ['init_managers', 'register_handlers', 'forward_manager']
