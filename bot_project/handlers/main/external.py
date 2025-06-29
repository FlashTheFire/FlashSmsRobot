import asyncio
import logging
import os
import re
import sqlite3
import html
from typing import List, Dict, Any, Optional, Tuple, Set

from datetime import datetime
from telethon import TelegramClient, functions, types, errors, events
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ForceReply, Message
from utils.functions import small_caps, large_nums, AfterMin
from handlers.methods.purchase.made_purchase import purchase_manager
from termcolor import colored

# Setup logging
tlogging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=tlogging_format, handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

# Constants
ADMIN_USER_ID = 1889471360  # Replace with your Telegram user ID
SESSIONS_DIR = "sessions"
os.makedirs(SESSIONS_DIR, exist_ok=True)

# API Credentials (Replace with your own)
FORWARD_API_ID = 26383754
FORWARD_API_HASH = "f743596f09f383e7bbcc62ce62367f06"
CONTACT_API_ID = 20729573
CONTACT_API_HASH = "6bc09cbaa7d0471944875c202fec8b5b"
DESTINATION_CHAT_ID = 5716978793       # Where to send the parsed OTP info


class TelegramLogHandler(logging.Handler):
    """Sends log records to Telegram"""
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

class SessionManager:
    """Manages user session locks"""
    def __init__(self):
        self.locks: Dict[int, asyncio.Lock] = {}
        
    def get_lock(self, user_id: int) -> asyncio.Lock:
        if user_id not in self.locks:
            self.locks[user_id] = asyncio.Lock()
        return self.locks[user_id]

class ForwardManager:
    """
    Forwards messages from source chats to a destination chat with filtering,
    controls, and logging.
    """
    # Callback identifiers
    entry = "ForwardManager:"
    CB_START = entry + "start"
    CB_STOP = entry + "stop"
    CB_SHOW_LOGS = entry + "show_logs"
    CB_TOGGLE_LOGS = entry + "toggle_logs"
    CB_CHECK_NUM = entry + "check_nums"
    CB_LOGIN = entry + "login"
    CB_LOGOUT = entry + "logout"
    CB_ADD_APP = entry + "add_app"
    CB_REMOVE_APP = entry + "remove_app"
    CB_ADD_COUNTRY = entry + "add_country"
    CB_REMOVE_COUNTRY = entry + "remove_country"
    CB_SHOW_LISTS = entry + "show_lists"
    cb_list = [CB_START, CB_STOP, CB_SHOW_LOGS, CB_TOGGLE_LOGS, CB_CHECK_NUM,
               CB_LOGIN, CB_LOGOUT, CB_ADD_APP, CB_REMOVE_APP,
               CB_ADD_COUNTRY, CB_REMOVE_COUNTRY, CB_SHOW_LISTS]

    def __init__(
        self,
        source_chats: List[str],
        dest_chat: str
    ):
        self.source_chats = source_chats
        self.dest_chat = dest_chat
        self.bot: Optional[AsyncTeleBot] = None
        self.contact_clients: Dict[int, TelegramClient] = {}
        self.session_manager = SessionManager()

        session_path = self._contact_session_file(ADMIN_USER_ID)
        self.forward_client: Optional[TelegramClient] = TelegramClient(
            session_path,
            FORWARD_API_ID,
            FORWARD_API_HASH,
            connection_retries=5,
            auto_reconnect=True
        )

        self.enabled = False
        self.log_buffer: List[str] = []
        self.logging_enabled = True
        self.app_list: List[str] = []
        self.country_list: List[str] = []
        self.active_tasks: Set[asyncio.Task] = set()

        self.logger = logging.getLogger("ForwardManager")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        self.logger.handlers.clear()

        self.login_states: Dict[int, Dict] = {}
        self.filter_states: Dict[int, str] = {}

    async def init_managers(self, bot: AsyncTeleBot) -> bool:
        try:
            self.bot = bot
            self._setup_logging()
            await self.start_forward_client()
            return True
        except Exception as e:
            self.logger.exception("Init error: %s", e)
            if self.bot:
                await self.safe_send(ADMIN_USER_ID, f"<b>❌ Initialization Failed</b>\n<code>{e}</code>")
            return False

    def _contact_session_file(self, user_id: int) -> str:
        return os.path.join(SESSIONS_DIR, f"contact_{user_id}.session")

    def _setup_logging(self):
        if not self.bot:
            return
        self.logger.handlers.clear()
        if self.logging_enabled:
            handler = TelegramLogHandler(self.bot, ADMIN_USER_ID)
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
            self.logger.addHandler(handler)
            self.logger.info("Telegram logging enabled")

    def _control_keyboard(self, user_id: int) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        if user_id == ADMIN_USER_ID:
            kb.row(
                InlineKeyboardButton("▶️ Start" if not self.enabled else "⏹ Stop",
                                     callback_data=self.CB_START if not self.enabled else self.CB_STOP),
                InlineKeyboardButton("📋 Filters", callback_data=self.CB_SHOW_LISTS)
            )
            kb.row(
                InlineKeyboardButton("📝 Logs", callback_data=self.CB_SHOW_LOGS),
                InlineKeyboardButton("💡 Logging", callback_data=self.CB_TOGGLE_LOGS)
            )
            kb.row(
                InlineKeyboardButton("➕ App", callback_data=self.CB_ADD_APP),
                InlineKeyboardButton("➖ App", callback_data=self.CB_REMOVE_APP)
            )
            kb.row(
                InlineKeyboardButton("🌍 Country", callback_data=self.CB_ADD_COUNTRY),
                InlineKeyboardButton("🗺️ Remove", callback_data=self.CB_REMOVE_COUNTRY)
            )
        kb.row(
            InlineKeyboardButton("🔑 Login" if not os.path.exists(self._contact_session_file(user_id))
                else "🚪 Logout",
                callback_data=self.CB_LOGIN if not os.path.exists(self._contact_session_file(user_id))
                else self.CB_LOGOUT),
            InlineKeyboardButton("📞 Numbers", callback_data=self.CB_CHECK_NUM)
        )
        return kb
    
    async def unmask_number(self, masked: str, candidates: list[str], element: str) -> str:
        """
        Given something like '7747600•••007' or '7747600***007' (element='•' or '*'),
        build a regex '^7747600\\d{3}007$' and return the one candidate that matches,
        or return masked if none.
        """
        if element not in ("*", "•"):
            raise ValueError("`element` must be '*' or '•'")

        # Build the regex by walking through `masked`
        regex = ["^"]
        i = 0
        L = len(masked)
        while i < L:
            if masked[i] == element:
                # count how many in a row
                j = i
                while j < L and masked[j] == element:
                    j += 1
                count = j - i
                regex.append(f"\\d{{{count}}}")
                i = j
            else:
                # escape any regex-special char
                regex.append(re.escape(masked[i]))
                i += 1
        regex.append("$")

        pattern = "".join(regex)
        # Now find the one candidate that matches
        for num in candidates:
            if re.fullmatch(pattern, num):
                return num
        return masked
    def wrap(self, s: str, n: int = 24) -> str:
        """
        Word‑aware wrap per original line: no line exceeds n letters/spaces, and words aren't split.
        Preserves empty lines and wraps each input line independently.
        """
        lines_out = []
        for line in s.splitlines():
            if not line.strip():
                # Preserve blank lines
                lines_out.append("")
                continue
            words = line.split()
            current = ""
            count = 0
            for w in words:
                # count only letters and spaces in the word
                w_len = sum(1 for ch in w if ch.isalpha() or ch == ' ')
                sep = ' ' if current else ''
                # if adding this word exceeds limit, wrap
                if count + w_len + (1 if current else 0) > n:
                    lines_out.append(current)
                    current = w
                    count = w_len
                else:
                    current = current + sep + w
                    count += w_len + (1 if sep else 0)
            # append the last line for this input line
            if current:
                lines_out.append(current)
        return "\n".join(lines_out)

    async def register_handlers(self, bot: AsyncTeleBot):
        self.bot = bot
        @bot.message_handler(commands=['user_control'])
        async def cmd_control(message: Message):
            await bot.send_message(
                message.chat.id,
                "⚡ <b>Tᴇʟᴇɢʀᴀᴍ Cᴏɴᴛʀᴏʟ Pᴀɴᴇʟ</b>",
                parse_mode="HTML",
                reply_markup=self._control_keyboard(message.from_user.id)
            )
        @bot.channel_post_handler()
        async def otp_handler(msg: Message) -> None:
            print(msg.text)
            pattern = re.compile(r"""
                🔥\s*TG\s*TECH\s*RECEIVER\s*✨\s*
                \n+
                ⏰\s*Time:\s*(?P<time>[^\n]+)\s*
                \n+
                🌍\s*Country:\s*(?P<country>[^\n🇦-🇿]+)(?P<flag>[\U0001F1E6-\U0001F1FF]{2})\s*
                \n+
                ⚙️\s*Service:\s*(?P<service>[^\n]+)\s*
                \n+
                ☎️\s*Number:\s*(?P<number>[^\n]+)\s*
                \n+
                🔑\s*OTP:\s*(?P<otp>[^\n]+)\s*
                \n+
                📩\s*Full\s*Message:\s*\n
                (?P<full_message>.*?)(?=(?:\n🔥\s*TG\s*TECH|\Z))
            """, re.VERBOSE | re.IGNORECASE | re.MULTILINE | re.DOTALL)

            def parse_fields(text: str, time: str) -> Optional[Dict[str, Any]]:
                match = pattern.search(text)
                if not match:
                    return None

                raw_time = match["time"].strip()
                try:
                    parsed_time = datetime.strptime(raw_time, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    parsed_time = raw_time

                return {
                    "time": parsed_time,
                    "country": match["country"].strip(),
                    "service": match["service"].strip(),
                    "number": match["number"].strip(),
                    "otp": match["otp"].strip(),
                    "amount": "0.49",
                    "flag": match["flag"].strip(),
                    "full_message": self.wrap(match["full_message"].strip()),
                    "time": time,
                    "number_data": {
                        "national_code": match["number"].strip()[:2],
                        "national_number": match["number"].strip()[2:]
                    }
                }

            def build_message(data: Dict[str, Any], small_cap) -> str:
                mask = lambda s: s[:4] + "•"*(len(s)-8) + s[-4:]
                message = (
                    f"📜 <b>Oʀᴅᴇʀ Lᴏɢ</b> <b>[</b> <code>{str(data['service']).translate(small_cap)}</code> <b>]</b>\n\n"

                    f"💎 <b>Aᴍᴏᴜɴᴛ</b> » <code>{str(data['amount']).translate(small_cap)}</code> <i>Pᴏɪɴᴛs</i>\n"
                    f"🌍 <b>Rᴇɢɪᴏɴ</b> » <b>{str(data['country']).translate(small_cap)}</b> <b>[</b> <code>{data['flag']}</code> <b>]</b>\n\n"

                    f"📞 <b>Nᴜᴍʙᴇʀ</b> » <code>{str(data['number_data']['national_code']).translate(small_cap)}</code> {str(mask(str(data['number_data']['national_number']))).translate(small_cap)}\n"
                    f"💬 <b>Sᴍs Lɪsᴛ</b> » <code>{data['otp']}</code>\n\n"
                    
                    f"✅ <b>Sᴛᴀᴛᴜs</b> » <code>Cᴏᴍᴘʟᴇᴛᴇᴅ</code>\n"
                    f"🗓️ <b>Tɪᴍᴇ</b> » {data['time']}\n\n"

                    f"<blockquote expandable><pre><code class=\"language-• Sᴍs ❯ \">{str(data['full_message']).translate(small_cap)}</code></pre></blockquote>"
                )

                return message
            

            try:
                text = msg.text or ""
                parsed = parse_fields(text, await AfterMin(0))
                if not parsed:
                    print("Forwarded message didn’t match OTP format, skipping.")
                    return
                parsed['number_data']['national_code'], parsed['number_data']['national_number'] = await purchase_manager.format_phone_number(parsed['number'])
                small_cap = await small_caps()
                keyboard = InlineKeyboardMarkup()
                keyboard.add(
                    InlineKeyboardButton(
                        text="⚡️ Fʟᴀsʜ Sᴍs Bᴏᴛ",
                        url=f"https://t.me/FlashSms_Bot?start=start"), 
                    InlineKeyboardButton(
                        text="🔗 Sʜᴀʀᴇ Us",
                        url="https://t.me/share/url?url=https://t.me/FlashSms_Bot?start=start&text=%E2%9A%A1%EF%B8%8F%20F%CA%9F%E1%B4%80%EA%9C%B1%CA%9C%20F%CA%80%E1%B4%87%E1%B4%87%20S%E1%B4%8D%EA%9C%B1%20C%CA%9C%E1%B4%80%C9%B4%C9%B4%E1%B4%87%CA%9F%20%E2%9D%AF%0A%0A%F0%9F%93%B2%20W%E1%B4%80%C9%B4%E1%B4%9B%20T%E1%B4%8F%20R%E1%B4%87%E1%B4%84%E1%B4%87%C9%AA%E1%B4%A0%E1%B4%87%20OTPs%20F%CA%80%E1%B4%8F%E1%B4%8D%20W%CA%9C%E1%B4%80%E1%B4%9B%EA%9C%B1A%E1%B4%98%E1%B4%98%20%26%20T%E1%B4%87%CA%9F%E1%B4%87%C9%A2%CA%80%E1%B4%80%E1%B4%8D%20O%C9%B4%20U%C9%B4%CA%9F%C9%AA%E1%B4%8D%C9%AA%E1%B4%9B%E1%B4%87%E1%B4%85%20N%E1%B4%9C%E1%B4%8D%CA%99%E1%B4%87%CA%80s%20F%E1%B4%8F%CA%80%20F%CA%80%E1%B4%87%E1%B4%87%3F%0A%0A%F0%9F%94%97%20G%E1%B4%87%E1%B4%9B%20S%E1%B4%9B%E1%B4%80%CA%80%E1%B4%9B%E1%B4%87%E1%B4%85%20W%C9%AA%E1%B4%9B%CA%9C%20F%CA%9F%E1%B4%80%EA%9C%B1%CA%9CS%E1%B4%8D%EA%9C%B1%20%E2%80%93%20Y%E1%B4%8F%E1%B4%9C%CA%80%20O%C9%B4%E1%B4%87-S%E1%B4%9B%E1%B4%8F%E1%B4%98%20S%E1%B4%8F%CA%9F%E1%B4%9C%E1%B4%9B%C9%AA%E1%B4%8F%C9%B4%20F%E1%B4%8F%CA%80%20R%E1%B4%87%E1%B4%84%E1%B4%87%C9%AA%E1%B4%A0%C9%AA%C9%B4%C9%A2%20OTPs%21%0A%0A%F0%9F%8C%8D%20A%E1%B4%A0%E1%B4%80%C9%AA%CA%9F%E1%B4%80%CA%99%CA%9F%E1%B4%87%20I%C9%B4%20170%2B%20C%E1%B4%8F%E1%B4%9C%C9%B4%E1%B4%9B%CA%80%C9%AA%E1%B4%87s%20%26%20S%E1%B4%9C%E1%B4%98%E1%B4%98%E1%B4%8F%CA%80%E1%B4%9B%C9%AA%C9%B4%C9%A2%201500%2B%20A%E1%B4%98%E1%B4%98s%20W%C9%AA%E1%B4%9B%CA%9C%20P%CA%80%E1%B4%87%E1%B4%8D%C9%AA%E1%B4%9C%E1%B4%8D-G%CA%80%E1%B4%80%E1%B4%85%E1%B4%87%20O%E1%B4%98%E1%B4%87%CA%80%E1%B4%80%E1%B4%9B%E1%B4%8F%CA%80s.%0Aa")
                    )
                try:
                    await self.bot.send_message("-1002898000668", build_message(parsed, small_cap), reply_markup=keyboard, parse_mode="HTML")
                except Exception as e:
                    print(f"Error: {e}")

                # try to unmask the number if it has a '*'
                if "*" in parsed["number"]:
                    CANDIDATES = await purchase_manager.order_manager.get_candidates()
                    full = await self.unmask_number(parsed["number"], CANDIDATES)
                    print(f"Unmasked {parsed['number']} → {full}")
                    parsed["number"] = full
                elif "•" in parsed["number"]:
                    CANDIDATES = await purchase_manager.order_manager.get_candidates()
                    full = await self.unmask_number(parsed["number"], CANDIDATES, "•")
                    print(f"Unmasked {parsed['number']} → {full}")
                    parsed["number"] = full

                order_id = f'987654321{parsed["number"]}'
                order_data = await purchase_manager.order_manager.get_order_data(order_id)
                if not order_data['response']:
                    print("Order not found.")
                    return
                order_data = order_data['result']
                SMS = str(parsed['otp']).replace(" ", "").replace("\n", "").replace("-", "")
                if SMS.isnumeric() and parsed['number'].isnumeric():
                    add_result = await purchase_manager.order_manager.manage_number_order(
                        redis_client=purchase_manager.redis_client,
                        country_id=order_data['country_id'],
                        server_id=order_data['server_id'],
                        app_id=order_data['app_id'],
                        operator="free",
                        order_id=order_data['order_id'],
                        action="add",
                        sms_code=SMS
                    )
                    print(colored(f"Add Code: {add_result}", "yellow"))
                    await bot.send_message(
                        chat_id=order_data['user_id'],
                        text=f"✅ <b>Sᴍs Rᴇᴄɪᴇᴠᴇᴅ »</b> <code>{SMS}</code> <b>[</b><code>{parsed['number']}</code><b>]</b>\n\n",
                        parse_mode="HTML"
                    )

                print("OTP forwarded successfully:", parsed)
            except Exception as exc:
                print("Unexpected error in otp_handler:", exc)



        @self.forward_client.on(events.NewMessage(chats=self.source_chats))
        async def on_new(event):
            if not self.enabled:
                return
            try:
                await self._forward_event(event)
            except (errors.ConnectionSystemEmptyError, errors.AlreadyInConversationError) as e:
                self.logger.warning(f"Connection issue: {e}")
                await asyncio.sleep(5)

        """Register bot event handlers"""
        if not self.bot:
            return
            
        @bot.callback_query_handler(func=lambda call: call.data in self.cb_list)
        async def handle_callbacks(call: CallbackQuery):
            data = call.data
            user_id = call.from_user.id
            chat_id = call.message.chat.id

            if data == self.CB_START:
                self.enabled = True
                self.logger.info("Forwarding STARTED")
                await self.safe_callback_query(call.id, "✅ Forwarding Started")

            elif data == self.CB_STOP:
                self.enabled = False
                self.logger.info("Forwarding STOPPED")
                await self.safe_callback_query(call.id, "⏹ Forwarding Stopped")

            elif data == self.CB_SHOW_LOGS:
                text = "\n".join(self.log_buffer[-20:] or ["(No Logs)"])
                for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
                    await self.safe_send(chat_id, f"📋 <b>System Logs</b> 📋\n<pre>{chunk}</pre>", parse_mode="HTML")
                await self.safe_callback_query(call.id)

            elif data == self.CB_TOGGLE_LOGS:
                self.logging_enabled = not self.logging_enabled
                self._setup_logging()
                status = "Enabled" if self.logging_enabled else "Disabled"
                await self.safe_callback_query(call.id, f"📊 Logging {status}")

            elif data == self.CB_SHOW_LISTS:
                apps = '\n'.join([f"• {app}" for app in self.app_list]) or '• None'
                countries = '\n'.join([f"• {country}" for country in self.country_list]) or '• None'
                await self.safe_send(
                    chat_id,
                    f"<b>📂 Active Filters</b>\n\n"
                    f"<b>Applications:</b>\n{apps}\n\n"
                    f"<b>Countries:</b>\n{countries}",
                    parse_mode="HTML"
                )
                await self.safe_callback_query(call.id)

            elif data in (self.CB_ADD_APP, self.CB_REMOVE_APP, self.CB_ADD_COUNTRY, self.CB_REMOVE_COUNTRY):
                action_map = {
                    self.CB_ADD_APP: ("Add App", "Enter app name to ADD:"),
                    self.CB_REMOVE_APP: ("Remove App", "Enter app name to REMOVE:"),
                    self.CB_ADD_COUNTRY: ("Add Country", "Enter country name to ADD:"),
                    self.CB_REMOVE_COUNTRY: ("Remove Country", "Enter country name to REMOVE:")
                }
                action, prompt = action_map[data]
                msg = await self.safe_send(chat_id, f"<b>⚙️ {action}</b>\n{prompt}", parse_mode="HTML", reply_markup=ForceReply(selective=True))
                self.filter_states[msg.message_id] = data
                await self.safe_callback_query(call.id)

            elif data == self.CB_CHECK_NUM:
                if self.login_states.get(user_id, {"state": "logged_out"}).get('state') == 'logged_in' or os.path.exists(self._contact_session_file(user_id)):
                    msg = await self.safe_send(
                        chat_id,
                        "<b>📱 Phone Number Checker</b>\n\nSend up to 20 phone numbers (one per line, without '+' or spaces):\n\n<code>919027839273</code>\n<code>918372673883</code>\n<code>918373737373</code>",
                        parse_mode="HTML",
                        reply_markup=ForceReply(selective=True))
                    self.filter_states[msg.message_id] = data
                else:
                    await self.safe_send(chat_id, "⚠️ <b>Please Log-In First!</b>\nThen You Can Use Number Checker.", parse_mode="HTML")
                await self.safe_callback_query(call.id)

            elif data == self.CB_LOGIN:
                await self.start_contact_login(user_id, chat_id)
                await self.safe_callback_query(call.id)

            elif data == self.CB_LOGOUT:
                await self.logout_user(user_id, chat_id)
                await self.safe_callback_query(call.id)
            
            # Update control panel UI
            try:
                await bot.edit_message_reply_markup(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=self._control_keyboard(user_id)
                )
            except Exception:
                pass


        @bot.message_handler(func=lambda m: (m.reply_to_message and m.reply_to_message.message_id in self.filter_states)
                                                     or m.from_user.id in self.login_states)
        async def handle_replies(message: Message):
            user_id = message.from_user.id
            chat_id = message.chat.id
            text = message.text.strip()
            reply_msg_id = message.reply_to_message.message_id

            if reply_msg_id in self.filter_states:
                action = self.filter_states.pop(reply_msg_id)

                if action == self.CB_ADD_APP:
                    await self._update_list(chat_id, text, self.app_list, "App", True)
                elif action == self.CB_REMOVE_APP:
                    await self._update_list(chat_id, text, self.app_list, "App", False)
                elif action == self.CB_ADD_COUNTRY:
                    await self._update_list(chat_id, text, self.country_list, "Country", True)
                elif action == self.CB_REMOVE_COUNTRY:
                    await self._update_list(chat_id, text, self.country_list, "Country", False)
                elif action == self.CB_CHECK_NUM:
                    #gtext = self.to_gtext(message.text)
                    #text = gtext
                    all_numbers = [
                        num.strip() 
                        for num in text.splitlines() 
                        if num.strip().isdigit()
                    ][:100]

                    chunks = [
                        all_numbers[i : i + 19] 
                        for i in range(0, len(all_numbers), 19)
                    ]

                    last_number = all_numbers[-1] if all_numbers else None
                    response = []
                    try:
                        main_results = await self.process_numbers(user_id, chat_id, chunks)
                        for num, user in main_results:
                            if user:
                                username = f"@{user.username}" if user.username else "No username"
                                response.append(
                                    "✅ <code>{}</code>" 
                                    "<b>[<b><a href='tg://openmessage?user_id={}'>{}</a><b>]</b>\n"
                                    "{}".format(
                                        num, user.id, 'Oᴘᴇɴ', 
                                        f"       • <a href='https://t.me/+{num}'>{username}</a>"
                                    )
                                )
                    except Exception as e:
                        await self.safe_send(chat_id, f"<code>{e}</code>")

                        # Send results
                    if not response:
                        response.append("❌ <b>Nᴏ Pʀᴏᴠɪᴅᴇᴅ Nᴜᴍʙᴇʀs Aʀᴇ Rᴇɢɪsᴛᴇʀᴇᴅ</b>")
                    result_text = "\n\n".join(response)
                    result_text += f"\n\n<b>Last Number:</b> <code>{last_number}</code>"
                    markup = InlineKeyboardMarkup()
                    markup.add(
                        InlineKeyboardButton("🔄 Cʜᴇᴄᴋ Mᴏʀᴇ Nᴜᴍʙᴇʀs", callback_data=self.CB_CHECK_NUM),
                    )
                    await self.safe_send(
                        chat_id,
                        f"📊 <b>Number Check Results</b>\n\n{result_text}",
                        parse_mode="HTML",
                        reply_markup=markup
                    )

            elif user_id in self.login_states:
                await self.handle_login_message(message)

    async def _update_list(self, chat_id, text, lst, label, add=True):
        """Update filter lists"""
        if add:
            if text not in lst:
                lst.append(text)
                await self.safe_send(chat_id, f"✅ <b>{label} Added</b>\n<code>{text}</code>", parse_mode="HTML")
            else:
                await self.safe_send(chat_id, f"⚠️ <b>{label} Exists</b>\n<code>{text}</code>", parse_mode="HTML")
        else:
            if text in lst:
                lst.remove(text)
                await self.safe_send(chat_id, f"❌ <b>{label} Removed</b>\n<code>{text}</code>", parse_mode="HTML")
            else:
                await self.safe_send(chat_id, f"⚠️ <b>{label} Not Found</b>\n<code>{text}</code>", parse_mode="HTML")
    
    async def safe_send(self, chat_id, text, **kwargs):
        """Safely send formatted messages with HTML + small caps + expandable blockquote."""
        try:
            # Ensure clean UTF-8
            text = text.encode('utf-8', 'ignore').decode()

            # Convert literal "\\n" sequences into newlines
            text = text.replace("\\n", "\n")

            # Load translation maps
            small_cap = await small_caps()
            large_num = await large_nums()

            # Capitalize each word, preserving newlines
            lines = text.split("\n")
            capitalized_lines = [" ".join(w.capitalize() for w in line.split()) for line in lines]
            text = "\n".join(capitalized_lines)

            # Apply small‑caps and large numbers
            text = text.translate(small_cap).translate(large_num)

            # Basic HTML/URL fixes
            text = (
                text
                .replace("ʙ>", "b>")
                .replace("ɪ>", "i>")
                .replace("ᴄᴏᴅᴇ>", "code>")
                .replace("ᴘʀᴇ>", "pre>")
                .replace("<ʙʟᴏᴄᴋǫᴜᴏᴛᴇ Exᴘᴀɴᴅᴀʙʟᴇ>", "<blockquote expandable>")
                .replace("ʙʟᴏᴄᴋǫᴜᴏᴛᴇ>", "blockquote>")
                .replace("<ᴀ Hʀᴇғ=", "<a href=")
                .replace("ᴀ>", "a>")
                .replace("<ᴀ", "<a")
                .replace("</ᴀ", "</a")
                .replace("ʜᴛᴛᴘs://ᴛ.ᴍᴇ", "https://t.me")
                .replace("ᴛ.ᴍᴇ", "t.me")
                .replace("ᴏᴘᴇɴᴍᴇssᴀɢᴇ", "openmessage")
                .replace("ᴜsᴇʀ_ɪᴅ", "user_id")
                .replace("ᴛɢ://", "tg://")
                .replace("[a href", "<a href")
            )

            # Fix malformed tg openmessage hrefs missing closing quote
            text = re.sub(
                r"(<a href='tg://openmessage\?user_id=\d+)(>)([^<]*>)(</a>)",
                r"\1'\2\3\4",
                text
            )

            # Fix nested <b> around links causing unbalanced tags
            text = re.sub(r"<b>\[<b><a", "<b>[<a", text)
            text = re.sub(r"</a><b>\]", "</a>]", text)

            # Send the fully‑processed message
            return await self.bot.send_message(chat_id, text, **kwargs)

        except Exception as e:
            self.logger.exception(f"Failed to send message: {e}")
            return None

    def to_gtext(self, input_text: str) -> str:
        # 1) Extract all digit runs
        raw_digits = re.findall(r'\d+', input_text)
        if not raw_digits:
            return ""

        # 2) Normalize: keep only 10-digit or 12-digit (starting with 91) numbers
        normalized = []
        for d in raw_digits:
            if len(d) == 12 and d.startswith("91"):
                normalized.append(int(d))
            elif len(d) == 10:
                normalized.append(int("91" + d))
            # Skip invalid lengths like 11, 13, etc.

        if not normalized:
            return ""

        # 3) Use the first valid number as the starting point
        start = normalized[0]
        count = len(normalized)

        # 4) Generate sequence
        sequence = [str(start + i).strip() for i in range(count)]

        # 5) Join with newlines only
        return "\n".join(sequence)

    async def safe_callback_query(self, callback_query_id, text=None, **kwargs):
        """Safely answer callback queries"""
        try:
            if text:
                text = text.encode('utf-8', 'ignore').decode('utf-8') 
                text = text[0].upper() + text[1:]  # Capitalize first letter
            await self.bot.answer_callback_query(callback_query_id, text, **kwargs)
        except Exception as e:
            self.logger.exception(f"Failed to answer callback query: {e}")


    async def shutdown(self):
        """Clean up clients and tasks on shutdown"""
        if self.forward_client:
            try:
                await self.forward_client.disconnect()
                if hasattr(self, 'forward_client_task'):
                    self.forward_client_task.cancel()
                    try:
                        await self.forward_client_task
                    except asyncio.CancelledError:
                        pass
            except Exception as e:
                self.logger.warning(f"Shutdown error: {e}")
        
        for user_id, client in list(self.contact_clients.items()):
            try:
                await client.disconnect()
            except Exception:
                pass
            finally:
                self.contact_clients.pop(user_id, None)
                
    async def start_forward_client(self):
        """Connect, authorize, cache peers, and start the loop."""
        try:
            # Ensure session is loaded and authorized
            await self.forward_client.start()
            # Cache entities for forwarding
            await self._cache_peers()
            self.logger.info("Forward client ready and peers cached")
            # Run background loop
            self.forward_client_task = asyncio.create_task(
                self.forward_client.run_until_disconnected()
            )
        except Exception as e:
            self.logger.exception("Client error: %s", e)
            if self.bot:
                await self.safe_send(ADMIN_USER_ID, f"<b>Client Error</b>\n<code>{e}</code>")

    async def _cache_peers(self):
        """Resolve and store source & destination as peer objects."""
        self.peers = {}
        for chat in [*self.source_chats, self.dest_chat]:
            username = chat if chat.startswith('@') else f'@{chat}'
            ent = await self.forward_client.get_entity(username)
            self.peers[chat] = ent
            self.logger.info(f"Cached peer {chat} -> {ent}")

    async def _forward_event(self, event: events.NewMessage.Event):
        if not self.enabled or not self.bot:
            return
        txt = event.message.text or ''
        app_match = any(re.search(rf'\b{re.escape(app)}\b', txt, re.IGNORECASE)
                        for app in self.app_list) if self.app_list else True
        country_match = any(re.search(rf'\b{re.escape(c)}\b', txt, re.IGNORECASE)
                             for c in self.country_list) if self.country_list else True
        if app_match and country_match:
            try:
                await self.forward_client.forward_messages(
                    self.peers[self.dest_chat],
                    event.message,
                    silent=True
                )
                log_msg = f"✅ Forwarded message: {event.message.id}"
                self.logger.info(log_msg)
                self.log_buffer.append(log_msg)
            except Exception as e:
                error_msg = f"❌ Forward error: {e}"
                self.logger.error(error_msg)
                self.log_buffer.append(error_msg)
                if self.bot:
                    await self.safe_send(ADMIN_USER_ID, f"⚠️ <b>Forward Error</b>\n<code>{error_msg}</code>")

    # Contact checker methods
    async def start_contact_login(self, user_id: int, chat_id: int):
        """Initiate login flow for contact checker"""
        self.login_states[user_id] = {
            'state': 'awaiting_phone',
            'chat_id': chat_id
        }
        await self.safe_send(
            chat_id,
            "📱 <b>Contact Checker Login</b>\n\n"
            "Send your phone number (with country code, without '+' or spaces):\n"
            "<i>Example:</i> <code>918372673883</code>",
            parse_mode="HTML",
            reply_markup=ForceReply(selective=True)
        )
    
    async def logout_user(self, user_id: int, chat_id: int, force=False, file_path=None):
        """Logout user and clean up session"""
        session_path = file_path or self._contact_session_file(user_id)
        if os.path.exists(session_path):
            try:
                os.remove(session_path)
                self.logger.info(f"Removed session file for user {user_id}")
            except Exception as e:
                self.logger.warning(f"Could not remove session file: {e}")

        if force or user_id in self.login_states:
            self.login_states.pop(user_id, None)

        await self.safe_send(chat_id, "✅ <b>Logged Out</b>\nContact checker session cleared", parse_mode="HTML")

    async def handle_login_message(self, message: Message):
        """Handle login process steps"""
        user_id = message.from_user.id
        state_data = self.login_states.get(user_id, {"state": "logged_out"})
        if not state_data:
            return

        text = message.text.strip()
        chat_id = state_data['chat_id']

        if state_data['state'] == 'awaiting_phone':
            phone = ''.join(filter(str.isdigit, text))
            if not phone or len(phone) < 8 or len(phone) > 15:
                await self.safe_send(chat_id, "❌ <b>Invalid Phone</b>\nSend digits only (e.g., 918372673883)", parse_mode="HTML")
                return

            session_path = self._contact_session_file(user_id)
            client = None
            try:
                # FIX: Create client with proper settings
                client = TelegramClient(
                    session_path, 
                    CONTACT_API_ID, 
                    CONTACT_API_HASH,
                    connection_retries=3,
                    auto_reconnect=False  # Important for short-lived operations
                )

                # FIX: Use single connection attempt with timeout
                await asyncio.wait_for(client.connect(), timeout=10)

                # FIX: Single code request with timeout
                await asyncio.wait_for(client.send_code_request(phone), timeout=10)

                state_data.update({
                    'state': 'awaiting_code',
                    'phone': phone,
                    'client': client
                })
                await self.bot.send_message(
                    chat_id,
                    "<a href='https://i.ibb.co/bM7nJ5bv/IMG-20250629-063110-295.jpg'>✉️</a> <b>Code Sent</b>\nPlease reply with the 5-digit code:",
                    parse_mode="HTML",
                    reply_markup=ForceReply(selective=True),
                    disable_web_page_preview=False
                )

            except errors.FloodWaitError as fwe:
                error_msg = f"⏳ <b>Flood Wait</b>\nTry again in {fwe.seconds} seconds"
                await self.safe_send(chat_id, error_msg, parse_mode="HTML")
                await self.logout_user(user_id, chat_id, force=True)
        
            except (errors.PhoneNumberInvalidError, errors.PhoneNumberBannedError):
                error_msg = "❌ <b>Invalid Phone</b>\nPlease check your number"
                await self.safe_send(chat_id, error_msg, parse_mode="HTML")
                await self.logout_user(user_id, chat_id, force=True)
        
            except asyncio.TimeoutError:
                error_msg = "⌛ <b>Connection Timeout</b>\nPlease try again later"
                await self.safe_send(chat_id, error_msg, parse_mode="HTML")
                await self.logout_user(user_id, chat_id, force=True)

            except Exception as e:
                error_msg = f"❌ <b>Error</b>\n<code>{html.escape(str(e))}</code>"
                await self.safe_send(chat_id, error_msg, parse_mode="HTML")
                await self.logout_user(user_id, chat_id, force=True)

            finally:
                # Ensure client is disconnected if not stored in state
                if client and ('client' not in state_data or state_data['client'] != client):
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
        elif state_data['state'] == 'awaiting_code':
            if not re.match(r'^\d{5}$', text):
                await self.safe_send(chat_id, "❌ <b>Invalid Code</b>\nSend 5-digit code only", parse_mode="HTML")
                return

            client = state_data['client']
            try:
                await client.sign_in(state_data['phone'], text)
                await self.safe_send(chat_id, "✅ <b>Login Successful</b>\nYou can now check numbers", parse_mode="HTML")
                await client.disconnect()
                self.login_states[user_id]['state'] = 'logged_in'
                if user_id == ADMIN_USER_ID:
                    await self.start_forward_client()


            except errors.SessionPasswordNeededError:
                state_data['state'] = 'awaiting_password'
                await self.safe_send(chat_id, "🔐 <b>2FA Required</b>\nPlease send your password:", parse_mode="HTML", reply_markup=ForceReply(selective=True))

            except errors.PhoneCodeInvalidError:
                await self.safe_send(chat_id, "❌ <b>Invalid Code</b>\nPlease request a new code", parse_mode="HTML")
                await self.logout_user(user_id, chat_id, force=True)

            except Exception as e:
                await self.safe_send(chat_id, f"❌ <b>Login Failed</b>\n<code>{str(e)}</code>", parse_mode="HTML")
                await self.logout_user(user_id, chat_id, force=True)

        elif state_data['state'] == 'awaiting_password':
            client = state_data['client']
            try:
                await client.sign_in(password=text)
                await self.safe_send(chat_id, "✅ <b>Login Successful</b>\nYou can now check numbers", parse_mode="HTML")
                await client.disconnect()
                self.login_states[user_id]['state'] = 'logged_in'
            except Exception as e:
                await self.safe_send(chat_id, f"❌ <b>2FA Failed</b>\n<code>{str(e)}</code>", parse_mode="HTML")

    async def check_numbers_registered(self, client: TelegramClient, numbers: List[str]) -> List[Tuple[str, Optional[types.User]]]:
        """Check if numbers are registered on Telegram"""
        contacts = [
            types.InputPhoneContact(
                client_id=idx,
                phone=num,
                first_name=f"Check_{idx}",
                last_name=""
            ) for idx, num in enumerate(numbers)
        ]
        
        try:
            import_result = await client(functions.contacts.ImportContactsRequest(contacts))
            user_map = {user.phone: user for user in import_result.users 
                        if isinstance(user, types.User) and user.phone}
            
            # Clean up imported contacts
            if import_result.imported:
                await client(functions.contacts.DeleteContactsRequest(
                    id=[types.InputUser(user_id=u.id, access_hash=u.access_hash) 
                        for u in import_result.users]
                ))
                
            return [(num, user_map.get(num)) for num in numbers]
        except Exception as e:
            self.logger.error(f"Contact check error: {e}")
            raise

    async def process_numbers(self, user_id: int, chat_id: int, numbers: List[str]):
        """Process and display number check results"""
        max_retries = 2
        retry_delay = 1  # seconds
        session_path = self._contact_session_file(user_id)
        lock = self.session_manager.get_lock(user_id)
        
        for attempt in range(max_retries):
            try:
                async with lock:
                    async with TelegramClient(session_path, CONTACT_API_ID, CONTACT_API_HASH) as client:
                        if not await client.is_user_authorized():
                            await self.safe_send(
                                chat_id,
                                "❌ <b>Session Expired</b>\nPlease log in again",
                                parse_mode="HTML"
                            )
                            return
                        main = []
                        for number in numbers:
                            if not number:
                                continue
                            results = await self.check_numbers_registered(client, number)
                            if results:
                                main.extend(results)
                        return main
            except (sqlite3.OperationalError, errors.FloodWaitError) as e:
                if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                    self.logger.warning(f"Database locked, retrying in {retry_delay}s")
                    await asyncio.sleep(retry_delay)
                elif isinstance(e, errors.FloodWaitError):
                    await self.safe_send(
                        chat_id,
                        f"⏳ <b>Flood Wait</b>\nPlease try again in {e.seconds} seconds",
                        parse_mode="HTML"
                    )
                    return
                else:
                    await self.safe_send(
                        chat_id,
                        f"❌ <b>Check Error</b>\n<code>{str(e)}</code>",
                        parse_mode="HTML"
                    )
                    return

# Instantiate ForwardManager
forward_manager = ForwardManager(
    source_chats=["TGTECHOTP", "tg_tech_receiver_bot"],
    dest_chat="flashthefiresms",
)

async def init_managers(user_manager: None, order_manager=None, bot: Optional[AsyncTeleBot] = None) -> bool:
    return await forward_manager.init_managers(bot)

async def register_handlers(bot: AsyncTeleBot):
    await forward_manager.register_handlers(bot)


__all__ = ['init_managers', 'register_handlers', 'forward_manager']