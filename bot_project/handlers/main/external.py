import asyncio
import logging
import os
import re
import sqlite3
from typing import List, Dict, Any, Optional, Tuple, Set

from telethon import TelegramClient, functions, types, errors, events
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ForceReply, Message

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
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

# UI Enhancement Functions
async def small_caps() -> dict:
    """Returns translation table for small caps conversion"""
    return str.maketrans(
        'abcdefghijklmnopqrstuvwxyz1234567890',
        'ᴀʙᴄᴅᴇғɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢ𝟷𝟸𝟹𝟺𝟻𝟼𝟽𝟾𝟿𝟶'
    )

async def large_nums() -> dict:
    """Returns translation table for large numbers conversion"""
    return str.maketrans(
        '𝟷𝟸𝟹𝟺𝟻𝟼𝟽𝟾𝟿𝟶',
        '1234567890'
    )

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
    # Callback identifiers
    entry = "ForwardManager:"
    CB_START = entry + "start"
    CB_STOP = entry + "stop"
    CB_SHOW_LOGS = entry + "show_logs"
    CB_TOGGLE_LOGS = entry + "toggle_logs"
    CB_CHECK_NUM = entry + "check_nums"
    CB_LOGOUT = entry + "logout"
    CB_LOGIN = entry + "login"
    CB_ADD_APP = entry + "add_app"
    CB_REMOVE_APP = entry + "remove_app"
    CB_ADD_COUNTRY = entry + "add_country"
    CB_REMOVE_COUNTRY = entry + "remove_country"
    CB_SHOW_LISTS = entry + "show_lists"
    cb_list = [CB_START, CB_STOP, CB_SHOW_LOGS, CB_TOGGLE_LOGS, CB_CHECK_NUM, 
               CB_LOGOUT, CB_LOGIN, CB_ADD_APP, CB_REMOVE_APP, 
               CB_ADD_COUNTRY, CB_REMOVE_COUNTRY, CB_SHOW_LISTS]

    def __init__(
        self,
        source_chats: List[str],
        dest_chat: str
    ):
        self.source_chats = source_chats
        self.dest_chat = dest_chat
        self.bot: Optional[AsyncTeleBot] = None
        self.forward_client: Optional[TelegramClient] = None
        self.contact_clients: Dict[int, TelegramClient] = {}
        self.session_manager = SessionManager()

        # Control states
        self.enabled = False
        self.log_buffer: List[str] = []
        self.logging_enabled = True
        self.app_list: List[str] = []
        self.country_list: List[str] = []
        self.active_tasks: Set[asyncio.Task] = set()

        # Setup logger
        self.logger = logging.getLogger("ForwardManager")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        self.logger.handlers.clear()
        
        # States
        self.login_states: Dict[int, Dict] = {}
        self.filter_states: Dict[int, str] = {}

    async def init_managers(self, bot: AsyncTeleBot) -> bool:
        """Initialize bot managers"""
        try:
            self.bot = bot
            self._setup_logging()
            await self.start_forward_client()
            return True
        except Exception as e:
            self.logger.exception("Init error: %s", e)
            await self.send_to_admin(f"<b>❌ Initialization Failed</b>\n<code>{e}</code>")
            return False

    def _session_file(self, user_id: int) -> str:
        return os.path.join(SESSIONS_DIR, f"forward_{user_id}.session")

    def _contact_session_file(self, user_id: int) -> str:
        return os.path.join(SESSIONS_DIR, f"contact_{user_id}.session")

    def _setup_logging(self):
        """Configure logging handlers"""
        if not self.bot:
            return
            
        self.logger.handlers.clear()
        if self.logging_enabled:
            handler = TelegramLogHandler(self.bot, ADMIN_USER_ID)
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
            self.logger.addHandler(handler)
            self.logger.info("Telegram logging enabled")

    def _control_keyboard(self, user_id: int) -> InlineKeyboardMarkup:
        """Generate control panel keyboard"""
        kb = InlineKeyboardMarkup()
        # Admin-only controls
        if user_id == ADMIN_USER_ID:
            # Row 1: Start/Stop and Login/Logout
            kb.row(
                InlineKeyboardButton("▶️ Start" if not self.enabled else "⏹ Stop", 
                                   callback_data=self.CB_START if not self.enabled else self.CB_STOP),
                InlineKeyboardButton("📋 Filters", callback_data=self.CB_SHOW_LISTS)
            )
            # Row 2: Logs and Logging toggle
            kb.row(
                InlineKeyboardButton("📝 Logs", callback_data=self.CB_SHOW_LOGS),
                InlineKeyboardButton("💡 Logging", callback_data=self.CB_TOGGLE_LOGS)
            )
            # Row 3: App management
            kb.row(
                InlineKeyboardButton("➕ App", callback_data=self.CB_ADD_APP),
                InlineKeyboardButton("➖ App", callback_data=self.CB_REMOVE_APP)
            )
            # Row 4: Country management
            kb.row(
                InlineKeyboardButton("🌍 Country", callback_data=self.CB_ADD_COUNTRY),
                InlineKeyboardButton("🗺️ Remove", callback_data=self.CB_REMOVE_COUNTRY)
            )
        # Row 5: Login/Logout and Numbers
        kb.row(
            InlineKeyboardButton("🔑 Login" if not os.path.exists(self._contact_session_file(user_id)) 
                else "🚪 Logout", 
                callback_data=self.CB_LOGIN if not os.path.exists(self._contact_session_file(user_id)) 
                else self.CB_LOGOUT),
            InlineKeyboardButton("📞 Numbers", callback_data=self.CB_CHECK_NUM)
        )
        return kb

    async def register_handlers(self):
        """Register bot event handlers"""
        if not self.bot:
            return

        @self.forward_client.on(events.NewMessage(chats=self.source_chats))
        async def on_new(event):
            if not self.enabled:
                return
            try:
                await self._forward_event(event)
            except (errors.ConnectionError, errors.AlreadyInConversationError) as e:
                self.logger.warning(f"Connection issue: {e}")
                await asyncio.sleep(5)
                await self.start_forward_client()  # Reinitialize client

        @self.bot.message_handler(commands=['user_control'])
        async def cmd_control(message: Message):
            user_id = message.from_user.id
            await self.bot.send_message(
                message.chat.id,
                "⚡ <b>Tᴇʟᴇɢʀᴀᴍ Cᴏɴᴛʀᴏʟ Pᴀɴᴇʟ</b>",
                parse_mode="HTML",
                reply_markup=self._control_keyboard(user_id)
            )

        @self.bot.callback_query_handler(func=lambda call: call.data in self.cb_list)
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
                if self.login_states.get(user_id).get('state') == 'logged_in' and os.path.exists(self._contact_session_file(user_id)):
                    msg = await self.safe_send(
                        chat_id,
                        "<b>📱 Phone Number Checker</b>\n\nSend up to 19 phone numbers (one per line, without '+' or spaces):\n\n<code>919027839273</code>\n<code>918372673883</code>\n<code>918373737373</code>",
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
                await self.bot.edit_message_reply_markup(
                    chat_id=chat_id, 
                    message_id=call.message.message_id, 
                    reply_markup=self._control_keyboard(user_id))
            except Exception:
                pass

        @self.bot.message_handler(func=lambda m: m.reply_to_message and m.reply_to_message.message_id in self.filter_states or m.from_user.id in self.login_states)
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
                    all_numbers = [
                        num.strip() 
                        for num in text.splitlines() 
                        if num.strip().isdigit()
                    ][:100]

                    chunks = [
                        all_numbers[i : i + 19] 
                        for i in range(0, len(all_numbers), 19)
                    ]

                    main_results = []
                    response = []
                    for chunk in chunks:
                        if not chunk:
                            continue
                        results = await self.process_numbers(user_id, chat_id, chunk)
                        main_results.extend(results)

                        for num, user in results:
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

                        # Send results
                    if not response:
                        response.append("❌ <b>Nᴏ Pʀᴏᴠɪᴅᴇᴅ Nᴜᴍʙᴇʀs Aʀᴇ Rᴇɢɪsᴛᴇʀᴇᴅ</b>")
                    result_text = "\n\n".join(response)
                    last_number = all_numbers[-1] if all_numbers else None
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
        try:
            session_path = self._session_file(ADMIN_USER_ID)
            self.forward_client = TelegramClient(
                session_path, 
                FORWARD_API_ID, 
                FORWARD_API_HASH,
                connection_retries=5,
                auto_reconnect=True
            )
            await self.forward_client.connect()
            
            if not await self.forward_client.is_user_authorized():
                await self.send_to_admin(" Forward Client Not Authorized ")
                return
            
            # Add proper task management
            self.forward_client_task = asyncio.create_task(
                self.forward_client.run_until_disconnected()
            )
            
            # Add DC migration handling
            @self.forward_client.on(events.DCChangeEvent)
            async def handle_dc_change(event):
                self.logger.info(f"Migrated to DC {event.new_dc}")
                await asyncio.sleep(1)  # Brief pause before reconnecting
                await self.forward_client.reconnect()
            
            self.logger.info("Forward client started")
        except Exception as e:
            self.logger.exception("Client error: %s", e)
            await self.send_to_admin(f" <b>Client Error</b>\n<code>{e}</code>")

    async def _forward_event(self, event: events.NewMessage.Event):
        """Handle new messages and forward them"""
        if not self.enabled or not self.bot:
            return
            
        txt = event.message.text or ''
        
        # Apply filters
        app_match = any(re.search(rf'\b{re.escape(app)}\b', txt, re.IGNORECASE) 
                      for app in self.app_list) if self.app_list else True
        country_match = any(re.search(rf'\b{re.escape(c)}\b', txt, re.IGNORECASE) 
                        for c in self.country_list) if self.country_list else True
        
        if app_match and country_match:
            try:
                await self.forward_client.forward_messages(
                    self.dest_chat,
                    event.message,
                    silent=True
                )
                log_msg = f"✅ Forwarded message: {event.message.id}"
                self.logger.info(log_msg)
                self.log_buffer.append(log_msg)
            except Exception as e:
                error_msg = f"❌ Forward error: {str(e)}"
                self.logger.error(error_msg)
                self.log_buffer.append(error_msg)
                await self.send_to_admin(f"⚠️ <b>Forward Error</b>\n<code>{error_msg}</code>")

    async def send_to_admin(self, message: str):
        """Send message to admin"""
        if self.bot:
            await self.safe_send(ADMIN_USER_ID, message, parse_mode="HTML")

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
            "<i>Example: 254700112233</i>",
            parse_mode="HTML",
            reply_markup=ForceReply(selective=True)
        )
    
    async def logout_user(self, user_id: int, chat_id: int, force=False):
        """Logout user and clean up session"""
        session_path = self._contact_session_file(user_id)
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
        state_data = self.login_states.get(user_id)
        if not state_data:
            return

        text = message.text.strip()
        chat_id = state_data['chat_id']

        if state_data['state'] == 'awaiting_phone':
            if not re.match(r'^\d{8,15}$', text):
                await self.safe_send(chat_id, "❌ <b>Invalid Phone</b>\nSend digits only (e.g., 254700112233)", parse_mode="HTML")
                return
            for _ in range(10):
                try:
                    session_path = self._contact_session_file(user_id)
                    break
                except Exception as e:
                    pass
                    await asyncio.sleep(0.1)

            try:
                for _ in range(15):
                    try:
                        client = TelegramClient(session_path, CONTACT_API_ID, CONTACT_API_HASH)
                        break
                    except Exception as e:
                        pass
                    await asyncio.sleep(0.5)
                for _ in range(10):
                    try:
                        await client.connect()
                        break
                    except Exception as e:
                        pass
                    await asyncio.sleep(1)
                for _ in range(5):
                    try:
                        await client.send_code_request(text)
                        break
                    except Exception as e:
                        pass
                    await asyncio.sleep(2)

                state_data.update({
                    'state': 'awaiting_code',
                    'phone': text,
                    'client': client
                })
                await self.bot.send_message(chat_id, "<a href='https://i.ibb.co/bM7nJ5bv/IMG-20250629-063110-295.jpg'>✉️</a> <b>Code Sent</b>\nPlease reply with the 5-digit code:", parse_mode="HTML", reply_markup=ForceReply(selective=True))
            except errors.FloodWaitError as fwe:
                await self.safe_send(chat_id, f"⏳ <b>Flood Wait</b>\nTry again in {fwe.seconds} seconds", parse_mode="HTML")
                await self.logout_user(user_id, chat_id, force=True)

            except errors.PhoneNumberInvalidError:
                await self.safe_send(chat_id, "❌ <b>Invalid Phone</b>\nPlease check your number", parse_mode="HTML")
                await self.logout_user(user_id, chat_id, force=True)

            except Exception as e:
                await self.safe_send(chat_id, f"❌ <b>Error</b>\n<code>{str(e)}</code>", parse_mode="HTML")
                await self.logout_user(user_id, chat_id, force=True)

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
                            
                        results = await self.check_numbers_registered(client, numbers)
                        return results
                    break  # Break on success
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
    await forward_manager.register_handlers()


__all__ = ['init_managers', 'register_handlers', 'forward_manager']