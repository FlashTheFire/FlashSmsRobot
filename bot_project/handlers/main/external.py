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

# Setup logging
tlogging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=tlogging_format, handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

# Constants
ADMIN_USER_ID = 1889471360
SESSIONS_DIR = "sessions"
os.makedirs(SESSIONS_DIR, exist_ok=True)
MAX_PARALLEL_CLIENTS = 5
BATCH_SIZE = 19
MAX_NUMBERS_PER_CLIENT = 100

# API Credentials
FORWARD_API_ID = 26383754
FORWARD_API_HASH = "f743596f09f383e7bbcc62ce62367f06"
CONTACT_API_ID = 20729573
CONTACT_API_HASH = "6bc09cbaa7d0471944875c202fec8b5b"
DESTINATION_CHAT_ID = 5716978793

class UserAccount:
    """Represents a user account with session details"""
    def __init__(self, user_id: int, account_id: str, phone: str, session_file: str):
        self.user_id = user_id
        self.account_id = account_id
        self.phone = phone
        self.session_file = session_file
        self.full_name: Optional[str] = None
        self.username: Optional[str] = None
        self.telegram_id: Optional[int] = None
        self.last_checked: Optional[datetime] = None

class SessionManager:
    """Manages user session locks and accounts"""
    def __init__(self):
        self.locks: Dict[int, asyncio.Lock] = {}
        self.user_accounts: Dict[int, Dict[str, UserAccount]] = {}
        self.active_accounts: Dict[int, str] = {}  # user_id -> active_account_id
        
    def get_lock(self, user_id: int) -> asyncio.Lock:
        if user_id not in self.locks:
            self.locks[user_id] = asyncio.Lock()
        return self.locks[user_id]
    
    def add_account(self, account: UserAccount):
        if account.user_id not in self.user_accounts:
            self.user_accounts[account.user_id] = {}
        self.user_accounts[account.user_id][account.account_id] = account
        
    def get_account(self, user_id: int, account_id: str) -> Optional[UserAccount]:
        return self.user_accounts.get(user_id, {}).get(account_id)
    
    def get_accounts(self, user_id: int) -> List[UserAccount]:
        return list(self.user_accounts.get(user_id, {}).values())
    
    def set_active_account(self, user_id: int, account_id: str):
        self.active_accounts[user_id] = account_id
        
    def get_active_account(self, user_id: int) -> Optional[UserAccount]:
        account_id = self.active_accounts.get(user_id)
        if account_id:
            return self.get_account(user_id, account_id)
        return None
    
    def remove_account(self, user_id: int, account_id: str):
        if user_id in self.user_accounts and account_id in self.user_accounts[user_id]:
            del self.user_accounts[user_id][account_id]
            if user_id in self.active_accounts and self.active_accounts[user_id] == account_id:
                del self.active_accounts[user_id]

class ForwardManager:
    """Manages message forwarding and number checking with multiple accounts"""
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
    CB_SWITCH_ACCOUNT = entry + "switch_account"
    CB_ACCOUNT_DETAILS = entry + "account_details"
    CB_CHECK_MESSAGES = entry + "check_messages"
    cb_list = [CB_START, CB_STOP, CB_SHOW_LOGS, CB_TOGGLE_LOGS, CB_CHECK_NUM,
               CB_LOGIN, CB_LOGOUT, CB_ADD_APP, CB_REMOVE_APP, CB_SWITCH_ACCOUNT,
               CB_ADD_COUNTRY, CB_REMOVE_COUNTRY, CB_SHOW_LISTS, CB_ACCOUNT_DETAILS, CB_CHECK_MESSAGES]
    def __init__(self, source_chats: List[str], dest_chat: str):
        self.source_chats = source_chats
        self.dest_chat = dest_chat
        self.bot = None
        self.contact_clients = {}
        self.session_manager = SessionManager()
        
        # Initialize forward client with phone number from environment
        session_path = self._contact_session_file(ADMIN_USER_ID, "admin")
        self.forward_client = TelegramClient(
            session_path, 
            FORWARD_API_ID, 
            FORWARD_API_HASH,
            connection_retries=5,
            auto_reconnect=True
        )
        
        # Set phone number from environment variable
        self.admin_phone = os.getenv('ADMIN_PHONE', "254798325694")
        if not self.admin_phone:
            self.logger.error("ADMIN_PHONE environment variable not set!")

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
        self.account_states: Dict[int, Dict] = {}

    def _contact_session_file(self, user_id: int, account_id: str) -> str:
        return os.path.join(SESSIONS_DIR, f"contact_{user_id}_{account_id}.session")

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

    def _setup_logging(self):
        if not self.bot:
            return
        self.logger.handlers.clear()
        if self.logging_enabled:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
            self.logger.addHandler(handler)
            self.logger.info("Console logging enabled")

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
        
        # Account management buttons
        active_account = self.session_manager.get_active_account(user_id)
        account_text = active_account.account_id if active_account else "No Account"
        kb.row(
            InlineKeyboardButton(f"🔑 {account_text}", callback_data=self.CB_SWITCH_ACCOUNT),
            InlineKeyboardButton("🔍 Details", callback_data=self.CB_ACCOUNT_DETAILS)
        )
        kb.row(
            InlineKeyboardButton("📩 Check Msgs", callback_data=self.CB_CHECK_MESSAGES),
            InlineKeyboardButton("📞 Numbers", callback_data=self.CB_CHECK_NUM)
        )
        return kb

    async def get_account_details(self, user_id: int, account_id: str) -> str:
        """Retrieve session details for a given account"""
        account = self.session_manager.get_account(user_id, account_id)
        if not account:
            return "❌ Account not found"
        
        details = [
            f"📱 <b>Account:</b> <code>{account.account_id}</code>",
            f"📞 <b>Phone:</b> <code>{account.phone}</code>",
            f"👤 <b>Name:</b> {account.full_name or 'N/A'}",
            f"🔗 <b>Username:</b> @{account.username}" if account.username else "🔗 <b>Username:</b> N/A",
            f"🆔 <b>Telegram ID:</b> <code>{account.telegram_id}</code>" if account.telegram_id else "🆔 <b>Telegram ID:</b> N/A",
            f"⏱️ <b>Last Checked:</b> {account.last_checked.strftime('%Y-%m-%d %H:%M')}" if account.last_checked else "⏱️ <b>Last Checked:</b> Never"
        ]
        return "\n".join(details)

    async def check_account_messages(self, user_id: int, account_id: str) -> str:
        """Check for upcoming messages in an account"""
        account = self.session_manager.get_account(user_id, account_id)
        if not account:
            return "❌ Account not found"
        
        try:
            session_path = self._contact_session_file(user_id, account_id)
            async with TelegramClient(session_path, CONTACT_API_ID, CONTACT_API_HASH) as client:
                if not await client.is_user_authorized():
                    return "❌ Session expired. Please log in again."
                
                # Get unread messages
                dialogs = await client.get_dialogs(limit=10)
                unread = [d for d in dialogs if d.unread_count > 0]
                
                if not unread:
                    return "📭 No unread messages"
                
                messages = []
                for dialog in unread[:5]:  # Limit to 5 conversations
                    entity = dialog.entity
                    name = entity.title if hasattr(entity, 'title') else entity.first_name
                    messages.append(f"💬 {name}: {dialog.unread_count} unread")
                
                account.last_checked = datetime.now()
                return "📬 <b>Unread Messages:</b>\n" + "\n".join(messages)
        except Exception as e:
            return f"❌ Error checking messages: {str(e)}"

    async def process_numbers(self, user_id: int, chat_id: int, numbers: List[str]) -> List[Tuple[str, Optional[types.User]]]:
        """Process numbers with parallel clients and batch processing"""
        accounts = self.session_manager.get_accounts(user_id)
        if not accounts:
            await self.safe_send(chat_id, "❌ No active accounts. Please log in first.")
            return []
        
        # Split numbers into chunks for parallel processing
        num_chunks = [numbers[i:i + MAX_NUMBERS_PER_CLIENT] for i in range(0, len(numbers), MAX_NUMBERS_PER_CLIENT)]
        account_chunks = [accounts[i % len(accounts)] for i in range(len(num_chunks))]
        
        # Limit to MAX_PARALLEL_CLIENTS
        if len(num_chunks) > MAX_PARALLEL_CLIENTS:
            num_chunks = num_chunks[:MAX_PARALLEL_CLIENTS]
            account_chunks = account_chunks[:MAX_PARALLEL_CLIENTS]
        
        # Process chunks in parallel
        tasks = []
        for account, chunk in zip(account_chunks, num_chunks):
            tasks.append(self._process_number_chunk(user_id, account.account_id, chunk))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Aggregate results and handle errors
        final_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(f"Error in chunk {i}: {result}")
                await self.safe_send(chat_id, f"❌ Error processing chunk: {result}")
            elif result:
                final_results.extend(result)
        
        return final_results

    async def _process_number_chunk(self, user_id: int, account_id: str, numbers: List[str]) -> List[Tuple[str, Optional[types.User]]]:
        """Process a chunk of numbers with a specific account"""
        session_path = self._contact_session_file(user_id, account_id)
        try:
            async with TelegramClient(session_path, CONTACT_API_ID, CONTACT_API_HASH) as client:
                if not await client.is_user_authorized():
                    return []
                
                results = []
                # Process in batches of BATCH_SIZE
                for i in range(0, len(numbers), BATCH_SIZE):
                    batch = numbers[i:i + BATCH_SIZE]
                    batch_results = await self.check_numbers_registered(client, batch)
                    results.extend(batch_results)
                return results
        except Exception as e:
            self.logger.error(f"Error processing chunk for account {account_id}: {e}")
            return []

    async def register_handlers(self, bot: AsyncTeleBot):
        self.bot = bot 
        if not self.bot:
            return
        @bot.message_handler(commands=['reconnect'])
        async def cmd_reconnect(message: Message):
            if message.from_user.id == ADMIN_USER_ID:
                try:
                    await forward_manager.shutdown()
                    await asyncio.sleep(2)
                    await forward_manager.start_forward_client()
                    await bot.reply_to(message, "✅ Forward client reconnected")
                except Exception as e:
                    await bot.reply_to(message, f"❌ Error reconnecting: {str(e)}")

        @bot.message_handler(commands=['admin_control'])
        async def cmd_admin_login(message: Message):
            if message.from_user.id == ADMIN_USER_ID:
                await self.safe_send(
                    message.chat.id,
                    "📱 <b>Admin Login</b>\n\n"
                    "Please send your phone number (with country code, no +):\n"
                    "<i>Example:</i> <code>918372673883</code>",
                    parse_mode="HTML",
                    reply_markup=ForceReply(selective=True)
                )
        
        @bot.message_handler(func=lambda m: m.reply_to_message and 
                            m.reply_to_message.text and 
                            "ᴀᴅᴍɪɴ Lᴏɢɪɴ" in m.reply_to_message.text and
                            m.from_user.id == ADMIN_USER_ID)
        async def handle_admin_phone(message: Message):
            await self.handle_admin_login(message)

        @bot.message_handler(commands=['user_control'])
        async def cmd_control(message: Message):
            await bot.send_message(
                message.chat.id,
                "⚡ <b>Telegram Control Panel</b>",
                parse_mode="HTML",
                reply_markup=self._control_keyboard(message.from_user.id)
            )
        
        @self.forward_client.on(events.NewMessage(chats=self.source_chats))
        async def on_new(event):
            """Handle new messages from source chats"""
            if not self.enabled:
                return
            try:
                await self._forward_event(event)
            except (errors.ConnectionSystemEmptyError, errors.AlreadyInConversationError) as e:
                self.logger.warning(f"Connection issue: {e}")
                await asyncio.sleep(5)
        
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
                account = self.session_manager.get_active_account(user_id)
                if account:
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
            
            elif data == self.CB_SWITCH_ACCOUNT:
                user_id = call.from_user.id
                accounts = self.session_manager.get_accounts(user_id)
                
                if not accounts:
                    await self.safe_callback_query(call.id, "No accounts available")
                    return
                
                kb = InlineKeyboardMarkup()
                for account in accounts:
                    kb.add(InlineKeyboardButton(
                        f"{account.account_id} ({account.phone})",
                        callback_data=f"{self.CB_SWITCH_ACCOUNT}:{account.account_id}"
                    ))
                kb.add(InlineKeyboardButton("➕ New Account", callback_data=self.CB_LOGIN))
                
                await self.safe_edit_message(
                    call.message.chat.id,
                    call.message.message_id,
                    "🔑 <b>Select Account</b>",
                    reply_markup=kb
                )
                await self.safe_callback_query(call.id)
            
            elif data.startswith(self.CB_SWITCH_ACCOUNT + ":"):
                account_id = call.data.split(":", 1)[1]
                user_id = call.from_user.id
                self.session_manager.set_active_account(user_id, account_id)
                await self.safe_callback_query(call.id, f"✅ Active: {account_id}")
                # Update control panel
                await self.safe_edit_message(
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=self._control_keyboard(user_id)
                )
            
            elif data == self.CB_ACCOUNT_DETAILS:
                user_id = call.from_user.id
                account = self.session_manager.get_active_account(user_id)
                if account:
                    details = await self.get_account_details(user_id, account.account_id)
                    await self.safe_send(call.message.chat.id, details, parse_mode="HTML")
                else:
                    await self.safe_send(call.message.chat.id, "❌ No active account selected")
                await self.safe_callback_query(call.id)
            
            elif data == self.CB_CHECK_MESSAGES:
                user_id = call.from_user.id
                account = self.session_manager.get_active_account(user_id)
                if account:
                    messages = await self.check_account_messages(user_id, account.account_id)
                    await self.safe_send(call.message.chat.id, messages, parse_mode="HTML")
                else:
                    await self.safe_send(call.message.chat.id, "❌ No active account selected")
                await self.safe_callback_query(call.id)

        @bot.message_handler(func=lambda m: (m.reply_to_message and m.reply_to_message.message_id in self.filter_states) or m.from_user.id in self.login_states)
        async def handle_replies(message: Message):
            user_id = message.from_user.id
            chat_id = message.chat.id
            text = message.text.strip()
            
            if not message.reply_to_message:
                return
                
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
                    ][:20]

                    if not all_numbers:
                        await self.safe_send(chat_id, "❌ No valid numbers provided")
                        return

                    try:
                        results = await self.process_numbers(user_id, chat_id, all_numbers)
                        response = []
                        for num, user in results:
                            if user:
                                username = f"@{user.username}" if user.username else "No username"
                                response.append(
                                    f"✅ <code>{num}</code> [<a href='tg://openmessage?user_id={user.id}'>Open</a>]\n"
                                    f"       • <a href='https://t.me/+{num}'>{username}</a>"
                                )
                            else:
                                response.append(f"❌ <code>{num}</code> - Not registered")
                        
                        if not response:
                            response.append("❌ No valid results")
                            
                        result_text = "\n\n".join(response)
                        markup = InlineKeyboardMarkup()
                        markup.add(
                            InlineKeyboardButton("🔄 Check More Numbers", callback_data=self.CB_CHECK_NUM),
                        )
                        await self.safe_send(
                            chat_id,
                            f"📊 <b>Number Check Results</b>\n\n{result_text}",
                            parse_mode="HTML",
                            reply_markup=markup
                        )

                    except Exception as e:
                        await self.safe_send(chat_id, f"❌ Error: {str(e)}")

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
        """Safely send formatted messages with HTML"""
        try:
            # Basic text sanitization
            text = html.escape(text)
            return await self.bot.send_message(
                chat_id, 
                text, 
                parse_mode="HTML",
                disable_web_page_preview=True,
                **kwargs
            )
        except Exception as e:
            self.logger.exception(f"Failed to send message: {e}")
            return None

    async def safe_callback_query(self, callback_query_id, text=None, **kwargs):
        """Safely answer callback queries"""
        try:
            if text:
                text = text[0].upper() + text[1:]  # Capitalize first letter
            await self.bot.answer_callback_query(callback_query_id, text, **kwargs)
        except Exception as e:
            self.logger.exception(f"Failed to answer callback query: {e}")


    async def shutdown(self):
        """Clean up clients and tasks on shutdown"""
        if self.forward_client:
            try:
                await self.forward_client.disconnect()
            except Exception as e:
                self.logger.warning(f"Shutdown error: {e}")
        
        for user_id, client in list(self.contact_clients.items()):
            try:
                await client.disconnect()
            except Exception:
                pass
            finally:
                self.contact_clients.pop(user_id, None)
    
    async def handle_admin_login(self, message: Message):
        """Handle admin login process"""
        if message.from_user.id != ADMIN_USER_ID:
            return
            
        self.admin_phone = message.text.strip()
        try:
            await self.start_forward_client()
            await self.safe_send(
                ADMIN_USER_ID,
                "✅ <b>Admin session initialized</b>\n"
                "Forwarding service should now work properly",
                parse_mode="HTML"
            )
        except Exception as e:
            await self.safe_send(
                ADMIN_USER_ID,
                f"❌ <b>Login failed</b>\n<code>{html.escape(str(e))}</code>",
                parse_mode="HTML"
            )

    
    async def start_forward_client(self):
        """Connect, authorize, cache peers, and start the loop."""
        try:
            if not self.admin_phone:
                raise ValueError("Admin phone number not configured")
                
            # Modified to use pre-configured phone number
            await self.forward_client.start(phone=lambda: self.admin_phone)
            await self._cache_peers()
            self.logger.info("Forward client ready and peers cached")
            asyncio.create_task(self.forward_client.run_until_disconnected())
        except Exception as e:
            self.logger.exception("Client error: %s", e)
            if self.bot:
                await self.safe_send(
                    ADMIN_USER_ID,
                    f"<b>⚠️ Client Error</b>\n"
                    f"<code>{html.escape(str(e))}</code>\n\n"
                    f"Please re-login using /admin_login",
                    parse_mode="HTML"
                )

    async def _cache_peers(self):
        """Resolve and store source & destination as peer objects."""
        self.peers = {}
        for chat in [*self.source_chats, self.dest_chat]:
            try:
                ent = await self.forward_client.get_entity(chat)
                self.peers[chat] = ent
                self.logger.info(f"Cached peer {chat} -> {ent.id}")
            except Exception as e:
                self.logger.error(f"Error caching peer {chat}: {e}")

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
            'state': 'awaiting_account_id',
            'chat_id': chat_id
        }
        await self.safe_send(
            chat_id,
            "📱 <b>Account Setup</b>\n\n"
            "Send a unique name for this account (e.g., 'work' or 'personal'):",
            parse_mode="HTML",
            reply_markup=ForceReply(selective=True)
        )

    async def logout_user(self, user_id: int, chat_id: int, account_id: str):
        """Logout user and clean up session"""
        session_path = self._contact_session_file(user_id, account_id)
        if os.path.exists(session_path):
            try:
                os.remove(session_path)
                self.logger.info(f"Removed session file for account {account_id}")
            except Exception as e:
                self.logger.warning(f"Could not remove session file: {e}")

        self.session_manager.remove_account(user_id, account_id)
        await self.safe_send(chat_id, "✅ <b>Logged Out</b>\nAccount session cleared", parse_mode="HTML")

    async def safe_edit_message(self, chat_id: int, message_id: int, text: str = None, **kwargs):
        """Safely edit an existing message"""
        try:
            if text:
                # Apply text formatting
                text = html.escape(text)
            await self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                parse_mode="HTML",
                **kwargs
            )
        except Exception as e:
            self.logger.exception(f"Failed to edit message: {e}")

    async def handle_login_message(self, message: Message):
        """Handle login process steps with account management"""
        user_id = message.from_user.id
        state_data = self.login_states.get(user_id, {})
        if not state_data:
            return

        text = message.text.strip()
        chat_id = state_data['chat_id']

        if state_data['state'] == 'awaiting_account_id':
            # Validate account ID
            if not re.match(r"^[a-zA-Z0-9_\-]{3,20}$", text):
                await self.safe_send(
                    chat_id,
                    "❌ <b>Invalid Account ID</b>\n"
                    "Use 3-20 characters (letters, numbers, _, -)",
                    parse_mode="HTML"
                )
                return
            
            # Check if account ID already exists
            if self.session_manager.get_account(user_id, text):
                await self.safe_send(
                    chat_id,
                    "❌ <b>Account ID Exists</b>\n"
                    "Please choose a different name",
                    parse_mode="HTML"
                )
                return
            
            self.login_states[user_id].update({
                'state': 'awaiting_phone',
                'account_id': text
            })
            await self.safe_send(
                chat_id,
                "📱 <b>Contact Checker Login</b>\n\n"
                "Send your phone number (with country code, without '+' or spaces):\n"
                "<i>Example:</i> <code>918372673883</code>",
                parse_mode="HTML",
                reply_markup=ForceReply(selective=True)
            )

        elif state_data['state'] == 'awaiting_phone':
            phone = ''.join(filter(str.isdigit, text))
            if not phone or len(phone) < 8 or len(phone) > 15:
                await self.safe_send(chat_id, "❌ <b>Invalid Phone</b>\nSend digits only", parse_mode="HTML")
                return

            account_id = state_data['account_id']
            session_path = self._contact_session_file(user_id, account_id)
            
            # Create account object
            account = UserAccount(user_id, account_id, phone, session_path)
            self.session_manager.add_account(account)
            self.session_manager.set_active_account(user_id, account_id)
            
            try:
                client = TelegramClient(
                    session_path, 
                    CONTACT_API_ID, 
                    CONTACT_API_HASH,
                    connection_retries=3,
                    auto_reconnect=False
                )
                await client.connect()
                await client.send_code_request(phone)
                
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

            except Exception as e:
                error_msg = f"❌ <b>Login Error</b>\n<code>{str(e)}</code>"
                await self.safe_send(chat_id, error_msg, parse_mode="HTML")
                del self.login_states[user_id]

        elif state_data['state'] == 'awaiting_code':
            if not re.match(r'^\d{5}$', text):
                await self.safe_send(chat_id, "❌ <b>Invalid Code</b>\nSend 5-digit code only", parse_mode="HTML")
                return

            client = state_data['client']
            account = self.session_manager.get_account(user_id, state_data['account_id'])
            try:
                await client.sign_in(state_data['phone'], text)
                
                # Retrieve and store account details
                me = await client.get_me()
                account = self.session_manager.get_account(user_id, state_data['account_id'])
                if account:
                    account.full_name = f"{me.first_name or ''} {me.last_name or ''}".strip()
                    account.username = me.username
                    account.telegram_id = me.id
                
                await self.safe_send(chat_id, "✅ <b>Login Successful</b>\nYou can now check numbers", parse_mode="HTML")
                await client.disconnect()
                del self.login_states[user_id]

            except errors.SessionPasswordNeededError:
                state_data['state'] = 'awaiting_password'
                await self.safe_send(chat_id, "🔐 <b>2FA Required</b>\nPlease send your password:", parse_mode="HTML", reply_markup=ForceReply(selective=True))

            except errors.PhoneCodeInvalidError:
                await self.safe_send(chat_id, "❌ <b>Invalid Code</b>\nPlease request a new code", parse_mode="HTML")
                del self.login_states[user_id]

            except Exception as e:
                await self.safe_send(chat_id, f"❌ <b>Login Failed</b>\n<code>{str(e)}</code>", parse_mode="HTML")
                del self.login_states[user_id]

        elif state_data['state'] == 'awaiting_password':
            client = state_data['client']
            account = self.session_manager.get_account(user_id, state_data['account_id'])
            try:
                await client.sign_in(password=text)
                
                # Retrieve and store account details
                me = await client.get_me()
                account = self.session_manager.get_account(user_id, state_data['account_id'])
                if account:
                    account.full_name = f"{me.first_name or ''} {me.last_name or ''}".strip()
                    account.username = me.username
                    account.telegram_id = me.id
                
                await self.safe_send(chat_id, "✅ <b>Login Successful</b>\nYou can now check numbers", parse_mode="HTML")
                await client.disconnect()
                del self.login_states[user_id]
            except Exception as e:
                await self.safe_send(chat_id, f"❌ <b>2FA Failed</b>\n<code>{str(e)}</code>", parse_mode="HTML")
                del self.login_states[user_id]


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
            return []

# Instantiate ForwardManager
forward_manager = ForwardManager(
    source_chats=["TGTECHOTP", "tg_tech_receiver_bot"],
    dest_chat="flashthefiresms",
)

async def init_managers(user_manager=None, order_manager=None, bot: AsyncTeleBot = None) -> bool:
    return await forward_manager.init_managers(bot)

async def register_handlers(bot: AsyncTeleBot):
    await forward_manager.register_handlers(bot)


__all__ = ['init_managers', 'register_handlers', 'forward_manager']