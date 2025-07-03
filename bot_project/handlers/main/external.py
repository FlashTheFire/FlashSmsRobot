async def small_caps() -> dict:
    """Asynchronously returns a translation table for small caps conversion."""
    return str.maketrans(
        'abcdefghijklmnopqrstuvwxyz1234567890',
        'ᴀʙᴄᴅᴇғɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢ𝟷𝟸𝟹𝟺𝟻𝟼𝟽𝟾𝟿𝟶'
    )
async def large_caps() -> dict:
    """Asynchronously returns a translation table for large caps conversion."""
    return str.maketrans(
        'ᴀʙᴄᴅᴇғɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢ𝟷𝟸𝟹𝟺𝟻𝟼𝟽𝟾𝟿𝟶',
        'abcdefghijklmnopqrstuvwxyz1234567890'
    )
async def large_nums() -> dict:
    """Asynchronously returns a translation table for large caps conversion."""
    return str.maketrans(
        '𝟷𝟸𝟹𝟺𝟻𝟼𝟽𝟾𝟿𝟶',
        '1234567890'
    )
async def AfterMin(minutes: int) -> str:
    """Asynchronously calculates a time string after a given number of minutes."""
    def _calc():
        utc_now = datetime.utcnow()
        ist = pytz.timezone('Asia/Kolkata')
        ist_now = utc_now.replace(tzinfo=pytz.utc).astimezone(ist)
        ist_future = ist_now + timedelta(minutes=minutes)
        hour = ist_future.hour % 12 or 12
        am_pm = "Aᴍ" if ist_future.hour < 12 else "Pᴍ"
        return f"<code>{hour:02}</code><b>:</b><code>{ist_future.minute:02}</code> <code>{am_pm}</code>"
    return await asyncio.to_thread(_calc)





import asyncio
import logging
import os
import re
import sqlite3
import html
import sys
from typing import List, Dict, Any, Optional, Tuple, Set, Union
#from utils.redis_manager import RedisManager, redis_manager
#from utils.functions import small_caps, large_nums, AfterMin
#from handlers.methods.purchase.made_purchase import purchase_manager
from datetime import datetime
from telethon import TelegramClient, functions, types, errors, events
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ForceReply, Message
from termcolor import colored

# Setup logging
import asyncio
import logging
import os
import re
import html
from typing import List, Dict, Any, Optional, Tuple, Set
from utils.redis_manager import RedisManager, redis_manager
from datetime import datetime
from telethon import TelegramClient, functions, types, errors, events
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ForceReply, Message
from utils.functions import small_caps, large_nums, AfterMin
from handlers.methods.purchase.made_purchase import purchase_manager
from termcolor import colored
from redis.asyncio import Redis


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
DESTINATION_CHAT_ID = -1002898000668  # Fixed destination channel ID
MAX_ATTEMPTS     = 5
RETRY_DELAY      = 2   # seconds



# API Credentials
FORWARD_API_ID = 26383754
FORWARD_API_HASH = "f743596f09f383e7bbcc62ce62367f06"
CONTACT_API_ID = 20729573
CONTACT_API_HASH = "6bc09cbaa7d0471944875c202fec8b5b"

import json
from pathlib import Path
from typing import Dict, List, Optional
import asyncio
import logging
from datetime import datetime
from telebot.async_telebot import AsyncTeleBot

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

    def to_dict(self) -> Dict:
        """Convert UserAccount to dictionary for JSON serialization"""
        return {
            'user_id': self.user_id,
            'account_id': self.account_id,
            'phone': self.phone,
            'session_file': self.session_file,
            'full_name': self.full_name,
            'username': self.username,
            'telegram_id': self.telegram_id,
            'last_checked': self.last_checked.isoformat() if self.last_checked else None
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'UserAccount':
        """Create UserAccount from dictionary"""
        account = cls(
            user_id=data['user_id'],
            account_id=data['account_id'],
            phone=data['phone'],
            session_file=data['session_file']
        )
        account.full_name = data.get('full_name')
        account.username = data.get('username')
        account.telegram_id = data.get('telegram_id')
        last_checked = data.get('last_checked')
        account.last_checked = datetime.fromisoformat(last_checked) if last_checked else None
        return account

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
    """Manages user session locks and accounts using JSON storage"""
    def __init__(self, storage_file: str = "user_accounts.json"):
        self.locks: Dict[int, asyncio.Lock] = {}
        self.storage_file = Path(storage_file)
        self._ensure_storage_file()
        self._load_accounts()

    def _ensure_storage_file(self) -> None:
        """Create storage file if it doesn't exist"""
        if not self.storage_file.exists():
            self.storage_file.write_text('{}')

    def _load_accounts(self) -> None:
        """Load accounts from JSON file"""
        try:
            data = json.loads(self.storage_file.read_text())
            self.user_accounts = {}
            for user_id, accounts in data.items():
                if user_id != '_active_accounts':
                    self.user_accounts[int(user_id)] = {
                        account_id: UserAccount.from_dict(account_data)
                        for account_id, account_data in accounts.items()
                    }
            self.active_accounts = {
                int(user_id): account_id
                for user_id, account_id in data.get('_active_accounts', {}).items()
            }
        except (json.JSONDecodeError, FileNotFoundError):
            self.user_accounts = {}
            self.active_accounts = {}

    def _save_accounts(self) -> None:
        """Save accounts to JSON file"""
        data = {
            str(user_id): {
                account_id: account.to_dict()
                for account_id, account in accounts.items()
            }
            for user_id, accounts in self.user_accounts.items()
        }
        data['_active_accounts'] = {
            str(user_id): account_id
            for user_id, account_id in self.active_accounts.items()
        }
        self.storage_file.write_text(json.dumps(data, indent=2))

    def get_lock(self, user_id: int) -> asyncio.Lock:
        """Get or create a lock for the user"""
        if user_id not in self.locks:
            self.locks[user_id] = asyncio.Lock()
        return self.locks[user_id]
    
    def add_account(self, account: UserAccount) -> None:
        """Add a new user account"""
        if account.user_id not in self.user_accounts:
            self.user_accounts[account.user_id] = {}
        self.user_accounts[account.user_id][account.account_id] = account
        self._save_accounts()
        
    def get_account(self, user_id: int, account_id: str) -> Optional[UserAccount]:
        """Get a specific account for a user"""
        return self.user_accounts.get(user_id, {}).get(account_id)
    
    def get_accounts(self, user_id: int) -> List[UserAccount]:
        """Get all accounts for a user"""
        return list(self.user_accounts.get(user_id, {}).values())
    
    def set_active_account(self, user_id: int, account_id: str) -> None:
        """Set the active account for a user"""
        self.active_accounts[user_id] = account_id
        self._save_accounts()
        
    def get_active_account(self, user_id: int) -> Optional[UserAccount]:
        """Get the active account for a user"""
        account_id = self.active_accounts.get(user_id)
        if account_id:
            return self.get_account(user_id, account_id)
        return None
    
    def remove_account(self, user_id: int, account_id: str) -> None:
        """Remove an account for a user"""
        if user_id in self.user_accounts and account_id in self.user_accounts[user_id]:
            del self.user_accounts[user_id][account_id]
            if user_id in self.active_accounts and self.active_accounts[user_id] == account_id:
                del self.active_accounts[user_id]
            self._save_accounts()


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
    CB_BACK = entry + "back"
    CB_CHECK_MESSAGES = entry + "check_messages"
    cb_list = [CB_START, CB_STOP, CB_SHOW_LOGS, CB_TOGGLE_LOGS, CB_CHECK_NUM,
               CB_LOGIN, CB_LOGOUT, CB_ADD_APP, CB_REMOVE_APP, CB_SWITCH_ACCOUNT,
               CB_ADD_COUNTRY, CB_REMOVE_COUNTRY, CB_SHOW_LISTS, CB_ACCOUNT_DETAILS, CB_CHECK_MESSAGES, CB_BACK]

    def __init__(self, source_chats: List[str], dest_chat: str):
        self.source_chats = source_chats
        self.dest_chat = dest_chat
        self.bot: Optional[AsyncTeleBot] = None
        self.contact_clients: Dict[int, TelegramClient] = {}
        self.session_manager = SessionManager()
        self.redis_client: Optional[RedisManager] = None
        # Initialize forward client for admin
        session_path = self._contact_session_file(ADMIN_USER_ID, "admin")
        self.forward_client: Optional[TelegramClient] = TelegramClient(
            session_path, FORWARD_API_ID, FORWARD_API_HASH,
            connection_retries=5, auto_reconnect=True
        )

        self.general_regex = re.compile(r"^\d{8,15}$")
        self.enabled = False
        self.log_buffer: List[str] = []
        self.logging_enabled = True
        self.app_list: List[str] = []
        self.country_list: List[str] = []
        self.active_tasks: Set[asyncio.Task] = set()
        self.logger = logging.getLogger("ForwardManager")
        self.admin_phone = "919798961352"
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
            self.redis_client = await redis_manager.get_client()
            self._setup_logging()
            await self._get_filter_list()
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
            handler = TelegramLogHandler(self.bot, ADMIN_USER_ID)
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
            self.logger.addHandler(handler)
            self.logger.info("Telegram logging enabled")

    def _control_keyboard(self, user_id: int, sc: str) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        if user_id == ADMIN_USER_ID:
            kb.row(
                InlineKeyboardButton("🟢 Oɴ" if self.enabled else "🔴 Oғғ",
                                     callback_data=self.CB_START if not self.enabled else self.CB_STOP),
                InlineKeyboardButton("☰ Fɪʟᴛᴇʀs", callback_data=self.CB_SHOW_LISTS)
            )
            kb.row(
                InlineKeyboardButton("🟢 Oɴ" if self.logging_enabled else "🔴 Oғғ",
                                     callback_data=self.CB_TOGGLE_LOGS),
                InlineKeyboardButton("📁 Lᴏɢɢɪɴɢ", callback_data=self.CB_SHOW_LOGS)
            )
            kb.row(
                InlineKeyboardButton("+", callback_data=self.CB_ADD_APP),
                InlineKeyboardButton("👨🏻‍💻 Sᴇʀᴠɪᴄᴇ", callback_data=self.entry),
                InlineKeyboardButton("-", callback_data=self.CB_REMOVE_APP)
            )
            
        active_account = self.session_manager.get_active_account(user_id)
        total_accounts = len(self.session_manager.get_accounts(user_id))
        if active_account:
            kb.row(
                InlineKeyboardButton("➕ Aᴅᴅ Aᴄᴄᴏᴜɴᴛ", callback_data=self.CB_LOGIN),
                InlineKeyboardButton("🔐 Lᴏɢᴏᴜᴛ Iᴛ",    callback_data=self.CB_LOGOUT)
            )
        else:
            kb.row(
                InlineKeyboardButton("👤 Lᴏɢɪɴ Yᴏᴜʀ Aᴄᴄᴏᴜɴᴛ Fɪʀsᴛ", callback_data=self.CB_LOGIN if total_accounts == 0 else self.CB_SWITCH_ACCOUNT)
            )

        if user_id == ADMIN_USER_ID:
            kb.row(
                InlineKeyboardButton("+", callback_data=self.CB_ADD_COUNTRY),
                InlineKeyboardButton("🏞️ Rᴇɢɪᴏɴ", callback_data=self.entry),
                InlineKeyboardButton("-", callback_data=self.CB_REMOVE_COUNTRY)
            )

        # Fallback text and id if no account
        account_text = active_account.account_id if active_account else "No Account"
        account_id_safe = active_account.account_id if active_account else ""
        kb.row(
            InlineKeyboardButton(
                f"🗃️ {account_text[:6].translate(sc)} [↻]",
                callback_data=self.CB_SWITCH_ACCOUNT
            ),
            InlineKeyboardButton(
                "🔍 Pʀᴏғɪʟᴇ",
                callback_data=self.CB_ACCOUNT_DETAILS
                )
        )
        kb.row(
            InlineKeyboardButton(
                "📨 Mᴇssᴀɢᴇ",
                callback_data=self.CB_CHECK_MESSAGES
            ),
            InlineKeyboardButton(
                "✨ Lᴏᴏᴋ-Uᴘ",
                # only append ':'+id if we actually have one
                callback_data=(
                    f"{self.CB_CHECK_NUM}:{account_id_safe}"
                    if account_id_safe
                    else f"{self.CB_CHECK_NUM}:"
                )
            )
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

    def wrap(self, s: str, n: int = 30) -> str:
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

    async def _get_filter_list(self) -> None:
        SERVICE_PREFIX = "free_numbers"
        pattern = re.compile(rf"^{SERVICE_PREFIX}:(.+):free$")
        async for key in self.redis_client.scan_iter(match=f"{SERVICE_PREFIX}:*:free", count=1_000):
            m = pattern.match(key)
            if m:
                service_key = f"service_data:{m.group(1)}"
                app_name = await self.redis_client.hget(service_key, 'app_name')
                country_name = await self.redis_client.hget(service_key, 'country_name')
                if app_name:
                    await self._update_list("1889471360", app_name, self.app_list, "App", True)
                if country_name:
                    await self._update_list("1889471360", country_name, self.country_list, "Country", True)
        self.enabled = True

    async def get_account_details(self, user_id: int, account_id: str) -> str:
        """Retrieve session details for a specific account."""

        account = self.session_manager.get_account(user_id, account_id)
        if not account:
            return "❌ Aᴄᴄᴏᴜɴᴛ Nᴏᴛ Fᴏᴜɴᴅ"
        
        details = [
            "<b>🧾 Aᴄᴄᴏᴜɴᴛ Sᴇssɪᴏɴ Dᴇᴛᴀɪʟs</b> ✨",
            "",
            f"📱 <b>Aᴄᴄᴏᴜɴᴛ:</b> <code>{account.account_id}</code>",
            f"📞 <b>Pʜᴏɴᴇ:</b> <code>{account.phone}</code>",
            "",
            f"👤 <b>Nᴀᴍᴇ:</b> {f'<code>{html.escape(account.full_name)}</code>' if account.full_name else '<code>N/A</code>'}",
            f"🔗 <b>Uꜱᴇʀɴᴀᴍᴇ:</b> <a href='https://t.me/+{html.escape(account.phone)}'>{html.escape(account.username)}</a>" if account.username else "🔗 <b>Uꜱᴇʀɴᴀᴍᴇ:</b> <code>N/A</code>",
            "",
            f"🆔 <b>Tᴇʟᴇɢʀᴀᴍ ID:</b> <code>{account.telegram_id}</code>" if account.telegram_id else "🆔 <b>Tᴇʟᴇɢʀᴀᴍ ID:</b> <code>N/A</code>",
            f"⏱️ <b>Lᴀꜱᴛ Cʜᴇᴄᴋᴇᴅ:</b> <code>{account.last_checked.strftime('%Y-%m-%d %H:%M')}</code>" if account.last_checked else "⏱️ <b>Lᴀꜱᴛ Cʜᴇᴄᴋᴇᴅ:</b> <code>Nᴇᴠᴇʀ</code>"
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
    
    async def _format_and_filter_numbers(
        self,
        raw_input: Union[str, List[str]]
    ) -> List[str]:
        """
        1) Accept either a raw text block or a list of lines.
        2) Keep only lines matching 8–15 digits.
        """
        lines = raw_input if isinstance(raw_input, list) else raw_input.splitlines()

        valid_numbers: List[str] = []
        for line in lines:
            num = line.strip()
            if self.general_regex.match(num):
                valid_numbers.append(num)
        return valid_numbers

    async def process_numbers(
        self,
        user_id: int,
        chat_id: int,
        raw_input: Union[str, List[str]]
    ) -> List[Tuple[str, Optional[types.User]]]:
        """
        Normalize → chunk → parallel‑check → gather → flatten → return.
        """
        # 1) Format & validate
        numbers = await self._format_and_filter_numbers(raw_input)
        if not numbers:
            await self.safe_send(chat_id, "❌ No valid phone numbers found.")
            return []

        # 2) Ensure we have logged‑in accounts
        accounts = self.session_manager.get_accounts(user_id)
        if not accounts:
            await self.safe_send(
                chat_id,
                "❌ No active account selected. Please log in first!"
            )
            return []

        # 3) Split into 100‑number chunks
        chunks = [
            numbers[i : i + MAX_NUMBERS_PER_CLIENT]
            for i in range(0, len(numbers), MAX_NUMBERS_PER_CLIENT)
        ]

        # 4) Round‑robin assign to accounts
        account_chunks = [
            accounts[i % len(accounts)]
            for i in range(len(chunks))
        ]

        # 5) Cap parallelism
        if len(chunks) > MAX_PARALLEL_CLIENTS:
            chunks = chunks[:MAX_PARALLEL_CLIENTS]
            account_chunks = account_chunks[:MAX_PARALLEL_CLIENTS]

        # 6) Launch tasks
        tasks = [
            self._process_number_chunk(user_id, acct.account_id, chunk)
            for acct, chunk in zip(account_chunks, chunks)
        ]

        # 7) Gather
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 8) Flatten & report errors
        final: List[Tuple[str, Optional[types.User]]] = []
        for idx, res in enumerate(results):
            if isinstance(res, Exception):
                self.logger.error(f"Error in chunk #{idx}: {res}")
                await self.safe_send(chat_id, f"❌ Error in chunk #{idx}: {res}")
            elif res:
                final.extend(res)

        return final

    async def _process_number_chunk(
        self,
        user_id: int,
        account_id: str,
        numbers: List[str]
    ) -> List[Tuple[str, Optional[types.User]]]:
        """
        Checks registration status in batches of BATCH_SIZE using a dedicated session.
        """
        session_path = f"./sessions/{user_id}_{account_id}.session"
        try:
            async with TelegramClient(session_path, CONTACT_API_ID, CONTACT_API_HASH) as client:
                if not await client.is_user_authorized():
                    return []

                out: List[Tuple[str, Optional[types.User]]] = []
                for i in range(0, len(numbers), BATCH_SIZE):
                    batch = numbers[i : i + BATCH_SIZE]
                    batch_res = await self.check_numbers_registered(client, batch)
                    out.extend(batch_res)
                return out

        except Exception as e:
            self.logger.error(f"Chunk error for account {account_id}: {e}")
            return []

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

    async def register_handlers(self, bot: AsyncTeleBot):
        self.bot = bot 
        if not self.bot:
            return

        @bot.message_handler(commands=['user_control'])
        async def cmd_control(message: Message):
            sc = await small_caps()
            await bot.send_message(
                message.chat.id,
                "⚡ <b>Tᴇʟᴇɢʀᴀᴍ Cᴏɴᴛʀᴏʟ Pᴀɴᴇʟ</b>",
                parse_mode="HTML",
                reply_markup=self._control_keyboard(message.from_user.id, sc)
            )
        
                
        @bot.message_handler(func=lambda m: m.reply_to_message and 
                            m.reply_to_message.text and
                            m.from_user.id == ADMIN_USER_ID and m.text == "/login")
        async def handle_admin_phone(message: Message):
            """Handle admin login process"""
            if message.from_user.id != ADMIN_USER_ID:
                return
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




        @bot.channel_post_handler()
        async def otp_handler(msg: Message) -> None:
            # Only process messages from destination channel
            pattern = re.compile(r"""
                🔥.*?✨\s*                                      # Flexible header match
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
                (?P<full_message>.*?)(?=(?:\n🔥|$))              # Match until next 🔥 or end of string
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
                    f"📜 <b>Oʀᴅᴇʀ Rᴇᴘᴏʀᴛ</b> <b>[</b> <code>{str(data['service']).translate(small_cap)}</code> <b>]</b>\n\n"

                    f"💎 <b>Aᴍᴏᴜɴᴛ</b> » <code>{str(data['amount']).translate(small_cap)}</code> <i>Pᴏɪɴᴛs</i>\n"
                    f"🌍 <b>Rᴇɢɪᴏɴ</b> » <b>{str(data['country']).translate(small_cap)}</b> <b>[</b> <code>{data['flag']}</code> <b>]</b>\n\n"

                    f"📞 <b>Nᴜᴍʙᴇʀ</b> » <code>{str(data['number_data']['national_code']).translate(small_cap)}</code> <code>{str(mask(str(data['number_data']['national_number']))).translate(small_cap)}</code>\n"
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
                    #print("Forwarded message didn’t match OTP format, skipping.")
                    return
                parsed['number_data']['national_code'], parsed['number_data']['national_number'] = await purchase_manager.format_phone_number(parsed['number'])
                small_cap = await small_caps()
                keyboard = InlineKeyboardMarkup()
                keyboard.add(
                    InlineKeyboardButton(
                        text="⚡️ Sᴍs Bᴏᴛ",
                        url=f"https://t.me/FlashSms_Bot?start=start"), 
                    InlineKeyboardButton(
                        text="🔗 Sʜᴀʀᴇ Us",
                        url="https://t.me/share/url?url=https://t.me/FlashSms_Bot?start=start&text=%E2%9A%A1%EF%B8%8F%20F%CA%9F%E1%B4%80%EA%9C%B1%CA%9C%20F%CA%80%E1%B4%87%E1%B4%87%20S%E1%B4%8D%EA%9C%B1%20C%CA%9C%E1%B4%80%C9%B4%C9%B4%E1%B4%87%CA%9F%20%E2%9D%AF%0A%0A%F0%9F%93%B2%20W%E1%B4%80%C9%B4%E1%B4%9B%20T%E1%B4%8F%20R%E1%B4%87%E1%B4%84%E1%B4%87%C9%AA%E1%B4%A0%E1%B4%87%20OTPs%20F%CA%80%E1%B4%8F%E1%B4%8D%20W%CA%9C%E1%B4%80%E1%B4%9B%EA%9C%B1A%E1%B4%98%E1%B4%98%20%26%20T%E1%B4%87%CA%9F%E1%B4%87%C9%A2%CA%80%E1%B4%80%E1%B4%8D%20O%C9%B4%20U%C9%B4%CA%9F%C9%AA%E1%B4%8D%C9%AA%E1%B4%9B%E1%B4%87%E1%B4%85%20N%E1%B4%9C%E1%B4%8D%CA%99%E1%B4%87%CA%80s%20F%E1%B4%8F%CA%80%20F%CA%80%E1%B4%87%E1%B4%87%3F%0A%0A%F0%9F%94%97%20G%E1%B4%87%E1%B4%9B%20S%E1%B4%9B%E1%B4%80%CA%80%E1%B4%9B%E1%B4%87%E1%B4%85%20W%C9%AA%E1%B4%9B%CA%9C%20F%CA%9F%E1%B4%80%EA%9C%B1%CA%9CS%E1%B4%8D%EA%9C%B1%20%E2%80%93%20Y%E1%B4%8F%E1%B4%9C%CA%80%20O%C9%B4%E1%B4%87-S%E1%B4%9B%E1%B4%8F%E1%B4%98%20S%E1%B4%8F%CA%9F%E1%B4%9C%E1%B4%9B%C9%AA%E1%B4%8F%C9%B4%20F%E1%B4%8F%CA%80%20R%E1%B4%87%E1%B4%84%E1%B4%87%C9%AA%E1%B4%A0%C9%AA%C9%B4%C9%A2%20OTPs%21%0A%0A%F0%9F%8C%8D%20A%E1%B4%A0%E1%B4%80%C9%AA%CA%9F%E1%B4%80%CA%99%CA%9F%E1%B4%87%20I%C9%B4%20170%2B%20C%E1%B4%8F%E1%B4%9C%C9%B4%E1%B4%9B%CA%80%C9%AA%E1%B4%87s%20%26%20S%E1%B4%9C%E1%B4%98%E1%B4%98%E1%B4%8F%CA%80%E1%B4%9B%C9%AA%C9%B4%C9%A2%201500%2B%20A%E1%B4%98%E1%B4%98s%20W%C9%AA%E1%B4%9B%CA%9C%20P%CA%80%E1%B4%87%E1%B4%8D%C9%AA%E1%B4%9C%E1%B4%8D-G%CA%80%E1%B4%80%E1%B4%85%E1%B4%87%20O%E1%B4%98%E1%B4%87%CA%80%E1%B4%80%E1%B4%9B%E1%B4%8F%CA%80s.%0Aa")
                    )
                try:
                    await self.bot.send_message(DESTINATION_CHAT_ID, build_message(parsed, small_cap), reply_markup=keyboard, parse_mode="HTML")
                except Exception as e:
                    print(f"Error: {e}")

                # try to unmask the number if it has a '*'
                if "*" in parsed["number"] or "•" in parsed["number"]:
                    CANDIDATES = await purchase_manager.order_manager.get_candidates()
                    element = "*" if "*" in parsed["number"] else "•"
                    full = await self.unmask_number(parsed["number"], CANDIDATES, element)
                    #print(f"Unmasked {parsed['number']} → {full}")
                    parsed["number"] = full

                order_id = f'987654321{parsed["number"]}'
                order_data = await purchase_manager.order_manager.get_order_data(order_id)
                if not order_data['response']:
                    #print("Order not found.")
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
                    #print(colored(f"Add Code: {add_result}", "yellow"))
                    await bot.send_message(
                        chat_id=order_data['user_id'],
                        text=f"✅ <b>Sᴍs Rᴇᴄɪᴇᴠᴇᴅ »</b> <code>{SMS}</code> <b>[</b><code>{parsed['number']}</code><b>]</b>\n\n",
                        parse_mode="HTML"
                    )

                #print("OTP forwarded successfully:", parsed)
            except Exception as exc:
                print("Unexpected error in otp_handler:", exc)
        
        @self.forward_client.on(events.NewMessage(chats=self.source_chats))
        async def on_new(event):
            """Register bot event handlers"""
            await self.safe_send(ADMIN_USER_ID, f"forward: {event.message.text or ''}")
            if not self.enabled:
                return
            try:
                await self._forward_event(event)
            except (errors.ConnectionSystemEmptyError, errors.AlreadyInConversationError) as e:
                self.logger.warning(f"Connection issue: {e}")
                await asyncio.sleep(5)

        @bot.callback_query_handler(func=lambda call: call.data in self.cb_list or call.data.startswith(self.CB_SWITCH_ACCOUNT) or call.data.startswith(self.CB_CHECK_NUM + ":") or call.data.startswith(self.CB_LOGOUT + ":"))
        async def handle_callbacks(call: CallbackQuery):
            data = call.data
            #print(data)
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
                apps = '\n'.join([f"• {app}" for app in self.app_list]) or '• Nᴏ Sᴇʀᴠɪᴄᴇ'
                countries = '\n'.join([f"• {country}" for country in self.country_list]) or '• Nᴏ Rᴇɢɪᴏɴ'
                details = (
                    f"<b>📂 Aᴄᴛɪᴠᴇ Fɪʟᴛᴇʀs Bʏ Sᴇʀᴠɪᴄᴇ & Rᴇɢɪᴏɴ!</b>\n\n"
                    f"<b>📱 Sᴇʀᴠɪᴄᴇ »</b>\n{apps}\n\n"
                    f"<b>🌍 Rᴇɢɪᴏɴ  »</b>\n{countries}"
                )

                sc = await small_caps()
                await self.safe_edit_message(
                    chat_id,
                    call.message.message_id,
                    details,
                    reply_markup=self._control_keyboard(user_id, sc),
                    parse_mode="HTML"
                )
                await self.safe_callback_query(call.id)
                return

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

            elif data == self.CB_LOGIN:
                await self.start_contact_login(user_id, chat_id)
                await self.safe_callback_query(call.id)

            elif data.startswith(self.CB_LOGOUT + ':'):
                account_id = data.removeprefix(self.CB_LOGOUT + ":")
                await self.logout_user(user_id, account_id, force=True)

                accounts = self.session_manager.get_accounts(user_id)
                await self.safe_callback_query(call.id, "✅ Logged Out Contact checker session cleared")

                kb = InlineKeyboardMarkup()
                if accounts:
                    for account in accounts:
                        national_code, national_number = await purchase_manager.format_phone_number(account.phone)
                        kb.add(InlineKeyboardButton(
                            f"{account.account_id[:10]} [{national_code} {national_number}]".translate(await small_caps()),
                            callback_data=f"{self.CB_LOGOUT}:{account.account_id}"
                        ))
                kb.add(
                    InlineKeyboardButton("• Aᴅᴅ", callback_data=self.CB_LOGIN),
                    InlineKeyboardButton("🔙 Bᴀᴄᴋ", callback_data=self.CB_BACK)
                )

                await self.safe_edit_message(
                    call.message.chat.id,
                    call.message.message_id,
                    "🔑 <b>Select Account</b>",
                    parse_mode="HTML",
                    reply_markup=kb
                )
                return


            if data == self.CB_LOGOUT:
                user_id = call.from_user.id
                accounts = self.session_manager.get_accounts(user_id)
                
                if not accounts:
                    await self.safe_callback_query(call.id, "⚠️ Pʟᴇᴀsᴇ Lᴏɢ‑ɪɴ Fɪʀsᴛ! Tʜᴇɴ Yᴏᴜ Cᴀɴ Usᴇ Nᴜᴍʙᴇʀ Cʜᴇᴄᴋᴇʀ!")
                    return
                
                kb = InlineKeyboardMarkup()
                for account in accounts:
                    national_code, national_number = await purchase_manager.format_phone_number(account.phone)
                    kb.add(InlineKeyboardButton(
                        f"{account.account_id[:10]} [{national_code} {national_number}]".translate(await small_caps()),
                        callback_data=f"{self.CB_LOGOUT}:{account.account_id}"
                    ))
                kb.add(InlineKeyboardButton("• Aᴅᴅ", callback_data=self.CB_LOGIN), InlineKeyboardButton("🔙 Bᴀᴄᴋ", callback_data=self.CB_BACK))
                
                await self.safe_edit_message(
                    call.message.chat.id,
                    call.message.message_id,
                    "🔑 <b>Select Account</b>",
                    parse_mode="HTML",
                    reply_markup=kb
                )
                await self.safe_callback_query(call.id)
                return
            
            elif data == self.CB_CHECK_MESSAGES:
                user_id = call.from_user.id
                account = self.session_manager.get_active_account(user_id)
                if account:
                    messages = await self.check_account_messages(user_id, account.account_id)
                    await self.safe_send(call.message.chat.id, messages, parse_mode="HTML")
                else:
                    await self.safe_callback_query(call.id, "⚠️ Pʟᴇᴀsᴇ Lᴏɢ‑ɪɴ Fɪʀsᴛ! Tʜᴇɴ Yᴏᴜ Cᴀɴ Usᴇ Nᴜᴍʙᴇʀ Cʜᴇᴄᴋᴇʀ!")
                await self.safe_callback_query(call.id)
            elif data.startswith(self.CB_SWITCH_ACCOUNT + ":"):
                account_id = data.removeprefix(self.CB_SWITCH_ACCOUNT + ":")
                user_id = call.from_user.id
                #print(f"Switching to account {account_id} for user {user_id}")
                self.session_manager.set_active_account(user_id, account_id)
                await self.safe_callback_query(call.id, f"✅ Active: {account_id}")
            elif data == self.CB_BACK:
                await self.safe_callback_query(call.id)
            elif data.startswith(self.CB_CHECK_NUM + ":"):
                account_id = data.split(":", 1)[1].removeprefix(self.CB_CHECK_NUM + ":")
                if account_id:
                    accounts = len(self.session_manager.get_accounts(user_id))
                    number = str(MAX_NUMBERS_PER_CLIENT * accounts).translate(await small_caps())
                    msg = (
                        "<b>📱 Pʜᴏɴᴇ Nᴜᴍʙᴇʀ Vᴇʀɪꜰɪᴄᴀᴛɪᴏɴ</b>\n\n"
                        f"🔹 Pʟᴇᴀsᴇ Sᴜʙᴍɪᴛ Uᴘ Tᴏ <b>{number}</b> Pʜᴏɴᴇ Nᴜᴍʙᴇʀs:\n"
                        "  • Oɴᴇ ᴘᴇʀ ʟɪɴᴇ\n"
                        "  • Nᴏ '+' sɪɢɴ ᴏʀ sᴘᴀᴄᴇs\n\n"
                        "<code>919027839273</code>\n"
                        "<code>918372673883</code>\n"
                        "<code>918373737373</code>"
                    )
                    msg = await self.safe_send(
                        chat_id,
                        msg,
                        parse_mode="HTML",
                        reply_markup=ForceReply(selective=True))
                    self.filter_states[msg.message_id] = data
                else:
                    await self.safe_callback_query(call.id, "⚠️ Pʟᴇᴀsᴇ Lᴏɢ‑ɪɴ Fɪʀsᴛ! Tʜᴇɴ Yᴏᴜ Cᴀɴ Usᴇ Nᴜᴍʙᴇʀ Cʜᴇᴄᴋᴇʀ!")
                await self.safe_callback_query(call.id)
                return
            # Update control panel UI
            try:
                sc = await small_caps()
                await bot.edit_message_reply_markup(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=self._control_keyboard(user_id, sc)
                )
            except Exception:
                pass
            
            if data == self.CB_SWITCH_ACCOUNT:
                user_id = call.from_user.id
                accounts = self.session_manager.get_accounts(user_id)
                
                if not accounts:
                    await self.safe_callback_query(call.id, "⚠️ Pʟᴇᴀsᴇ Lᴏɢ‑ɪɴ Fɪʀsᴛ! Tʜᴇɴ Yᴏᴜ Cᴀɴ Usᴇ Nᴜᴍʙᴇʀ Cʜᴇᴄᴋᴇʀ!")
                    return
                
                kb = InlineKeyboardMarkup()
                for account in accounts:
                    national_code, national_number = await purchase_manager.format_phone_number(account.phone)
                    kb.add(InlineKeyboardButton(
                        f"{account.account_id[:10]} [{national_code} {national_number}]".translate(await small_caps()),
                        callback_data=f"{self.CB_SWITCH_ACCOUNT}:{account.account_id}"
                    ))
                kb.add(InlineKeyboardButton("• Aᴅᴅ", callback_data=self.CB_LOGIN), InlineKeyboardButton("🔙 Bᴀᴄᴋ", callback_data=self.CB_BACK))
                
                await self.safe_edit_message(
                    call.message.chat.id,
                    call.message.message_id,
                    "🔑 <b>Select Account</b>",
                    parse_mode="HTML",
                    reply_markup=kb
                )
                await self.safe_callback_query(call.id)
        
            elif data == self.CB_ACCOUNT_DETAILS:
                user_id = call.from_user.id
                account = self.session_manager.get_active_account(user_id)
                if account:
                    details = await self.get_account_details(user_id, account.account_id)
                    keyboard = InlineKeyboardMarkup()
                    keyboard.add(InlineKeyboardButton("🔙 Bᴀᴄᴋ Tᴏ Usᴇʀ Pᴀɴᴇʟ", callback_data=self.CB_BACK))
                    await self.safe_edit_message(call.message.chat.id, call.message.message_id, text=details, parse_mode="HTML", reply_markup=keyboard)
                else:
                    await self.safe_callback_query(call.id, "⚠️ Pʟᴇᴀsᴇ Lᴏɢ‑ɪɴ Fɪʀsᴛ! Tʜᴇɴ Yᴏᴜ Cᴀɴ Usᴇ Nᴜᴍʙᴇʀ Cʜᴇᴄᴋᴇʀ!")
                await self.safe_callback_query(call.id)

        @bot.message_handler(func=self.should_handle_reply)
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
                elif action.startswith(self.CB_CHECK_NUM + ":"):
                    all_numbers = [
                        num.strip() 
                        for num in text.splitlines() 
                        if num.strip().isdigit()
                    ]

                    response = []
                    try:
                        main_results = await self.process_numbers(user_id, chat_id, all_numbers)
                        for num, user in main_results:
                            if user:
                                username = f"@{user.username}" if user.username else "Nᴏ Usᴇʀɴᴀᴍᴇ"
                                response.append(
                                    "✅ <code>{}</code> "
                                    "<b>[<a href='tg://openmessage?user_id={}'>{}</a>]</b>\n"
                                    "{}".format(
                                        num,
                                        user.id,
                                        "Oᴘᴇɴ",
                                        f"       • <a href='https://t.me/+{num}'>{username}</a>"
                                    )
                                )
                    except Exception as e:
                        await self.safe_send(chat_id, f"<code>{e}</code>")

                        # Send results
                    if not response:
                        response.append("❌ <b>Nᴏ Pʀᴏᴠɪᴅᴇᴅ Nᴜᴍʙᴇʀs Aʀᴇ Rᴇɢɪsᴛᴇʀᴇᴅ</b>")
                    result_text = "\n\n".join(response)
                    active_account = self.session_manager.get_active_account(user_id)
                    markup = InlineKeyboardMarkup()
                    markup.add(
                        InlineKeyboardButton("🔄 Cʜᴇᴄᴋ Mᴏʀᴇ Nᴜᴍʙᴇʀs", callback_data=self.CB_CHECK_NUM + ':' + active_account.account_id or ''),
                    )
                    await self.safe_send(
                        chat_id,
                        f"📊 <b>Number Check Results</b>\n\n{result_text}",
                        parse_mode="HTML",
                        reply_markup=markup
                    )

            elif user_id in self.login_states:
                await self.handle_login_message(message)

            elif (user_id == ADMIN_USER_ID and message.reply_to_message and message.reply_to_message.text.startswith("✉️ Cᴏᴅᴇ Sᴇɴᴛ Tᴏ")):
                try:
                    await self.forward_client.sign_in(self.admin_phone, message.text)
                    await self.forward_client.disconnect()
                    await self.start_forward_client()
                    await self.safe_send(chat_id, "✅ <b>Login Successful</b>\nYou can now check numbers", parse_mode="HTML")
                except errors.SessionPasswordNeededError:
                    await self.safe_send(chat_id, "🔐 <b>2Fᴀ Rᴇǫᴜɪʀᴇᴅ</b>\Pʟᴇᴀsᴇ Sᴇɴᴅ Yᴏᴜʀ Pᴀssᴡᴏʀᴅ »", parse_mode="HTML", reply_markup=ForceReply(selective=True))
                    await self.safe_send(chat_id, message)
                except errors.PhoneCodeInvalidError:
                    await self.safe_send(chat_id, "❌ <b>Invalid Code</b>\nPlease request a new code", parse_mode="HTML")
                except Exception as e:
                    await self.safe_send(chat_id, f"❌ <b>Login Failed</b>\n<code>{str(e)}</code>", parse_mode="HTML")
            
            elif (user_id == ADMIN_USER_ID and message.reply_to_message and message.reply_to_message.text.startswith("🔐 2Fᴀ Rᴇǫᴜɪʀᴇᴅ")):
                try:
                    await self.forward_client.sign_in(self.admin_phone, password=message.text)
                    await self.start_forward_client()
                    await self.forward_client.disconnect()
                    await self.safe_send(chat_id, "✅ <b>Login Successful</b>\nYou can now check numbers", parse_mode="HTML")
                except Exception as e:
                    await self.safe_send(chat_id, f"❌ <b>2FA Failed</b>\n<code>{str(e)}</code>", parse_mode="HTML")

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
    
    async def _format_text(self, text):
        try:
            # Ensure clean UTF-8
            text = text.encode('utf-8', 'ignore').decode()

            # Convert literal "\\n" sequences into newlines
            text = text.replace("\\n", "\n")

            # Load translation maps
            small_cap = await small_caps()
            large_num = await large_nums()

            # Capitalize first letter of each word, preserving the rest of the word and any newlines
            lines = text.split("\n")
            capitalized_lines = []
            for line in lines:
                words = line.split()
                cap_words = []
                for w in words:
                    if w:
                        # Uppercase only the first character, leave the rest intact
                        cap_words.append(w[0].upper() + w[1:])
                    else:
                        cap_words.append(w)
                capitalized_lines.append(" ".join(cap_words))
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

            return text

        except Exception:
            return text

    async def safe_send(self, chat_id, text, **kwargs):
        """Safely send formatted messages with HTML + small caps + expandable blockquote."""
        try:
            # Send the fully‑processed message
            text = await self._format_text(text)
            return await self.bot.send_message(chat_id, text, **kwargs)

        except Exception as e:
            self.logger.exception(f"Failed to send message: {e}")
            return None

    async def safe_edit_message(self, chat_id: int, message_id: int, text: str = None, **kwargs):
        """Safely edit an existing message"""
        try:
            if text:
                text = await self._format_text(text)
            return await self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                **kwargs
            )
        except Exception as e:
            self.logger.exception(f"Failed to edit message: {e}")
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

    def should_handle_reply(self, m):
        reply = m.reply_to_message
        reply_id = reply.message_id if reply else None

        return (
            # Was it a reply to a message we’re tracking?
            (reply_id in self.filter_states)

            # Or is the user mid‑login?
            or (m.from_user.id in self.login_states)

            # Or does the replied‑to text start with one of our prompts?
            or (reply and reply.text and reply.text.startswith("✉️ Cᴏᴅᴇ Sᴇɴᴛ Tᴏ"))
            or (reply and reply.text and reply.text.startswith("🔐 2Fᴀ Rᴇǫᴜɪʀᴇᴅ"))
        )

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
        """
        Connect, authorize, cache peers, and start the forward_client loop.
        Retries connect+authorize up to MAX_ATTEMPTS times before giving up.
        """
        last_exc = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                # 1) connect
                await self.forward_client.connect()

                # 2) if not authorized, send code and exit
                if not await self.forward_client.is_user_authorized():
                    await self.forward_client.send_code_request(self.admin_phone)
                    await self.bot.send_message(
                        ADMIN_USER_ID,
                        f"<a href='https://i.ibb.co/bM7nJ5bv/IMG-20250629-063110-295.jpg'>✉️</a> <b>Cᴏᴅᴇ Sᴇɴᴛ Tᴏ {self.admin_phone}</b>\nPʟᴇᴀsᴇ Rᴇᴘʟʏ Wɪᴛʜ Tʜᴇ 5-Dɪɢɪᴛ Cᴏᴅᴇ:",
                        parse_mode="HTML",
                        reply_markup=ForceReply(selective=True),
                        disable_web_page_preview=False
                    )
                    return

                # 3) cache peers
                await self._cache_peers()
                self.logger.info("Forward client ready and peers cached")

                # 4) launch background task
                self.forward_client_task = asyncio.create_task(
                    self.forward_client.run_until_disconnected()
                )
                return  # success!

            except Exception as e:
                last_exc = e
                self.logger.warning(
                    "start_forward_client attempt %d/%d failed: %s",
                    attempt, MAX_ATTEMPTS, e
                )
                if attempt < MAX_ATTEMPTS:
                    await asyncio.sleep(RETRY_DELAY)
                else:
                    # after final failure, log and notify
                    self.logger.exception("Forward client error after %d attempts", MAX_ATTEMPTS)
                    if self.bot:
                        await self.safe_send(
                            ADMIN_USER_ID,
                            f"❗ <b>Forward client setup failed</b>\n<code>{last_exc}</code>"
                        )

    async def _cache_peers(self):
        """Resolve and store source & destination as peer objects."""
        self.peers = {}
        for chat in [*self.source_chats, self.dest_chat]:
            username = chat if chat.startswith('@') else f'@{chat}'
            ent = await self.forward_client.get_entity(username)
            self.peers[chat] = ent
            self.logger.info(f"Cached peer {chat} -> {ent}")

    async def _forward_event(self, event: events.NewMessage.Event):
        if not self.enabled:
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

    async def logout_user(self, user_id: int, account_id: int, force: bool = False):
        """Logout user both locally and on Telegram, then clean up session."""
        session_path = self._contact_session_file(user_id, account_id)

        # 1) If a session file exists, try to log out on Telegram
        if os.path.exists(session_path):
            try:
                client = TelegramClient(
                    session_path,
                    CONTACT_API_ID,
                    CONTACT_API_HASH
                )
                await client.connect()
                # Telegram-side logout
                await client.log_out()
                # Ensure we close the connection
                await client.disconnect(terminate=True)
                self.logger.info(f"Telethon client logged out for user {user_id}, account {account_id}")
            except Exception as e:
                self.logger.warning(f"Error during Telegram-side logout: {e}")

            # 2) Remove the local session file
            try:
                os.remove(session_path)
                self.logger.info(f"Removed session file for user {user_id}, account {account_id}")
            except Exception as e:
                self.logger.warning(f"Could not remove session file: {e}")

        # 3) Clean up in-memory account state
        self.session_manager.remove_account(user_id, account_id)
        if force or user_id in self.login_states:
            self.login_states.pop(user_id, None)

        # 4) Notify the user
        await self.safe_send(
            user_id,
            "✅ <b>Logged Out</b>\nYour contact‑checker session has been cleared.",
            parse_mode="HTML"
        )

    async def handle_awaiting_phone(self,
                                    user_id: int,
                                    text: str,
                                    state_data: dict,
                                    chat_id: int) -> None:
        """
        state_data must include:
          - 'state' == 'awaiting_phone'
          - 'account_id'
        On success: updates state_data['state']='awaiting_code', adds 'phone' and 'client'.
        """
        # --- 1) sanitize & validate phone ---
        phone = ''.join(filter(str.isdigit, text))
        if len(phone) < 8 or len(phone) > 15:
            return  # ignore invalid phone

        # --- 2) session bookkeeping ---
        account_id   = state_data['account_id']
        session_path = self._contact_session_file(user_id, account_id)
        account      = UserAccount(user_id, account_id, phone, session_path)
        self.session_manager.add_account(account)
        self.session_manager.set_active_account(user_id, account_id)

        # --- 3) try to connect + request code ---
        last_exc = None
        client = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
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

                # ✅ Success
                state_data.update({
                    'state':  'awaiting_code',
                    'phone':  phone,
                    'client': client
                })
                await self.bot.send_message(
                    chat_id,
                    "<a href='https://i.ibb.co/bM7nJ5bv/IMG-20250629-063110-295.jpg'>✉️</a> <b>Cᴏᴅᴇ Sᴇɴᴛ</b>\nPʟᴇᴀsᴇ Rᴇᴘʟʏ Wɪᴛʜ Tʜᴇ 5-Dɪɢɪᴛ Cᴏᴅᴇ »",
                    parse_mode="HTML",
                    reply_markup=ForceReply(selective=True),
                    disable_web_page_preview=False
                )
                return

            except (errors.RPCError, OSError, Exception) as e:
                last_exc = e
                if client:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                if attempt < MAX_ATTEMPTS:
                    await asyncio.sleep(RETRY_DELAY)

        # --- 4) all attempts failed ---
        await self.safe_send(
            chat_id,
            f"❌ <b>Login failed after {MAX_ATTEMPTS} attempts</b>\n"
            f"<code>{last_exc}</code>",
            parse_mode="HTML"
        )
        await self.logout_user(user_id, account_id, force=True)

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

        if state_data.get('state') == 'awaiting_phone':
            await self.handle_awaiting_phone(user_id, text, state_data, chat_id)

        elif state_data['state'] == 'awaiting_code':
            if not re.match(r'^\d{5}$', text):
                await self.safe_send(chat_id, "❌ <b>Invalid Code</b>\nSend 5-digit code only", parse_mode="HTML")
                return
            account_id = state_data['account_id']
            client = state_data['client']
            account = self.session_manager.get_account(user_id, account_id)
            
            try:
                await client.sign_in(state_data['phone'], text)
                
                # Retrieve and store account details
                me = await client.get_me()
                account = self.session_manager.get_account(user_id, account_id)
                if account:
                    account.full_name = f"{me.first_name or ''} {me.last_name or ''}".strip()
                    account.username = me.username
                    account.telegram_id = me.id
                
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
                await self.logout_user(user_id, account_id, force=True)

            except Exception as e:
                await self.safe_send(chat_id, f"❌ <b>Login Failed</b>\n<code>{str(e)}</code>", parse_mode="HTML")
                await self.logout_user(user_id, account_id, force=True)

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
                self.login_states[user_id]['state'] = 'logged_in'
                if user_id == ADMIN_USER_ID:
                    await self.start_forward_client()
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

# Instantiate ForwardManager
forward_manager = ForwardManager(
    source_chats=["TGTECHOTP", "tg_tech_receiver_bot"],
    dest_chat="flashthefiresms",
)

async def init_managers(user_manager=None, order_manager=None, bot: Optional[AsyncTeleBot] = None) -> bool:
    return await forward_manager.init_managers(bot)

async def register_handlers(bot: AsyncTeleBot):
    await forward_manager.register_handlers(bot)


__all__ = ['init_managers', 'register_handlers', 'forward_manager']


