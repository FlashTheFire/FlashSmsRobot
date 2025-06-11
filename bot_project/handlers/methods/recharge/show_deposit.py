import string, json, random, time, csv, io, asyncio, secrets, hashlib
from typing import Any, Dict, List, Optional, Tuple
from aiocsv import AsyncWriter
import asyncio
import random
import uuid
import json
import time
import hashlib
from datetime import datetime, timedelta
import secrets

from telebot.async_telebot import AsyncTeleBot
from telebot.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputMediaPhoto,
    CallbackQuery,
    Message,
    InputMediaVideo,
    InputMediaAnimation,
    ForceReply
)


# Local imports – ensure these modules are available in your project.
from utils.redis_keys import RedisKeys
from redis import WatchError
from utils.functions import AfterMin, format_currency, qr_code, encode_order_id, decode_barcode_id
from handlers.manager.operation import UserManagement, OrderManagement, DepositManagement
from handlers.security import RateLimiter, TransactionGuard
from utils.config import DEPOSIT_TIMEOUT, INR_RATE, PAYMENT_GATEWAY_API, PAYMENT_GATEWAY_API_KEY
from utils.redis_manager import redis_manager, RedisManager
from utils.cache_manager import cache_manager, CachePrefix
from utils.config import LOADING_GIF, MIN_DEPOSIT
from redis.asyncio.client import Redis
import string
from functools import lru_cache, partial
#logger = logging.getLogger(__name__)

# ───────────────────────── constants & helpers ─────────────────────────────
REDEEM_CODE_PREFIX    = "redeem_code:"
REDEEM_CODE_USAGE_SET = "redeem_code:used:"
REDEEM_CODE_LOG_LIST  = "redeem_code:log:"
ALPHABET              = string.ascii_uppercase + string.digits
CODE_LEN              = 12
STATS_CB              = "stats"
REVOKE_CB             = "revoke"
EXPORT_CB             = "export"
CODE_KEY              = lambda c: f"{REDEEM_CODE_PREFIX}{c}"
USAGE_SET             = lambda c: f"{REDEEM_CODE_USAGE_SET}{c}"
LOG_LIST              = lambda c: f"{REDEEM_CODE_LOG_LIST}{c}"
ADMIN_USER_IDS        = {"5716978793", "5716978794"}
CODE_TTL              = 7 * 24 * 3600  # expire codes after 7 days


def serialize_meta(meta: Dict[str, Any]) -> Dict[str, str]:
    return {k: json.dumps(v) if isinstance(v, (dict, list)) else str(v) for k, v in meta.items()}

def authorize_admin(user_id: int) -> bool:
    return str(user_id) in ADMIN_USER_IDS

async def expire_old_codes():
    cursor = b"0"
    while True:
        cursor, keys = await redis_manager.redis_client.scan(cursor=cursor, match=f"{REDEEM_CODE_PREFIX}*", count=100)
        for key in keys:
            ttl = await redis_manager.redis_client.ttl(key)
            if ttl == -1:
                await redis_manager.redis_client.expire(key, CODE_TTL)
        if cursor == b"0": break





class ShowDepositManager:
    """
    Real-time deposit tracking system with exponential backoff and circuit breaking.
    This class handles deposit record management (using Redis) and the QR deposit flow.
    """
    __slots__ = (
        'bot',
        'check_interval',
        'deposit_manager',       # retained for backward compatibility if needed
        'user_manager',
        'input_validator',
        'transaction_guard',
        'rate_limiter',
        'redis_client',
        '_initialized'
    )

    def __init__(self, check_interval: int = 30):
        self.check_interval = check_interval
        self.bot: Optional[AsyncTeleBot] = None
        self.deposit_manager: Optional[DepositManagement] = None  # no longer used for deposit ops
        self.user_manager: Optional[UserManagement] = None
        self.input_validator: Optional[Any] = None
        self.transaction_guard: Optional[TransactionGuard] = None
        self.rate_limiter: Optional[RateLimiter] = None
        self.redis_client: Optional[RedisManager] = None
        self._initialized = False

    async def init_managers(self, deposit_mgr: DepositManagement, user_mgr: UserManagement, bot: AsyncTeleBot) -> bool:
        """
        Initialize required components for deposit handling asynchronously.
        """
        try:
            if not all([deposit_mgr, user_mgr, bot]):
                #await logger.error("Missing required components for initialization")
                return False

            self.deposit_manager = deposit_mgr  # retained for compatibility if needed
            self.user_manager = user_mgr
            self.bot = bot

            # Retrieve additional attributes from the bot, if they exist.
            self.input_validator = getattr(bot, "input_validator", None)
            self.transaction_guard = getattr(bot, "transaction_guard", None)

            self.redis_client = await redis_manager.get_client()
            if not self.redis_client:
                raise ConnectionError("Failed to establish Redis connection")
            self.rate_limiter = RateLimiter(
                redis_client=self.redis_client,
                duration=60,
                max_requests=10
            )
            
            self._initialized = True
            #await logger.info("Deposit managers initialized successfully")
            return True
        except Exception as e:
            #await logger.error(f"Initialization error: {e}")
            return False

    async def handle_qr_deposit(self, call: CallbackQuery):
        """
        Handle the QR code deposit flow by editing the current message with deposit options.
        """
        try:
            keyboard = InlineKeyboardMarkup()
            keyboard.row(
                InlineKeyboardButton("🪙 Tʀx", callback_data="/Trx"),
                InlineKeyboardButton("🏆 Rᴇᴅᴇᴇᴍ", callback_data="ask_redeem_code"),
                InlineKeyboardButton("💰 Iɴʀ", callback_data="USER:DEPOSIT:QR")
            )
            keyboard.row(
                InlineKeyboardButton("🔙 Bᴀᴄᴋ Tᴏ Hᴏᴍᴇ Pᴀɢᴇ", callback_data='start')
            )

            caption = (
                "<b>🔥 Fʟᴀsʜ Dᴇᴘᴏsɪᴛ Pᴀɢᴇ 》</b>\n"
                "<b>Hᴇʀᴇ Yᴏᴜ Cᴀɴ Aᴅᴅ Fᴜɴᴅs Tᴏ Yᴏᴜʀ Wᴀʟʟᴇᴛ!</b>\n\n"
                "<code>❒</code> <code>1</code> <b>Iɴʀ</b>   <b>»</b> <code>1</code> 💎 <b>||</b> "
                "<code>1</code> Tʀx  <b>»</b> <code>25</code> 💎\n\n"
                "➕ <b>Sᴇʟᴇᴄᴛ Dᴇᴘᴏsɪᴛ Mᴇᴛʜᴏᴅ, Aʟʟ Dᴇᴘᴏsɪᴛ Aᴍᴏᴜɴᴛ Wɪʟʟ Bᴇ Cᴏɴᴠᴇʀᴛᴇᴅ Tᴏ Pᴏɪɴᴛ</b>"
                "<code>(💎)</code>"
            )

            media = InputMediaPhoto(
                media='https://i.postimg.cc/hGZ2G2v5/IMG-20240620-025944-733.jpg',
                caption=caption,
                parse_mode='HTML'
            )

            await asyncio.gather(
                self.bot.edit_message_media(
                    media=media,
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    reply_markup=keyboard
                ),
                #await logg_info(f"QR deposit handled successfully for chat_id: {call.message.chat.id}")
            )
        except Exception as e:
            await asyncio.gather(
                #await logg_error(f"QR deposit handler error: {e}"),
                self.bot.answer_callback_query(
                    call.id, "🚫 Failed to process QR deposit", show_alert=True
                )
            )

    async def _build_deposit_data(self, data: Dict, deposit_id: str) -> Dict:
        """
        Build the deposit data structure from the provided input asynchronously.
        """
        utc_now = str(datetime.utcnow())

        return {
            "deposit_id": str(deposit_id),
            "message_id": str(data['message_id'].message_id) if hasattr(data['message_id'], 'message_id') else str(data['message_id']),
            "user_id": str(data['user_id']),
            "server_id": str(data['server_id']),
            "valid_until": data['valid_until'],
            "file_id": str(data['file_id']),
            "deposit_status": "PENDING",
            "deposit_history": json.dumps([{
                "timestamp": str(time.time()),
                "action": "DEPOSIT_CREATED"
            }]),
            "created_at": utc_now,
            "recorded_at": time.time(),
            "amount": data.get('amount', 0),
            "currency": data.get('currency', 'INR'),
            "payment_method": data.get('payment_method', 'QR')
        }

    async def _create_deposit_record(self, data: Dict, deposit_id: str) -> str:
        """
        Create a deposit record in the database asynchronously.
        """
        
        deposit_data = await self._build_deposit_data(data, deposit_id)
        response = await self.deposit_manager.add_deposit_data(deposit_id, data['user_id'], deposit_data)
        
        if not response.get('response'):
            raise Exception("⚠️ DEPOSIT DATA STORAGE FAILED")

        return response['result']

    async def start_deposit(self, call: CallbackQuery) -> None:
        """Initiate the deposit process by creating a deposit record and displaying the QR code for payment."""
        try:
            user_id = str(call.from_user.id)
            keyboard = InlineKeyboardMarkup()
            keyboard.row(
                InlineKeyboardButton("✘ Cᴀɴᴄᴇʟ Dᴇᴘᴏsɪᴛ", switch_inline_query_current_chat='#HɪsᴛᴏʀʏDᴇᴘᴏsɪᴛ'),
                InlineKeyboardButton("ⓘ Hᴇʟᴘ & Sᴜᴘᴘᴏʀᴛ", callback_data="USER:HELP")
            )

            caption = (
                "<b>🔥 Yᴏᴜʀ Fʟᴀsʜ Qʀ-Cᴏᴅᴇ 》</b>\n\n"
                "💰 <b>Mɪɴ Aᴍᴏᴜɴᴛ  »</b>  <code>₹{}</code>  <code>〚</code><code>💎 {}</code><code>〛</code>\n"
                "💳 <b>Dᴇᴘᴏsɪᴛ Iᴅ  »</b>  [ <code>{}</code> ]\n"
                "⏳ <b>Pᴀʏ Uɴᴅᴇʀ  »</b>  {} <b>[</b><code>{}</code> <code>Mɪɴ</code><b>]</b>\n\n"
                "📌 <b>Sᴄᴀɴ Tʜɪs Qʀ Aɴᴅ Pᴀʏ Fʀᴏᴍ Aɴʏ Pᴀʏᴍᴇɴᴛ Aᴘᴘ.</b>"
            )
            print("media", LOADING_GIF)
            loading_msg = await self.bot.edit_message_media(
                media=InputMediaAnimation(
                    media=LOADING_GIF, 
                    caption=caption.format('⩇⩇', '⩇⩇', '⩇⩇⩇⩇⩇⩇⩇⩇⩇⩇⩇⩇', '⩇⩇:⩇⩇ Pᴍ', '⩇⩇'), 
                    parse_mode="HTML"
                ),
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=keyboard
            )

            server_id = 1
            valid_until = await AfterMin(int(DEPOSIT_TIMEOUT))

            async with TransactionGuard(self.redis_client):
                deposit_id_resp = await self.deposit_manager.create_deposit_id(user_id=user_id)
                if not isinstance(deposit_id_resp, dict) or not deposit_id_resp.get('response'):
                    raise Exception("Failed to create deposit ID")

                deposit_id = deposit_id_resp['result']
                qr_image = await qr_code(deposit_id=deposit_id, size=380, position=(1470, 550), radius=20)
                print("qr_image", qr_image)
                msg = await self.bot.edit_message_media(
                    media=InputMediaPhoto(
                        media=qr_image, 
                        caption=caption.format(MIN_DEPOSIT, MIN_DEPOSIT, deposit_id, valid_until, int(DEPOSIT_TIMEOUT)), 
                        parse_mode="HTML"
                    ),
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    reply_markup=keyboard
                )

                deposit_data = {
                    "deposit_id": deposit_id,
                    "user_id": user_id,
                    "message_id": msg.message_id,
                    "server_id": server_id,
                    "valid_until": valid_until,
                    "file_id": msg.photo[-1].file_id 
                }

                await self._create_deposit_record(deposit_data, deposit_id)

                asyncio.create_task(self._delayed_message_edit(deposit_data, keyboard, caption, MIN_DEPOSIT, deposit_id, valid_until, int(DEPOSIT_TIMEOUT)-1))

        except Exception as e:
            print(f"🚫 Failed to start deposit: {str(e)}")

    async def _delayed_message_edit(self, deposit_data, keyboard, caption, MIN_DEPOSIT, deposit_id, valid_until, DEPOSIT_TIMEOUT):
        await asyncio.sleep(1)
        try:
            updated_caption = caption.format(MIN_DEPOSIT, MIN_DEPOSIT, deposit_id, valid_until, f"{int(DEPOSIT_TIMEOUT):02d}")
            
            await self.bot.edit_message_text(
                chat_id=deposit_data['user_id'],
                message_id=deposit_data['message_id'],
                text=updated_caption,
                parse_mode="HTML",
                reply_markup=keyboard
            )
        except Exception as e:
            print(e)
            pass

    async def _generate_code(self) -> str:
        raw = ''.join(secrets.choice(string.digits) for _ in range(CODE_LEN-4))
        chk = str(sum(int(x) for x in raw) % 10000).zfill(4)
        return await encode_order_id(f"{raw}{chk}")

    async def build_redeem_success_card(self, amount: float, code: str) -> Tuple[str, InlineKeyboardMarkup]:
        keyboard_for_send = InlineKeyboardMarkup()
        keyboard_for_send.row(
            InlineKeyboardButton(
                "🛒 Bᴜʏ Sᴇʀᴠɪᴄᴇ Nᴏᴡ",
                switch_inline_query_current_chat=""
            )
        )
        msg = (
            "<b>#Rᴇᴅᴇᴇᴍ_Cᴏᴅᴇ_Cʀᴇᴅɪᴛ ❯</b>\n\n"
            "<b>Tʀᴀɴsᴀᴄᴛɪᴏɴ Dᴇᴛᴀɪʟs</b>\n"
            f"<b>💰 Aᴍᴏᴜɴᴛ Cʀᴇᴅɪᴛᴇᴅ »</b> <code>{amount}</code> 💎\n"
            f"<b>🔑 Cᴏᴅᴇ Usᴇᴅ »</b> <code>{code}</code>\n\n"
            "<b>🏛 Bᴀʟᴀɴᴄᴇ Uᴘᴅᴀᴛᴇ 》</b>\n"
            f"<i>Sᴜᴄᴄᴇssғᴜʟʟʏ Cʀᴇᴅɪᴛᴇᴅ</i> <code>{amount}</code> 💎\n"
            "<i>Tᴏ Yᴏᴜʀ Aᴄᴄᴏᴜɴᴛ.</i>"
        )
        return msg, keyboard_for_send

    async def _process_create_redeem(self, message: Message):
        parts = message.text.strip().split("|") + [""] * 4
        amt_str, scope, param, max_str = [p.strip() for p in parts[:4]]
        try:
            if '-' in amt_str:
                min_amt, max_amt = map(float, amt_str.split('-',1))
                amount = round(random.uniform(min_amt, max_amt),2)
            else:
                amount = float(amt_str)

            max_uses = max(int(max_str),1)
            if scope == 'Aʟʟ': eligible = []
            elif scope == 'Uɪᴅ': eligible = [param]
            elif scope == 'Lɪsᴛ': eligible = [u for u in param.split(',') if u]
            else: raise ValueError('Invalid scope')
            code = await self._generate_code()
            meta = {
                'code': code, 'amount': amount, 'scope': scope,
                'eligible_users': eligible, 'max_uses': max_uses,
                'redeemed': 0, 'active': True, 'created_at': time.time()
            }
            await redis_manager.redis_client.hset(CODE_KEY(code), mapping=serialize_meta(meta))
            await redis_manager.redis_client.expire(CODE_KEY(code), CODE_TTL)
            kb = InlineKeyboardMarkup()
            kb.row(
                InlineKeyboardButton("📊 Cᴏᴅᴇ Sᴛᴀᴛs", callback_data=f"{STATS_CB}:{await decode_barcode_id(code)}"),
                InlineKeyboardButton("🔔 Rᴇᴅᴇᴇᴍ", callback_data=f"redeem:{await decode_barcode_id(code)}")
            )
            kb.row(
                InlineKeyboardButton("🗑️ Rᴇᴠᴏᴋᴇ", callback_data=f"{REVOKE_CB}:{await decode_barcode_id(code)}")
            )
            text = (
                "<b>#Rᴇᴅᴇᴇᴍ_Cᴏᴅᴇ ❯</b>\n"
                f"<b>🔑 Cᴏᴅᴇ »</b> <code>{code}</code> | <b>💰 Aᴍᴏᴜɴᴛ »</b> {amount}💎\n"
                f"<b>🎯 Scope »</b> <code>{scope}</code> | <b>♾ Max »</b> <code>{max_uses}</code>\n"
                f"<b>⏰ Expires »</b> {datetime.utcnow()+timedelta(seconds=CODE_TTL):%Y-%m-%d %H:%M UTC}"
            )
            await self.bot.send_message(
                message.chat.id,
                text=text, parse_mode='html', reply_markup=kb
            )
        except Exception as e:
            await self.bot.reply_to(message, f"🚫 {e}", parse_mode='html')

    async def _process_redeem(self, message: Message, code: str):
        uid = str(message.from_user.id)
        key, usage, log = CODE_KEY(code), USAGE_SET(code), LOG_LIST(code)
        meta = await redis_manager.redis_client.hgetall(key)
        if not meta:
            return await self.bot.reply_to(message, "🚫 Iɴᴠᴀʟɪᴅ Cᴏᴅᴇ", parse_mode='html')
        meta = {k: json.loads(v) if k=='eligible_users' else v for k,v in meta.items()}
        if meta.get('active')!='True':
            return await self.bot.reply_to(message, "🚫 Cᴏᴅᴇ Rᴇᴠᴏᴋᴇᴅ", parse_mode='html')
        if await redis_manager.redis_client.ttl(key)<=0:
            return await self.bot.reply_to(message, "🚫 Cᴏᴅᴇ E❨ᴘɪʀᴇᴅ❩", parse_mode='html')
        if meta['eligible_users'] and uid not in meta['eligible_users']:
            return await self.bot.reply_to(message, "🚫 Nᴏ Pᴇʀᴍɪssɪᴏɴ", parse_mode='html')
        async with redis_manager.redis_client.pipeline() as pipe:
            while True:
                try:
                    await pipe.watch(key, usage)
                    if await pipe.sismember(usage, uid): raise ValueError("Already redeemed")
                    if int(meta['redeemed'])>=int(meta['max_uses']): raise ValueError("Max uses reached")
                    pipe.multi()
                    pipe.hincrby(key, 'redeemed', 1)
                    pipe.sadd(usage, uid)
                    pipe.lpush(log, json.dumps({'uid':uid,'ts':time.time()}))
                    await pipe.execute()
                    break
                except WatchError:
                    continue
                except ValueError as ve:
                    return await self.bot.reply_to(message, f"🚫 {ve}", parse_mode='html')
                    # credit the user (re-use your earlier helper)
            deposit_id_resp = await self.deposit_manager.create_deposit_id(user_id=uid)
            if not isinstance(deposit_id_resp, dict) or not deposit_id_resp.get('response'):
                raise Exception("Failed to create deposit ID")
            deposit_id = deposit_id_resp['result']            # use code itself as deposit_id
            response = await self.deposit_manager.add_deposit_data(
                deposit_id=deposit_id,
                user_id=uid,
                data={
                    "deposit_amount": float(meta["amount"]),
                    "payment_method":"REDEEMCODE",
                    "user_id": str(uid),
                    "deposit_id": str(deposit_id),
                    "deposit_status":"COMPLETED",
                    "deposit_history": json.dumps([
                        {"timestamp": str(time.time()), "action": "DEPOSIT_CREATED"},
                        {"timestamp": str(time.time()), "action": "DEPOSIT_CONFIRMED"},
                    ]),
                    "server_id": str(2)
                }
            )
            print("deposit added:")
            print(response)
        msg, kb = await self.build_redeem_success_card(float(meta['amount']), code)
        await self.bot.send_message(message.chat.id, msg, parse_mode='html', reply_markup=kb)
        await self.deposit_manager.send_deposit_notification(
            self.bot,
            uid,
            float(meta["amount"]),
            deposit_id,
            code,
            "RᴇᴅᴇᴇᴍCᴏᴅᴇ",
            await AfterMin(int(DEPOSIT_TIMEOUT))
        )

    async def _handle_stats(self, call: CallbackQuery, code: str):
        key, log = CODE_KEY(code), LOG_LIST(code)
        meta = await redis_manager.redis_client.hgetall(key)
        logs = await redis_manager.redis_client.lrange(log, 0, 20)
        redeemed = int(meta.get('redeemed',0)); maxu=int(meta.get('max_uses',0))
        total = redeemed * float(meta.get('amount',0))
        last5 = logs[:5]
        txt = (
            "<b>📊 Cᴏᴅᴇ Sᴛᴀᴛs ❯</b>\n\n"
            f"<b>Codᴇ:</b> <code>{code}</code>\n"
            f"<b>Redeemed:</b> {redeemed}/{maxu}\n"
            f"<b>Total:</b> ₹{total:.2f}\n\n"
            "<b>》Last 5 Users:</b>\n" + "\n".join(
                f"• <code>{json.loads(x)['uid']}</code> @ {datetime.utcfromtimestamp(json.loads(x)['ts']):%H:%M}" for x in last5
            )
        )
        await self.bot.edit_message_text(
            text=txt, chat_id=call.message.chat.id, message_id=call.message.message_id,
            parse_mode='html', reply_markup=call.message.reply_markup
        )
        await self.bot.answer_callback_query(call.id)

    async def _handle_revoke(self, call: CallbackQuery, code: str):
        if not authorize_admin(call.from_user.id):
            return await self.bot.answer_callback_query(call.id, "🚫 Unauthorized")
        await redis_manager.redis_client.hset(CODE_KEY(code), mapping={'active':'False'})
        await self.bot.answer_callback_query(call.id, f"✅ Cᴏᴅᴇ {code} Rᴇᴠᴏᴋᴇᴅ")

    async def _handle_export(self, call: CallbackQuery, code: str):
        if not authorize_admin(call.from_user.id):
            return await  self.bot.answer_callback_query(call.id, "🚫 Unauthorized", show_alert=True)
        logs = await redis_manager.redis_client.lrange(LOG_LIST(code), 0, -1)
        if not logs:
            return await self.bot.answer_callback_query(call.id, "— No Logs —", show_alert=True)
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(['Usᴇʀ Iᴅ','Timestamp'])
        for x in logs:
            d = json.loads(x)
            writer.writerow([d['uid'], datetime.utcfromtimestamp(d['ts'])])
        buf.seek(0)
        await self.bot.send_document(
            call.message.chat.id,
            (io.BytesIO(buf.read().encode()), f"log_{code}.csv")
        )
        await self.bot.answer_callback_query(call.id, f"✅ Lᴏɢ Exportᴇᴅ for <code>{code}</code>", parse_mode='html')


# Create a global instance of DepositManager.
deposit_manager = ShowDepositManager()


async def init_managers(order_manager: OrderManagement, user_manager: UserManagement, bot: AsyncTeleBot) -> bool:
    """
    Initialize the deposit management system asynchronously.
    """
    return await deposit_manager.init_managers(bot.deposit_manager, user_manager, bot)


async def register_handlers(bot: AsyncTeleBot) -> None:
    """
    Register deposit-related bot handlers asynchronously.
    """
    asyncio.create_task(expire_old_codes())

    @bot.message_handler(commands=['AdminStart'])
    async def _start(m: Message):
        keyboard_for_send = InlineKeyboardMarkup()
        keyboard_for_send.row(
            InlineKeyboardButton("➕ Cʀᴇᴀᴛᴇ Cᴏᴅᴇ", callback_data="create_redeem")
        )
        msg = (
            "<b>Wᴇʟᴄᴏᴍᴇ to RᴇᴅᴇᴇᴍBᴏᴛ Pro</b>\n"
            "Uѕe the buttons below to manage your promo codes."
        )
        await bot.send_message(m.chat.id, msg, parse_mode='html', reply_markup=keyboard_for_send)

    @bot.callback_query_handler(lambda c: c.data == 'create_redeem')
    async def _ask_create(call: CallbackQuery):
        prompt = (
            "<b>» Eɴᴛᴇʀ Rᴇᴅᴇᴇᴍ Dᴇᴛᴀɪʟs</b>\n\n"
            "<b>Fᴏʀᴍᴀᴛ:</b>\n"
            "<code>&lt;Aᴍᴏᴜɴᴛ&gt;|&lt;Sᴄᴏᴘᴇ&gt;|&lt;Pᴀʀᴀᴍ&gt;|&lt;Mᴀx_Usᴇs&gt;</code>\n\n"
            "<b>Sᴄᴏᴘᴇ Oᴘᴛɪᴏɴs:</b>\n"
            "🔹 <b>Aʟʟ</b> – Aɴʏʙᴏᴅʏ Cᴀɴ Rᴇᴅᴇᴇᴍ <i>(Pᴀʀᴀᴍ = Eᴍᴘᴛʏ)</i>\n"
            "🔹 <b>Uɪᴅ</b> – Oɴᴇ Sᴘᴇᴄɪꜰɪᴄ Usᴇʀ <i>(Pᴀʀᴀᴍ = Usᴇʀ_Iᴅ)</i>\n"
            "🔹 <b>Lɪsᴛ</b> – Mᴜʟᴛɪᴘʟᴇ Usᴇʀs <i>(Pᴀʀᴀᴍ = Cᴏᴍᴍᴀ-Sᴇᴘᴀʀᴀᴛᴇᴅ ᴜsᴇʀ_ɪᴅs)</i>\n\n"
            "<b>Exᴀᴍᴘʟᴇs:</b>\n"
            "🔸 <code>50|Aʟʟ||100</code> — ₹50 Cᴏᴅᴇ, Aɴʏᴏɴᴇ, 100 ʀᴇᴅᴇᴍᴘᴛɪᴏɴs\n"
            "🔸 <code>60|Uɪᴅ|123456789|1</code> — ₹60 Cᴏᴅᴇ Fᴏʀ Usᴇʀ <code>123456789</code>, Sɪɴɢʟᴇ Usᴇ\n"
            "🔸 <code>75|Lɪsᴛ|1,2,3,4|4</code> — 4 Usᴇʀs, Oɴᴇ Rᴇᴅᴇᴍᴘᴛɪᴏɴ Eᴀᴄʜ"
        )
        await bot.send_message(
            call.message.chat.id, prompt,
            reply_markup=ForceReply(selective=True), parse_mode='html'
        )
        await bot.answer_callback_query(call.id)

    @bot.message_handler(func=lambda m: (
        m.reply_to_message and m.reply_to_message.text and '» Eɴᴛᴇʀ' in m.reply_to_message.text
    ))
    async def _on_create(m: Message):
        asyncio.create_task(deposit_manager._process_create_redeem(m))

    @bot.callback_query_handler(lambda c: c.data.startswith(f"{STATS_CB}:"))
    async def _cb_stats(c: CallbackQuery):
        code = await encode_order_id(c.data.split(':', 1)[1])
        await deposit_manager._handle_stats(c, code)

    @bot.callback_query_handler(lambda c: c.data.startswith('redeem:'))
    async def _cb_redeem(c: CallbackQuery):
        code = await encode_order_id(c.data.split(':', 1)[1])
        prompt = f"<b>❯ Redeem Code » <code>{code}</code></b>"
        await bot.send_message(
            c.message.chat.id, prompt,
            reply_markup=ForceReply(selective=True), parse_mode='html'
        )
        await bot.answer_callback_query(c.id)

    @bot.callback_query_handler(lambda c: c.data == "ask_redeem_code")
    async def prompt_redeem_code(call: CallbackQuery):
        await bot.send_message(
            chat_id=call.message.chat.id,
            text="❯ Eɴᴛᴇʀ Rᴇᴅᴇᴇᴍ Cᴏᴅᴇ",
            reply_markup=ForceReply(selective=True),
            parse_mode="html",
        )
        await bot.answer_callback_query(call.id)

    @bot.message_handler(func=lambda m: (
        m.reply_to_message and m.reply_to_message.text and
        m.reply_to_message.text.startswith("❯ Eɴᴛᴇʀ Rᴇᴅᴇᴇᴍ Cᴏᴅᴇ")
    ))
    async def handle_redeem_reply(m: Message):
        try:
            code = m.text.strip().upper()
            asyncio.create_task(deposit_manager._process_redeem(m, code))
        except Exception as e:
            await bot.send_message(
                m.chat.id,
                "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ…",
                parse_mode="html"
            )

    @bot.callback_query_handler(lambda c: c.data.startswith(f"{REVOKE_CB}:"))
    async def _cb_revoke(c: CallbackQuery):
        if not c.message:
            return await bot.answer_callback_query(c.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ...")
        code = await encode_order_id(c.data.split(':', 1)[1])
        await deposit_manager._handle_revoke(c, code)

    @bot.callback_query_handler(lambda c: c.data.startswith(f"{EXPORT_CB}:"))
    async def _cb_export(c: CallbackQuery):
        if not c.message:
            return await bot.answer_callback_query(c.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ...")
        code = c.data.split(':', 1)[1]
        await deposit_manager._handle_export(c, code)


    @bot.callback_query_handler(func=lambda call: call.data.startswith("USER:DEPOSIT"))
    async def handle_deposit_callback(call: CallbackQuery):
        try:
            if call.data == "USER:DEPOSIT":
                await deposit_manager.handle_qr_deposit(call)
            elif call.data == "USER:DEPOSIT:QR":
                await deposit_manager.start_deposit(call)
            elif call.data == "USER:DEPOSIT:CHECK":
                await bot.answer_callback_query(
                    call.id, "Payment check not implemented yet", show_alert=True
                )
            else:
                #await logger.warning("Unhandled deposit action: %s", call.data)
                await bot.answer_callback_query(
                    call.id, "🚫 Unhandled deposit action", show_alert=True
                )
        except ValueError as ve:
            #await logger.error("ValueError in deposit callback: %s", ve)
            await bot.answer_callback_query(
                call.id, "🚫 Invalid request format", show_alert=True
            )
        except Exception as e:
            #await logger.error(f"Deposit callback error: {e}")
            await bot.answer_callback_query(
                call.id, "🚫 System error occurred", show_alert=True
            )

    @bot.callback_query_handler(func=lambda call: call.data.startswith("USER:HELP"))
    async def handle_help_callback(call: CallbackQuery):
        try:
            help_text = (
                "<b>Deposit Help & Support</b>\n\n"
                "1. To make a deposit, select your deposit method.\n"
                "2. Follow the instructions provided to complete the payment.\n"
                "3. If you encounter issues, contact support."
            )
            await bot.answer_callback_query(call.id)
            await bot.send_message(call.message.chat.id, help_text, parse_mode='HTML')
        except Exception as e:
            #await logger.error(f"Help callback error: {e}")
            await bot.answer_callback_query(call.id, "🚫 Failed to display help", show_alert=True)


__all__ = ['init_managers', 'register_handlers']




