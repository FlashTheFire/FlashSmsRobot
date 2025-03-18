from telebot.asyncio_storage.redis_storage import redis
from utils.functions import get_api_info, fetch_url_str, small_caps, decode_barcode_id, encode_order_id
from utils.redis_manager import redis_manager
from handlers.manager.operation import OrderManagement, UserManagement
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
from telebot.async_telebot import AsyncTeleBot
from handlers.security import RateLimiter, InputValidator, TransactionGuard
from typing import Dict, Any, Optional, Union
import asyncio
import json
import aiohttp
from utils.redis_keys import RedisKeys 
from functools import partial
from typing import Optional

class UserPurchaseStatusManagement:
    def __init__(self) -> None:
        """Initialize all manager references to None."""
        self.order_manager: Optional[OrderManagement] = None
        self.user_manager: Optional[UserManagement] = None
        self.rate_limiter: Optional[RateLimiter] = None
        self.input_validator: Optional[InputValidator] = None
        self.transaction_guard: Optional[TransactionGuard] = None
        self.bot: Optional[AsyncTeleBot] = None
        self._initialized: bool = False
        self.redis_client: Optional[Any] = None

    async def init_managers(self, order_mgr: OrderManagement, user_mgr: UserManagement, bot: AsyncTeleBot) -> bool:
        """
        Initialize required components for order status handling asynchronously.
        Both OrderManagement and UserManagement must be provided along with a bot instance.
        """
        try:
            if not all([order_mgr, user_mgr, bot]):
                # Log error here if needed.
                return False

            self.order_manager = order_mgr
            self.user_manager = user_mgr
            self.bot = bot
            self.input_validator = bot.input_validator
            self.transaction_guard = bot.transaction_guard

            redis_client = await redis_manager.get_client()
            self.redis_client = redis_client
            self.rate_limiter = RateLimiter(
                redis_client=redis_client,
                duration=60,
                max_requests=30
            )

            self._initialized = True
            return True
        except Exception as e:
            # Log error if needed.
            return False

    async def cancel_number_api(self, server_id: int, order_id: str, sms_list: Optional[str] = None) -> Dict[str, Any]:
        """Call external API to cancel the phone number/order."""
        try:
            server_name, api_key = await get_api_info(int(server_id))
            url = f"https://{server_name}/stubs/handler_api.php?api_key={api_key}&action=setStatus&id={order_id}&status=8"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        text = await response.text()
                        # Print/log asynchronously if needed.
                        await asyncio.to_thread(print, f'{text}\n\n{sms_list}')
                        if text in ["NO_ACTIVATION"]:
                            if not sms_list or sms_list in ["", "[]"]:
                                return {"response": True, "text": "<blockquote><b>✅ Nᴜᴍʙᴇʀ Cᴀɴᴄᴇʟʟᴇᴅ Sᴜᴄᴄᴇssғᴜʟʟʏ</b></blockquote>"}
                            else:
                                return {"response": False, "text": "<blockquote><b>❌ Fᴀɪʟᴇᴅ Tᴏ Cᴀɴᴇʟ: Nᴏ Aᴄᴛɪᴠᴀᴛɪᴏɴ Fᴏᴜɴᴅ</b></blockquote>"}
                        elif text in ["EARLY_CANCEL_DENIED"]:
                            return {"response": False, "text": "<blockquote><b>🕔 Yᴏᴜ Nᴇᴇᴅ Tᴏ Wᴀɪᴛ Lᴏɴɢᴇʀ Bᴇᴄᴀᴜsᴇ Eᴀʀʟʏ Cᴀɴᴇʟʟᴀᴛɪᴏɴ Wᴀs Dᴇɴɪᴇᴅ..</b></blockquote>"}
                        elif text in ["ACCESS_CANCEL", "ACCESS_CANCEL_ALREADY", "STATUS_CANCEL", "ALREADY_CANCELED"]:
                            return {"response": True, "text": "<blockquote><b>✅ Nᴜᴍʙᴇʀ Cᴀɴᴄᴇʟʟᴇᴅ Sᴜᴄᴄᴇssғᴜʟʟʏ</b></blockquote>"}
                    return {"response": False, "text": f"<blockquote><b>❌ Fᴀɪʟᴇᴅ Tᴏ Cᴀɴᴇʟ Nᴜᴍʙᴇʀ: {text}</b></blockquote>"}
        except aiohttp.ClientError as e:
            return {"response": False, "text": "<blockquote><b>⚠️ Nᴇᴛᴡᴏʀᴋ Eʀʀᴏʀ Wʜɪʟᴇ Cᴀɴᴇʟʟɪɴɢ Tʜᴇ Nᴜᴍʙᴇʀ...</b></blockquote>"}
        except Exception as e:
            return {"response": False, "text": "<blockquote><b>⚠️ Uɴᴇxᴘᴇᴄᴛᴇᴅ Eʀʀᴏʀ Wʜɪʟᴇ Cᴀɴᴇʟʟɪɴɢ Tʜᴇ Nᴜᴍʙᴇʀ...</b></blockquote>"}

    async def _acquire_transaction_lock(self, guard, transaction_key, input_data) -> bool:
        """Acquire transaction lock with error handling."""
        if not await guard.acquire_lock(transaction_key):
            try:
                if isinstance(input_data, CallbackQuery):
                    await self.bot.answer_callback_query(
                        input_data.id,
                        "🔒 Aɴᴏᴛʜᴇʀ Tʀᴀɴsᴀᴄᴛɪᴏɴ Iɴ Pʀᴏɢʀᴇss, Pʟᴇᴀsᴇ Wᴀɪᴛ...", 
                        show_alert=True
                    )
                else:
                    await self.bot.send_message(
                        input_data.chat.id,
                        "🔒 Aɴᴏᴛʜᴇʀ Tʀᴀɴsᴀᴄᴛɪᴏɴ Iɴ Pʀᴏɢʀᴇss, Pʟᴇᴀsᴇ Wᴀɪᴛ...",
                        parse_mode='html'
                    )
            except Exception as e:
                print(f"Error sending message: {e}")
                pass
            return False
        return True

    async def _parse_status_cancel_input(self, input_data: Union[CallbackQuery, Message], is_callback: bool) -> Dict[str, Any]:
        """
        Parse and validate the incoming data for status cancellation.
        Returns a dictionary with keys: valid, chat_id, message_id, barcode_id, order_id, user_id.
        """
        result: Dict[str, Any] = {}
        if is_callback and input_data.message:
            result["chat_id"] = input_data.message.chat.id
            result["message_id"] = input_data.message.message_id
        else:
            result["chat_id"] = input_data.chat.id
            result["message_id"] = input_data.message_id
        data = input_data.data if is_callback else input_data.text
        parts = data.split(':')

        # If not a callback, remove the command message to keep the chat clean.
        if not is_callback:
            await self.bot.delete_message(chat_id=result["chat_id"], message_id=result["message_id"])

        if len(parts) < 2:
            error_message = "<blockquote><b>❌ Iɴᴠᴀʟɪᴅ Iɴᴘᴜᴛ Fᴏʀᴍᴀᴛ. Pʟᴇᴀsᴇ Tʀʏ Aɢᴀɪɴ.</b></blockquote>"
            if is_callback:
                await self.bot.answer_callback_query(input_data.id, error_message, show_alert=True)
            else:
                await self.bot.send_message(result["chat_id"], error_message, parse_mode='html')
            result["valid"] = False
            return result

        barcode_id = parts[1].strip()
        try:
            if barcode_id.isdigit():
                order_id = int(barcode_id)
            else:
                order_id = await decode_barcode_id(barcode_id)
        except ValueError:
            error_message = "<blockquote><b>❌ Iɴᴠᴀʟɪᴅ Iɴᴘᴜᴛ Fᴏʀᴍᴀᴛ. Pʟᴇᴀsᴇ Tʀʏ Aɢᴀɪɴ.</b></blockquote>"
            if is_callback:
                await self.bot.answer_callback_query(input_data.id, error_message, show_alert=True)
            else:
                await self.bot.send_message(result["chat_id"], error_message, parse_mode='html')
            result["valid"] = False
            return result

        result["barcode_id"] = barcode_id
        result["order_id"] = order_id
        result["user_id"] = parts[2] if len(parts) > 2 else (input_data.from_user.id if input_data.from_user else None)
        result["valid"] = True
        return result

    async def _check_rate_limit(self, order_user_id, input_data, is_callback, chat_id, message_id) -> bool:
        """
        Check if the rate limit is exceeded and notify the user if so.
        """
        if not await self.rate_limiter.limit(key="made_purchase", user_id=order_user_id):
            remaining, _ = await self.rate_limiter.remaining_limit(key="made_purchase", user_id=order_user_id)
            rate_limit_message = f"⏳ Rᴀᴛᴇ Lɪᴍɪᴛ Exᴄᴇᴇᴅᴇᴅ. Pʟᴇᴀsᴇ Tʀʏ Aɢᴀɪɴ Iɴ {remaining} Sᴇᴄᴏɴᴅs...."
            if is_callback:
                await self.bot.answer_callback_query(input_data.id, rate_limit_message, show_alert=True)
            else:
                await self.bot.send_message(chat_id, rate_limit_message, parse_mode='html', reply_to_message_id=message_id)
            return False
        return True

    async def _build_cancellation_response(self, order_info: dict) -> (InlineKeyboardMarkup, str, str, str):
        """
        Build the inline keyboard and cancellation message text.
        Returns the keyboard, text message, and phone number parts.
        """
        keyboard = InlineKeyboardMarkup()
        keyboard.row(
            InlineKeyboardButton(
                "↻ Cʜᴀɴɢᴇ Cᴏᴜɴᴛʀʏ",
                switch_inline_query_current_chat=f"#AᴘᴘIᴅ:{order_info.get('app_id', '')}"
            ),
            InlineKeyboardButton(
                "↻ Bᴜʏ Aɢᴀɪɴ",
                callback_data=(
                    f"purchase:{order_info['app_id']}:"
                    f"{order_info['order_amount']}:"
                    f"{order_info['server_id']}:"
                    f"{order_info['country_id']}:"
                    f"{order_info['country_code']}"
                )
            )
        )

        translated_app_name = order_info['app_name'].translate(await small_caps())
        number_parts = json.loads(order_info['order_number']) if isinstance(order_info.get('order_number'), str) else []
        number_part1 = number_parts[0] if len(number_parts) > 0 else ""
        number_part2 = number_parts[1] if len(number_parts) > 1 else ""

        text = (
            f"<blockquote><b>📦 {translated_app_name} [</b> 💎 "
            f"<code>{order_info['order_amount']}</code> <b>][</b> "
            f"<code>{order_info['country_code']}</code> <b>][ </b>"
            f"<code>{order_info['server_id']}</code><b> ]</b></blockquote>\n\n"
            f"<b>📞 Nᴜᴍʙᴇʀ »</b> <code>{number_part1}</code> <code>{number_part2}</code>\n\n"
            f"<b>⏱️ Oʀᴅᴇʀ Is Cᴀɴᴄᴇʟʟᴇᴅ [</b><code>Rᴇғᴜɴᴅᴇᴅ</code><b>]</b>"
        )
        return keyboard, text, number_part1, number_part2

    async def _process_cancel_flow(self, input_data: Union[CallbackQuery, Message], is_callback: bool,
                                     chat_id, message_id, order_id, user_id) -> None:
        """
        Process the cancellation steps: retrieve order info, check rate limits,
        call the cancel number API, update the order status, update the message, and trigger follow-up tasks.
        """
        order_data = await self.order_manager.get_order_data(order_id)
        if not order_data.get('response'):
            error_message = "<blockquote><b>👨🏻‍💻 Bᴀᴅ Aᴄᴛɪᴏɴ Pᴇʀғᴏʀᴍᴇᴅ, Yᴏᴜ Nᴇᴇᴅ Tᴏ Cᴏɴᴛᴀᴄᴛ Cᴜsᴛᴏᴍᴇʀ Sᴜᴘᴘᴏʀᴛ Fʀᴏᴍ Hᴇʟᴘ Dᴇsᴋ...</b></blockquote>"
            await self.bot.send_message(chat_id, error_message, parse_mode='html', reply_to_message_id=message_id)
            return

        order_info = order_data.get('result', {})
        if not order_info:
            await self.bot.send_message(chat_id, "<blockquote><b>❌ Oʀᴅᴇʀ Iɴғᴏʀᴍᴀᴛɪᴏɴ Nᴏᴛ Fᴏᴜɴᴅ.</b></blockquote>", parse_mode='html', reply_to_message_id=message_id)
            return

        order_user_id = order_info.get('user_id')
        server_id = order_info.get('server_id')
        api_order_id = order_info.get('order_id')
        orig_message_id = order_info.get('message_id')
        sms_list = order_info.get('sms_list')

        if not await self._check_rate_limit(order_user_id, input_data, is_callback, chat_id, message_id):
            return

        if order_info.get('order_status') == 'CANCELLED':
            await self.bot.send_message(
                chat_id=order_user_id,
                text="<blockquote><b>✘ Tʜᴇ Nᴜᴍʙᴇʀ Is Cᴀɴᴄᴇʟʟᴇᴅ Aʟʀᴇᴀᴅʏ Aɴᴅ Rᴇғᴜɴᴅ Wᴀs Sᴜᴄᴄᴇssғᴜʟʟʏ Iɴɪᴛɪᴀᴛᴇᴅ...</b></blockquote>",
                reply_to_message_id=message_id,
                parse_mode='html'
            )
            return

        result = await self.cancel_number_api(server_id, api_order_id, sms_list)
        if not result.get('response'):
            await self.bot.send_message(
                chat_id=order_user_id,
                text=f"<blockquote><b>{result.get('text', 'Uɴᴋɴᴏᴡɴ Eʀʀᴏʀ')}</b></blockquote>",
                reply_to_message_id=message_id,
                parse_mode='html'
            )
            return

        cancel_result = await self.order_manager.cancel_order(order_id, order_user_id, status='CANCELLED')
        if not cancel_result.get('response'):
            await self.bot.send_message(
                chat_id=order_user_id,
                text="<blockquote><b>👨🏻‍💻 Bᴀᴅ Aᴄᴛɪᴏɴ Pᴇʀғᴏʀᴍᴇᴅ, Yᴏᴜ Nᴇᴇᴅ Tᴏ Cᴏɴᴛᴀᴄᴛ Cᴜsᴛᴏᴍᴇʀ Sᴜᴘᴘᴏʀᴛ Fʀᴏᴍ Hᴇʟᴘ Dᴇsᴋ...</b></blockquote>",
                reply_to_message_id=message_id,
                parse_mode='html'
            )
            return

        keyboard, text, number_part1, number_part2 = await self._build_cancellation_response(order_info)

        try:
            await self.bot.edit_message_text(
                chat_id=order_user_id,
                text=text,
                parse_mode='html',
                message_id=orig_message_id,
                reply_markup=keyboard
            )
        except Exception as e:
            msg = await self.bot.send_message(
                chat_id=order_user_id,
                text=text,
                parse_mode='html',
                reply_markup=keyboard
            )
            orig_message_id = msg.message_id

        await self.bot.send_message(
            chat_id=order_user_id,
            text="<blockquote expandable><b>⚡️Oʀᴅᴇʀ Hᴀs Bᴇᴇɴ Cᴀɴᴄᴇʟʟᴇᴅ Sᴜᴄᴄᴇssғᴜʟʟʏ, Aɴᴅ Tʜᴇ Rᴇғᴜɴᴅ Hᴀs Bᴇᴇɴ Cʀᴇᴅɪᴛᴇᴅ Tᴏ Yᴏᴜʀ Aᴄᴄᴏᴜɴᴛ!</b></blockquote>",
            parse_mode='html',
            reply_to_message_id=orig_message_id
        )

        details = {
            "status": True,
            "order_id": order_id,
            "number": number_part2,
            "code": number_part1,
            "app_id": order_info['app_id'],
            "app_name": order_info['app_name'].translate(await small_caps()),
            "server_id": order_info['server_id'],
            "app_price": order_info['order_amount'],
            "country_id": order_info['country_id'],
            "country_code": order_info['country_code'],
            "country_name": order_info['country_name'],
            "user_id": order_user_id,
            "valid_status": "⏱️ Oʀᴅᴇʀ Is Cᴀɴᴄᴇʟʟᴇᴅ"
        }

        # Run the follow-up tasks concurrently.
        from handlers.main.show_wallet import wallet_manager
        tasks = [
            self.user_manager.send_order_report(self.bot, "edit_message_text", order_id, order_user_id, '-1002203139746', details),
            self.user_manager.user_metrics_report(self.bot, 'edit_message_text', order_user_id, '-1002203139746'),
            wallet_manager.process_wallet_update(order_user_id)
        ]
        await asyncio.gather(*tasks)

    async def handle_status_cancel(self, input_data: Union[CallbackQuery, Message]) -> None:
        """
        Handle a cancel order callback or message.
        Expected data format: "status_cancel:<barcode_id>:<user_id>" (when coming from a callback)
        or "#SᴛᴀᴛᴜsCᴀɴᴄᴇʟ:<barcode_id>:<user_id>" when coming as text.
        """
        is_callback = isinstance(input_data, CallbackQuery)
        parsed = await self._parse_status_cancel_input(input_data, is_callback)
        if not parsed.get("valid"):
            return

        chat_id = parsed["chat_id"]
        message_id = parsed["message_id"]
        order_id = parsed["order_id"]
        user_id = parsed["user_id"]

        transaction_key = RedisKeys.transaction_lock_key(user_id, f"cancel:{order_id}")
        async with TransactionGuard(self.redis_client) as guard:
            if not await self._acquire_transaction_lock(guard, transaction_key, input_data):
                return
            try:
                await self._process_cancel_flow(input_data, is_callback, chat_id, message_id, order_id, user_id)
            except Exception as e:
                error_message = "<blockquote><b>👨🏻‍💻 Bᴀᴅ Aᴄᴛɪᴏɴ Pᴇʀғᴏʀᴍᴇᴅ, Yᴏᴜ Nᴇᴇᴅ Tᴏ Cᴏɴᴛᴀᴄᴛ Cᴜsᴛᴏᴍᴇʀ Sᴜᴘᴘᴏʀᴛ Fʀᴏᴍ Hᴇʟᴘ Dᴇsᴋ...</b></blockquote>"
                if is_callback:
                    await self.bot.answer_callback_query(input_data.id, error_message.replace("<blockquote>", "").replace("</blockquote>", ""), show_alert=True)
                else:
                    await self.bot.send_message(chat_id, error_message, parse_mode='html', reply_to_message_id=message_id)
            finally:
                await guard.release_lock(transaction_key)

# Global instance and interface
purchase_status = UserPurchaseStatusManagement()

async def init_managers(order_manager: OrderManagement, user_manager: UserManagement, bot: AsyncTeleBot) -> bool:
    return await purchase_status.init_managers(order_manager, user_manager, bot)

async def register_handlers(bot: AsyncTeleBot) -> bool:
    @bot.callback_query_handler(func=lambda call: call.data.startswith("status_cancel:"))
    async def status_cancel_handler(call: CallbackQuery):
        try:
            process_purchase = partial(
                purchase_status.handle_status_cancel,
                call
            )
            asyncio.create_task(process_purchase())
        except ValueError:
            asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", show_alert=True))
        except Exception as e:
            #logger.error(f"Callback error: {e}")
            asyncio.create_task(bot.answer_callback_query(call.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ", show_alert=True))


    @bot.message_handler(func=lambda message: message.text and message.text.startswith("#SᴛᴀᴛᴜsCᴀɴᴄᴇʟ:"))
    async def status_cancel_text_handler(message: Message):
        try:
            process_purchase = partial(
                purchase_status.handle_status_cancel,
                message
            )
            asyncio.create_task(process_purchase())
        except ValueError:
            asyncio.create_task(bot.send_message(message.chat.id, "🚫 Iɴᴠᴀʟɪᴅ Rᴇǫᴜᴇsᴛ Fᴏʀᴍᴀᴛ", reply_to_message_id=message.message_id, parse_mode='html'))
        except Exception as e:
            #logger.error(f"Message error: {e}")
            asyncio.create_task(bot.send_message(message.chat.id, "🚫 Sʏsᴛᴇᴍ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ", reply_to_message_id=message.message_id, parse_mode='html'))

__all__ = ['init_managers', 'register_handlers', 'purchase_status']
