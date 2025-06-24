#!/usr/bin/env python3
import sys
import os
import asyncio
import functools
import contextlib
from typing import Optional, Tuple

from aiohttp import web
import ssl
from aiohttp import web
from telebot.async_telebot import AsyncTeleBot
from telebot.types import Update, InputMediaPhoto, Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from utils.cache_manager import cache_manager

from utils.config import BOT_TOKEN, CHANNEL_ID, START_PAGE
from utils.redis_manager import redis_manager
from handlers.manager.operation import (
    FinancialManagement, UserManagement, OrderManagement, DepositManagement,
    get_async_logger
)
from handlers.security import InputValidator, TransactionGuard
from handlers.methods.purchase import made_purchase, show_country, show_servers, order_status
from handlers.main import inline_query, message_handler, show_refferal, show_menu, top_services, show_wallet, show_support, support_management
from handlers.methods.purchase.order_tracker import init_managers as order_tracker_init, register_handlers as order_tracker_register, order_tracker
from handlers.methods.recharge.deposit_tracker import init_managers as deposit_tracker_init, register_handlers as deposit_tracker_register, deposit_tracker
from handlers.methods.recharge import show_deposit
from handlers.methods.history import show_history
from handlers.main.inline_query import UserSearchManagement
from handlers.methods.admin import admin_panel
from api.sms_api import init_app


class TelegramBot:
    CERT_PATH = r"C:\Users\LOQ\OneDrive\Desktop\Coding-Flash\flash_sms\server.crt"
    KEY_PATH  = r"C:\Users\LOQ\OneDrive\Desktop\Coding-Flash\flash_sms\server.key"

    def __init__(self):
        self.bot: Optional[AsyncTeleBot] = None
        self.services_initialized: bool = False
        self.user_manager: Optional[UserManagement] = None
        self.order_manager: Optional[OrderManagement] = None
        self.deposit_manager: Optional[DepositManagement] = None
        
        # Webhook configuration
        self.use_webhook: bool = os.getenv("USE_WEBHOOK", "false").lower() == "true"
        self.webhook_host: str = os.getenv("WEBHOOK_HOST", "https://yourdomain.com")
        self.webhook_path: str = f'/webhook/{BOT_TOKEN}'
        self.webhook_url: str = f'{self.webhook_host}{self.webhook_path}'
         
    @staticmethod
    async def safe_call(func, *args, retries=3, **kwargs):
        """Execute a function with retry logic."""
        for attempt in range(retries):
            try:
                return await func(*args, **kwargs)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                try:
                    logger = await get_async_logger()
                    await logger.warning(f"{func.__name__} failed on attempt {attempt + 1}: {e}")
                    await asyncio.sleep(2 ** attempt)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    print(f"Failed to log error: {e}")
                    
    @contextlib.asynccontextmanager
    async def initialize_services(self):
        """Initialize all required services."""
        try:
            await redis_manager.ensure_connection()
            if not await self.initialize_managers():
                raise Exception("Failed to initialize managers")
            if not await self.initialize_bot():
                raise Exception("Failed to initialize bot")
            logger = await get_async_logger()
            await logger.info("Managers and security components initialized successfully")
            self.services_initialized = True
            yield
        except Exception as e:
            logger = await get_async_logger()
            await logger.error(f"Error during service initialization: {str(e)}")
            raise
        finally:
            await self.safe_call(self.shutdown)
            await self.safe_call(redis_manager.close)
 
    async def handle_webhook(self, request: web.Request) -> web.Response:
        """Handle incoming webhook requests from Telegram."""
        if request.headers.get('Content-Type') == 'application/json':
            try:
                json_data = await request.json()
                update = Update.de_json(json_data)
                await self.bot.process_new_updates([update])
                return web.Response(text="OK")
            except Exception as e:
                logger = await get_async_logger()
                await logger.error(f"Error processing webhook update: {e}")
                return web.Response(status=500, text="Error")
        return web.Response(status=403, text="Forbidden")

    async def setup_webhook(self) -> None:
        """Configure webhook settings for the bot."""
        try:
            await self.bot.remove_webhook()
            await self.bot.set_webhook(url=self.webhook_url)
            logger = await get_async_logger()
            await logger.info("Webhook configured successfully")
        except Exception as e:
            logger = await get_async_logger()
            await logger.error(f"Failed to set webhook: {e}")
    
    async def start_server(self):
        """
        Start the combined Telegram + SMS aiohttp server on port 8443
        using AppRunner instead of web.run_app().
        """
        # Step 1: Init aiohttp app with Redis and API
        app = await init_app(self.bot)

        # Step 2: Add Telegram webhook route
        #app.router.add_post(self.webhook_path, self.handle_webhook, allow_head=False)

        # Step 3: (optional) hello GET route
        async def hello(request):
            return web.Response(text="Hello from combined server")
        app.router.add_get("/", hello, allow_head=False)

        # Step 4: Setup Telegram webhook (optional)
        #if self.use_webhook:
        #    await self.setup_webhook()

        # Step 5: Run aiohttp server on current loop
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host="0.0.0.0", port=8443)
        await site.start()
        logger = await get_async_logger()
        await logger.info("✅ Combined server started on port 8443")
        return runner  # return this so you can later call await runner.cleanup()

    '''async def start_server(self):
        # ─── 1) Main app on HTTPS ────────────────────────────────────────────
        app = await init_app(self.bot)

        # Optional “hello” route
        app.router.add_get("/", self.hello, allow_head=False)

        # Prepare SSL context
        ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_ctx.load_cert_chain(
            certfile=self.CERT_PATH,
            keyfile=self.KEY_PATH
        )

        # AppRunner for your real API
        runner = web.AppRunner(app)
        await runner.setup()

        # Serve HTTPS on 8443
        https_site = web.TCPSite(
            runner,
            host="0.0.0.0",
            port=8443,
            ssl_context=ssl_ctx
        )
        await https_site.start()


        # ─── 2) Redirect app on HTTP ─────────────────────────────────────────
        redirect_app = web.Application()
        # catch-all route: redirect everything to HTTPS
        redirect_app.router.add_route("*", "/{tail:.*}", self.redirect_to_https)

        redirect_runner = web.AppRunner(redirect_app)
        await redirect_runner.setup()

        # Serve HTTP on 8080
        redirect_site = web.TCPSite(
            redirect_runner,
            host="0.0.0.0",
            port=8080
        )
        await redirect_site.start()


        # ─── 3) Logging & Return ──────────────────────────────────────────────
        logger = await get_async_logger()
        await logger.info("✅ HTTP → HTTPS redirect running on :8080 → :8443; API on HTTPS :8443")

        # Return both runners so you can later clean them up if needed
        return runner'''
    '''async def start_server(self):
        # ─── 1) Build app ───────────────────────────────────────────────────
        app = await init_app(self.bot)
        app.router.add_get("/", self.hello, allow_head=False)

        # ─── 2) SSL context ─────────────────────────────────────────────────
        ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_ctx.load_cert_chain(
            certfile=self.CERT_PATH,
            keyfile=self.KEY_PATH
        )

        # ─── 3) Run only HTTPS ──────────────────────────────────────────────
        runner = web.AppRunner(app)
        await runner.setup()

        https_site = web.TCPSite(
            runner,
            host="0.0.0.0",
            port=8443,
            ssl_context=ssl_ctx
        )
        await https_site.start()

        # ─── 4) Log & return ────────────────────────────────────────────────
        logger = await get_async_logger()
        await logger.info("✅ Serving HTTPS on port 8443 only")
        return runner'''
    
    async def hello(self, request):
        return web.Response(text="Hello from combined server")

    async def redirect_to_https(self, request):
        # Build the same URL but force https + port 8443
        url = request.url.with_scheme("https").with_port(8443)
        raise web.HTTPPermanentRedirect(location=str(url))
    



    async def initialize_managers(self) -> bool:
        """Initialize all required managers."""
        try:
            self.user_manager = UserManagement(redis_manager, BOT_TOKEN, CHANNEL_ID)
            self.order_manager = OrderManagement(redis_manager)
            self.deposit_manager = DepositManagement(redis_manager)
            
            # Initialize loggers for each manager
            await self.user_manager._init_logger()
            await self.order_manager._init_logger()
            await self.deposit_manager._init_logger()
            
            # Initialize search indexes
            await self.user_manager._init_search_indexes()
            await self.order_manager._init_search_indexes()
            await self.deposit_manager._init_search_indexes()
            return True
        except Exception as e:
            logger = await get_async_logger()
            await logger.error(f"Failed to initialize managers: {e}")
            return False

    async def initialize_bot(self) -> bool:
        """Initialize the Telegram bot and its components."""
        try:
            self.bot = AsyncTeleBot(BOT_TOKEN)
            self.bot.input_validator = InputValidator()
            self.bot.transaction_guard = TransactionGuard(await redis_manager.get_client())
            self.bot.user_manager = self.user_manager
            self.bot.order_manager = self.order_manager
            self.bot.deposit_manager = self.deposit_manager
            self.bot.aggregator = FinancialManagement(
                self.deposit_manager, 
                self.order_manager, 
                self.user_manager
            )

            # Initialize trackers
            await self._initialize_trackers()
            return True
        except Exception as e:
            logger = await get_async_logger()
            await logger.error(f"Failed to initialize bot: {e}")
            return False

    async def _initialize_trackers(self) -> None:
        """Initialize order and deposit trackers."""
        # Initialize and register order tracker
        await order_tracker_init(self.order_manager, self.user_manager, self.bot)
        await order_tracker_register(self.bot)
        await order_tracker.start()

        # Initialize and register deposit tracker
        await deposit_tracker_init(self.deposit_manager, self.user_manager, self.bot)
        await deposit_tracker_register(self.bot)
        await deposit_tracker.start()

    async def shutdown(self) -> None:
        """Gracefully shutdown all components."""
        if order_tracker:
            await order_tracker.stop()
        if deposit_tracker:
            await deposit_tracker.stop()
        if self.bot:
            await self.bot.close_session()

    async def register_handlers(self) -> bool:
        """Register all message handlers with the bot."""
        if not self.services_initialized:
            logger = await get_async_logger()
            await logger.error("Cannot register handlers: services not initialized")
            return False

        handlers = [
            (show_menu, "show_menu"),
            (show_wallet, "show_wallet"),
            (made_purchase, "made_purchase"),
            (order_status, "order_status"),
            (show_servers, "show_servers"),
            (show_country, "show_country"),
            (show_deposit, "show_deposit"),
            (show_history, "show_history"),
            (support_management, "support_management"),
            (top_services, "top_services"),
            (show_refferal, "show_refferal"),
            (show_support, "show_support"),
            (admin_panel, "admin_panel"),
            (inline_query, "inline_query")
            #(message_handler, "message_handler"),
        ]

        success = True
        for handler, name in handlers:
            try:
                if hasattr(handler, 'init_managers'):
                    await handler.init_managers(
                        user_manager=self.user_manager,
                        order_manager=self.order_manager,
                        bot=self.bot
                    )
                await handler.register_handlers(self.bot)
                logger = await get_async_logger()
                await logger.info(f"Handler registered: {name}")
            except Exception as e:
                logger = await get_async_logger()
                await logger.error(f"Failed to register handler {name}: {e}")
                success = False
        await cache_manager.get_redis()
        return success

    async def start_polling(self) -> None:
        """Start the bot in polling mode."""
        for attempt in range(3):
            try:
                logger = await get_async_logger()
                await logger.info("Starting bot polling...")
                await self.bot.polling(non_stop=True, timeout=60)
                break
            except Exception as e:
                logger = await get_async_logger()
                await logger.error(f"Polling failed on attempt {attempt + 1}: {e}")
                await asyncio.sleep(5)


async def main():
    """Entry point of the application."""
    bot = TelegramBot()
    
    # Create tasks for both the bot and the periodic updater
    async with bot.initialize_services():
        try:
            if bot.use_webhook:
                #await bot.setup_webhook()
                await bot.bot.delete_webhook()
                runner = await bot.start_server()
            else:
                # Delete webhook before starting polling mode
                await bot.bot.delete_webhook()
            update_task = asyncio.create_task(periodic_update(update=bot.use_webhook, bot=bot.bot))
            handler_task = asyncio.create_task(bot.register_handlers())
            polling_task = asyncio.create_task(bot.bot.polling(non_stop=True, timeout=60))
            
            await asyncio.gather(handler_task, polling_task, update_task)
            if bot.use_webhook:
                await asyncio.Event().wait()  # keep running
        except Exception as e:
            print(f"Startup error: {e}")
        finally:
            if bot.use_webhook:
                await runner.cleanup()
                print("Server shutdown complete.")

if __name__ == "__main__":
    from handlers.manager.auto_updater import periodic_update
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Error in main: {e}")
        pass
    print("Bot stopped.")
