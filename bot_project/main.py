#!/usr/bin/env python3
import sys
import os
import asyncio
import functools
import contextlib
from typing import Optional, Tuple

from aiohttp import web
import ssl
from telebot.async_telebot import AsyncTeleBot
from telebot.types import Update, InputMediaPhoto, Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from utils.cache_manager import cache_manager

from utils.config import BOT_TOKEN, CHANNEL_ID, START_PAGE
from utils.redis_manager import redis_manager
from handlers.manager.operation import (
    FinancialManagement, UserManagement, OrderManagement, DepositManagement,
    get_async_logger, user_mgr, order_mgr, deposit_mgr, financial_mgr
)
from handlers.security import InputValidator, TransactionGuard
from handlers.methods.purchase import made_purchase, show_country, show_servers, order_status
from handlers.main import inline_query, message_handler, show_refferal, show_menu, top_services, show_wallet, show_support, support_management, external
from handlers.main.external import forward_manager, ForwardManager
from handlers.methods.purchase.order_tracker import init_managers as order_tracker_init, register_handlers as order_tracker_register, order_tracker
from handlers.methods.recharge.deposit_tracker import init_managers as deposit_tracker_init, register_handlers as deposit_tracker_register, deposit_tracker
from handlers.methods.recharge import show_deposit
from handlers.methods.history import show_history
from handlers.main.inline_query import UserSearchManagement
from handlers.methods.admin import admin_panel
from api.sms_api import init_app


class TelegramBot:
    # SSL paths read from env — only needed in webhook/HTTPS mode
    CERT_PATH = os.getenv("SSL_CERT_PATH", "/app/certs/server.crt")
    KEY_PATH  = os.getenv("SSL_KEY_PATH",  "/app/certs/server.key")

    def __init__(self):
        self.bot: Optional[AsyncTeleBot] = None
        self.services_initialized: bool = False
        self.user_manager: Optional[UserManagement] = None
        self.order_manager: Optional[OrderManagement] = None
        self.deposit_manager: Optional[DepositManagement] = None
        self.forward_manager: Optional[ForwardManager] = None
        
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

        # Step 2: Add optional hello GET route for health checks
        async def hello(request):
            return web.Response(text="Hello from FlashSmsRobot combined server")
        app.router.add_get("/", hello, allow_head=False)

        # Step 3: Run aiohttp server
        runner = web.AppRunner(app)
        await runner.setup()

        # Determine if we should use SSL
        ssl_ctx = None
        if os.path.exists(self.CERT_PATH) and os.path.exists(self.KEY_PATH):
            try:
                ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
                ssl_ctx.load_cert_chain(certfile=self.CERT_PATH, keyfile=self.KEY_PATH)
                logger = await get_async_logger()
                await logger.info("SSL certificates found. Starting HTTPS server.")
            except Exception as e:
                logger = await get_async_logger()
                await logger.error(f"Failed to load SSL certificates: {e}.")
                ssl_ctx = None

        # Webhook mode requires HTTPS — abort rather than silently serve HTTP
        if self.use_webhook and ssl_ctx is None:
            logger = await get_async_logger()
            await logger.error(
                "Webhook mode is enabled but SSL context could not be loaded "
                f"(CERT_PATH={self.CERT_PATH}, KEY_PATH={self.KEY_PATH}). "
                "Aborting startup."
            )
            raise RuntimeError(
                "Webhook mode requires valid SSL certificates. "
                "Set USE_WEBHOOK=false or provide valid SSL_CERT_PATH / SSL_KEY_PATH."
            )

        site = web.TCPSite(runner, host="0.0.0.0", port=8443, ssl_context=ssl_ctx)
        await site.start()
        
        logger = await get_async_logger()
        protocol = "HTTPS" if ssl_ctx else "HTTP"
        await logger.info(f"✅ Combined {protocol} server started on port 8443")
        return runner

    
    async def hello(self, request):
        return web.Response(text="Hello from combined server")

    async def redirect_to_https(self, request):
        # Build the same URL but force https + port 8443
        url = request.url.with_scheme("https").with_port(8443)
        raise web.HTTPPermanentRedirect(location=str(url))
    



    async def initialize_managers(self) -> bool:
        """Initialize all required managers using global instances."""
        try:
            self.user_manager = user_mgr
            self.order_manager = order_mgr
            self.deposit_manager = deposit_mgr
            
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
            self.bot.aggregator = financial_mgr
            self.forward_manager = forward_manager

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
        await order_tracker_init(order_manager=self.order_manager, user_manager=self.user_manager, bot=self.bot)
        await order_tracker_register(self.bot)
        await order_tracker.start()

        # Initialize and register deposit tracker
        await deposit_tracker_init(deposit_manager=self.deposit_manager, user_manager=self.user_manager, bot=self.bot)
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
        if self.forward_manager:
            await self.forward_manager.shutdown()

    async def register_handlers(self) -> bool:
        """Register all message handlers with the bot."""
        if not self.services_initialized:
            logger = await get_async_logger()
            await logger.error("Cannot register handlers: services not initialized")
            return False

        handlers = [
            (show_menu, "show_menu"),
            (external, "external"),
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
            (inline_query, "inline_query"),
            (message_handler, "message_handler"),
        ]

        success = True
        for handler, name in handlers:
            try:
                if hasattr(handler, 'init_managers'):
                    import inspect
                    sig = inspect.signature(handler.init_managers)
                    kwargs = {}
                    if 'user_manager' in sig.parameters: kwargs['user_manager'] = self.user_manager
                    if 'order_manager' in sig.parameters: kwargs['order_manager'] = self.order_manager
                    if 'deposit_manager' in sig.parameters: kwargs['deposit_manager'] = self.deposit_manager
                    if 'bot' in sig.parameters: kwargs['bot'] = self.bot
                    
                    await handler.init_managers(**kwargs)
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
        runner = None
        update_task = None
        polling_task = None
        try:
            # Always run the combined API server on 8443 for v1 routes
            runner = await bot.start_server()

            # Register handlers
            await bot.register_handlers()

            # Start periodic update
            from handlers.manager.auto_updater import periodic_update
            update_task = asyncio.create_task(periodic_update(update=True, bot=bot.bot))

            if bot.use_webhook:
                # Webhook mode: setup webhook and wait for periodic update
                await bot.setup_webhook()
                await update_task
            else:
                # Polling mode: clear webhook and run polling concurrently
                await bot.bot.delete_webhook()
                polling_task = asyncio.create_task(bot.bot.polling(non_stop=True, timeout=60))
                await asyncio.gather(polling_task, update_task)
        except Exception as e:
            logger = await get_async_logger()
            await logger.error(f"Startup error: {e}")
        finally:
            # Cancel any still-running background tasks before tearing down the server
            for _task in (update_task, polling_task):
                if _task is not None and not _task.done():
                    _task.cancel()
                    try:
                        await _task
                    except (asyncio.CancelledError, Exception) as _ce:
                        try:
                            _logger = await get_async_logger()
                            await _logger.info(f"Background task cancelled during shutdown: {_ce}")
                        except Exception:
                            pass
            if runner is not None:
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
