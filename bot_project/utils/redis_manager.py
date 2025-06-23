import redis.asyncio as redis
import logging
import asyncio
from typing import Optional
from dotenv import load_dotenv
from utils.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_DB

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('application.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('redis_manager')

class RedisManager:
    def __init__(self):
        self.redis_client = None
        self.connection_lock = asyncio.Lock()

        # Tweak these to taste:
        self.MAX_RETRIES = 10             # up from 3 → more attempts
        self.RETRY_DELAY = 2              # base delay in seconds
        self.MAX_BACKOFF = 30             # cap exponential back‐off
        self.SOCKET_TIMEOUT = 10          # per‐operation timeout
        self.SOCKET_CONNECT_TIMEOUT = 10  # how long to wait for TCP

        self.POOL_SIZE = 20
        self.HEALTH_CHECK_INTERVAL = 15

    async def connect(self) -> bool:
        """Establish connection pool and do an initial ping (with timeout)."""
        if self.redis_client:
            return True

        async with self.connection_lock:
            if self.redis_client:
                return True

            try:
                pool = redis.ConnectionPool(
                    host=REDIS_HOST,
                    port=REDIS_PORT,
                    db=REDIS_DB,
                    # password=REDIS_PASSWORD,  # uncomment if you need auth
                    decode_responses=True,
                    max_connections=self.POOL_SIZE,
                    socket_timeout=self.SOCKET_TIMEOUT,
                    socket_connect_timeout=self.SOCKET_CONNECT_TIMEOUT,
                    socket_keepalive=True,
                    health_check_interval=self.HEALTH_CHECK_INTERVAL
                )
                self.redis_client = redis.Redis(
                    connection_pool=pool,
                    retry_on_timeout=True,
                    socket_timeout=self.SOCKET_TIMEOUT,
                    socket_connect_timeout=self.SOCKET_CONNECT_TIMEOUT,
                    decode_responses=True
                )

                # give Redis up to SOCKET_CONNECT_TIMEOUT + a bit for PING
                await asyncio.wait_for(self.redis_client.ping(), timeout=self.SOCKET_CONNECT_TIMEOUT + 5)
                logger.info("Successfully connected to Redis")
                return True

            except asyncio.TimeoutError:
                logger.error("Redis ping timeout during connect()")
            except Exception as e:
                logger.error(f"Failed to connect to Redis: {e}")

            # if anything went wrong, reset client and return False
            self.redis_client = None
            return False

    async def ensure_connection(self) -> bool:
        """Make sure we have a live connection; retry with exponential backoff."""
        # quick check first
        if self.redis_client:
            try:
                await self.redis_client.ping()
                return True
            except Exception:
                self.redis_client = None  # force reconnect

        # retry loop
        for attempt in range(1, self.MAX_RETRIES + 1):
            if await self.connect():
                return True

            # exponential backoff
            delay = min(self.RETRY_DELAY * (2 ** (attempt - 1)), self.MAX_BACKOFF)
            logger.warning(f"Redis connection attempt {attempt}/{self.MAX_RETRIES} failed. Retrying in {delay}s…")
            await asyncio.sleep(delay)

        logger.error("All Redis connection attempts failed.")
        return False

    async def get_client(self) -> Optional[redis.Redis]:
        """Return a connected client, or None if we couldn’t connect."""
        if not await self.ensure_connection():
            return None
        return self.redis_client

    async def close(self):
        if self.redis_client:
            await self.redis_client.close()
            self.redis_client = None
            logger.info("Redis connection closed.")

# Singleton instance
redis_manager = RedisManager()
