import redis.asyncio as redis
import logging
import os
import asyncio
from typing import Optional
from dotenv import load_dotenv
from utils.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_DB

# Load environment variables
load_dotenv()

# Setup logging
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
        self.MAX_RETRIES = 3
        self.RETRY_DELAY = 1
        self.POOL_SIZE = 10
        self.SOCKET_TIMEOUT = 10
        self.SOCKET_CONNECT_TIMEOUT = 5
        
    async def connect(self) -> bool:
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
                    #password=REDIS_PASSWORD,
                    decode_responses=True,
                    max_connections=self.POOL_SIZE,
                    socket_timeout=self.SOCKET_TIMEOUT,
                    socket_connect_timeout=self.SOCKET_CONNECT_TIMEOUT,
                    socket_keepalive=True,
                    health_check_interval=15
                )
                self.redis_client = redis.Redis(
                    connection_pool=pool,
                    socket_timeout=self.SOCKET_TIMEOUT,
                    socket_connect_timeout=self.SOCKET_CONNECT_TIMEOUT,
                    retry_on_timeout=True,
                    decode_responses=True
                )
                
                try:
                    await asyncio.wait_for(self.redis_client.ping(), timeout=5.0)
                    logger.info("Successfully connected to local Redis")
                    return True
                except asyncio.TimeoutError:
                    logger.error("Redis ping timeout")
                    self.redis_client = None
                    return False
                
            except Exception as e:
                logger.error(f"Failed to connect to Redis: {e}")
                self.redis_client = None
                return False
                
    async def ensure_connection(self) -> bool:
        if self.redis_client:
            try:
                await self.redis_client.ping()
                return True
            except:
                self.redis_client = None
                
        for attempt in range(self.MAX_RETRIES):
            try:
                if await self.connect():
                    return True
                    
                delay = min(self.RETRY_DELAY * (2 ** attempt), 4)
                logger.warning(f"Redis connection attempt {attempt + 1} failed. Retrying in {delay}s...")
                await asyncio.sleep(delay)
                
            except Exception as e:
                logger.error(f"Connection attempt {attempt + 1} failed: {e}")
                await asyncio.sleep(self.RETRY_DELAY * (2 ** attempt))
                
        return False
        
    async def get_client(self) -> Optional[redis.Redis]:
        return self.redis_client
        
    async def close(self):
        if self.redis_client:
            await self.redis_client.close()
            self.redis_client = None
            logger.info("Redis connection closed.")

redis_manager = RedisManager()
