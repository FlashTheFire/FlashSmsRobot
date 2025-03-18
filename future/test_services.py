import asyncio
import redis.asyncio as redis
from termcolor import colored

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
SERVICE_PREFIX = "*cache*"#"deposit_data:info:7284105161736940"

async def create_redis_client():
    return await redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True
    )

async def clear_cache_data(redis_client):
    keys = await redis_client.keys(f"{SERVICE_PREFIX}")
    if keys:
        print(colored(f"Clearing {len(keys)} existing records...", "yellow"))
        await redis_client.delete(*keys)
        print(colored("Existing records cleared successfully!", "green"))

async def main():
    redis_client = await create_redis_client()
    try:
        await clear_cache_data(redis_client)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await redis_client.aclose()

if __name__ == "__main__":
    asyncio.run(main())
