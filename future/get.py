import asyncio
import json
import logging
import os
import aiofiles
import aiohttp

import redis.asyncio as redis
from telebot.async_telebot import AsyncTeleBot
from telebot import types

# === Configuration ===
LOCAL_REDIS_URL = "redis://localhost:6379/0"  # Local Redis instance URL
TELEGRAM_BOT_TOKEN = "7128013478:AAGwmYGSGSbyEAYnySG8nh6TAAC6fxHHJho"
ADMIN_CHAT_ID = "5716978793"  # Admin chat identifier (as string or int)
DOWNLOAD_FILE = "downloaded_dump.json"  # Local file name for the downloaded dump

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


async def download_file(url: str, filename: str) -> bool:
    """
    Downloads a file from the given URL and saves it to 'filename' asynchronously.
    Returns True on success, False on failure.
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    file_data = await response.read()
                    async with aiofiles.open(filename, "wb") as f:
                        await f.write(file_data)
                    logging.info(f"Downloaded file from {url} to {filename}")
                    return True
                else:
                    logging.error(f"Failed to download file. HTTP status code: {response.status}")
                    return False
    except Exception as e:
        logging.error(f"Error downloading file: {e}")
        return False


async def restore_to_local_redis(filename: str):
    """
    Reads the JSON dump file and restores the keys to the local Redis instance.
    Expects the dump format: { key: {"type": key_type, "value": value} }.
    """
    try:
        async with aiofiles.open(filename, "r") as f:
            content = await f.read()
        data = json.loads(content)
    except Exception as e:
        logging.error(f"Error reading dump file: {e}")
        return

    r = redis.from_url(LOCAL_REDIS_URL, decode_responses=True)
    for key, key_data in data.items():
        try:
            key_type = key_data.get("type")
            value = key_data.get("value")
            # Delete key before restoration to avoid conflicts.
            await r.delete(key)
            if key_type == "string":
                await r.set(key, value)
            elif key_type == "list":
                if isinstance(value, list):
                    await r.rpush(key, *value)
            elif key_type == "set":
                if isinstance(value, list):
                    await r.sadd(key, *value)
            elif key_type == "hash":
                if isinstance(value, dict):
                    await r.hset(key, mapping=value)
            elif key_type == "zset":
                if isinstance(value, list):
                    # Expecting a list of [member, score] pairs (JSON converts tuples to lists)
                    mapping = {member: score for member, score in value}
                    await r.zadd(key, mapping)
            elif key_type == "ReJSON-RL":
                # For ReJSON, assume value is a JSON string. Restore using JSON.SET.
                await r.execute_command("JSON.SET", key, ".", value)
            else:
                logging.warning(f"Unsupported key type {key_type} for key {key}. Skipping.")
        except Exception as e:
            logging.error(f"Error restoring key {key}: {e}")
    await r.aclose()
    logging.info("Restoration to local Redis completed.")


# --- Telegram Bot Setup for Receiving the Restore Command ---

bot = AsyncTeleBot(TELEGRAM_BOT_TOKEN)

@bot.message_handler(commands=["restore"])
async def handle_restore(message: types.Message):
    """
    Handler for the /restore command.
    Expects the command format: /restore <dump_file_url>
    Only accepts commands from the configured ADMIN_CHAT_ID.
    """
    # Verify sender is the admin.
    if str(message.chat.id) != str(ADMIN_CHAT_ID):
        await bot.send_message(message.chat.id, "Unauthorized.")
        return

    parts = message.text.split()
    if len(parts) != 2:
        await bot.send_message(message.chat.id, "Usage: /restore <dump_file_url>")
        return

    file_url = parts[1]
    await bot.send_message(message.chat.id, f"Downloading dump file from:\n{file_url}")
    success = await download_file(file_url, DOWNLOAD_FILE)
    if success:
        await bot.send_message(message.chat.id, "File downloaded successfully. Starting restoration to local Redis...")
        await restore_to_local_redis(DOWNLOAD_FILE)
        await bot.send_message(message.chat.id, "Restoration completed.")
    else:
        await bot.send_message(message.chat.id, "Failed to download the dump file.")


async def main():
    logging.info("Local receiver bot started. Waiting for /restore command...")
    # Start polling for Telegram messages.
    await bot.infinity_polling()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Local receiver interrupted by user. Exiting...")