import os
import sys
from typing import Optional, Dict, Any, Tuple, List, Union
import logging
import asyncio
import aiohttp
import pytz
import asyncio
import logging
from datetime import datetime

import requests
import json
import io
import json
from termcolor import colored
from colorama import Fore, Style, init as colorama_init
import redis.asyncio as redis
from utils.redis_manager import RedisManager, redis_manager
from more_itertools import chunked
from utils.config import  WEBHOOK_HOST as FIVE_SIM_URL, URL
from handlers.manager.operation import (
    FiveSimManagement, FastSmsManagement, SmsHubManagement, GrizzlySmsManagement,
    SmsBowerManagement, VakSmsManagement, TigerSmsManagement, SmsActivateManagement
)
from utils.api import SMS_PROVIDERS_ID, SMS_PROVIDERS_KEY
from telebot.async_telebot import AsyncTeleBot
import io
import re
from utils.config import ADMIN_ID
import handlers.manager.operation as _ops

import io
import json
import logging
import aiohttp
from aiohttp import FormData


# -------------------- logging Configuration --------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# -------------------- Constants and Globals --------------------
SERVICE_PREFIX = "service_data"
REDIS_KEY_PRICE_MAP = "main_data:price-country"
colorama_init(autoreset=True)
IST = pytz.timezone("Asia/Kolkata")

# ----------------- SMS Providers ------------------


# -------------------- DataTransformer Class --------------------
class DataTransformer:
    def __init__(self, server_ids: List[str], sms_providers: Dict[str, Any], redis_client: redis.Redis):
        self.server_ids = server_ids
        self.sms_providers = sms_providers
        self.redis_client = redis_client
        self.app_mapping = {}
        self.country_map = []
        self.SCAN_COUNT = 1_000

    async def initialize(self):
        """Initialize Redis client and load mappings."""
        await self.load_app_code_mapping()
        await self.load_country_data()

    async def load_country_data(self) -> None:
        """Load country data from Redis and store it as a dictionary."""
        try:
            data = await self.redis_client.json().get('main_data:details:country_data')
            if not data:
                logging.warning("No country data found in Redis")
                self.country_map = {}
            else:
                if isinstance(data, dict):
                    self.country_map = data
                elif isinstance(data, list):
                    # Convert list to a dict with keys as string indices
                    self.country_map = {str(index): item for index, item in enumerate(data)}
                else:
                    self.country_map = {}
            print(colored(f"Country data loaded successfully, length: {len(self.country_map)}", "green"))
        except Exception as e:
            logging.error(f"Error loading country data from Redis: {e}")
            self.country_map = {}

    async def load_app_code_mapping(self) -> None:
        """Load app code mapping from Redis (Super Fast)"""
        try:
            data = await self.redis_client.json().get('main_data:service:app_data')
            if not data:
                self.app_mapping = {}
                self.app_code_map = {}
                return
            
            self.app_mapping = {}
            self.app_code_map = {}

            for app_name, details in data.items():
                app_id = details.get("app_id")
                codes = details.get("code")

                # Store full mapping
                mapping = {
                    "app_name": app_name,
                    "app_id": app_id,
                    "app_code": codes
                }
                self.app_mapping[app_name] = mapping

                # Store fast lookup {code -> mapping}
                if isinstance(codes, list):
                    for code in codes:
                        self.app_code_map[code] = mapping
                elif isinstance(codes, str):
                    self.app_code_map[codes] = mapping

        except Exception as e:
            logging.error(f"Error loading app data from Redis: {e}")
            self.app_mapping = {}
            self.app_code_map = {}

    def find_mapping(self, api_app_key: str, server_id: str):
        """
        Fast lookup for app mapping using precomputed dictionary.
        """
        mapping = self.app_code_map.get(api_app_key)
        if not mapping:
            return None  # No matching code found

        code_field = mapping["app_code"]
        server_id = str(server_id)  # Ensure it's a string

        if isinstance(code_field, list):
            # Fast condition checking without redundant loops
            if (
                (api_app_key == code_field[0] and server_id in {'1', '2', '3', '4', '5', '6'}) or
                (len(code_field) > 1 and api_app_key == code_field[1] and server_id in {}) or
                (api_app_key in code_field)
            ):
                return mapping
        else:
            if api_app_key == code_field:
                return mapping
        
        return None  # No valid mapping found

    def transform_data(self, fetched_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        with open("fetched_data.json", "w") as f:
            json.dump(fetched_data, f, indent=4)
        transformed: List[Dict[str, Any]] = []

        for record_id, country_info in self.country_map.items():
            country_entry = {
                "record_id": record_id,
                "name":       country_info.get("country_name", "Unknown"),
                "code":       country_info.get("country_code", ""),
                "flag_url":   country_info.get("flag_url", ""),
                "servers":    [],
            }

            for server_id, srv_payload in fetched_data.items():
                prov        = self.sms_providers.get(server_id, {})
                # try with the string key, if that returns falsy (None or empty), try the int key
                country_data = srv_payload.get(str(record_id)) or srv_payload.get(int(record_id))
                if not country_data:
                    logging.warning(f"No data for country {record_id} on server {server_id}")
                    continue

                apps: List[Dict[str, Any]] = []
                for api_app_key, details in country_data.items():
                    if not isinstance(details, dict):
                        logging.error(f"Unexpected details format for {api_app_key}: {details!r}")
                        continue

                    # parse the single price:count pair
                    price = count = 0
                    for price_str, count_str in details.items():
                        try:
                            price = float(price_str)
                            count = int(count_str)
                        except Exception as e:
                            logging.error(f"Conversion error for '{api_app_key}': {e}")
                        break

                    mapping = self.find_mapping(api_app_key, server_id)
                    if mapping:
                        apps.append({
                            "app_name": mapping["app_name"],
                            "app_id":   mapping["app_id"],
                            "code":     mapping["app_code"],
                            "price":    price,
                            "count":    count,
                        })

                country_entry["servers"].append({
                    "server_id":   server_id,
                    "server_name": prov.get("url", "Unknown"),
                    "apps":        apps,
                })

            if country_entry["servers"]:
                transformed.append(country_entry)

        # Sort and write out
        transformed.sort(key=lambda x: x["name"] or "")
        with open("transformed_data.json", "w") as f:
            json.dump(transformed, f, indent=4)
        return transformed




# -------------------- AutoUpdater Class --------------------
class AutoUpdater:
    SCAN_COUNT = 100       # how many keys SCAN returns per round
    BATCH_SIZE = 50        # how many keys to fetch type+value per pipeline

    def __init__(self):
        self.price_country_mapping: Dict[str, Dict[str, str]] = {}
        self.sms_providers = SMS_PROVIDERS_ID
        self.bot: Optional[AsyncTeleBot] = None

        # Convert flat set into a dict mapping: {class_name: provider_id}
        self.services = [
            (getattr(_ops, class_name), provider_id)
            for class_name, provider_id in SMS_PROVIDERS_KEY.items()
        ]

        self.redis_client: Optional[redis.Redis] = None

    async def initialize(self, bot: AsyncTeleBot = None):
        """Initialize Redis client."""
        self.bot = bot
        self.redis_client = await redis_manager.get_client()

    @staticmethod
    def sanitize_text(text):
        """Sanitize text by removing unwanted characters or formatting."""
        return text.strip() if isinstance(text, str) else str(text)

    @staticmethod
    def encode_for_redis(text):
        """Encode text for Redis storage; if a list, join with commas."""
        if isinstance(text, str):
            return text
        elif isinstance(text, list):
            return ','.join(map(str, text))
        return str(text)

    @staticmethod
    def safe_str(val):
        """Convert None to an empty string; otherwise, return the string representation."""
        return "" if val is None else str(val)

    @staticmethod
    def chunker(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            try:
                yield lst[i:i + n]
            except TypeError as e:
                print(f"ERROR:root:Error in update_data: unhashable type: 'slice' use .get and print: {e}")

    async def save_price_mapping(self, redis_client: redis.Redis) -> None:
        """Save the price-country mapping to Redis."""
        await redis_client.json().set(REDIS_KEY_PRICE_MAP, '$', self.price_country_mapping)

    async def load_price_mapping(self, redis_client: redis.Redis) -> None:
        """Load the price-country mapping from Redis."""
        stored_mapping = await redis_client.json().get(REDIS_KEY_PRICE_MAP)
        if stored_mapping:
            self.price_country_mapping = stored_mapping

    async def update_price_mapping(self, app_id: str, price: str, country_id: str) -> None:
        """Update the price-country mapping for a specific app."""
        if app_id not in self.price_country_mapping:
            self.price_country_mapping[app_id] = {}
        self.price_country_mapping[app_id][price] = country_id

    async def queue_app(
        self,
        pipe: redis.Redis.pipeline,
        app: Dict[str, Any],
        server_data: Optional[Dict[str, Any]],
        country_data: Dict[str, Any],
        server_id: int,
        matches: List[str]
    ) -> None:
        """Parse & sanitize, then queue HSETNX/HSET into pipe without executing."""
        # — parse & sanitize exactly as before —
        country_id   = country_data["record_id"]
        app_id       = self.safe_str(app.get("app_id"))
        app_name     = self.safe_str(app.get("app_name"))
        raw_price    = app.get("price")
        try:
            app_price = float(raw_price)
        except (TypeError, ValueError):
            logging.warning(f"Invalid price for app {app_id!r}: {raw_price!r}; defaulting to 0.0")
            app_price = 0.0

        try:
            app_count = int(app.get("count", 0))
        except (TypeError, ValueError):
            app_count = 0

        codes = app.get("code")
        code_field = (
            self.encode_for_redis(codes)
            if isinstance(codes, list)
            else self.safe_str(codes)
        )
        first_code = codes[0] if isinstance(codes, list) and codes else code_field
        server_name_val = server_data.get(first_code) if server_data else "any"

        redis_key = f"{SERVICE_PREFIX}:{country_id}:{server_id}:{app_id}"

        # optional skip
        if matches and f"{country_id}:{server_id}:{app_id}" in matches:
            logging.info(f"Skipping app {app_name} ({code_field})")
            return

        # queue flag‐sets
        pipe.hsetnx(redis_key, "is_show_country", "True")
        pipe.hsetnx(redis_key, "is_show_server",  "True")
        pipe.hsetnx(redis_key, "is_show_app",     "True")

        # build the hash and queue it
        mapping = {
            "country_id":   self.safe_str(country_id),
            "country_name": self.safe_str(country_data["name"]),
            "country_code": self.safe_str(country_data["code"]),
            "server_id":    self.safe_str(server_id),
            "server_name":  self.safe_str(server_name_val),
            "app_id":       app_id,
            "app_name":     app_name,
            "app_code":     code_field,
            "app_price":    app_price,
            "app_count":    app_count,
            "search_tags":  app_name.replace(" ", "").lower(),
        }
        pipe.hset(redis_key, mapping=mapping)

        # still update your external price mapping immediately
        await self.update_price_mapping(app_id, app_price, country_id)

    async def process_server(
        self,
        pipe: redis.Redis.pipeline,
        server: Dict[str, Any],
        country_data: Dict[str, Any],
        matches: List[str]
    ) -> None:
        """Queue up all app‐level writes for this server into the provided pipeline."""
        server_id = int(server["server_id"])
        server_data = None
        
        if server_id == 1:
            five_sim = FiveSimManagement()
            server_data = await five_sim.get_servers(country_data["record_id"])
            print(colored(f"Server Data: {server_data}", "blue"))
        
        for app in server["apps"]:
            await self.queue_app(pipe, app, server_data, country_data, server_id, matches)

    async def insert_data(self, data: List[Dict[str, Any]]) -> None:
        """Insert transformed data into Redis using one pipeline per country‐batch."""
        print(colored("\n\n=== Starting Data Insertion ===", "cyan"))
        await self.load_price_mapping(self.redis_client)

        # pre‐compute matches
        PREFIX = "free_numbers"
        match_pattern = f"{PREFIX}:*:free"
        matches: list[str] = []

        # Use connection pool via async context manager
        async with self.redis_client as r:
            cursor = 0
            # Loop until SCAN returns cursor=0
            while True:
                cursor, keys = await r.scan(cursor=cursor, match=match_pattern, count=10_000)
                if keys:
                    # Fast parsing via split
                    matches.extend(
                        ":".join(key.split(":")[1:-1])
                        for key in keys
                        if key.startswith(f"{PREFIX}:") and key.endswith(":free")
                    )
                # Give up control so we don't starve the loop
                await asyncio.sleep(0)
                if cursor == 0:
                    break
        print(colored(f"Matches found: {matches}", "green"))

        # split into country‐batches of N=1 (change N if you want larger batches)
        batches = list(chunked(data, 1))

        for idx, batch in enumerate(batches, 1):
            country_id = batch[0].get("record_id")
            print(colored(f"\n--- Batch {idx}/{len(batches)}: country {country_id} ---", "blue"))

            # non‐blocking delete of old keys
            """old_keys = []
            async for key in self.redis_client.scan_iter(match=f"{SERVICE_PREFIX}:{country_id}:*", count=1_000):
                old_keys.append(key)
            if old_keys:
                print(colored(f"Unlinking {len(old_keys)} old keys …", "yellow"))
                # UNLINK frees in background
                await self.redis_client.unlink(*old_keys)
                print(colored("Old keys unlinked.", "green"))"""

            # create one pipeline for this batch
            pipe = self.redis_client.pipeline()

            # queue all servers/apps
            tasks = []
            for country_data in batch:
                for server in country_data["servers"]:
                    tasks.append(
                        self.process_server(
                            pipe=pipe,
                            server=server,
                            country_data=country_data,
                            matches=matches
                        )
                    )

            # run through all queue_app calls (they’ll in turn await any price‐map updates)
            await asyncio.gather(*tasks)

            # execute the entire batch pipeline in one go
            try:
                await pipe.execute()
                print(colored(f"Executed pipeline for batch {idx}", "green"))
            except Exception as e:
                print(colored(f"Pipeline failed on batch {idx}: {e}", "red"))

            # persist your price mappings
            await self.save_price_mapping(self.redis_client)

            # give control back to the event loop
            if idx < len(batches):
                await asyncio.sleep(0)

        print(colored("\n=== Data Insertion Complete ===", "cyan"))

    async def fetch_transform_data(self):
        """Fetch and transform data from all SMS services."""
        server_ids = [sn for _, sn in self.services]
        transformer = DataTransformer(server_ids, self.sms_providers, self.redis_client)
        await transformer.initialize()  # Initialize Redis client and load mappings
        
        logging.info(f"Server IDs: {server_ids}")
        whole_data = {}
        
        try:
            whole_data = {}
            # Fetch fresh data from all services
            print(colored("Fetching fresh data from all services...", "blue"))
            print(colored(f"self.services IDs: {self.services}", "blue"))
            for ServiceClass, service_name in self.services:
                if str(service_name):
                    try:
                        async with ServiceClass() as service:
                            logging.info(f"Fetching data from {service}...")
                            print(colored(f"Fetching data from {service_name}...", "blue"))
                            data = await service.fetch_all_data()
                            logging.info(f"Received data from {service_name}.")
                            if hasattr(ServiceClass, 'select_best_service'):
                                best_data = ServiceClass.select_best_service(data)
                                logging.info(f"Selected best data from {service_name}.")
                            else:
                                best_data = data
                            whole_data[service_name] = best_data
                    except Exception as e:
                        logging.error(f"Error fetching data from {service_name}: {e}")

            logging.info("Data successfully transformed and stored in Redis")
            data = transformer.transform_data(whole_data)
            return data
        except Exception as e:
            logging.error(f"An error occurred while processing the data: {str(e)}")
            return None

    async def update_data(self):
        """Main update function that orchestrates the entire update process."""
        try:
            data = await self.fetch_transform_data()
            if data:
                print(colored("Data successfully transformed and stored in Redis", "green"))
                await self.insert_data(data)
                logging.info("Data update completed successfully")
        except Exception as e:
            logging.error(f"Error in update_data: {e}")
    
    async def recover_data(self, url: str):
        """
        Handles the /add command. Expects a URL in the message text.
        If valid, it will fetch and import Redis keys from that dump.
        """
        try:
            print(url)
            if not url.startswith("http"):
                print("❌ Invalid URL. Must start with http or https.")
                return

            await self.bot.send_message(ADMIN_ID, f"⏳ Importing Redis data from:\n{url}")
            await self.import_redis_dump(url)
            await self.bot.send_message(ADMIN_ID, "✅ Redis import complete.")
            return True
        except Exception as e:
            logging.error(f"[add_dump_from_url] Error: {e}")
            return False


    async def load_old_data_from_url(self, url: str) -> Dict[str, Any]:
        """
        Fetches JSON from either 0x0.st (GET) or temp.sh (POST) and returns as a flat dict.
        """
        if not url:
            logging.warning("[AutoUpdate.load_old_data_from_url] Empty URL")
            return {}

        try:
            timeout = aiohttp.ClientTimeout(total=20)

            # Case: temp.sh requires POST
            if "temp.sh" in url:
                match = re.search(r"https?://temp\.sh/([^/]+)/([^/]+)", url)
                if not match:
                    logging.error(f"[AutoUpdate.load_old_data_from_url] Invalid temp.sh URL: {url}")
                    return {}
                temp_id, filename = match.groups()
                post_url = f"https://temp.sh/{temp_id}/{filename}"
                resp = requests.post(post_url)
                if not resp.ok:
                    logging.error(f"[AutoUpdate.load_old_data_from_url] temp.sh POST failed: {resp.status_code}")
                    return {}
                text = resp.text

            # Case: normal URL (e.g., 0x0.st) uses GET
            else:
                async with aiohttp.ClientSession(timeout=timeout) as sess:
                    async with sess.get(url) as resp:
                        if resp.status != 200:
                            logging.error(f"[AutoUpdate.load_old_data_from_url] GET {url} failed: {resp.status}")
                            return {}
                        text = await resp.text()

            # Parse JSON
            data = json.loads(text)

            # ✅ Flatten nested list-wrapped dicts
            if isinstance(data, list):
                while isinstance(data, list) and len(data) == 1:
                    data = data[0]
                if isinstance(data, dict):
                    return data
                else:
                    logging.warning("[AutoUpdate.load_old_data_from_url] Unexpected nested list structure")
            elif isinstance(data, dict):
                return data
            else:
                logging.warning("[AutoUpdate.load_old_data_from_url] Unexpected JSON structure")

        except Exception as e:
            logging.error(f"[AutoUpdate.load_old_data_from_url] Error fetching or parsing JSON: {e}")

        return {}
    async def import_redis_dump(self, url: str, chunk_size: int = 1000) -> None:
        """
        Download a dump from URL and recreate the keys in Redis.

        :param url: URL to download from
        :param chunk_size: Number of records to process at once (default: 1000)
        """
        data = await self.load_old_data_from_url(url)
        if not data:
            logging.warning(f"[AutoUpdate] No data from {url}")
            return

        r = await redis_manager.get_client()
        total = len(data)
        restored = 0

        # Helper to enqueue the right command for each record
        def enqueue(pipe, key, rec):
            t, val = rec.get("type"), rec.get("value")
            if t == "string":
                pipe.set(key, val)
            elif t == "list" and isinstance(val, list):
                pipe.rpush(key, *val)
            elif t == "set" and isinstance(val, list):
                pipe.sadd(key, *val)
            elif t == "hash" and isinstance(val, dict):
                pipe.hset(key, mapping=val)
            elif t == "zset" and isinstance(val, list):
                # .zadd takes score/member pairs
                mapping = {m: s for m, s in val}
                pipe.zadd(key, mapping)
            elif t in ("ReJSON","ReJSON-RL"):
                payload = json.dumps(val)
                pipe.execute_command("JSON.SET", key, "$", payload)
            else:
                logging.warning(f"[AutoUpdate] Skipping unsupported type '{t}' for '{key}'")

        # Process in chunks
        for batch in chunked(data.items(), chunk_size):
            pipe = r.pipeline()
            for key, record in batch:
                enqueue(pipe, key, record)

            try:
                await pipe.execute()
                restored += len(batch)
                logging.info(f"[AutoUpdate] Restored {restored}/{total} keys")
            except Exception as e:
                logging.error(f"[AutoUpdate] Pipeline failed on batch ending at key '{batch[-1][0]}': {e}")

            # Optional: tiny sleep to prevent hammering Redis
            await asyncio.sleep(0.01)

        logging.info(f"[AutoUpdate] Completed import of {total} keys")

    async def dump_redis_data(self) -> Dict[str, Any]:
        """
        Non-blocking bulk dump of keys matching certain prefixes,
        batching into pipelines so Redis never blocks or slows down.
        """
        r = await redis_manager.get_client()
        result: Dict[str, Any] = {}

        prefixes = (
            "user_data:",
            "order_data:info:",
            "deposit_data:info:",
            "free_numbers:",
            "image_data:",
            "secure_data:",
            "main_data:",
        )

        def should_include(key: str) -> bool:
            return any(key.startswith(pref) for pref in prefixes)

        # 1) collect all candidate keys via scan_iter
        keys: List[str] = []
        async for raw in r.scan_iter(count=self.SCAN_COUNT):
            k = raw.decode() if isinstance(raw, (bytes, bytearray)) else raw
            if should_include(k):
                keys.append(k)

        # 2) process in chunks
        for i in range(0, len(keys), self.SCAN_COUNT):
            batch = keys[i : i + self.SCAN_COUNT]

            # 2a) pipeline #1: fetch types for the whole batch
            pipe = r.pipeline()
            for k in batch:
                pipe.type(k)
            types = await pipe.execute()

            # 2b) pipeline #2: fetch values based on type
            pipe = r.pipeline()
            for k, t_raw in zip(batch, types):
                # normalize to str
                t = t_raw.decode() if isinstance(t_raw, (bytes, bytearray)) else str(t_raw)
                if t == "string":
                    pipe.get(k)
                elif t == "list":
                    pipe.lrange(k, 0, -1)
                elif t == "set":
                    pipe.smembers(k)
                elif t == "hash":
                    pipe.hgetall(k)
                elif t == "zset":
                    pipe.zrange(k, 0, -1, withscores=True)
                elif t in ("ReJSON", "ReJSON-RL"):
                    pipe.execute_command("JSON.GET", k)
                else:
                    # unsupported → queue a placeholder
                    pipe.execute_command("PING")
            raws = await pipe.execute()

            # 2c) decode & record
            for k, t_raw, raw in zip(batch, types, raws):
                t = t_raw.decode() if isinstance(t_raw, (bytes, bytearray)) else str(t_raw)

                # skip if we had a placeholder PING
                if raw == b"PONG":
                    continue

                # normalize raw → Python
                if isinstance(raw, (bytes, bytearray)):
                    try:
                        v = raw.decode()
                    except UnicodeError:
                        v = raw
                elif isinstance(raw, list):
                    v = [item.decode() if isinstance(item, (bytes, bytearray)) else item
                         for item in raw]
                elif isinstance(raw, dict):
                    v = {
                        (bk.decode() if isinstance(bk, (bytes, bytearray)) else bk):
                        (bv.decode() if isinstance(bv, (bytes, bytearray)) else bv)
                        for bk, bv in raw.items()
                    }
                else:
                    v = raw

                result[k] = {"type": t, "value": v}

            # 2d) give other tasks a chance
            await asyncio.sleep(0)

        logging.info(f"[AutoUpdate.dump_redis_data] Dumped {len(result)} keys")
        return result

    async def upload_from_redis_key(self, raw: Dict[str, Any]) -> str:
        """
        Uploads Redis JSON dump to 0x0.st or temp.sh and returns the first successful upload URL.
        """

        if not raw:
            logging.error("[AutoUpdate.upload_from_redis_key] No data to upload")
            return ""

        json_bytes = io.BytesIO(json.dumps(raw, indent=2).encode("utf-8"))
        json_bytes.seek(0)
        
        # ✅ Try temp.sh next
        try:
            response = requests.post("https://temp.sh/upload", files={
                "file": ("flashsms.json", json_bytes, "application/json")
            })
            response.raise_for_status()
            url = response.text.strip()
            logging.info(f"[AutoUpdate.upload_from_redis_key] Uploaded to temp.sh: {url}")
            return url
        except Exception as e:
            logging.error(f"[AutoUpdate.upload_from_redis_key] temp.sh failed: {e}")


        # Reset buffer before retry
        json_bytes.seek(0)

        # ✅ Try 0x0.st first
        try:
            session = requests.Session()
            session.headers.pop("User-Agent", None)
            response = session.post("https://0x0.st", files={
                "file": ("flash-data.json", json_bytes, "application/json")
            })
            if response.status_code == 200:
                url = response.text.strip()
                logging.info(f"[AutoUpdate.upload_from_redis_key] Uploaded to 0x0.st: {url}")
                return url
            else:
                logging.warning(f"[AutoUpdate.upload_from_redis_key] 0x0.st failed: {response.status_code} - {response.text}")
        except Exception as e:
            logging.warning(f"[AutoUpdate.upload_from_redis_key] 0x0.st error: {e}")

        return ""

    async def send_dump_link(self, chat_id: int, file_url: str) -> None:
        """Send the dump URL to a Telegram chat."""
        if not file_url:
            logging.warning("[AutoUpdate.send_dump_link] Empty URL")
            return
        try:
            text = f"🔗 Redis dump: {file_url}"
            try:
                await self.bot.send_message(chat_id, text)
            except Exception as e:
                logging.error(f"[AutoUpdate.send_dump_link] Telegram error: {e}")
            logging.info("[AutoUpdate.send_dump_link] Sent link")
        except Exception as e:
            logging.error(f"[AutoUpdate.send_dump_link] Telegram error: {e}")

    async def save_data_cycle(self) -> None:
        """
        Full cycle: dump → save → upload → notify admin.
        """
        try:
            data = await self.dump_redis_data()
            url = await self.upload_from_redis_key(data)
            print(f"[AutoUpdate.save_data_cycle] URL: {url}")
            await self.send_dump_link(ADMIN_ID, url)
        except Exception as e:
            logging.error(f"[AutoUpdate.save_data_cycle] Failed cycle: {e}")

# Initialize the auto updater
auto_updater = AutoUpdater()



async def periodic_save_cycle(bot: AsyncTeleBot = None):
    """
    Saves data every 30 minutes.
    """
    last_run_min = -1

    while True:
        try:
            now_ist = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(IST)
            if now_ist.minute % 30 == 0 and now_ist.minute != last_run_min:
                await auto_updater.initialize(bot=bot)
                logging.info(f"Running save_data_cycle at {now_ist}")
                await auto_updater.save_data_cycle()
                last_run_min = now_ist.minute
            await asyncio.sleep(30)
        except Exception as e:
            logging.error(f"Error in save_data_cycle: {e}")
            await asyncio.sleep(60)


async def periodic_init_update(bot: AsyncTeleBot = None):
    """
    Runs initialize + update_data at 00:00 and 12:00 IST.
    """
    last_run_hour = -1

    while True:
        try:
            now_ist = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(IST)
            if now_ist.minute == 0 and now_ist.hour in (0, 12) and now_ist.hour != last_run_hour:
                logging.info(f"Running init + update_data at {now_ist}")
                await auto_updater.initialize(bot=bot)
                await auto_updater.update_data()
                last_run_hour = now_ist.hour
            await asyncio.sleep(30 * 120)
        except Exception as e:
            logging.error(f"Error in init_update: {e}")
            await asyncio.sleep(60)


async def periodic_update(update: bool = False, bot: AsyncTeleBot = None):
    """
    Starts both periodic background tasks:
    - Save cycle every 10 minutes
    - Update at 00:00 and 12:00 IST
    """

    # Run one-time update if requested
    if update:
        if not hasattr(auto_updater, 'initialized'):
            await auto_updater.initialize(bot=bot)
            redis_client = await redis_manager.get_client()
            keys = [key async for key in redis_client.scan_iter(match='service_data:*', count=1000)]
            print(colored(f"[AutoUpdate.periodic_update] Found {len(keys)} service_data keys", "green"))
            if len(keys) == 0:
                await auto_updater.recover_data(url=URL)
                auto_updater.initialized = True
                logging.info("Ran one-time initial update")
                await auto_updater.update_data()
                logging.info("Ran one-time save cycle")

    # Launch tasks in background
    #asyncio.create_task(periodic_save_cycle(bot=bot))
    asyncio.create_task(periodic_init_update(bot=bot))

    while True:
        await asyncio.sleep(3600)  # Keep parent task alive
