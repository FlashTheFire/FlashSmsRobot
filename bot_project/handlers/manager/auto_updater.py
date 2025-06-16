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
from utils.config import  WEBHOOK_HOST as FIVE_SIM_URL, REDIS_DUMP_KEY, SERVICE_DATA_KEY
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
                country_data = srv_payload.get(str(record_id))
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

    async def process_app_data(
        self,
        pipe: redis.Redis,
        app: Dict[str, Any],
        server_data: Optional[Dict[str, Any]],
        country_data: Dict[str, Any],
        server_id: int,
        matches: List[str]
    ) -> None:
        """Process individual app data and update Redis."""
        country_id = country_data["record_id"]
        country_name = country_data["name"]
        display_flag = country_data["code"]
        app_codes = app.get("code")
        app_id = self.safe_str(app.get("app_id"))
        app_price = self.safe_str(app.get("price"))
        if isinstance(app_codes, list):
            first_code = app_codes[0]
        else:
            first_code = app_codes

        code_field = self.encode_for_redis(app_codes) if isinstance(app_codes, list) else self.safe_str(app_codes)
        server_name_val = server_data.get(first_code) if server_data else "any"

            
        redis_key = f"{SERVICE_PREFIX}:{country_id}:{server_id}:{app_id}"
        await pipe.hsetnx(redis_key, "is_show_country", "True")
        await pipe.hsetnx(redis_key, "is_show_server",  "True")
        await pipe.hsetnx(redis_key, "is_show_app", "True")
        if matches:
            #print(colored(f"Matches found: {matches}", "green"))
            if str(f"{country_id}:{server_id}:{app_id}") in matches:
                redis_data = {
                    "country_id": self.safe_str(country_id),
                    "country_name": self.safe_str(country_name),
                    "country_code": self.safe_str(display_flag),
                    "server_name": 'free',
                    "server_id": self.safe_str(server_id),
                    "app_id": app_id,
                    "app_name": self.safe_str(app.get("app_name")),
                    "app_code": self.safe_str(code_field),
                    "app_price": 0.01,
                    "app_count": self.safe_str(app.get("count")),
                    "search_tags": self.safe_str(f"{app.get('app_name')}").replace(" ", "").lower()
                }
        else:
            redis_data = {
                "country_id": self.safe_str(country_id),
                "country_name": self.safe_str(country_name),
                "country_code": self.safe_str(display_flag),
                "server_name": self.safe_str(server_name_val or 'any'),
                "server_id": self.safe_str(server_id),
                "app_id": app_id,
                "app_name": self.safe_str(app.get("app_name")),
                "app_code": self.safe_str(code_field),
                "app_price": app_price,
                "app_count": self.safe_str(app.get("count")),
                "search_tags": self.safe_str(f"{app.get('app_name')}").replace(" ", "").lower()
            }
        if redis_data:
            await self.update_price_mapping(app_id, app_price, country_id)
            pipe.hset(redis_key, mapping=redis_data)
            #print("The field 'is_adjustable' exist")
        #print(colored(f"    ✓ Added: {app.get('app_name')} {app_codes} | Price: ${app_price:<6} | Stock: {app.get('count')}", "green"))

    async def process_server(
        self,
        pipe: redis.Redis,
        server: Dict[str, Any],
        country_data: Dict[str, Any],
        matches: List[str]
    ) -> None:
        """Process server data and update Redis."""
        server_id = int(server["server_id"])
        server_data = None
        
        if int(server_id) == int(1):
            five_sim = FiveSimManagement()
            server_data = await five_sim.get_servers(country_data["record_id"])
            #print(colored(f"Server data: {len(server_data)}", "red"))
        
        tasks = []
        for app in server["apps"]:
            tasks.append(self.process_app_data(pipe, app, server_data, country_data, server_id, matches))
        
        await asyncio.gather(*tasks)
        await pipe.execute()

    async def insert_data(self, data: List[Dict[str, Any]]) -> None:
        """Insert transformed data into Redis asynchronously."""
        print(colored("\n\n=== Starting Data Insertion ===", "cyan"))
        await self.load_price_mapping(self.redis_client)
        pattern = re.compile(r'^free_numbers:(.+):free$')
        matches = []    
        
        async for key in self.redis_client.scan_iter(match='free_numbers:*', count=1000):
            m = pattern.match(key)
            if m:
                # group(1) is the “60:1:659” par
                matches.append(m.group(1))
        print(colored(f"Matches found: {matches}", "green"))
        batches = list(self.chunker(data, 1))
        
        for batch_index, batch in enumerate(batches, start=1):
            tasks = []
            country_id = batch[0].get("record_id")
            print(colored(f"Processing batch {batch_index}/{len(batches)} for country {country_id}", "blue"))
            if country_id is None:
                print(f"Error in update_data: missing country_id in {batch[0].get('record_id')}")

            keys = await self.redis_client.keys(f"{SERVICE_PREFIX}:{country_id}:*")
            if keys:
                print(colored(f"Clearing {len(keys)} existing records...", "yellow"))
                await self.redis_client.delete(*keys)
            print(colored("Existing records cleared successfully!", "green"))
            for country_data in batch:
                for server in country_data["servers"]:
                    tasks.append(self.process_server(pipe=self.redis_client.pipeline(), server=server, country_data=country_data, matches=matches))
                                
            await asyncio.gather(*tasks)
            print(colored(f"\n=== Completed Batch {batch_index}/{len(batches)} ===", "cyan"))
                
            # Update persistent data
            await self.save_price_mapping(self.redis_client)
            
            if batch_index < len(batches):
                await asyncio.sleep(0.001)
    
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

            #transformed = transformer.transform_data(whole_data)
            await self.redis_client.json().set(SERVICE_DATA_KEY, '$', whole_data)
            logging.info("Data successfully transformed and stored in Redis")
            #return transformed
        except Exception as e:
            logging.error(f"An error occurred while processing the data: {str(e)}")
            return None

    async def update_data(self):
        """Main update function that orchestrates the entire update process."""
        try:
            #await self.fetch_transform_data()
            data = await self.redis_client.json().get(SERVICE_DATA_KEY) or {}
            server_ids = [sn for _, sn in self.services]
            transformer = DataTransformer(server_ids, self.sms_providers, self.redis_client)
            await transformer.initialize()  
            data = transformer.transform_data(data)
            if data:
                await self.insert_data(data)
                logging.info("Data update completed successfully")
        except Exception as e:
            logging.error(f"Error in update_data: {e}")

    async def load_old_data(self) -> Dict[str, Any]:
        """Load JSON dump stored under REDIS_DUMP_KEY in Redis."""
        try:
            raw = await self.redis_client.execute_command("JSON.GET", REDIS_DUMP_KEY, "$")
            if not raw:
                return {}
            parsed = json.loads(raw)
            if isinstance(parsed, list) and len(parsed) == 1 and isinstance(parsed[0], dict):
                return parsed[0]
            if isinstance(parsed, dict):
                return parsed
        except Exception as e:
            logging.error(f"[AutoUpdate.load_old_data] Failed to GET JSON: {e}")
        return {}

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

    async def import_redis_dump(self, url: str) -> None:
        """Download a dump from URL and recreate the keys in Redis."""
        data = await self.load_old_data_from_url(url)
        if not data:
            logging.warning(f"[AutoUpdate.import_redis_dump] No data from {url}")
            return

        r = await redis_manager.get_client()
        for key, record in data.items():
            t = record.get("type")
            val = record.get("value")
            try:
                if t == "string":
                    await r.set(key, val)

                elif t == "list":
                    if isinstance(val, list):
                        await r.rpush(key, *val)

                elif t == "set":
                    if isinstance(val, list):
                        await r.sadd(key, *val)

                elif t == "hash":
                    if isinstance(val, dict):
                        await r.hset(key, mapping=val)

                elif t == "zset":
                    if isinstance(val, list):
                        await r.zadd(key, *{m: s for m, s in val}.items())

                elif t in ("ReJSON-RL", "ReJSON"):
                    payload = json.dumps(val)
                    await r.execute_command("JSON.SET", key, "$", payload)

                else:
                    logging.warning(f"[AutoUpdate.import_redis_dump] Skipped unsupported type '{t}' for '{key}'")
                    continue

                logging.info(f"[AutoUpdate.import_redis_dump] Restored '{key}' as {t}")

            except Exception as e:
                logging.error(f"[AutoUpdate.import_redis_dump] Failed to restore '{key}': {e}")

        logging.info(f"[AutoUpdate.import_redis_dump] Completed import of {len(data)} keys")

    async def save_data_to_redis(self, data: Dict[str, Any]) -> None:
        try:            
            r = await redis_manager.get_client()
            await r.json().set(REDIS_DUMP_KEY, ".", data)
            logging.info(f"[AutoUpdate.save_data_to_redis] Saved {len(data)} entries")
        except Exception as e:
            logging.error(f"[AutoUpdate.save_data_to_redis] Error: {e}")


    async def dump_redis_data(self) -> Dict[str, Any]:
        """
        Asynchronously scan & dump only:
          - user_data:*
          - order_data:info:*
          - deposit_data:info:*
          - free_numbers:*
          - image_data:*
          - secure_data:*
          - main_data:*  (except the REDIS_DUMP_KEY itself)
        into a { key: {"type": t, "value": v} } dict,
        yielding back to the event loop at each stage.
        """
        r = await redis_manager.get_client()
        result: Dict[str, Any] = {}

        # all positive prefixes
        prefixes = (
            "user_data:",
            "order_data:info:",
            "deposit_data:info:",
            "free_numbers:",
            "image_data:",
            "secure_data:",
        )

        async def should_include(key: str) -> bool:
            # include any matching prefix
            if any(key.startswith(pref) for pref in prefixes):
                return True
            # include main_data:* but skip the exact dump key
            if key.startswith("main_data:") and key not in [REDIS_DUMP_KEY, SERVICE_DATA_KEY]:
                return True
            return False

        async def process_batch(batch: List[str]):
            # 1) pipeline to fetch types
            pipe = r.pipeline()
            for k in batch:
                pipe.type(k)
            types = await pipe.execute()
            await asyncio.sleep(0)

            # 2) pipeline to fetch values
            pipe = r.pipeline()
            for idx, k in enumerate(batch):
                t = types[idx]
                if isinstance(t, bytes):
                    t = t.decode()
                if t == "string":
                    pipe.get(k)
                elif t == "list":
                    pipe.lrange(k, 0, -1)
                elif t == "set":
                    pipe.smembers(k)
                elif t == "hash":
                    pipe.hgetall(k)
                elif t == "zset":
                    pipe.zrange(k, 0, -1, "WITHSCORES")
                elif t in ("ReJSON", "ReJSON-RL"):
                    pipe.execute_command("JSON.GET", k)
                else:
                    pipe.ping()
            values = await pipe.execute()
            await asyncio.sleep(0)

            # 3) decode & stash
            for idx, k in enumerate(batch):
                t = types[idx]
                if isinstance(t, bytes):
                    t = t.decode()
                raw = values[idx]
                if t == "string":
                    v = raw.decode() if isinstance(raw, bytes) else raw
                elif t in ("list", "set"):
                    v = [x.decode() if isinstance(x, bytes) else x for x in raw]
                elif t == "hash":
                    v = {
                        (bk.decode() if isinstance(bk, bytes) else bk):
                        (bv.decode() if isinstance(bv, bytes) else bv)
                        for bk, bv in raw.items()
                    }
                elif t == "zset":
                    v = [(m.decode() if isinstance(m, bytes) else m, s) for m, s in raw]
                elif t in ("ReJSON", "ReJSON-RL"):
                    # r.json().get() will already return a Python object,
                    # so just use it directly if it's not bytes/str:
                    if isinstance(raw, (bytes, bytearray, str)):
                        # only decode+loads if we really have text
                        js = raw.decode() if isinstance(raw, (bytes, bytearray)) else raw
                        try:
                            v = json.loads(js)
                        except json.JSONDecodeError:
                            v = js
                    else:
                        # already a dict/list
                        v = raw
                else:
                    continue

                result[k] = {"type": t, "value": v}

        # 0) SCAN loop
        cursor = b"0"
        to_process: List[str] = []

        while cursor != 0:
            cursor, keys = await r.scan(cursor=cursor, count=self.SCAN_COUNT)
            for raw in keys:
                k = raw.decode() if isinstance(raw, bytes) else raw
                if await should_include(k):
                    to_process.append(k)

            # when we have enough, process a batch
            while len(to_process) >= self.BATCH_SIZE:
                batch = to_process[: self.BATCH_SIZE]
                to_process = to_process[self.BATCH_SIZE :]
                await process_batch(batch)

            await asyncio.sleep(0)

        # leftover
        if to_process:
            await process_batch(to_process)

        logging.info(f"[AutoUpdate.dump_redis_data] Dumped {len(result)} keys")
        return result

    async def upload_from_redis_key(self) -> str:
        """
        Uploads Redis JSON dump to 0x0.st or temp.sh and returns the first successful upload URL.
        """
        r = await redis_manager.get_client()
        raw = await r.json().get(REDIS_DUMP_KEY)

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
            await self.bot.send_message(chat_id, text)
            logging.info("[AutoUpdate.send_dump_link] Sent link")
        except Exception as e:
            logging.error(f"[AutoUpdate.send_dump_link] Telegram error: {e}")

    async def save_data_cycle(self) -> None:
        """
        Full cycle: dump → save → upload → notify admin.
        """
        try:
            data = await self.dump_redis_data()
            await self.save_data_to_redis(data)
            url = await self.upload_from_redis_key()
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
            await asyncio.sleep(30)
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
                auto_updater.initialized = True
                logging.info("Ran one-time initial update")
                await auto_updater.update_data()
                logging.info("Ran one-time save cycle")
            else:
                await auto_updater.save_data_cycle()
                logging.info("Ran one-time save cycle")
    # Launch tasks in background
    asyncio.create_task(periodic_save_cycle(bot=bot))
    asyncio.create_task(periodic_init_update(bot=bot))

    while True:
        await asyncio.sleep(3600)  # Keep parent task alive
