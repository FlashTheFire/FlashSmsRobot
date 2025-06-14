import os
import sys
from typing import Optional, Dict, Any, Tuple, List, Union
import logging
import asyncio
import aiohttp
import json
from termcolor import colored
from colorama import Fore, Style, init as colorama_init
import redis.asyncio as redis
from utils.redis_manager import RedisManager, redis_manager
from utils.config import  WEBHOOK_HOST as FIVE_SIM_URL
import datetime
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


# -------------------- logging Configuration --------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# -------------------- Constants and Globals --------------------
SERVICE_PREFIX = "service_data"
REDIS_KEY_PRICE_MAP = "main_data:price-country"
colorama_init(autoreset=True)
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
        # Dump raw fetched_data for post-mortem
        with open("fetched_data.json", "w") as f:
            json.dump(fetched_data, f, indent=2)
        # Only log actual problems from here on
        
        # Load the data from a file for testing
        with open("fetched_data.json", "r") as f:
            fetched_data = json.load(f)
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
    REDIS_DUMP_KEY = "main_data:service:main_data_json"

    def __init__(self):
        self.price_country_mapping: Dict[str, Dict[str, str]] = {}
        self.sms_providers = SMS_PROVIDERS_ID

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
        if int(server_id) == 1:
            print(f"Country ({country_id}): {country_name} [{display_flag}]")
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
        if int(server_id) == 1:
            print(f"Redis Key: {redis_key}")
        await pipe.hsetnx(redis_key, "is_show_country", "True")
        await pipe.hsetnx(redis_key, "is_show_server",  "True")
        await pipe.hsetnx(redis_key, "is_show_app", "True")

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
        
        if server_id == 1:
            five_sim = FiveSimManagement()
            server_data = await five_sim.get_servers(country_data["record_id"])
            print(colored(f"Server data: {len(server_data)}", "red"))
        
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
        # Load persistent country data (or initialize if not exists)
        print(colored(f"Loading persistent country data...", "blue"))
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
                await asyncio.sleep(0.1)
    
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

            transformed = transformer.transform_data(whole_data)
            await self.redis_client.json().set('main_data:service:main_data', '$', whole_data)
            logging.info("Data successfully transformed and stored in Redis")
            return transformed
        except Exception as e:
            logging.error(f"An error occurred while processing the data: {str(e)}")
            return None

    async def update_data(self):
        """Main update function that orchestrates the entire update process."""
        try:
            data = await self.fetch_transform_data() # await self.redis_client.json().get('main_data:service:main_data') or {} #y
            if data:
                await self.insert_data(data)
                logging.info("Data update completed successfully")
        except Exception as e:
            logging.error(f"Error in update_data: {e}")

    async def load_old_data(self) -> Dict[str, Any]:
        """
        Load previously dumped data (a JSON object) from Redis under REDIS_DUMP_KEY.
        Returns an empty dict if the key does not exist or on error.
        """
        try:
            raw = await self.redis_client.execute_command("JSON.GET", self.REDIS_DUMP_KEY, "$")
            if not raw:
                return {}
            parsed = json.loads(raw)
            if isinstance(parsed, list) and len(parsed) == 1 and isinstance(parsed[0], dict):
                return parsed[0]
            if isinstance(parsed, dict):
                return parsed
            return {}
        except Exception as e:
            logging.error(f"[AutoUpdate.load_old_data] Failed to GET JSON from Redis: {e}")
            return {}

    async def load_old_data_from_url(self, url: str) -> Dict[str, Any]:
        """
        Fetches a JSON payload from the given URL and returns it as a dict.
        If the fetch fails or the content is not valid JSON, returns {}.
        """
        if not url:
            logging.warning("[AutoUpdate.load_old_data_from_url] Empty URL provided, returning {}.")
            return {}

        try:
            timeout = aiohttp.ClientTimeout(total=20)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        logging.error(f"[AutoUpdate.load_old_data_from_url] GET {url} failed with {resp.status}")
                        return {}
                    text = await resp.text()
        except Exception as e:
            logging.error(f"[AutoUpdate.load_old_data_from_url] Exception while fetching {url}: {e}")
            return {}

        try:
            data = json.loads(text)
            # unwrap [ {...} ] → {...}
            if isinstance(data, list) and len(data) == 1 and isinstance(data[0], dict):
                return data[0]
            if isinstance(data, dict):
                return data
            logging.warning(f"[AutoUpdate.load_old_data_from_url] JSON fetched from {url} was not a dict; returning {{}}.")
            return {}
        except json.JSONDecodeError as e:
            logging.error(f"[AutoUpdate.load_old_data_from_url] Invalid JSON at {url}: {e}")
            return {}
    async def import_redis_dump(self, url: str) -> None:
        """
        Download the JSON dump from `url`, then iterate its keys
        and re-create them in Redis with the correct type & value.
        """
        data = await self.load_old_data_from_url(url)
        if not data:
            logging.warning(f"[AutoUpdate.import_redis_dump] No data fetched from {url}. Nothing to import.")
            return

        r: redis.Redis = self.redis_client
        for key, record in data.items():
            t = record.get("type")
            val = record.get("value")

            try:
                if t == "string":
                    await r.set(key, val)

                elif t == "list":
                    # overwrite
                    await r.delete(key)
                    # assuming val is a list
                    if isinstance(val, list) and val:
                        await r.rpush(key, *val)

                elif t == "set":
                    await r.delete(key)
                    if isinstance(val, list) and val:
                        await r.sadd(key, *val)

                elif t == "hash":
                    # val is a dict
                    await r.delete(key)
                    if isinstance(val, dict) and val:
                        await r.hset(key, mapping=val)

                elif t == "zset":
                    # val is list of [member,score] pairs
                    await r.delete(key)
                    if isinstance(val, list) and val:
                        # flatten to (score, member) tuples
                        await r.zadd(key, *{member: score for member, score in val}.items())

                elif t in ("ReJSON-RL", "ReJSON"):
                    # val already JSON-serializable
                    payload = json.dumps(val)
                    await r.execute_command("JSON.SET", key, "$", payload)

                else:
                    logging.warning(f"[AutoUpdate.import_redis_dump] Unsupported type '{t}' for key '{key}', skipping.")
                    continue

                logging.info(f"[AutoUpdate.import_redis_dump] Restored key '{key}' [{t}].")

            except Exception as e:
                logging.error(f"[AutoUpdate.import_redis_dump] Failed to restore '{key}': {e}")

        logging.info(f"[AutoUpdate.import_redis_dump] Import completed: {len(data)} keys processed.")

    async def save_data_to_redis(self, data: Dict[str, Any]) -> None:
        """
        Store `data` (a Python dict) into Redis under REDIS_DUMP_KEY as JSON.
        Overwrites any previous dump in that key.
        """
        try:
            # Wrap in a list so JSON.GET → “[ {…} ]”
            payload = json.dumps([data])
            await self.redis_client.execute_command(
                "JSON.SET",
                self.REDIS_DUMP_KEY,
                "$",
                payload
            )
            logging.info(f"[AutoUpdate.save_data_to_redis] Saved {len(data)} entries under '{self.REDIS_DUMP_KEY}'")
        except Exception as e:
            logging.error(f"[AutoUpdate.save_data_to_redis] Error saving data to Redis: {e}")

    async def dump_redis_data(self) -> Dict[str, Any]:
        """
        - SCAN in batches of 100 keys.
        - Skip keys already present in old_data (loaded from Redis or URL).
        - Filter to:
            • image_data:*
            • user_data:*
            • order_data:info:*
            • deposit_data:info:*
            • main_data:* (except exactly REDIS_DUMP_KEY)
        - Pipeline TYPE and then pipeline the correct “fetch” (GET, LRANGE, HMGET, etc.).
        - Return a dict mapping key → {"type": <type>, "value": <value>}
        """
        r = self.redis_client
        result: Dict[str, Any] = {}
        cursor = 0

        try:
            while True:
                cursor, keys = await r.scan(cursor=cursor, count=100)
                if keys:
                    filtered = []
                    for raw_key in keys:
                        key = raw_key.decode() if isinstance(raw_key, bytes) else raw_key
                        if (
                            key.startswith("image_data:")
                            or key.startswith("user_data:")
                            or key.startswith("order_data:info:")
                            or key.startswith("deposit_data:info:")
                            or (key.startswith("main_data:") and key not in [self.REDIS_DUMP_KEY, "main_data:service:main_data"])
                        ):
                            filtered.append(key)

                    if filtered:
                        # 1) Pipeline TYPE for all filtered keys
                        pipe = r.pipeline()
                        for k in filtered:
                            pipe.type(k)
                        types = await pipe.execute()

                        # 2) Pipeline the appropriate fetch commands
                        pipe = r.pipeline()
                        for idx, key_name in enumerate(filtered):
                            t = types[idx].decode() if isinstance(types[idx], bytes) else types[idx]
                            if t == "string":
                                pipe.get(key_name)
                            elif t == "list":
                                pipe.lrange(key_name, 0, -1)
                            elif t == "set":
                                pipe.smembers(key_name)
                            elif t == "hash":
                                pipe.hgetall(key_name)
                            elif t == "zset":
                                pipe.zrange(key_name, 0, -1, "WITHSCORES")
                            elif t in ("ReJSON-RL", "ReJSON"):
                                pipe.execute_command("JSON.GET", key_name)
                            else:
                                pipe.execute_command("PING")  # placeholder for unsupported types
                        values = await pipe.execute()

                        # 3) Decode and store in `result`
                        for idx, key_name in enumerate(filtered):
                            t = types[idx].decode() if isinstance(types[idx], bytes) else types[idx]
                            raw_val = values[idx]

                            if t == "string":
                                val = raw_val.decode() if isinstance(raw_val, bytes) else raw_val

                            elif t == "list":
                                val = [item.decode() if isinstance(item, bytes) else item for item in raw_val]

                            elif t == "set":
                                val = [item.decode() if isinstance(item, bytes) else item for item in raw_val]

                            elif t == "hash":
                                d = {}
                                for bkey, bval in raw_val.items():
                                    k_dec = bkey.decode() if isinstance(bkey, bytes) else bkey
                                    v_dec = bval.decode() if isinstance(bval, bytes) else bval
                                    d[k_dec] = v_dec
                                val = d

                            elif t == "zset":
                                lst = []
                                for member, score in raw_val:
                                    m_dec = member.decode() if isinstance(member, bytes) else member
                                    lst.append((m_dec, score))
                                val = lst

                            elif t in ("ReJSON-RL", "ReJSON"):
                                # JSON.GET may return a Python list/dict already, or a JSON str/bytes
                                if isinstance(raw_val, (bytes, str)):
                                    js = raw_val.decode() if isinstance(raw_val, bytes) else raw_val
                                    try:
                                        val = json.loads(js)
                                    except json.JSONDecodeError:
                                        val = js
                                else:
                                    val = raw_val

                            else:
                                # skip unsupported types
                                continue

                            result[key_name] = {"type": t, "value": val}

                if cursor == 0:
                    break

        except Exception as e:
            logging.error(f"[AutoUpdate.dump_redis_data] Error during SCAN/DUMP: {e}")

        logging.info(f"[AutoUpdate.dump_redis_data] Completed dump: {len(result)} total keys.")
        return result
    async def upload_from_redis_key(self) -> str:
        """
        Fetches the JSON payload stored under REDIS_DUMP_KEY (via JSON.GET),
        re-serializes it to bytes in-memory, uploads it to tmpfiles.org,
        and returns the public URL. Returns an empty string on any failure.
        """
        upload_url = "https://tmpfiles.org/api/v1/upload"
        try:
            # 1) Fetch the stored JSON from Redis
            raw = await self.redis_client.execute_command(
                "JSON.GET",
                self.REDIS_DUMP_KEY,
                "$"
            )
            if not raw:
                logging.error(f"[AutoUpdate.upload_from_redis_key] No data at key '{self.REDIS_DUMP_KEY}'")
                return ""

            # 2) Normalize to JSON bytes
            try:
                parsed = json.loads(raw) if isinstance(raw, str) else raw
            except Exception:
                parsed = raw

            payload_bytes = json.dumps(parsed, indent=2).encode()

            # 3) Build in-memory form data for aiohttp
            form = aiohttp.FormData()
            form.add_field(
                "file",
                io.BytesIO(payload_bytes),
                filename="redis_dump.json",
                content_type="application/json"
            )

            # 4) POST to tmpfiles.org
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(upload_url, data=form) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        # tmpfiles.org returns JSON like {"status":"ok","data":{"url":"https://..."}}
                        url = data.get("data", {}).get("url", "")
                        logging.info(f"[AutoUpdate.upload_from_redis_key] Uploaded Redis JSON → {url}")
                        return url
                    else:
                        text = await resp.text()
                        logging.error(f"[AutoUpdate.upload_from_redis_key] Upload failed ({resp.status}): {text}")
                        return ""
        except Exception as e:
            logging.error(f"[AutoUpdate.upload_from_redis_key] Exception during upload: {e}")
            return ""

    async def send_dump_link(self, chat_id: int, file_url: str) -> None:
        """
        Send a Telegram message to `chat_id` with the given `file_url`.
        If `file_url` is empty, does nothing.
        """
        if not file_url:
            logging.warning("[AutoUpdate.send_dump_link] Empty file_url; skipping send.")
            return

        try:
            text = f"Redis dump available here:\n{file_url}"
            await self.bot.send_message(chat_id, text)
            logging.info("[AutoUpdate.send_dump_link] Sent dump URL to admin.")
        except Exception as e:
            logging.error(f"[AutoUpdate.send_dump_link] Error sending Telegram message: {e}")
    
    async def save_data_cycle(self):
        try:
            # 1) Dump new data
            dumped = await auto_updater.dump_redis_data()

            # 2) Save it under the ReJSON key
            await auto_updater.save_data_to_redis(dumped)

            # 3) Upload directly from that Redis key (no temp file)
            file_url = await auto_updater.upload_from_redis_key()

            # 4) Send the URL to your admin chat
            await auto_updater.send_dump_link(ADMIN_ID, file_url)

        except Exception as dump_ex:
            logging.error(f"[periodic_update] Redis dump/upload/send failed: {dump_ex}")



# Initialize the auto updater
auto_updater = AutoUpdater()

async def periodic_update(update: bool = False, bot: AsyncTeleBot = None):
    """
    Periodic update function that runs at even hours (0, 2, 4, ..., 22, 24).
    Additionally, on every even hour:00, it will:
      1. initialize & update_data
      2. dump Redis into a single JSON key
      3. upload that JSON directly from Redis → 0x0.st
      4. send the resulting link to ADMIN_ID
    """
    while True:
        try:
            if os.environ.get('USE_WEBHOOK', 'false').lower() == 'true':
                current_time = datetime.datetime.now().time()
                # Trigger at every hour and minute == 30
                if current_time.minute == 30:
                    # ──────────────────────────────────────────────────────
                    # Step A: existing initialize & update_data
                    await auto_updater.initialize(bot=bot)
                    await auto_updater.update_data()
                    # ──────────────────────────────────────────────────────
                    await auto_updater.save_data_cycle()
                    await asyncio.sleep(1 * 30 * 60)
                    # ──────────────────────────────────────────────────────

                # If `update` is True, run once on first invocation
                elif update:
                    #if not hasattr(auto_updater, 'initialized'):
                    #    await auto_updater.initialize(bot=bot)
                    #    await auto_updater.update_data()

                    #    auto_updater.initialized = True
                    # ──────────────────────────────────────────────────────
                    await auto_updater.save_data_cycle()
                    await asyncio.sleep(1 * 30 * 60)
                    # ──────────────────────────────────────────────────────

            # Check again in 1 minute
            await asyncio.sleep(60)

        except Exception as e:
            logging.error(f"Error in periodic_update: {e}")
            await asyncio.sleep(60)
