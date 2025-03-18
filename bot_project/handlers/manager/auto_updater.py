import os
import sys
from typing import Optional, Dict, Any, Tuple, List, Union
import logging
import asyncio
import aiohttp
import json
from termcolor import colored
import redis.asyncio as redis
from utils.redis_manager import RedisManager, redis_manager
from utils.config import  WEBHOOK_HOST as FIVE_SIM_URL
import datetime
from handlers.manager.operation import (
    FiveSimManagement, FastSmsManagement, SmsHubManagement, GrizzlySmsManagement,
    SmsBowerManagement, VakSmsManagement, TigerSmsManagement, SmsActivateManagement
)
from utils.api import SMS_PROVIDERS_ID

# -------------------- logging Configuration --------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# -------------------- Constants and Globals --------------------
SERVICE_PREFIX = "service_data"
REDIS_KEY_PRICE_MAP = "main_data:price-country"

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
        """Load country data from Redis and ensure it is a list."""
        try:
            data = await self.redis_client.json().get('main_data:service:country_data')
            if not data:
                #logging.warning("No country data found in Redis")
                self.country_map = []
            else:
                # If data is a dict, convert its values into a list.
                if isinstance(data, dict):
                    self.country_map = list(data.values())
                elif isinstance(data, list):
                    self.country_map = data
                else:
                    self.country_map = []
        except Exception as e:
            #logging.error(f"Error loading country data from Redis: {e}")
            self.country_map = []

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
            # logging.error(f"Error loading app data from Redis: {e}")
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
                (api_app_key == code_field[0] and server_id in {'1', '2'}) or
                (len(code_field) > 1 and api_app_key == code_field[1] and server_id in {'3', '4', '5', '6'}) or
                (api_app_key in code_field)
            ):
                return mapping
        else:
            if api_app_key == code_field:
                return mapping
        
        return None  # No valid mapping found

    def transform_data(self, fetched_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Transforms the old-style API responses into a list of country dictionaries.
        Expected old structure:
          { "record_id": { "app_key": { "price": "count", ... }, ... } }
        """
        transformed = []

        for country in self.country_map:
            record_id = country.get("record_id")
            country_entry = {
                "record_id": record_id,
                "name": country.get("name"),
                "code": country.get("code"),
                "servers": []
            }

            for server_id in self.server_ids:
                server_details = self.sms_providers.get(server_id, {})
                server_name = server_details.get("url", "Unknown")
                # Look up provider data by its key (e.g. "FastSms")
                server_response = fetched_data.get(server_id, {})

                apps = []
                if server_response and isinstance(server_response, dict):
                    country_api_data = server_response.get(record_id)
                    if country_api_data and isinstance(country_api_data, dict):
                        for api_app_key, details in country_api_data.items():
                            # Handle both response types:
                            for price_str, count_str in details.items():
                                try:
                                    price = float(price_str)
                                    count = int(count_str)
                                except Exception as e:
                                    #logging.error(f"Conversion error for '{api_app_key}': {e}")
                                    continue
                                break  # Only one key-value pair expected

                            mapping_info = self.find_mapping(api_app_key, server_id)
                            if mapping_info:
                                app_entry = {
                                    "app_name": mapping_info["app_name"],
                                    "app_id": mapping_info["app_id"],
                                    "code": mapping_info["app_code"],
                                    "price": price,
                                    "count": count
                                }
                                apps.append(app_entry)

                server_entry = {
                    "server_id": server_id,
                    "server_name": server_name,
                    "apps": apps
                }

                country_entry["servers"].append(server_entry)
            transformed.append(country_entry)

        return transformed


# -------------------- AutoUpdater Class --------------------
class AutoUpdater:
    def __init__(self):
        self.price_country_mapping: Dict[str, Dict[str, str]] = {}
        self.sms_providers = SMS_PROVIDERS_ID
        self.services = [
            # Uncomment additional services as needed
            (FiveSimManagement, '1'),
            (FastSmsManagement, '2'),
            (SmsHubManagement, '3'),
            (GrizzlySmsManagement, '4'),
            (SmsBowerManagement, '5'),
            (SmsActivateManagement, '6'),
            (VakSmsManagement, '7'),
            (TigerSmsManagement, '8'),
        ]
        self.redis_client: Optional[redis.Redis] = None

    async def initialize(self):
        """Initialize Redis client."""
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
            yield lst[i:i + n]

    async def save_price_mapping(self, redis_client: redis.Redis) -> None:
        """Save the price-country mapping to Redis."""
        await redis_client.json().set(REDIS_KEY_PRICE_MAP, '$', self.price_country_mapping)

    async def load_price_mapping(self, redis_client: redis.Redis) -> None:
        """Load the price-country mapping from Redis."""
        stored_mapping = await redis_client.json().get(REDIS_KEY_PRICE_MAP)
        if stored_mapping:
            self.price_country_mapping = stored_mapping

    async def update_price_mapping(self, app_id: str, price: str, country_flag: str) -> None:
        """Update the price-country mapping for a specific app."""
        if app_id not in self.price_country_mapping:
            self.price_country_mapping[app_id] = {}
        self.price_country_mapping[app_id][price] = country_flag

    async def process_server_data(self, session: aiohttp.ClientSession, country_id: str) -> Optional[Dict[str, Any]]:
        """Fetch and process server data for a country."""
        try:
            url = (
                f"{FIVE_SIM_URL}/stubs/handler_api.php"
                f"?action=getServer&country={country_id}"
            )
            print(colored(f"Fetching Server Data: {url}", "magenta"))
            async with session.get(url, timeout=10) as response:
                if response.status != 200:
                    #logging.error(f"HTTP error {response.status} when fetching server data for country {country_id}")
                    return None
                text = await response.text()
                #print(colored(f"{url}\n\n{text}", "cyan"))
                return json.loads(text) if text.strip() else None
        except Exception as e:
            #logging.error(f"Error fetching server data for country {country_id}: {e}")
            return None

    async def process_app_data(
        self,
        pipe: redis.client.Pipeline,
        app: Dict[str, Any],
        server_data: Optional[Dict[str, Any]],
        country_data: Dict[str, Any],
        server_id: int
    ) -> None:
        """Process individual app data and update Redis."""
        country_id = country_data["record_id"]
        country_name = country_data["name"]
        display_flag = country_data["code"]
        
        app_codes = app.get("code")
        app_id = self.safe_str(app.get("app_id"))
        app_price = self.safe_str(app.get("price"))
        
        if server_id == 1:
            if isinstance(app_codes, list):
                first_code = app_codes[0]
                server_name_val = server_data.get(first_code) if server_data else None
                code_field = self.encode_for_redis(app_codes)
                search_codes = ' '.join(app_codes)
            else:
                first_code = app_codes
                server_name_val = server_data.get(first_code) if server_data else None
                code_field = self.safe_str(app_codes)
                search_codes = code_field
        else:
            code_field = self.encode_for_redis(app_codes) if isinstance(app_codes, list) else self.safe_str(app_codes)
            search_codes = ' '.join(app_codes) if isinstance(app_codes, list) else code_field
            server_name_val = 'any'

        redis_key = f"{SERVICE_PREFIX}:{country_id}:{server_id}:{app_id}"
        await pipe.hsetnx(redis_key, "is_show_country", "True")
        await pipe.hsetnx(redis_key, "is_show_server",  "True")
        await pipe.hsetnx(redis_key, "is_show_app", "True")
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
        
        await self.update_price_mapping(app_id, app_price, display_flag)
        pipe.hset(redis_key, mapping=redis_data)
        #print(colored(f"    ✓ Added: {app.get('app_name')} {app_codes} | Price: ${app_price:<6} | Stock: {app.get('count')}", "green"))

    async def process_server(
        self,
        pipe: redis.client.Pipeline,
        server: Dict[str, Any],
        country_data: Dict[str, Any],
        session: aiohttp.ClientSession
    ) -> None:
        """Process server data and update Redis."""
        server_id = int(server["server_id"])
        server_data = None
        
        if server_id == 1:
            server_data = await self.process_server_data(session, country_data["record_id"])
        
        tasks = []
        for app in server["apps"]:
            tasks.append(self.process_app_data(pipe, app, server_data, country_data, server_id))
        
        await asyncio.gather(*tasks)
        await pipe.execute()

    async def insert_data(self, data: List[Dict[str, Any]]) -> None:
        """Insert transformed data into Redis asynchronously."""
        # Clear existing keys
        #keys = await self.redis_client.keys("{SERVICE_PREFIX}:*")
        #if keys:
        #    print(colored(f"Clearing {len(keys)} existing records...", "yellow"))
        #    await self.redis_client.delete(*keys)
        #    print(colored("Existing records cleared successfully!", "green"))
        
        print(colored("\n\n=== Starting Data Insertion ===", "cyan"))
        await self.load_price_mapping(self.redis_client)
        
        # Load persistent country data (or initialize if not exists)
        batches = list(self.chunker(data, 1))
        
        async with aiohttp.ClientSession() as session:
            for batch_index, batch in enumerate(batches, start=1):
                tasks = []
                for country_data in batch:
                    for server in country_data["servers"]:
                        # Schedule the process_server coroutine without awaiting immediately
                        tasks.append(self.process_server(self.redis_client.pipeline(), server, country_data, session))
                                    
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
        
        #logging.info(f"Server IDs: {server_ids}")
        whole_data = {}
        
        try:
            whole_data = await self.redis_client.json().get('main_data:service:main_data') or {}
            # Fetch fresh data from all services
            for ServiceClass, service_name in self.services:
                try:
                    async with ServiceClass() as service:
                        #logging.info(f"Fetching data from {service_name}...")
                        data = await service.fetch_all_data()
                        #logging.info(f"Received data from {service_name}.")
                        if hasattr(ServiceClass, 'select_best_service'):
                            best_data = ServiceClass.select_best_service(data)
                            #logging.info(f"Selected best data from {service_name}.")
                        else:
                            best_data = data
                        whole_data[service_name] = best_data
                except Exception as e:
                    logging.error(f"Error fetching data from {service_name}: {e}")
            
            transformed = transformer.transform_data(whole_data)
            await self.redis_client.json().set('main_data:service:main_data', '$', whole_data)
            #logging.info("Data successfully transformed and stored in Redis")
            return transformed
        except Exception as e:
            #logging.error(f"An error occurred while processing the data: {str(e)}")
            return None

    async def update_data(self):
        """Main update function that orchestrates the entire update process."""
        try:
            data = await self.fetch_transform_data()
            if data:
                await self.insert_data(data)
                #logging.info("Data update completed successfully")
        except Exception as e:
            logging.error(f"Error in update_data: {e}")

# Initialize the auto updater
auto_updater = AutoUpdater()

async def periodic_update(update: bool = False):
    """Periodic update function that runs at even hours (0, 2, 4, ..., 22, 24)."""
    while True:
        try:
            current_time = datetime.datetime.now().time()
            # Check if current time is at an even hour and minute is 0
            if current_time.hour % 2 == 0 and current_time.minute == 0:
                await auto_updater.initialize()  # Initialize before update
                await auto_updater.update_data()
                # Sleep for 2 hours to avoid multiple updates in the same hour
                await asyncio.sleep(2 * 60 * 60)
            elif update:
                # Run once on first run
                if not hasattr(auto_updater, 'initialized'):
                    await auto_updater.initialize()  # Initialize before update
                    await auto_updater.update_data()
                    auto_updater.initialized = True
                    # Sleep for 2 hours to avoid multiple updates in the same hour
                    await asyncio.sleep(2 * 60 * 60)
            # Check every minute for the target time
            await asyncio.sleep(60)
        except Exception as e:
            logging.error(f"Error in periodic update: {str(e)}")
            await asyncio.sleep(60)  # Wait a minute before retrying on error
