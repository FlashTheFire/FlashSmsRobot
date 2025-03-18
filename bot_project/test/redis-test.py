'''import random

# Predefined countries and services
countries = [
    {"record_id": "1", "code": "🇮🇳", "name": "India"},
    {"record_id": "2", "code": "🇺🇸", "name": "USA"},
    {"record_id": "3", "code": "🇷🇺", "name": "Russia"},
    {"record_id": "4", "code": "🇦🇺", "name": "Australia"},
    {"record_id": "5", "code": "🇧🇷", "name": "Brazil"},
    {"record_id": "6", "code": "🇨🇦", "name": "Canada"},
    {"record_id": "7", "code": "🇨🇳", "name": "China"},
    {"record_id": "8", "code": "🇯🇵", "name": "Japan"},
    {"record_id": "9", "code": "🇩🇪", "name": "Germany"},
    {"record_id": "10", "code": "🇫🇷", "name": "France"},
    {"record_id": "11", "code": "🇮🇹", "name": "Italy"},
    {"record_id": "12", "code": "🇰🇷", "name": "South Korea"},
    {"record_id": "13", "code": "🇲🇽", "name": "Mexico"},
    {"record_id": "14", "code": "🇿🇦", "name": "South Africa"},
    {"record_id": "15", "code": "🇬🇧", "name": "United Kingdom"},
    {"record_id": "16", "code": "🇪🇸", "name": "Spain"},
    {"record_id": "17", "code": "🇹🇷", "name": "Turkey"},
    {"record_id": "18", "code": "🇦🇷", "name": "Argentina"},
    {"record_id": "19", "code": "🇸🇪", "name": "Sweden"},
    {"record_id": "20", "code": "🇳🇱", "name": "Netherlands"}
]


services = [
    {"name": "Swiggy", "record_id": 101},
    {"name": "Tata Neu", "record_id": 102},
    {"name": "Telegram", "record_id": 103},
    {"name": "WhatsApp", "record_id": 104},
    {"name": "Spotify", "record_id": 105},
    {"name": "Netflix", "record_id": 106},
    {"name": "Amazon Prime", "record_id": 107},
    {"name": "Uber", "record_id": 108},
    {"name": "Zoom", "record_id": 109},
    {"name": "Flipkart", "record_id": 110}
]

# Generate data
app_data = []
for country in countries:
    country_data = {
        "record_id": country["record_id"],
        "code": country["code"],
        "name": country["name"],
        "apps": []
    }
    for service in services:
        # Add multiple entries for some services
        num_entries = random.randint(3, 5)  # 1 to 3 entries per service
        for _ in range(num_entries):
            app_entry = {
                "record_id": service["record_id"],  # Consistent record ID for each service
                "name": service["name"],
                "price": round(random.uniform(1.0, 20.0), 2),
                "server": random.randint(1, 6),
                "count": random.randint(1, 100)
            }
            country_data["apps"].append(app_entry)
    app_data.append(country_data)

# Print the generated data
#from pprint import pprint
#pprint(app_data)

# Save the generated data to a file
import json

with open('app_data.json', 'w') as f:
    json.dump(app_data, f, indent=4)



import redis
import asyncio

async def delete_index(index_name: str, delete_documents: bool = True):
    """
    Deletes an index in Redisearch.
    
    Parameters:
        index_name (str): The name of the index to delete.
        delete_documents (bool): Whether to delete associated documents (default is True).
    """
    client = redis.Redis(
        host="redis-16106.c305.ap-south-1-1.ec2.redns.redis-cloud.com",
        port=16106,
        password="dW6AGa56NCFa5c4CnTwkStIfv126TsYA",
        decode_responses=True,
    )
    #try:
    #    command = ["FT.DROPINDEX", index_name]
    #    if delete_documents:
    #        command.append("DD")
    #    
    #    response = client.execute_command(*command)
    #    print(f"Index '{index_name}' deleted successfully.")
    #except Exception as e:
    #    print(f"Error deleting index '{index_name}': {e}")


'''

import asyncio
import redis.asyncio as redis

# Redis connection setup
async def setup_redis():
    return redis.Redis(
        host="redis-16106.c305.ap-south-1-1.ec2.redns.redis-cloud.com",
        port=16106,
        password="dW6AGa56NCFa5c4CnTwkStIfv126TsYA",
        decode_responses=True,
    )

# Function to delete keys matching a pattern
async def delete_keys_by_pattern(redis_client, pattern):
    cursor = "0"  # Start scanning from cursor 0
    keys_deleted = 0

    print(f"Scanning and deleting keys matching: '{pattern}'")
    while cursor != 0 or keys_deleted == 0:
        cursor, keys = await redis_client.scan(cursor=cursor, match=pattern, count=100)
        if keys:
            await redis_client.delete(*keys)
            keys_deleted += len(keys)
            print(f"Deleted {len(keys)} keys: {keys}")
    
    print(f"Total keys deleted: {keys_deleted}")

# Main coroutine
async def main():
    redis_client = await setup_redis()
    try:
        await delete_keys_by_pattern(redis_client, "server_data*")
    finally:
        await redis_client.aclose()



#if __name__ == "__main__":
#    asyncio.run(main())


'''
if __name__ == "__main__":
    asyncio.run(delete_index("country_index"))  # Replace 'INDEX_NAME' with your actual index name



import json 
import telebot
from utils.config import BOT_TOKEN

API_TOKEN = 'YOUR_BOT_API_TOKEN'
bot = telebot.TeleBot(BOT_TOKEN)


@bot.message_handler(commands=['start'])
def send_welcome(message):
    reply_markup = {
        'inline_keyboard': [
            [{'text': 'Copy to Clipboard', 'copy_text': {'text': 'Hi there!'}}]
        ]
    }
    # Serialize the reply_markup to a JSON string
    reply_markup_json = json.dumps(reply_markup)
    bot.send_message(
        message.chat.id,
        "Copy message from Below Button 👇",
        reply_markup=reply_markup_json
    )
bot.polling()


import itertools

def generate_case_variations(word):
    """Generate all possible case variations of a given word."""
    return [''.join(p) for p in itertools.product(*[(char.lower(), char.upper()) for char in word])]

# Generate all variations for the word "start"
variations = generate_case_variations("start")
print(variations)



import os
import shutil

def delete_pycache_dirs(directory):
    for dirpath, dirnames, filenames in os.walk(directory):
        if '__pycache__' in dirnames:
            pycache_dir = os.path.join(dirpath, '__pycache__')
            shutil.rmtree(pycache_dir)
            print(f"Deleted: {pycache_dir}")

delete_pycache_dirs('C:/Users/LOQ/OneDrive/Desktop/flash_sms/bot_project')



import random
import itertools


class FixedLengthConverter:
    def __init__(self, range_start=1, range_end=2000, seed=None):
        """
        Initialize the converter with a fixed range and optional randomization seed.
        
        Args:
        - range_start (int): Start of the integer range.
        - range_end (int): End of the integer range.
        - seed (int): Optional seed for reproducible random mappings.
        """
        self.range_start = range_start
        self.range_end = range_end
        
        # Generate all possible unique three-character combinations
        characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        combinations = [''.join(p) for p in itertools.permutations(characters, 3)]
        
        # Ensure we have enough combinations for the given range
        required_combinations = range_end - range_start + 1
        if required_combinations > len(combinations):
            raise ValueError("Range exceeds the number of unique three-character combinations available.")
        
        # Shuffle combinations for unpredictability
        if seed is not None:
            random.seed(seed)
        random.shuffle(combinations)
        
        # Map integers to unique three-character strings
        self.int_to_str_map = {i: combinations[i - range_start] for i in range(range_start, range_end + 1)}
        self.str_to_int_map = {v: k for k, v in self.int_to_str_map.items()}
    
    def int_to_str(self, number):
        """
        Convert an integer to its corresponding three-character string.
        
        Args:
        - number (int): The integer to convert.
        
        Returns:
        - str: The corresponding three-character string.
        """
        if number < self.range_start or number > self.range_end:
            raise ValueError(f"Number must be within the range {self.range_start} to {self.range_end}.")
        return self.int_to_str_map[number]
    
    def str_to_int(self, string):
        """
        Convert a three-character string back to its corresponding integer.
        
        Args:
        - string (str): The string to convert.
        
        Returns:
        - int: The corresponding integer.
        """
        if string not in self.str_to_int_map:
            raise ValueError(f"String '{string}' is not a valid mapping.")
        return self.str_to_int_map[string]


converter = FixedLengthConverter(range_start=1, range_end=2000, seed=42)
integer_value = 1
string_representation = converter.int_to_str(integer_value)
print(f"Integer: {integer_value} -> String: {string_representation}")
converted_back = converter.str_to_int(string_representation)
print(f"String: {string_representation} -> Integer: {converted_back}")


import redis
from redis.exceptions import RedisError
from utils.config import REDIS_HOST, REDIS_PORT, REDIS_PASS as REDIS_PASSWORD

r = redis_client = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True
    )

# Set a field in a hash with TTL (60 seconds)
r.hset('myhash', 'field1', 'value1')
r.hexpire('myhash', 60, 'field1')  # Set TTL for 'field1' to 60 seconds

# Set a field in a hash without TTL
r.hset('myhash', 'field2', 'value2')

# Verify the result
print(r.hget('myhash', 'field1'))  # Output: b'value1'
print(r.hget('myhash', 'field2'))  # Output: b'value2'

# Optionally, check the TTL for 'field1'
ttl = r.httl('myhash', 'field1')
print(f'TTL for "field1": {ttl} seconds')




import asyncio
import aiohttp
import logging
from datetime import datetime
from utils.functions import get_api_info

class StatusChecker:
    def __init__(self):
        """
        Initialize the StatusChecker with a status mapping dictionary.
        """
        self.status_map = {
            "STATUS_OK": ("RECEIVED", lambda x: x.split(':', 1)[1].strip() if ':' in x else ""),
            "STATUS_WAIT_CODE": ("WAITING", lambda _: "WAITING"),
            "STATUS_WAIT_RETRY": ("WAITING_NEXT", lambda _: "WAITING_NEXT"),
            "STATUS_WAIT_RESEND": ("WAITING_NEXT", lambda _: "WAITING_NEXT"),
            "STATUS_CANCEL": ("CANCELED", lambda _: "CANCELED"),
            "NO_ACTIVATION": ("NOT_FOUND", lambda _: "NOT_FOUND"),
            "ERROR_SQL": ("NOT_FOUND", lambda _: "NOT_FOUND"),
            "BAD_KEY": ("NOT_FOUND", lambda _: "NOT_FOUND"),
            "BAD_ACTION": ("NOT_FOUND", lambda _: "NOT_FOUND"),
        }
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.session.close()

    async def fetch_status(self, server_id, order_id, user_id):
        """
        Fetch the status of a specific order.
        :param server_id: The server ID.
        :param order_id: The order ID.
        :param user_id: The user ID.
        :return: A dictionary containing the processed status and related information.
        """
        try:
            api_name, api_key = get_api_info(server_id)
            url = f"https://{api_name}/stubs/handler_api.php?api_key={api_key}&action=getStatus&id={order_id}"

            async with self.session.get(url) as response:
                response_text = await response.text()
                status_key = response_text.split(':', 1)[0]
                status, extractor = self.status_map.get(status_key, ("UNKNOWN", lambda x: x))
                sms_data = extractor(response_text)

                return {
                    'response': status,
                    'sms': sms_data,
                    'server': server_id,
                    'order_id': order_id,
                    'user_id': user_id,
                    'time': datetime.utcnow().isoformat(),
                }

        except aiohttp.ClientError as e:
            logging.error(f"Network error fetching status for order {order_id} on server {server_id}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error fetching status for order {order_id} on server {server_id}: {e}")

        return {
            'response': "ERROR",
            'sms': None,
            'server': server_id,
            'order_id': order_id,
            'user_id': user_id,
            'time': datetime.utcnow().isoformat(),
            'error': str(e),
        }

    async def process_orders(self, batch_size=None, interval=5):
        """
        Process a batch of orders to fetch their statuses.
        :param batch_size: The number of orders to process at once (None for unlimited).
        :param interval: Time interval (in seconds) between batches.
        """
        while True:
            orders = await get_orders()
            if not orders:
                logging.info("No orders to process, waiting for new orders...")
                await asyncio.sleep(interval)
                continue

            if batch_size is None:
                batch_size = len(orders)

            for i in range(0, len(orders), batch_size):
                batch = orders[i:i + batch_size]
                tasks = [
                    self.fetch_status(order['server_id'], order['order_id'], order['chat_id'])
                    for order in batch
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        logging.error(f"Error in task: {result}")
                    else:
                        logging.info(f"Processed result: {result}")
                await asyncio.sleep(interval)

async def get_orders():
    """
    Fetch the list of orders dynamically (replace with actual logic to fetch orders).
    This function can retrieve orders from a database, API, or any other data source.
    """
    # Example orders, this should be replaced with your dynamic fetching logic.
    return [
        {"server_id": 2, "order_id": 101, "chat_id": 1001}
    ]

async def main():
    """
    Main function to initialize the StatusChecker and process orders.
    """
    async with StatusChecker() as checker:
        await checker.process_orders(batch_size=None, interval=10)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    asyncio.run(main())
'''


