import asyncio
import redis.asyncio as redis
from redis.commands.search.aggregation import AggregateRequest, Reducer
from redis.commands.search.field import TextField, NumericField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType

SERVICE_INDEX = "service_index"

async def search_advanced(redis_client, query=None, offset=0, limit=50):
    if query:
        query_str = (
            f'(@app_name:"{query}"* => {{$weight: 5.0}} | '
            f'@app_name:"*{query}*" => {{$weight: 3.0}} | '
            f'@app_name:"{query}" => {{$weight: 4.0}}) @app_count:[1 +inf]'
        )
    else:
        query_str = '@app_count:[1 +inf]'

    agg_request = (
        AggregateRequest(query_str)
        .group_by(
            ["@app_id"],
            [
                Reducer.firstvalue("@app_name").alias("app_name"),
                Reducer.firstvalue("@app_code").alias("app_code"),
                Reducer.sum("@app_count").alias("total_stock"),
                Reducer.min("@app_price").alias("lowest_price")
            ]
        )
        .sort_by("lowest_price", asc=True)
        .limit(offset, limit)
    )

    result = await redis_client.ft(SERVICE_INDEX).aggregate(agg_request)
    return result

async def main():
    redis_client = redis.Redis(
        host="redis-16106.c305.ap-south-1-1.ec2.redns.redis-cloud.com",
        port=16106,
        password="dW6AGa56NCFa5c4CnTwkStIfv126TsYA",
        decode_responses=True
    )
    result = await search_advanced(redis_client, query="example", offset=0, limit=10)
    print(result)
    await redis_client.aclose()

if __name__ == "__main__":
    asyncio.run(main())


















'''





"""
data: A list of dictionaries, where each dictionary represents a country and its data.
A country's data includes the record_id, name, code, and servers.
A server is a dictionary with keys server_id and apps, where apps is a list of dictionaries with keys name, price, and count.
"""
import asyncio
import time
from redis.commands.search.query import Query
from redis.exceptions import RedisError
import redis.asyncio as redis
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
import json
from termcolor import colored

# Replace these with your actual Redis configuration
REDIS_HOST = "redis-16106.c305.ap-south-1-1.ec2.redns.redis-cloud.com"
REDIS_PORT = 16106
REDIS_PASSWORD = "dW6AGa56NCFa5c4CnTwkStIfv126TsYA"
REDIS_DB = 0








# Replace these with your actual index and prefix names
SERVICE_INDEX = "service_index"
SERVICE_PREFIX = "service_data"

# Constants for country codes and flags
COUNTRY_CODES = {
    "\ud83c\uddee\ud83c\uddf3": "🇮🇳",  # India
    "\ud83c\uddfa\ud83c\uddf8": "🇺🇸",  # USA
    "\ud83c\udde8\ud83c\udde6": "🇨🇦",  # Canada
    "\ud83c\udde6\ud83c\uddfa": "🇦🇺",  # Australia
    "\ud83c\udde9\ud83c\uddea": "🇩🇪",  # Germany
    "\ud83c\uddeb\ud83c\uddf7": "🇫🇷",  # France
    "\ud83c\uddef\ud83c\uddf5": "🇯🇵",  # Japan
    "\ud83c\udde7\ud83c\uddf7": "🇧🇷",  # Brazil
    "\ud83c\uddee\ud83c\uddf9": "🇮🇹",  # Italy
    "\ud83c\uddea\ud83c\uddf8": "🇪🇸",  # Spain
    "\ud83c\uddec\ud83c\udde7": "🇬🇧",  # United Kingdom
    "\ud83c\uddf2\ud83c\uddfd": "🇲🇽",  # Mexico
    "\ud83c\uddf0\ud83c\uddf7": "🇰🇷",  # South Korea
    "\ud83c\uddf7\ud83c\uddfa": "🇷🇺",  # Russia
    "\ud83c\uddf3\ud83c\uddf1": "🇳🇱"   # Netherlands
}

def sanitize_text(text):
    """Sanitize text by removing unwanted characters or formatting."""
    return text.strip() if isinstance(text, str) else str(text)

def format_flag(code):
    """Format flag emoji for display and storage"""
    return COUNTRY_CODES.get(code, code)

def encode_for_redis(text):
    """Encode text for Redis storage"""
    if isinstance(text, str):
        return COUNTRY_CODES.get(text, text)
    elif isinstance(text, list):
        return ','.join(map(str, text))
    return str(text)

async def create_index(redis_client):
    """Creates RediSearch index with the defined schema."""
    try:
        try:
            await redis_client.ft(SERVICE_INDEX).dropindex()
        except:
            pass

        schema = (
            TextField("record_id", sortable=True),
            TextField("country_name", sortable=True),
            TextField("country_code", sortable=True),
            TextField("server_id", sortable=True),
            TextField("app_id"),
            TextField("app_name", weight=5.0),
            TextField("app_code"),
            NumericField("app_price", sortable=True),
            NumericField("app_count", sortable=True),
            TextField("search_tags", weight=1.0)
        )

        await redis_client.ft(SERVICE_INDEX).create_index(
            schema,
            definition=IndexDefinition(
                prefix=[SERVICE_PREFIX],
                language="english"
            )
        )
        print(colored("Index created successfully.", "green"))
    except Exception as e:
        print(colored(f"Error creating index: {e}", "red"))
        raise

async def insert_data(redis_client, data, key):
    """Insert data into Redis with proper indexing"""
    keys = await redis_client.keys(f"{key}*") #{SERVICE_PREFIX}
    if keys:
        print(colored(f"Clearing {len(keys)} existing records...", "yellow"))
        await redis_client.delete(*keys)
        print(colored("Existing records cleared successfully!", "green"))
    return
    print(colored("\n\n=== Starting Data Insertion ===", "cyan"))
    
    for record in data:
        country_name = record["name"]
        country_code = record["code"]
        country_id = record['record_id']
        display_flag = format_flag(country_code)
        
        print(colored(f"\nProcessing Country: {country_name} {display_flag}", "blue"))
        
        for server in record["servers"]:
            server_id = server["server_id"]
            
            for app in server["apps"]:
                redis_key = f"{SERVICE_PREFIX}:{country_id}:{server_id}:{app['app_id']}"
                
                app_codes = app.get("code", [])
                if isinstance(app_codes, str):
                    app_codes = [app_codes]
                
                redis_data = {
                    "country_id": str(country_id),
                    "country_name": country_name,
                    "country_code": display_flag,
                    "server_id": str(server_id),
                    "app_id": str(app['app_id']),
                    "app_name": app['name'],
                    "app_code": encode_for_redis(app_codes),
                    "app_price": str(app['price']),
                    "app_count": str(app['count']),
                    "search_tags": f"{country_name} {app['name']} {server_id} {' '.join(app_codes)}".lower()
                }

                try:
                    await redis_client.hset(redis_key, mapping=redis_data)
                    print(colored(f"    ✓ Added: {app['name']} | Price: ${app['price']:<6.2f} | Stock: {app['count']}", "green"))
                except Exception as e:
                    print(colored(f"    ✗ Error adding {app['name']}: {str(e)}", "red"))

    print(colored("\n=== Data Insertion Complete ===", "cyan"))

async def search_advanced(redis_client, app_name=None, country=None, server=None, min_price=None, max_price=None):
    """Enhanced search functionality with better error handling and result formatting"""
    try:
        query_parts = []
        #return
        if app_name:
            query_parts.append(f"(@app_name:{app_name})")
        if server:
            query_parts.append(f"(@server_id:{server})")
        if country:
            query_parts.append(f"(@country_name:{country})")
        if min_price is not None:
            query_parts.append(f"(@app_price:[{min_price} +inf])")
        if max_price is not None:
            query_parts.append(f"(@app_price:[-inf {max_price}])")

        query_string = " ".join(query_parts) if query_parts else "*"

        query = Query(query_string)\
            .sort_by("app_price", asc=True)\
            .return_fields("app_name", "country_name", "country_code", "app_price", "server_id", "app_count")

        results = await redis_client.ft(SERVICE_INDEX).search(query)

        if not results.docs:
            print(colored("\nNo results found", "yellow"))
            return

        # Process and display results
        summary = {
            "total_results": len(results.docs),
            "price_range": {"min": float('inf'), "max": float('-inf')},
            "countries": set(),
            "servers": set()
        }

        for doc in results.docs:
            price = float(doc.app_price)
            summary["price_range"]["min"] = min(summary["price_range"]["min"], price)
            summary["price_range"]["max"] = max(summary["price_range"]["max"], price)
            summary["countries"].add(f"{doc.country_name} {doc.country_code}")
            summary["servers"].add(doc.server_id)

        # Print summary
        print(colored("\n=== Search Results ===", "cyan"))
        print(f"Total Results: {summary['total_results']}")
        print(f"Price Range: ${summary['price_range']['min']:.2f} - ${summary['price_range']['max']:.2f}")
        print(f"Available in {len(summary['countries'])} countries")
        print(f"Across {len(summary['servers'])} servers")

        # Print detailed results
        print(colored("\nTop 3 Results:", "green"))
        for i, doc in enumerate(results.docs[:3], 1):
            print(f"\n{i}. {doc.app_name}")
            print(f"   Country: {doc.country_name} {doc.country_code}")
            print(f"   Server: {doc.server_id}")
            print(f"   Price: ${float(doc.app_price):.2f}")
            print(f"   Stock: {int(doc.app_count)}")

    except Exception as e:
        print(colored(f"Search error: {str(e)}", "red"))

async def create_redis_client():
    return await redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True
    )

async def test_search(redis_client):
    """Test search functionality with various scenarios"""
    print(colored("\n=== Testing Search Functionality ===", "cyan"))

    test_cases = [
        {
            "name": "Search by App Name (Swiggy)",
            "params": {"app_name": "swiggy"}
        },
        {
            "name": "Search by App Name (Neu)",
            "params": {"app_name": "neu"}
        },
        {
            "name": "Search with Price Range",
            "params": {"min_price": 10, "max_price": 30}
        }
    ]

    for test in test_cases:
        print(colored(f"\n>>> {test['name']}", "yellow"))
        await search_advanced(redis_client, **test['params'])




DEPOSIT_INFO_INDEX = "deposit_index"
DEPOSIT_INFO_PREFIX = "deposit_data:"


async def _init_search_indexes(redis_client) -> None:
    """Initialize Redis search indexes for deposits."""
    try:
        try:
            await redis_client.ft(DEPOSIT_INFO_INDEX).dropindex()
        except Exception:
            pass

        schema = (
            TextField("deposit_id", sortable=True),
            TextField("message_id", sortable=True),
            TextField("user_id", sortable=True),
            TextField("server_id", sortable=True),
            NumericField("amount", sortable=True),
            TextField("deposit_status"),
            TextField("created_at"),
            TextField("updated_at"),
            TextField("search_tags", weight=1.0)
        )

        await redis_client.ft(DEPOSIT_INFO_INDEX).create_index(
            schema,
            definition=IndexDefinition(
                prefix=[DEPOSIT_INFO_PREFIX],
                language="english"
            )
        )
        print("Deposit search indexes created successfully")
    except Exception as e:
        print(f"Error creating deposit search indexes: {e}")




async def main():
    """Main function with improved error handling"""
    redis_client = None
    try:
        redis_client = await create_redis_client()
        #await _init_search_indexes(redis_client)
        #await create_index(redis_client)
        # with open('data.json', 'r') as f:
        #    data = json.load(f)
        
        #await insert_data(redis_client, data, "cache")
        #await test_search(redis_client)

    except Exception as e:
        print(colored(f"Fatal error: {str(e)}", "red"))
    finally:
        if redis_client:
            await redis_client.aclose()
'''
'''
if __name__ == "__main__":
    asyncio.run(main())




import asyncio
import time
from redis.commands.search.query import Query
from redis.exceptions import RedisError
import redis.asyncio as redis
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
import json
from termcolor import colored
from datetime import datetime
from handlers.manager.operation import OrderManagement, DepositManagement

# Redis connection details
REDIS_HOST = "redis-16106.c305.ap-south-1-1.ec2.redns.redis-cloud.com"
REDIS_PORT = 16106
REDIS_PASSWORD = "dW6AGa56NCFa5c4CnTwkStIfv126TsYA"
REDIS_DB = 0

# Redis index and prefix constants
ORDER_INFO_INDEX = "order_index"
DEPOSIT_INFO_INDEX = "deposit_index"
ORDER_INFO_PREFIX = "order_data:"
DEPOSIT_INFO_PREFIX = "deposit_data:"

# Redis client creation
async def create_redis_client():
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        db=REDIS_DB,
        decode_responses=True
    )
'''
# Order Management Class
'''class OrderManagement:
    def __init__(self):
        pass

    async def create_index(self, redis_client):
        try:
            await redis_client.ft(ORDER_INFO_INDEX).dropindex()
        except:
            pass

        schema = (
            TextField("order_id", sortable=True),
            TextField("user_id", sortable=True),
            TextField("order_status", sortable=True),
            TextField("created_at", sortable=True),
            TextField("updated_at", sortable=True),
        )
        await redis_client.ft(ORDER_INFO_INDEX).create_index(
            schema,
            definition=IndexDefinition(prefix=[ORDER_INFO_PREFIX], index_type=IndexType.HASH)
        )

    async def _init_search_indexes(self, redis_client):
        await self.create_index(redis_client)

    async def search_orders_advanced(self, filters: dict, sort_by: str = None, sort_asc: bool = True, offset: int = 0, limit: int = 10) -> dict:
        try:
            query_str = self.build_query(filters)
            logger.info(f"Searching orders with query: {query_str}")
            
            query = Query(query_str).paging(offset, limit)
            if sort_by:
                query.sort_by(sort_by, asc=sort_asc)
            
            results = await redis_client.ft(ORDER_INFO_INDEX).search(query)
            orders = [
                {k: v for k, v in doc.__dict__.items() if not k.startswith('__')}
                for doc in results.docs
            ]
            return {'response': True, 'total': results.total, 'results': orders}
        except Exception as e:
            print(f"Error searching orders: {e}")
            return {'response': False, 'error': str(e)}

    def build_query(self, filters: dict) -> str:
        query_parts = []
        for field, value in filters.items():
            if isinstance(value, (list, tuple)):
                if isinstance(value, list):
                    options = '|'.join(map(str, value))
                    query_parts.append(f"@{field}:({options})")
                elif isinstance(value, tuple) and len(value) == 2:
                    start, end = map(str, value)
                    query_parts.append(f"@{field}:[{start} {end}]")
            else:
                query_parts.append(f"@{field}:{value}")
        return ' '.join(query_parts) if query_parts else '*'

# Deposit Management Class
class DepositManagement:
    def __init__(self):
        pass

    async def create_index(self, redis_client):
        try:
            await redis_client.ft(DEPOSIT_INFO_INDEX).dropindex()
        except:
            pass

        schema = (
            TextField("deposit_id", sortable=True),
            TextField("user_id", sortable=True),
            TextField("deposit_status", sortable=True),
            TextField("created_at", sortable=True),
            TextField("updated_at", sortable=True),
        )
        await redis_client.ft(DEPOSIT_INFO_INDEX).create_index(
            schema,
            definition=IndexDefinition(prefix=[DEPOSIT_INFO_PREFIX], index_type=IndexType.HASH)
        )

    async def _init_search_indexes(self, redis_client):
        await self.create_index(redis_client)

    async def search_deposits_advanced(self, filters: dict, sort_by: str = None, sort_asc: bool = True, offset: int = 0, limit: int = 10) -> dict:
        try:
            query_str = self.build_query(filters)
            print(f"Searching deposits with query: {query_str}")
            
            query = Query(query_str).paging(offset, limit)
            if sort_by:
                query.sort_by(sort_by, asc=sort_asc)
            
            results = await redis_client.ft(DEPOSIT_INFO_INDEX).search(query)
            deposits = [
                {k: v for k, v in doc.__dict__.items() if not k.startswith('__')}
                for doc in results.docs
            ]
            return {'response': True, 'total': results.total, 'results': deposits}
        except Exception as e:
            print(f"Error searching deposits: {e}")
            return {'response': False, 'error': str(e)}

    def build_query(self, filters: dict) -> str:
        query_parts = []
        for field, value in filters.items():
            if isinstance(value, (list, tuple)):
                if isinstance(value, list):
                    options = '|'.join(map(str, value))
                    query_parts.append(f"@{field}:({options})")
                elif isinstance(value, tuple) and len(value) == 2:
                    start, end = map(str, value)
                    query_parts.append(f"@{field}:[{start} {end}]")
            else:
                query_parts.append(f"@{field}:{value}")
        return ' '.join(query_parts) if query_parts else '*'
'''
'''
# Unified Search Function
async def search_history(order_mgr, deposit_mgr,history_type: str, filters: dict, sort_by: str = None, sort_asc: bool = True, offset: int = 0, limit: int = 10) -> dict:
    if history_type == 'order':
        return await order_mgr.search_orders_advanced(filters, sort_by, sort_asc, offset, limit)
    elif history_type == 'deposit':
        return await deposit_mgr.search_deposits_advanced(filters, sort_by, sort_asc, offset, limit)
    else:
        return {'response': False, 'error': 'Invalid history type. Use "order" or "deposit".'}

# Main Function
async def main():
    """Main function with improved error handling"""
    redis_client = None
    try:
        redis_client = await create_redis_client()
        order_mgr = OrderManagement()
        deposit_mgr = DepositManagement()
        
        await order_mgr._init_search_indexes(redis_client)
        await deposit_mgr._init_search_indexes(redis_client)

        # Example 1: Search orders by user and date range
        filters = {
            'user_id': '12345',
            'created_at': ('2023-10-01T00:00:00', '2023-10-31T23:59:59'),
            'order_status': ['COMPLETED', 'CANCELLED']
        }
        result = await search_history(order_mgr, deposit_mgr, 'order', filters, sort_by='created_at', limit=10)
        print(colored("Order Search Result:", "blue"))
        print(json.dumps(result, indent=2))

        # Example 2: Search deposits by amount and status
        filters = {
            'amount': (50.0, 200.0),
            'deposit_status': 'COMPLETED',
            'user_id': '67890'
        }
        result = await search_history(order_mgr, deposit_mgr, 'deposit', filters, sort_by='amount', sort_asc=False, offset=5, limit=5)
        print(colored("Deposit Search Result:", "green"))
        print(json.dumps(result, indent=2))

    except Exception as e:
        print(colored(f"Fatal error: {str(e)}", "red"))
    finally:
        if redis_client:
            await redis_client.aclose()

if __name__ == "__main__":
    asyncio.run(main())'''