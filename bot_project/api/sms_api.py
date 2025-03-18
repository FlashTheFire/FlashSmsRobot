import asyncio
import json
import logging
import time
from pathlib import Path
from functools import wraps
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict

from aiohttp import web
import aiohttp
import aiofiles
import redis.asyncio as redis

from utils.redis_manager import redis_manager
from handlers.main.inline_query import search_manager

# Configure logging
logging.basicConfig(level=logging.INFO)

# Redis configuration
REDIS_URL = "redis://localhost:6379"
RATE_LIMIT_KEY_PREFIX = "rate_limit:"
DEFAULT_RATE_LIMIT = 100  # requests per minute

class RateLimiter:
    """Rate limiter using Redis."""
    def __init__(self, redis_client: redis.Redis, limit: int = DEFAULT_RATE_LIMIT, window: int = 60):
        self.redis = redis_client
        self.limit = limit
        self.window = window

    async def is_rate_limited(self, key: str) -> bool:
        now = int(time.time())
        window_start = now - self.window
        async with self.redis.pipeline() as pipe:
            await pipe.zremrangebyscore(key, 0, window_start)
            await pipe.zcard(key)
            await pipe.zadd(key, {str(now): now})
            await pipe.expire(key, self.window)
            result = await pipe.execute()
            # result[1] holds the count after zcard
            current_count = result[1]
            return current_count > self.limit


def setup_redis() -> redis.Redis:
    """Setup Redis connection."""
    return redis.from_url(REDIS_URL, decode_responses=True)

def with_rate_limit(limit: int = DEFAULT_RATE_LIMIT, window: int = 60):
    """Decorator for rate limiting."""
    def decorator(handler):
        @wraps(handler)
        async def wrapper(request: web.Request) -> web.Response:
            redis_client = request.app.get('redis')
            if not redis_client:
                return await handler(request)
            # Use the remote IP as the rate limit key identifier
            client_ip = request.remote or "anonymous"
            key = f"{RATE_LIMIT_KEY_PREFIX}{client_ip}"
            limiter = RateLimiter(redis_client, limit, window)
            if await limiter.is_rate_limited(key):
                return web.Response(status=429, text="Too Many Requests")
            return await handler(request)
        return wrapper
    return decorator

async def load_mappings() -> Tuple[Dict, Dict]:
    """
    Asynchronously load the mappings:
      - Get the app mapping from Redis.
      - Load the country mapping from a JSON file.
    """
    try:
        redis_client = await redis_manager.get_client()
        country_mapping = await redis_client.json().get('main_data:service:country_code')
        app_mapping = await redis_client.json().get('main_data:service:app_data')

        #logging.info("Mappings loaded successfully")

        reverse_map = {}

        for app_name, details in app_mapping.items():
            codes = details.get("code")
            if isinstance(codes, list) and codes:  # If list & not empty, use first element
                reverse_map[app_name.lower().replace(" ", "")] = codes[0]
            elif isinstance(codes, str):  # If single code
                reverse_map[app_name.lower().replace(" ", "")] = codes

        return country_mapping, reverse_map

    except Exception as e:
        logging.error(f"Error loading mappings: {e}")
        return {}, {}

def get_app_code_from_mapping(app_name: str, app_mapping: Dict[str, str]) -> str:
    """Retrieve the app code given an app name from the mapping."""
    normalized_name = app_name.lower().replace(" ", "")  # Normalize input
    return app_mapping.get(normalized_name, app_name)  # Return code or original input


async def transform_json_structure(data: dict):
    """
    Transforms the input JSON structure into the desired format.
    For each country, maps country names to codes, processes each service,
    and selects the best server based on cost and count.
    """
    transformed = {}
    selected_servers = {}
    unvalid_servers = []

    country_mapping, app_mapping = await load_mappings()
    for country, services in data.items():
        if not isinstance(services, dict):
            logging.error(f"Skipping country '{country}': expected dict but got {type(services).__name__}")
            continue

        try:
            country_code = country_mapping.get(country.lower(), "none")
            if country_code not in transformed:
                transformed[country_code] = {}

            for service, servers in services.items():
                service_code = get_app_code_from_mapping(service, app_mapping)
                if not service_code:
                    logging.error(f"Invalid service: {service}")
                    continue

                if not isinstance(servers, dict):
                    logging.error(f"Skipping service '{service}' in country '{country}': expected dict but got {type(servers).__name__}")
                    continue

                valid_servers = [
                    (float(details.get("cost", 0)), int(details.get("count", 0)), server_name)
                    for server_name, details in servers.items()
                    if isinstance(details, dict) and int(details.get("count", 0)) > -1
                ]

                if not valid_servers:
                    logging.error(f"No valid servers found for service: {service}")
                    continue

                avg_cost = sum(cost for cost, _, _ in valid_servers) / len(valid_servers)
                low_cost_servers = [s for s in valid_servers if s[0] < avg_cost]

                if low_cost_servers:
                    avg_count = sum(count for _, count, _ in low_cost_servers) / len(low_cost_servers)
                    candidates = [s for s in low_cost_servers if s[1] > avg_count]
                else:
                    candidates = []

                best = max(candidates or valid_servers, key=lambda s: s[1] / s[0] if s[0] != 0 else float('inf'))
                cost, count, server_name = best
                transformed[country_code][service_code] = {f"{cost:.2f}": str(count)}
                selected_servers[service_code] = server_name

        except Exception as e:
            logging.error(f"Error processing country '{country}': {e}")
            continue

    #logging.info("Unselected Servers:")
    #logging.info(unvalid_servers)
    return transformed, selected_servers

async def fetch_with_retry(url: str, retries: int = 1):
    """
    Fetch JSON from a URL with asynchronous retry logic.
    Implements timeout handling, rate-limit checks, and exponential backoff.
    """
    timeout_duration = 20
    async with aiohttp.ClientSession() as session:
        for attempt in range(1, retries + 1):
            timeout = aiohttp.ClientTimeout(total=timeout_duration)
            try:
                async with session.get(url, timeout=timeout) as resp:
                    if resp.status == 500:
                        return 'NO_NUMBER'
                    if resp.status != 200:
                        retry_after = resp.headers.get("Retry-After")
                        wait_time = int(retry_after) if retry_after and retry_after.isdigit() else 1
                        logging.warning(
                            f"Rate limited when accessing {url}. Waiting {wait_time} seconds (attempt {attempt}/{retries})."
                        )
                        await asyncio.sleep(wait_time)
                        continue
                    resp.raise_for_status()
                    raw_response = await resp.text()
                    return json.loads(raw_response)
            except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
                logging.error(f"Error fetching {url}: {e}")
                if attempt < retries:
                    backoff = 2 ** (attempt - 1)
                    logging.info(f"Retrying {url} in {backoff} seconds (attempt {attempt}/{retries})...")
                    await asyncio.sleep(backoff)
                    timeout_duration *= 1.5
                else:
                    logging.error(f"Failed to fetch JSON from {url} after {retries} attempts.")
                    return None
            except Exception as e:
                logging.error(f"Unexpected error fetching {url}: {e}")
                if attempt < retries:
                    backoff = 2 ** (attempt - 1)
                    logging.info(f"Retrying {url} in {backoff} seconds (attempt {attempt}/{retries})...")
                    await asyncio.sleep(backoff)
                    timeout_duration *= 1.5
                else:
                    logging.error(f"Failed to fetch JSON from {url} after {retries} attempts.")
                    return None

@with_rate_limit()
async def handle_api_request(request: web.Request):
    """Handle API requests for SMS services with rate limiting and caching."""
    try:
        action = request.query.get('action')
        if not action:
            raise web.HTTPBadRequest(text=json.dumps({"error": "Missing action parameter"}))

        if action not in ['getPrices', 'getServer', 'getCountries']:
            return web.json_response(
                {"error": "Invalid action. Must be 'getPrices', 'getServer', or 'getCountries'."},
                status=400
            )


        if action == 'getCountries':
            # Provide a simple countries list
            countries = {"0": "russia", "1": "ukraine", "2": "kazakhstan", "4": "philippines",
                "6": "indonesia", "7": "malaysia", "8": "kenya", "9": "tanzania",
                "10": "vietnam", "11": "kyrgyzstan", "13": "israel", "14": "hongkong",
                "15": "poland", "16": "england", "17": "madagascar", "18": "dcongo",
                "19": "nigeria", "20": "macao", "21": "egypt", "22": "india",
                "23": "ireland", "24": "cambodia", "25": "laos", "26": "haiti",
                "27": "ivory", "28": "gambia", "29": "serbia", "31": "southafrica",
                "32": "romania", "33": "colombia", "34": "estonia", "35": "azerbaijan",
                "36": "canada", "37": "morocco", "38": "ghana", "39": "argentina",
                "40": "uzbekistan", "41": "cameroon", "42": "chad", "43": "germany",
                "44": "lithuania", "45": "croatia", "46": "sweden", "48": "netherlands",
                "49": "latvia", "50": "austria", "51": "belarus", "52": "thailand",
                "53": "saudiarabia", "54": "mexico", "55": "taiwan", "56": "spain",
                "58": "algeria", "59": "slovenia", "60": "bangladesh", "61": "senegal",
                "63": "czech", "64": "srilanka", "65": "peru", "66": "pakistan",
                "67": "newzealand", "68": "guinea", "70": "venezuela", "71": "ethiopia",
                "72": "mongolia", "73": "brazil", "74": "afghanistan", "75": "uganda",
                "76": "angola", "77": "cyprus", "78": "france", "79": "papua",
                "80": "mozambique", "81": "nepal", "82": "belgium", "83": "bulgaria",
                "84": "hungary", "85": "moldova", "86": "italy", "87": "paraguay",
                "88": "honduras", "89": "tunisia", "90": "nicaragua", "91": "timorleste",
                "92": "bolivia", "93": "costarica", "94": "guatemala", "97": "puertorico",
                "99": "togo", "100": "kuwait", "101": "salvador", "103": "jamaica",
                "104": "trinidad", "105": "ecuador", "106": "swaziland", "107": "oman",
                "108": "bosnia", "109": "dominican", "112": "panama", "114": "mauritania",
                "115": "sierraleone", "116": "jordan", "117": "portugal", "118": "barbados",
                "119": "burundi", "120": "benin", "123": "botswana", "128": "georgia",
                "129": "greece", "130": "guineabissau", "131": "guyana", "134": "saintkitts",
                "135": "liberia", "136": "lesotho", "137": "malawi", "138": "namibia",
                "140": "rwanda", "141": "slovakia", "142": "suriname", "143": "tajikistan",
                "145": "bahrain", "146": "reunion", "147": "zambia", "148": "armenia",
                "152": "burkinafaso", "154": "gabon", "155": "albania", "156": "uruguay",
                "157": "mauritius", "158": "bhutan", "159": "maldives", "160": "guadeloupe",
                "161": "turkmenistan", "162": "frenchguiana", "163": "finland",
                "164": "saintlucia", "165": "luxembourg", "166": "saintvincentgrenadines",
                "167": "equatorialguinea", "168": "djibouti", "169": "antiguabarbuda",
                "171": "montenegro", "172": "denmark", "173": "switzerland", "174": "norway",
                "175": "australia", "179": "aruba", "183": "northmacedonia",
                "184": "seychelles", "185": "newcaledonia", "186": "capeverde",
                "201": "gibraltar"}
            return web.json_response(countries, status=200)


        country_param = request.query.get('country', '22')
        remote_url = (
            f"http://api1.5sim.net/stubs/handler_api.php?"
            f"country={country_param}&api_key=d74c46dd007f4940bd37af35b8f39b64&action=getPrices"
        )

        data = await fetch_with_retry(remote_url)
        if data is None:
            return web.json_response({"error": "Failed to fetch data from remote API"}, status=500)

        if isinstance(data, dict) and "status" in data and "msg" in data:
            return web.json_response({"error": data.get("msg", "Unknown error from API")}, status=500)

        # Retrieve mappings from app state
        

        transformed_data, selected_servers = await transform_json_structure(data)
        if action == 'getPrices':
            return web.json_response(transformed_data)
        else:
            return web.json_response(selected_servers)

    except web.HTTPException:
        raise
    except Exception as e:
        logging.error(f"API request error: {str(e)}")
        return web.Response(
            status=500,
            text=json.dumps({"error": "Internal server error"}),
            content_type='application/json'
        )

async def setup_routes(app: web.Application):
    """Setup routes for the API server."""
    app.router.add_get('/stubs/handler_api.php', handle_api_request)

async def init_app() -> web.Application:
    """Initialize the aiohttp application with Redis, caching, and mappings."""
    app = web.Application()
    # Setup Redis and caching
    app['redis'] = setup_redis()
    # Load mappings asynchronously and store in app state
    await setup_routes(app)
    return app
