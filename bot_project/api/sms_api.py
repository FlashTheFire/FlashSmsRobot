# File: api/sms_api.py
import os
import time
import json
import redis
import logging
from typing import Dict, Optional, Tuple, List, Any, Union
from utils.redis_manager import redis_manager, RedisManager
from redis.exceptions import RedisError
from termcolor import colored
from utils.config import COMMISSION
from typing import Optional, Dict, Any
from redis.exceptions import RedisError
import logging
from aiohttp import web
import redis.asyncio as aioredis  # Ensure redis-py >= 4.2 is installed
from utils.functions import create_keyboard, convert_points, get_tg_profile_photo
from utils.redis_manager import redis_manager, RedisManager
from handlers.manager.operation import FinancialManagement, UserManagement, OrderManagement, financial_mgr, order_mgr
from handlers.methods.purchase.made_purchase import purchase_manager
from utils.config import LOADING_GIF
from telebot.async_telebot import AsyncTeleBot
from typing import Optional, Dict, Any
import json
import time
import aiofiles
from redis import Redis
from redis.asyncio import Redis as AsyncRedis
import asyncio
from functools import partial
from utils.redis_keys import RedisKeys
from handlers.security import RateLimiter, InputValidator, TransactionGuard
from handlers.methods.purchase.order_status import purchase_status
from handlers.manager.operation import order_mgr, user_mgr


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_RATE_LIMIT  = int(os.getenv("DEFAULT_RATE_LIMIT", 60))
DEFAULT_RATE_WINDOW = int(os.getenv("DEFAULT_RATE_WINDOW", 60))
CACHE_TTL           = int(os.getenv("CACHE_TTL", 60))
DEBUG_MODE          = True


# V1 base URL (legacy)
V1_BASE_PATH        = "/stubs/handler_api.php"
# V2 base URL prefix
V2_PREFIX           = "/v1"

# ─────────────────────────────────────────────────────────────────────────────
# Logging Setup
# ─────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger("combined_api")
logger.setLevel(logging.DEBUG if DEBUG_MODE else logging.INFO)
_stream_handler = logging.StreamHandler()
_stream_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
)
logger.addHandler(_stream_handler)

# ─────────────────────────────────────────────────────────────────────────────
# Globals (set during init_app)
# ─────────────────────────────────────────────────────────────────────────────
redis_client: Optional[redis.Redis] = None
rate_limiter: Optional["RateLimiter"] = None


# ─────────────────────────────────────────────────────────────────────────────
# Rate Limiter Definition
# ─────────────────────────────────────────────────────────────────────────────
class RateLimiter:
    """
    Simple sliding‐window rate limiter using Redis sorted sets.

    Each request is timestamped in a ZSET, and we remove entries older than
    `window` seconds. If the count exceeds `limit`, we return (True, 0, retry_after).
    Otherwise, we return (False, remaining_quota, 0).
    """
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    async def is_limited(
        self, client_key: str, limit: int, window: int
    ) -> Tuple[bool, int, int]:
        now = int(time.time())
        window_start = now - window
        redis_key = f"cache_data:rate_limit:api_key:{client_key}"
        try:
            async with self.redis.pipeline() as pipe:
                pipe.zremrangebyscore(redis_key, 0, window_start)
                pipe.zcard(redis_key)
                pipe.zadd(redis_key, {str(now): now})
                pipe.expire(redis_key, window)
                result = await pipe.execute()
            current_count = result[1]
            if current_count > limit:
                oldest = await self.redis.zrange(redis_key, 0, 0, withscores=True)
                if oldest:
                    oldest_ts = int(oldest[0][1])
                    retry_after = max(0, window - (now - oldest_ts))
                else:
                    retry_after = window
                return True, 0, retry_after
            remaining = limit - current_count
            return False, remaining, 0
        except redis.RedisError as e:
            logger.warning(f"Redis error in RateLimiter: {e}")
            return False, limit, 0


# ─────────────────────────────────────────────────────────────────────────────
# CombinedAPI: holds all route handlers
# ─────────────────────────────────────────────────────────────────────────────
class CombinedAPI:
    VALID_STATUSES = {-1, 1, 3, 6, 8}
    def __init__(self, redis_client: redis.Redis = None, rate_limiter: "RateLimiter" = None, bot: AsyncTeleBot = None):
        if redis_client is not None:
            self.redis_client = redis_client
        if rate_limiter is not None:
            self.rate_limiter = rate_limiter
        self.bot: Optional[AsyncTeleBot] = None
        self.service_index = os.getenv("SERVICE_INDEX", "service_index")
        self.order_index = os.getenv("ORDER_INDEX", "order_index")
        self.H_API_KEYS = "secure_data:user_data:api_keys"
        self.H_USER_KEYS = "secure_data:user_data:user_keys"

        self.user_aggregator: Optional[FinancialManagement] = financial_mgr

    async def enrich_user(self, user_id: Dict) -> Dict:
        """
        Given a user dict (must include 'id'), enriches it with:
            - total_orders_success
            - total_orders_cancel
            - total_orders
            - rating_out_of_10
        Uses RediSearch to fetch counts via FT.AGGREGATE.
        """

        async def count(user_id: str, statuses: str, amount: str = None) -> int:
            try:
                cmd = [
                    "FT.AGGREGATE", self.order_index,
                    f"@user_id:{user_id} @order_status:({statuses})",
                    "GROUPBY", "0",
                ]
                if amount:
                    cmd.extend(["REDUCE", "SUM", "1", f"@{amount}", "AS", "Total"])
                else:
                    cmd.extend(["REDUCE", "COUNT", "0", "AS", "count"])
                raw = await self.redis_client.execute_command(*cmd)
                return int(raw[1][1]) if raw and len(raw) > 1 else 0
            except Exception as e:
                logger.warning(f"Redis count error for {statuses} (user {user_id}): {e}")
                return 0

        uid = str(user_id)
        total_success = await count(uid, "PROCESSING|COMPLETED")
        total_cancel = await count(uid, "CANCELLED")
        total_orders = await count(uid, "PENDING|PROCESSING|CANCELLED|COMPLETED")
        total_pending = await count(uid, "PENDING", '@order_amount')

        rating = round((2 * (total_success) / total_orders) * 10, 1) if total_orders > 0 else 0.0

        return {
            "total_orders_success": total_success,
            "total_orders_cancel": total_cancel,
            "total_orders": total_orders,
            "total_pending": total_pending,
            "rating_out_of_10": rating,
        }


    async def app_data_prices(
        self,
        country_id: Optional[int] = None,
        app_id: Optional[int] = None,
        server_id: Optional[int] = None,
        api_id: Optional[int] = None
    ) -> Dict[str, Any]:
        def fld(val, id_field, name_field):
            if not val:
                return None
            return (
                f"@{id_field}:{val}"
                if str(val).isnumeric()
                else f"@{name_field}:(%%{val}%%|{val}*|{val})"
            )

        filters = list(filter(
            None,
            [
                fld(country_id, "country_id", "country_name"),
                fld(app_id,     "app_id",     "app_name"),
                f"@server_id:{server_id}" if server_id else None,
            ]
        ))

        if filters:
            filters.append("@app_price:[0.01 +inf]")

        q = " ".join(filters) if filters else "*"

        batch_size = 10000
        offset = 0
        all_rows = []

        while True:
            cmd = [
                "FT.AGGREGATE", self.service_index, q,
                "GROUPBY", "4", "@country_id", "@app_id", "@app_price", "@server_id",
                "REDUCE", "SUM", "1", "@app_count", "AS", "total_count",
                "LIMIT", str(offset), str(batch_size),
            ]
            try:
                resp = await self.redis_client.execute_command(*cmd)
            except RedisError as e:
                logger.exception("Aggregation failed: %s", e)
                raise RuntimeError(f"Redis aggregation error: {e}")

            if not isinstance(resp, list) or len(resp) < 2:
                break

            all_rows.extend(resp[1:])
            if len(resp) - 1 < batch_size:
                break
            offset += batch_size

        if api_id == 1:
            raw_data_1: Dict[str, Dict[str, Dict[str, int]]] = {}
            for row in all_rows:
                if not isinstance(row, list) or len(row) % 2 != 0:
                    continue
                rd = {row[i]: row[i + 1] for i in range(0, len(row), 2)}
                cid = rd.get("country_id")
                aid = rd.get("app_id")
                price = rd.get("app_price")
                count = rd.get("total_count")
                if None in (cid, aid, price, count):
                    continue
                try:
                    price_str = f"{float(price) * float(COMMISSION):.2f}"
                    total = int(count)
                except ValueError:
                    continue
                raw_data_1.setdefault(cid, {}).setdefault(aid, {})[price_str] = total
            sorted_data_1: Dict[str, Dict[str, Dict[str, int]]] = {}
            for cid_key in sorted(raw_data_1.keys(), key=lambda x: int(x)):
                sorted_data_1[cid_key] = {}
                for aid_key in sorted(raw_data_1[cid_key].keys(), key=lambda x: int(x)):
                    sorted_data_1[cid_key][aid_key] = dict(
                        sorted(
                            raw_data_1[cid_key][aid_key].items(),
                            key=lambda item: float(item[0]),
                        )
                    )
            return sorted_data_1

        elif api_id == 2:
            temp_data_2: Dict[str, Dict[str, Dict[str, Dict[str, Union[float, int]]]]] = {}
            for row in all_rows:
                if not isinstance(row, list) or len(row) % 2 != 0:
                    continue
                rd = {row[i]: row[i + 1] for i in range(0, len(row), 2)}
                cid = rd.get("country_id")
                aid = rd.get("app_id")
                price = rd.get("app_price")
                cnt = rd.get("total_count")
                srv = rd.get("server_id")
                if None in (cid, aid, price, cnt, srv):
                    continue
                try:
                    price_val = round(float(price) * float(COMMISSION), 2)
                    stock_val = int(cnt)
                    server_val = int(srv)
                except ValueError:
                    continue
                temp_data_2.setdefault(str(cid), {}).setdefault(str(aid), {})[str(server_val)] = {
                    "cost": price_val,
                    "count": stock_val
                }
            if country_id is not None:
                cid_str = str(country_id)
                return temp_data_2.get(cid_str, {str(country_id): {}})
            else:
                return temp_data_2

        else:
            return await self.app_data_prices(country_id, app_id, server_id, api_id=1)
    async def app_data_stock(
        self,
        country_id: Optional[int] = None,
        app_id: Optional[int] = None,
        server_id: Optional[int] = None,
        min_stock: int = 0,
        api_id: int = None
    ) -> Dict[str, Any]:
        query = " ".join(
            filter(None, [
                f"@country_id:{country_id}" if country_id is not None else None,
                f"@app_id:{app_id}" if app_id is not None else None,
                f"@server_id:{server_id}" if server_id is not None else None,
                f"@app_count:[{min_stock} +inf]"
            ])
        ) or "*"

        cmd = [
            "FT.AGGREGATE",
            self.service_index,
            query,
            "GROUPBY",
            "3",
            "@country_id",
            "@app_id",
            "@server_id",
            "REDUCE",
            "SUM",
            "1",
            "@app_count",
            "AS",
            "total_stock"
        ]

        try:
            resp = await self.redis_client.execute_command(*cmd)
        except RedisError as e:
            logger.exception("Stock aggregation failed: %s", e)
            raise RuntimeError(f"Redis aggregation error: {e}")

        if not isinstance(resp, list) or len(resp) < 2:
            return {}

        if api_id == 1:
            flat_result: Dict[str, int] = {}
            for row in resp[1:]:
                if not isinstance(row, list) or len(row) % 2 != 0:
                    continue
                try:
                    rd = {row[i]: row[i + 1] for i in range(0, len(row), 2)}
                    aid = rd.get("app_id")
                    sid = rd.get("server_id")
                    stock = int(rd.get("total_stock", 0))
                    if None in (aid, sid):
                        continue
                    key = f"{aid}_{sid}"
                    flat_result[key] = stock
                except (ValueError, TypeError):
                    continue
            return flat_result

        elif api_id == 2:
            countries_list = []
            temp_data: Dict[str, Dict[str, list]] = {}

            for row in resp[1:]:
                if not isinstance(row, list) or len(row) % 2 != 0:
                    continue
                try:
                    rd = {row[i]: row[i + 1] for i in range(0, len(row), 2)}
                    cid = rd.get("country_id")
                    aid = rd.get("app_id")
                    sid = rd.get("server_id")
                    stock = int(rd.get("total_stock", 0))
                    if None in (cid, aid, sid):
                        continue
                    cid_str = str(cid)
                    aid_str = str(aid)
                    sid_str = str(sid)
                    temp_data.setdefault(cid_str, {}).setdefault(aid_str, [])
                    temp_data[cid_str][aid_str].append({
                        "server_id": sid_str,
                        "stock": stock
                    })
                except (ValueError, TypeError):
                    continue
            for cid, apps in temp_data.items():
                country_obj = {
                    "country_id": cid,
                    "applications": []
                }
                for aid, servers in apps.items():
                    app_obj = {
                        "app_id": aid,
                        "servers": servers
                    }
                    country_obj["applications"].append(app_obj)
                countries_list.append(country_obj)
            return {"countries": countries_list}
        else:
            return await self.app_data_stock(country_id, app_id, server_id, min_stock, api_id=1)


    async def services_data(self, api_id: int = 1):
        try:
            data = await self.redis_client.json().get('main_data:service:app_data')
        except RedisError as e:
            logger.exception("Aggregation failed: %s", e)
            raise RuntimeError(f"Redis aggregation error: {e}")
        
        if data is None:
            return {}

        if api_id == 1:
            output = {
                "services": [
                    {
                        "id": int(v["app_id"]),
                        "name": k
                    }
                    for k, v in data.items()
                ],
                "status": "success"
            }
            return output
        elif api_id == 2:
            output = {
                "status": "success",
                "result": {
                    "service_list": [
                        {
                            "service_id": int(v.get("app_id", "")),
                            "service_name": k
                        }
                        for k, v in data.items()
                    ],
                    "total_services": len(data)
                }
            }
            return output
        else:
            return await self.services_data(api_id=1)
    async def countries_data(self, api_id: int = 1):
        try:
            data = await self.redis_client.json().get('main_data:details:country_data')
        except RedisError as e:
            logger.exception("Aggregation failed: %s", e)
            raise RuntimeError(f"Redis aggregation error: {e}")
        
        if data is None:
            return {}

        if api_id == 1:
            cleaned_output = {
                str(k): {
                    "id": int(k),
                    "name": v.get("country_name", "")
                }
                for k, v in data.items()
            }
            output = {
                "countries": list(cleaned_output.values()),
                "status": "success"
            }
            return output
        elif api_id == 2:
            original_data = data
            formatted_countries = {
                str(k): {
                    "id": int(k),
                    "name": v.get("country_name", ""),
                    "emoji": v.get("country_code", ""),
                    "flag_url": v.get("flag_url", "")
                }
                for k, v in original_data.items()
            }
            output = {
                "status": "success",
                "result": {
                    "country_list": list(formatted_countries.values()),
                    "total_countries": len(formatted_countries)
                }
            }
            return output
        else:
            return await self.countries_data(api_id=1)


    async def is_valid_api_key(self, key: str) -> bool:
        """
        Return True if `key` exists in H_API_KEYS (i.e. HGET(H_API_KEYS, key) != None).
        """
        try:
            val = await redis_client.hget(self.H_API_KEYS, key)
            return bool(val)
        except Exception as e:
            logger.warning(f"Redis error in is_valid_api_key: {e}")
            return False


    async def get_user_id_by_api_key(self, key: str) -> Optional[int]:
        """
        Return the integer user_id if `key` exists in H_API_KEYS.
        If not found or on error, return None.
        """
        try:
            raw = await redis_client.hget(self.H_API_KEYS, key)
            if raw is None:
                return None
            # raw is bytes (or str if decode_responses=True)
            raw_decoded = raw.decode() if isinstance(raw, bytes) else raw
            return int(raw_decoded)
        except (RuntimeError, aioredis.RedisError, ValueError) as e:
            logger.warning(f"Redis error in get_user_id_by_api_key: {e}")
            return None
    async def get_api_key_by_user_id(self, user_id: int) -> Optional[str]:
        """
        Return the API key string if `user_id` exists in H_USER_KEYS.
        No Python loop is used—just a direct HGET on the reverse‐lookup hash.
        """
        try:
            raw = await redis_client.hget(self.H_USER_KEYS, str(user_id))
            if raw is None:
                return None
            return raw.decode() if isinstance(raw, bytes) else raw
        except (RuntimeError, aioredis.RedisError) as e:
            logger.warning(f"Redis error in get_api_key_by_user_id: {e}")
            return None
    

    async def get_user_data(self, user_id: int) -> Optional[float]:
        """
        Return the user's balance if `user_id` exists in H_USER_BALANCES.
        No Python loop is used—just a direct HGET on the reverse‐lookup hash.
        """
        try:
            data = await self.user_aggregator.get_user(user_id)
            if not data or not data.get('response'):
                #loggging.error("User data response indicated failure.")
                return None
            #loggging.debug(f"Raw user data: {data}")
            user_profile = data.get("user_profile")
            current_balance = data["metrics"]["current_balance"]
            spend_balance = data["metrics"]["spend_balance"]
            total_deposits = data["metrics"]["deposits"]["total_amount"]
            target_currency = 'USD'
            timestamp = data["timestamp"]

            processed_data = {
                'user_id': user_id,
                'current_balance': current_balance,
                'total_deposits': total_deposits,
                'spend_balance': spend_balance,
                'target_currency': target_currency,
                'user_name': user_profile,
                'timestamp': timestamp
            }
            return processed_data
        except (RuntimeError, aioredis.RedisError, ValueError) as e:
            logger.warning(f"Redis error in get_user_balance: {e}")
            return {'user_id': user_id, 'current_balance': 0, 'total_deposits': 0, 'spend_balance': 0, 'target_currency': 'USD', 'user_name': '', 'timestamp': 0}
    
    
    # ─────────────────────────────────────────────────────────────────────────
    # Middleware: API Key + Rate Limiting
    # ─────────────────────────────────────────────────────────────────────────
    @staticmethod
    @web.middleware
    async def api_key_rate_limit_middleware(request: web.Request, handler):
        path = request.rel_url.path

        # 1) Allow /health with no API key required
        if path == "/health":
            return await handler(request)

        query_key = request.rel_url.query.get("api_key")
        auth_header = request.headers.get("Authorization", "")
        header_key = auth_header.replace("Bearer ", "") if auth_header.startswith("Bearer ") else None

        api_key = query_key or header_key
        source = 'query' if query_key else 'header' if header_key else None

        if not api_key:
            if source == 'header':
                return web.json_response(
                    {
                        "errors": [
                            {"code": "NO_KEY", "message": "API key is required in Authorization header."}
                        ]
                    },
                    status=400
                )
            return web.Response(text="NO_KEY", status=200)
        elif not await CombinedAPI().is_valid_api_key(api_key):
            if source == 'header':
                return web.json_response(
                    {
                        "errors": [
                            {"code": "BAD_KEY", "message": "Invalid API key provided in Authorization header."}
                        ]
                    },
                    status=401
                )
            return web.Response(text="BAD_KEY", status=200)
        try:
            is_lim, remaining, retry_after = await rate_limiter.is_limited(
                api_key, DEFAULT_RATE_LIMIT, DEFAULT_RATE_WINDOW
            )
        except (RuntimeError, redis.RedisError) as e:
            logger.warning(f"Redis error in is_limited: {e}")
            is_lim, remaining, retry_after = False, DEFAULT_RATE_LIMIT, 0

        headers = {
            "X-RateLimit-Limit": str(DEFAULT_RATE_LIMIT),
            "X-RateLimit-Remaining": str(max(remaining, 0)),
            "X-RateLimit-Window": str(DEFAULT_RATE_WINDOW),
        }
        if is_lim:
            headers["Retry-After"] = str(retry_after)
            return web.Response(text="RATE_LIMIT_EXCEEDED", status=429, headers=headers)
        request["api_key"] = api_key
        response = await handler(request)
        for k, v in headers.items():
            response.headers[k] = v
        return response

    # ─────────────────────────────────────────────────────────────────────────
    # /health route
    # ─────────────────────────────────────────────────────────────────────────
    async def handle_health(self, request: web.Request) -> web.Response:
        try:
            pong = await redis_client.ping()
            status = "ok" if pong else "degraded"
            return web.json_response({"status": status})
        except Exception:
            return web.json_response({"status": "down"}, status=503)

    async def handle_v2_user_profile(self, request: web.Request) -> web.Response:
        api_key = request.headers.get("Authorization", "").replace("Bearer ", "")
        if not api_key:
            return web.Response(text="NO_KEYS", status=200)
        # Fetch raw user data
        user_id = await self.get_user_id_by_api_key(api_key)
        u = await self.get_user_data(user_id)
        s = await self.enrich_user(user_id)

        final_json = {
            "userId": u["user_id"],
            "userName": u["user_name"],
            "currentBalance": u["current_balance"] or 0,
            "totalDeposits": u["total_deposits"] or 0,
            "spentBalance": u["spend_balance"] or 0,
            "currencyCode": u["target_currency"] or "USD",
            "freezedBalance": s["total_pending"] or 0,
            "userRating": s["rating_out_of_10"] or 0
        }

        return web.json_response(final_json)
        
    async def handle_v2_user_orders(self, request: web.Request) -> web.Response:
        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        if not token:
            return web.Response(text="Unauthorized", status=401)
        # DEMO data
        demo = {
            "Data": [{
                "id": 53533933,
                "phone": "+79085895281",
                "operator": "tele2",
                "product": "aliexpress",
                "price": 2,
                "status": "BANNED",
                "expires": "2020-06-28T16:32:43.307041Z",
                "sms": [],
                "created_at": "2020-06-28T16:17:43.307041Z",
                "country": "russia"
            }],
            "ProductNames": [],
            "Statuses": [],
            "Total": 3
        }
        return web.json_response(demo)
    async def handle_v2_user_payments(self, request: web.Request) -> web.Response:
        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        if not token:
            return web.Response(text="Unauthorized", status=401)
        # DEMO data
        demo = {
            "Data": [{
                "ID": 30011934,
                "TypeName": "charge",
                "ProviderName": "admin",
                "Amount": 100,
                "Balance": 100,
                "CreatedAt": "2020-06-24T15:37:08.149895Z"
            }],
            "PaymentTypes": [{"Name": "charge"}],
            "PaymentProviders": [{"Name": "admin"}],
            "Total": 1
        }
        return web.json_response(demo)
    async def handle_v2_user_max_prices(self, request: web.Request) -> web.Response:
        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        if not token:
            return web.Response(text="Unauthorized", status=401)

        if request.method == "GET":
            demo = [{
                "id": 14,
                "product": "telegram",
                "price": 11,
                "CreatedAt": "2020-06-24T15:37:08.149895Z"
            }]
            return web.json_response(demo)

        # For POST/DELETE: parse JSON body
        try:
            body = await request.json()
        except Exception:
            return web.Response(text="Invalid JSON", status=400)

        product = body.get("product_name")
        price = body.get("price")
        if not product:
            return web.Response(text="product_name missing", status=400)
        if request.method == "POST" and price is None:
            return web.Response(text="price missing", status=400)
        # DEMO: pretend we wrote to a DB and succeed
        return web.Response(text="OK")
    async def handle_v2_buy_activation(self, request: web.Request) -> web.Response:
        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        if not token:
            return web.Response(text="Unauthorized", status=401)
        # DEMO data:
        demo = {
            "id": 11631253,
            "phone": "+79000381454",
            "operator": "beeline",
            "product": "vkontakte",
            "price": 21,
            "status": "PENDING",
            "expires": "2018-10-13T08:28:38.809469028Z",
            "sms": None,
            "created_at": "2018-10-13T08:13:38.809469028Z",
            "forwarding": False,
            "forwarding_number": "",
            "country": "russia"
        }
        return web.json_response(demo)
    async def handle_v2_reuse(self, request: web.Request) -> web.Response:
        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        if not token:
            return web.Response(text="Unauthorized", status=401)
        demo = {"id": 11631253, "status": "OK"}
        return web.json_response(demo)
    async def handle_v2_check(self, request: web.Request) -> web.Response:
        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        if not token:
            return web.Response(text="Unauthorized", status=401)
        order_id = request.match_info.get("order_id")
        demo = {
            "id": int(order_id),
            "created_at": "2018-10-13T08:13:38.809469028Z",
            "phone": "+79000381454",
            "product": "vkontakte",
            "price": 21,
            "status": "RECEIVED",
            "expires": "2018-10-13T08:28:38.809469028Z",
            "sms": [{
                "created_at": "2018-10-13T08:20:38.809469028Z",
                "date": "2018-10-13T08:19:38Z",
                "sender": "VKcom",
                "text": "VK: 09363 - use this code to reclaim your suspended profile.",
                "code": "09363"
            }],
            "forwarding": False,
            "forwarding_number": "",
            "country": "russia"
        }
        return web.json_response(demo)
    async def handle_v2_finish(self, request: web.Request) -> web.Response:
        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        if not token:
            return web.Response(text="Unauthorized", status=401)
        order_id = request.match_info.get("order_id")
        demo = {
            "id": int(order_id),
            "created_at": "2018-10-13T08:13:38.809469028Z",
            "phone": "+79000381454",
            "product": "vkontakte",
            "price": 21,
            "status": "FINISHED",
            "expires": "2018-10-13T08:28:38.809469028Z",
            "sms": [{
                "created_at": "2018-10-13T08:20:38.809469028Z",
                "date": "2018-10-13T08:19:38Z",
                "sender": "VKcom",
                "text": "VK: 09363 - use this code to reclaim your suspended profile.",
                "code": "09363"
            }],
            "forwarding": False,
            "forwarding_number": "",
            "country": "russia"
        }
        return web.json_response(demo)
    async def handle_v2_cancel(self, request: web.Request) -> web.Response:
        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        if not token:
            return web.Response(text="Unauthorized", status=401)
        order_id = request.match_info.get("order_id")
        demo = {"id": int(order_id), "status": "CANCELED"}
        return web.json_response(demo)
    
    
    # ─────────────────────────────────────────────────────────────────────────
    # V2 (5sim) Prices
    # ─────────────────────────────────────────────────────────────────────────
    async def handle_v2_prices(self, request: web.Request) -> web.Response:
        country_id = request.rel_url.query.get("country")
        app_id = request.rel_url.query.get("product")
        server_id = request.rel_url.query.get("server")

        result = await self.app_data_prices(country_id=country_id, app_id=app_id, server_id=server_id, api_id=2)
        return web.json_response(result)
    async def handle_v2_get_number_status(self, request: web.Request) -> web.Response:
        country_id = request.rel_url.query.get("country")
        app_id = request.rel_url.query.get("product")
        server_id = request.rel_url.query.get("server")

        status = await self.app_data_stock(api_id=2, country_id=country_id, app_id=app_id, server_id=server_id)
        return web.json_response(status)
    async def handle_v2_services(self, request: web.Request) -> web.Response:
        app_data = await self.services_data(api_id=2)
        return web.json_response(app_data)
    async def handle_v2_countries(self, request: web.Request) -> web.Response:
        country_data = await self.countries_data(api_id=2)
        return  web.Response(
            text=json.dumps(country_data, ensure_ascii=False),
            content_type='application/json'
        )


    async def handle_get_number(self, user_id: int, server_id: int, app_id: int, country_id: int, input_price: float, ref_id: str, api_id: int) -> Dict[str, Any]:
        print("API ID: ", api_id)
        print("App ID: ", app_id)
        print("Country ID: ", country_id)
        print("Server ID: ", server_id)
        print("Input Price: ", input_price)
        print("Ref ID: ", ref_id)
        price = None if input_price is None else round(float(input_price) / float(COMMISSION), 2)
        app_data = await purchase_manager.fetch_app_data(app_id, country_id, server_id, price=price)
        if not app_data:
            return {"status": False, "message": F"WRONG_MAX_PRICE"}
        print("App Data: ", json.dumps(app_data, indent=4))
        price = round(float(app_data.get('app_price')) * float(COMMISSION), 2)
        if not await purchase_manager._handle_user_balance(user_id, price, user_id, None):
            return {"status": False, "message": "NO_BALANCE"}
        

        
        phone_result = await purchase_manager.fetch_phone_number(int(app_data['server_id']), app_data['app_code'], country_id, price=price, operator=app_data['server_name'], app_name=app_data['app_name'], chat_id=user_id, app_id=app_id)
        print(json.dumps(phone_result, indent=4))
        if not phone_result.get("status"):
            if phone_result.get("message"):
                return {"status": False, "message": "NO_NUMBERS"}
            else:
                return {"status": False, "message": json.dumps(phone_result)}
        full_data = {
            "server_id": app_data['server_id'],
            "app_code": app_data['app_code'],
            "country_id": country_id,
            "price": price,
            "operator": app_data['server_name'],
            "app_name": app_data['app_name'],
            "country_code": app_data['country_code'],
            "country_name": app_data['country_name'],
            "guard": None,
            "message_id": 0,
            "chat_id": user_id,
            "app_data": app_data,
            "app_id": app_id,
            "call_data": '',
            "user_id": user_id,
            "first_name": "API",
            "chat_type": "private",
            "call_chat_id": 0,
        }
        call = await purchase_manager.reconstruct_fake_call(full_data)
        order_id =await purchase_manager._finalize_purchase(
            call,
            phone_result,
            full_data,
            full_data['price'],
            full_data['country_id'],
            full_data['country_code'],
            full_data['country_name'],
            phone_result['service'],
            call.message,
            is_new=True,
            is_api=True,
            app_id=app_id,
            server_id=app_data['server_id']
        )
        #return {'status': True, 'order_id': order_id, 'number': number, 'code': code, 'service': service}

        return {"status": True, "order_id": order_id, "number": phone_result['number'], "code": phone_result['code'], "service": phone_result['service'], "country": app_data['country_name'], "price": price}
    
    async def _processing_order(self, order_id: str) -> None:
        """Finalize order completion with audit trail and error handling"""
        try:
            fields = {
                'order_status': 'PENDING'
            }
            await order_mgr.update_order_fields(order_id, fields=fields)
            print(f"Order {order_id} completed")
            
        except KeyError as e:
            print(f"Missing key in order_info for {order_id}: {e}")
        except json.JSONDecodeError as e:
            print(f"Invalid order_history JSON for {order_id}: {e}")
        except Exception as e:
            print(f"Failed to complete order {order_id}: {e}")


    async def handle_get_status(self, id: int, api_id: int = 1) -> Dict[str, Any]:
        order_key = f"order_data:info:{id}"
        result = await redis_client.hgetall(order_key)
        print(json.dumps(result, indent=4))
        if not result:
            return {"status": False, "message": "BAD_ID"}
        status = str(result['order_status'])


        if status == "CANCELLED":
            return {"status": False, "message": "STATUS_CANCEL"}
        elif status == "TIMEOUT":
            return {"status": True, "message": "STATUS_TIMEOUT"}
            
        elif status == "PENDING":
            if result.get("last_sms") and result.get("sms_list") != "[]":
                return {"status": True, "message": f"STATUS_OK:{result.get('last_sms')}"}
            else:
                return {"status": True, "message": "STATUS_WAIT_CODE"}

        elif status == "COMPLETED":
            return {"status": True, "message": F"STATUS_OK:{result.get('last_sms')}"}
            
        elif status == "PROCESSING":
            return {"status": True, "message": f"STATUS_WAIT_RETRY:{result.get('last_sms')}"}
        return status
    async def handle_set_status(self, status_id: int, order_id: int) -> None:
        order_key = f"order_data:info:{order_id}"
        print(f"handle_set_status: order_id={order_id}, status_id={status_id}") 
        result = await redis_client.hgetall(order_key)
        print(json.dumps(result, indent=4))
        if not result:
            return {"status": False, "message": "BAD_ID"}
        status = str(result['order_status'])
        ACTIVATION_STATUS = {
           -1: "CANCEL_ORDER",
            1: "SMS_SENT",
            3: "WAITING_FOR_ANOTHER_CODE",
            6: "COMPLETE_ACTIVATION",
            8: "CANCEL_ORDER",
        }
        work = ACTIVATION_STATUS.get(int(status_id), False)
        if not work:
            return {"status": False, "message": "BAD_STATUS"}

        if status == "CANCELLED":
            return {"status": False, "message": "BAD_STATUS"}
        elif status == "TIMEOUT":
            return {"status": True, "message": "BAD_STATUS"}
        
        if work == "CANCEL_ORDER":
            if status == "PENDING" and not result.get("last_sms", None):
                sms_list = json.loads(result.get('sms_list', '[]'))
                print("SMS List: ", json.dumps(sms_list, indent=4))
                print("SMS List Length: ", len(sms_list))
                if len(sms_list) != 0:
                    return {"status": False, "message": "BAD_ACTION"}

                # Proceed with cancellation
                api_result = await purchase_status.cancel_number_api(
                    result['server_id'],
                    result['order_id']
                )
                print("API Result: ", json.dumps(api_result, indent=4))
                if not api_result.get('response', False):
                    return {"status": False, "message": "BAD_ACTION"}
                number_parts = json.loads(result['order_number']) if isinstance(result.get('order_number'), str) else []
                number_part1 = number_parts[0] if len(number_parts) > 0 else ""
                number_part2 = number_parts[1] if len(number_parts) > 1 else ""
                details = {
                    "status": True,
                    "order_id": order_id,
                    "number": number_part2,
                    "code": number_part1,
                    "app_id": result['app_id'],
                    "app_name": result['app_name'],
                    "server_id": result['server_id'],
                    "app_price": result['order_amount'],
                    "country_id": result['country_id'],
                    "country_code": result['country_code'],
                    "msg_id": result['message_id'],
                    "country_name": result['country_name'],
                    "user_id": result['user_id'],
                    "valid_status": "⏱️ Oʀᴅᴇʀ Hᴀs Exᴘɪʀᴇᴅ"
                }

                await asyncio.gather(
                    order_mgr.cancel_order(order_id, result['user_id'], 'CANCELLED'),
                    user_mgr.send_order_report(self.bot, "edit_message_text", order_id, result['user_id'], '-1002203139746', details),
                    user_mgr.user_metrics_report(self.bot, 'edit_message_text', result['user_id'], '-1002203139746'),
                )
                return {"status": True, "message": "STATUS_CANCEL"}
            else:
                await redis_client.hset(order_key, "order_status", "CANCELLED")
                return {"status": True, "message": "BAD_STATUS"}
        
        elif work == "WAITING_FOR_ANOTHER_CODE":
            if status == "PENDING":
                sms_list = json.loads(result.get('sms_list', '[]'))
                if not len(sms_list):
                    return {"status": False, "message": "BAD_STATUS"}
            elif status == "COMPLETED":
                if float(result.get("recorded_at", 0)) > time.time() - 20 * 60:
                    await redis_client.hset(order_key, "order_status", "PROCESSING")
                    return {"status": True, "message": "ACCESS_RETRY_GET"}
                else:
                    return {"status": True, "message": "STATUS_TIMEOUT"}
            elif status == "PROCESSING":
                return {"status": True, "message": "STATUS_WAIT_RETRY"}

        elif work == "COMPLETE_ACTIVATION":
            if status == "PENDING":
                await redis_client.hset(order_key, "order_status", "COMPLETED")
                return {"status": True, "message": "COMPLETED"}   
            elif status == "COMPLETED":
                return {"status": True, "message": "COMPLETED"}
            elif status == "PROCESSING":
                return {"status": True, "message": "COMPLETED"}         
        
        elif work == "SMS_SENT":
            if status == "PENDING":
                sms_list = json.loads(result.get('sms_list', '[]'))
                if not len(sms_list):
                    await redis_client.hset(order_key, "order_status", "PROCESSING")
                    return {"status": False, "message": "ACCESS_RETRY_GET"}
            elif status == "COMPLETED":
                if float(result.get("recorded_at", 0)) > time.time() - 20 * 60:
                    await redis_client.hset(order_key, "order_status", "PROCESSING")
                    return {"status": True, "message": "ACCESS_RETRY_GET"}
                else:
                    return {"status": True, "message": "STATUS_TIMEOUT"}
            elif status == "PROCESSING":
                return {"status": True, "message": "STATUS_WAIT_RETRY"}

    # ─────────────────────────────────────────────────────────────────────────
    # V1 Legacy SMS API (/stubs/handler_api.php)
    # ─────────────────────────────────────────────────────────────────────────
    async def handle_v1_sms_api(self, request: web.Request) -> web.Response:
        """
        Unified handler for v1 SMS API actions in a single method.
        Supported actions: getPrices, getNumbersStatus, getNumber,
        setStatus, getStatus, getBalance, getServicesList, getCountriesList
        """
        params: Dict[str, str] = dict(request.rel_url.query)
        action = params.get("action")
        if not action:
            return web.json_response({"error": "BAD_ACTION"}, status=400)

        try:
            # Common parsing helpers
            def parse_int(key: str, error_code: str) -> Optional[int]:
                val = params.get(key)
                if val is None:
                    return None
                try:
                    return int(val)
                except ValueError:
                    raise web.HTTPBadRequest(text=error_code)

            def parse_float(key: str, error_code: str) -> Optional[float]:
                val = params.get(key)
                if val is None:
                    return None
                try:
                    return float(val)
                except ValueError:
                    raise web.HTTPBadRequest(text=error_code)

            # Dispatch within one function
            if action == "getPrices":
                operator_id = parse_int("operator", "BAD_OPERATOR")
                result = await self.app_data_prices(
                    country_id=params.get("country"),
                    app_id=params.get("service"),
                    server_id=operator_id,
                    api_id=1,
                )
                return web.json_response(result)

            if action == "getNumbersStatus":
                operator_id = parse_int("operator", "BAD_OPERATOR")
                result = await self.app_data_stock(
                    api_id=1,
                    country_id=params.get("country"),
                    app_id=params.get("service"),
                    server_id=operator_id,
                )
                return web.json_response(result)

            if action == "getNumber":
                user_id = await self.get_user_id_by_api_key(params.get("api_key"))
                # Required
                if not params.get("service"):
                    raise web.HTTPBadRequest(text="NO_SERVICE")
                if not params.get("country"):
                    raise web.HTTPBadRequest(text="NO_COUNTRY")

                service_id = parse_int("service", "BAD_SERVICE")
                country_id = parse_int("country", "BAD_COUNTRY")
                operator_id = parse_int("operator", "BAD_OPERATOR")
                max_price = parse_float("maxPrice", "BAD_MAX_PRICE")
                ref_id = parse_int("ref_id", "BAD_REF_ID")

                result = await self.handle_get_number(
                    user_id=user_id,
                    server_id=operator_id,
                    service_id=service_id,
                    country_id=country_id,
                    input_price=max_price,
                    ref_id=ref_id,
                    api_id=1,
                )
                if not result.get("status"):
                    code = result.get("message", "ERROR")
                    if code == "NO_NUMBERS":
                        raise web.HTTPBadRequest(text="NO_NUMBERS")
                    raise web.HTTPBadRequest(text=code)
                order_id = result["order_id"]
                code = result.get("code", "").lstrip("+")
                number = result.get("number", "")
                return web.Response(text=f"ACCESS_NUMBER:{order_id}:{code}{number}")

            if action == "setStatus":
                order_id = parse_int("id", "BAD_ID")
                status_id = parse_int("status", "BAD_STATUS")
                if status_id not in self.VALID_STATUSES:
                    raise web.HTTPBadRequest(text="BAD_STATUS")
                result = await self.handle_set_status(status_id=status_id, order_id=order_id)
                return web.json_response(result)

            if action == "getStatus":
                order_id = parse_int("id", "BAD_ID")
                result = await self.handle_get_status(order_id, api_id=1)
                return web.json_response(result)

            if action == "getBalance":
                user_id = await self.get_user_id_by_api_key(params.get("api_key"))
                data = await self.get_user_data(user_id=user_id)
                balance = data.get("current_balance", 0.0)
                return web.Response(text=f"ACCESS_BALANCE:{balance:.2f}")

            if action == "getServicesList":
                services = await self.services_data(api_id=1)
                return web.json_response(services)

            if action == "getCountriesList":
                countries = await self.countries_data(api_id=1)
                return web.json_response(countries)

            # Unknown action
            raise web.HTTPBadRequest(text="BAD_ACTION")

        except web.HTTPError as e:
            # Already formatted HTTP errors
            return web.Response(text=e.text, status=e.status)
        except Exception as exc:
            logger.exception("Error in V1 SMS API handler: %s", exc)
            return web.json_response({"error": "INTERNAL_ERROR"}, status=500)

    # ─────────────────────────────────────────────────────────────────────────
    # Register all routes on the given `app`
    # ─────────────────────────────────────────────────────────────────────────
    async def setup_routes(self, app: web.Application):
        # Attach middleware
        app.middlewares.append(CombinedAPI.api_key_rate_limit_middleware)

        # Healthcheck route
        app.router.add_get("/health", self.handle_health, allow_head=False)

        # V2 (5sim) user endpoints
        app.router.add_get(f"{V2_PREFIX}/user/profile", self.handle_v2_user_profile, allow_head=False)
        app.router.add_get(f"{V2_PREFIX}/user/orders", self.handle_v2_user_orders, allow_head=False)
        app.router.add_get(f"{V2_PREFIX}/user/payments", self.handle_v2_user_payments, allow_head=False)
        app.router.add_route("GET", f"{V2_PREFIX}/user/max-prices", self.handle_v2_user_max_prices)
        app.router.add_route("POST", f"{V2_PREFIX}/user/max-prices", self.handle_v2_user_max_prices)
        app.router.add_route("DELETE", f"{V2_PREFIX}/user/max-prices", self.handle_v2_user_max_prices)

        # V2 (5sim) guest endpoints
        app.router.add_get(f"{V2_PREFIX}/guest/prices", self.handle_v2_prices, allow_head=False)
        app.router.add_get(f"{V2_PREFIX}/guest/get-number-status", self.handle_v2_get_number_status, allow_head=False)

        app.router.add_get(f"{V2_PREFIX}/guest/services", self.handle_v2_services, allow_head=False)
        app.router.add_get(f"{V2_PREFIX}/guest/countries", self.handle_v2_countries, allow_head=False)

        # V2 purchase/ad-hoc endpoints
        app.router.add_get(f"{V2_PREFIX}/user/buy/activation/{{country}}/{{operator}}/{{product}}", self.handle_v2_buy_activation, allow_head=False)
        #app.router.add_get(f"{V2_PREFIX}/user/buy/hosting/{{country}}/{{operator}}/{{product}}", self.handle_v2_buy_hosting, allow_head=False)
        app.router.add_get(f"{V2_PREFIX}/user/reuse/{{product}}/{{number}}", self.handle_v2_reuse, allow_head=False)
        app.router.add_get(f"{V2_PREFIX}/user/check/{{order_id}}", self.handle_v2_check, allow_head=False)
        app.router.add_get(f"{V2_PREFIX}/user/finish/{{order_id}}", self.handle_v2_finish, allow_head=False)
        app.router.add_get(f"{V2_PREFIX}/user/cancel/{{order_id}}", self.handle_v2_cancel, allow_head=False)
        #app.router.add_get(f"{V2_PREFIX}/user/ban/{{order_id}}", self.handle_v2_ban, allow_head=False)
        #app.router.add_get(f"{V2_PREFIX}/user/sms/inbox/{{order_id}}", self.handle_v2_sms_inbox, allow_head=False)

        # V1 legacy route (single handler for all actions)
        app.router.add_route("GET", V1_BASE_PATH, self.handle_v1_sms_api)


# ─────────────────────────────────────────────────────────────────────────────
# in api/sms_api.py: init_app() creates Redis client on this same loop,
# registers middleware, then routes, and returns `app`.
# ─────────────────────────────────────────────────────────────────────────────
async def init_app(bot: AsyncTeleBot) -> web.Application:
    global redis_client, rate_limiter

    # 1) Build the aiohttp Application
    app = web.Application()

    # 2) Initialize Redis ON THIS LOOP
    redis_client = redis_manager.redis_client


    # 3) Create RateLimiter using that same Redis client
    rate_limiter = RateLimiter(redis_client)

    # 4) Register ALL routes (V1 + V2) onto `app`
    api = CombinedAPI(redis_client, rate_limiter, bot)
    await api.setup_routes(app)

    return app

