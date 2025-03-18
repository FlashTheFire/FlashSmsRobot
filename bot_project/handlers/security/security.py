import hashlib
import hmac
import secrets
from typing import Optional, Dict
from datetime import datetime, timedelta
import jwt
from functools import wraps
from telebot.async_telebot import AsyncTeleBot
from utils.redis_manager import redis_manager

class SecurityManager:
    def __init__(self, secret_key: str):
        self.secret_key = secret_key
        self._redis = redis_manager
        
    def generate_api_signature(self, params: dict) -> str:
        sorted_params = '&'.join(f"{k}={v}" for k, v in sorted(params.items()))
        return hmac.new(
            self.secret_key.encode(),
            sorted_params.encode(),
            hashlib.sha256
        ).hexdigest()
        
    def verify_api_signature(self, params: dict, signature: str) -> bool:
        expected = self.generate_api_signature(params)
        return hmac.compare_digest(signature, expected)
        
    async def generate_session_token(self, user_id: int, expires_in: int = 3600) -> str:
        payload = {
            'user_id': user_id,
            'exp': datetime.utcnow() + timedelta(seconds=expires_in),
            'jti': secrets.token_hex(16)
        }
        token = jwt.encode(payload, self.secret_key, algorithm='HS256')
        
        # Store token in Redis for blacklisting capability
        await self._redis.redis_client.setex(
            f"session:{user_id}:{payload['jti']}", 
            expires_in,
            token
        )
        return token
        
    async def verify_session_token(self, token: str) -> Optional[Dict]:
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=['HS256'])
            
            # Check if token is blacklisted
            stored_token = await self._redis.redis_client.get(
                f"session:{payload['user_id']}:{payload['jti']}"
            )
            if not stored_token:
                return None
                
            return payload
        except jwt.ExpiredSignatureError:
            return None
        except jwt.InvalidTokenError:
            return None
            
    async def blacklist_token(self, token: str):
        """Blacklist a token before its expiration"""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=['HS256'])
            await self._redis.redis_client.delete(
                f"session:{payload['user_id']}:{payload['jti']}"
            )
        except jwt.InvalidTokenError:
            pass

def require_auth(f):
    """Decorator to require authentication for bot handlers"""
    @wraps(f)
    async def decorated(bot: AsyncTeleBot, *args, **kwargs):
        message = args[0]
        user_id = message.from_user.id if message.from_user else None
        
        if not user_id:
            await bot.reply_to(message, "Authentication required.")
            return
            
        # Add user_id to kwargs for the handler
        kwargs['user_id'] = user_id
        return await f(bot, *args, **kwargs)
    return decorated
