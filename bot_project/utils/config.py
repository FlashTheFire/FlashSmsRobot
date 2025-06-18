import os
import sys
from dotenv import load_dotenv

load_dotenv()

def get_required_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        print(f"Error: Required environment variable {key} is not set")
        sys.exit(1)
    return value

# Required configurations
DEPOSIT_TIMEOUT = 10  # 15 minutes expiration time
INR_RATE = 1.0        # 1 INR = 1 Point (💎)
MIN_DEPOSIT = '1⩇'      # 1 Point (💎) minimum deposit amount
# config.py
DEPOSIT_CONFIG = {
    'currency': 'INR',
    'timeout': 15,
    'rate_limits': {
        'deposit': {'requests': 5, 'period': 60},
        'verification': {'requests': 10, 'period': 300}
    },
    'branding': {
        'qr_colors': {'dark': '#2a2f3d', 'light': '#ffffff'},
        'menu_image': 'https://example.com/deposit-banner.jpg'
    },
    'payment_methods': [
        {'id': 'upi', 'display_name': '💰 UPI'},
        {'id': 'card', 'display_name': '💳 Credit Card'}
    ]
}
APP_IMAGE_LIST = {
    '2203': 'https://i.ibb.co/Wvh4R4yX/image-removebg-preview.png',
}
PAYMENT_GATEWAY = {
    'endpoint': 'https://api.paymentgateway.com/v1/charges',
    'status_endpoint': 'https://api.paymentgateway.com/v1/status',
    'headers': {'Authorization': 'Bearer YOUR_API_KEY'}
}
# utils/config.py
import os

# Payment Gateway Configuration
PAYMENT_GATEWAY_API = os.getenv("PAYMENT_GATEWAY_API", "https://api.payment-gateway.com/v1")
PAYMENT_GATEWAY_API_KEY = os.getenv("PAYMENT_GATEWAY_API_KEY", "your_api_key_here")
INR_RATE = 1.0  # 1 INR = 1 Point
COMMISSION = os.getenv("COMMISSION", 1.25)  # 25% commission

BASE_TIMEOUT = os.getenv("BASE_TIMEOUT", 10)  # minutes
EXTENDED_TIMEOUT = os.getenv("EXTENDED_TIMEOUT", 20)  # minutes
CHECK_INTERVAL = os.getenv("CHECK_INTERVAL", 5)  # seconds
UPDATE_INTERVAL = os.getenv("UPDATE_INTERVAL", 60)  # seconds
BATCH_SIZE = os.getenv("BATCH_SIZE", 100)
ENV_FILE = os.getenv("ENV_FILE", ".env")


CHANNEL_ID = get_required_env("CHANNEL_ID")
BOT_TOKEN = get_required_env("BOT_TOKEN")
ADMIN_ID = os.getenv("ADMIN_ID", "5716978793")
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST")

# Optional configurations with sensible defaults
START_PAGE = os.getenv("START_PAGE", "default_start_page")
DEPOSIT_PAGE = os.getenv("DEPOSIT_PAGE", "default_deposit_page")
REFFERAL_PAGE = os.getenv("REFFERAL_PAGE", "default_referral_page")
LOADING_GIF = os.getenv("LOADING_GIF", "default_loading.gif")
WALLET_PAGE = os.getenv("WALLET_PAGE", "default_wallet_page")
DEPOSIT_INR_QR_CODE = os.getenv("DEPOSIT_INR_QR_CODE", "https://i.postimg.cc/1thT9t0C/image.png")

# Redis configuration with defaults
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")  # Empty string if not set
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# Service configuration
SMS_PROVIDERS = os.getenv("SMS_PROVIDERS", "default_provider")
APP_COUNT = int(os.getenv("APP_COUNT", 5))

SERVICE_INDEX = os.getenv("SERVICE_INDEX", "service_index")
SERVICE_PREFIX = os.getenv("SERVICE_PREFIX", "service_data:")

ORDER_INDEX = os.getenv("ORDER_INDEX", "order_index")
ORDER_PREFIX = os.getenv("ORDER_PREFIX", "order_data:")

URL = os.getenv("URL", "https://temp.sh/MkMsR/flashsms.json")

# Cache Configuration with reasonable defaults
CACHE_PREFIX = os.getenv("CACHE_PREFIX", "cache:")
INLINE_CACHE_PREFIX = f"{CACHE_PREFIX}inline_cache:"
CACHE_DURATION = int(os.getenv("CACHE_DURATION", 1800))  # 30 minutes
CACHE_RESULTS_PER_PAGE = int(os.getenv("CACHE_RESULTS_PER_PAGE", 10))
CACHE_EXPIRY = int(os.getenv("CACHE_EXPIRY", 300))  # 5 minutes
CACHE_KEY = os.getenv("CACHE_KEY", "cache-data:")
USER_IMAGE_HASH = os.getenv("USER_IMAGE_HASH", "image_data:user-profile")

# Validate critical configurations
def validate_config():
    if not BOT_TOKEN or len(BOT_TOKEN) < 20:
        print("Error: Invalid BOT_TOKEN configuration")
        sys.exit(1)
    
    if REDIS_PORT < 1 or REDIS_PORT > 65535:
        print("Error: Invalid REDIS_PORT configuration")
        sys.exit(1)
    
    if CACHE_DURATION < 0 or CACHE_EXPIRY < 0:
        print("Error: Cache durations cannot be negative")
        sys.exit(1)

validate_config()