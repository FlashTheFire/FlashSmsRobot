import logging
from typing import Optional, Dict, Any
import json
from datetime import datetime

class RedisKeys:
    """Class to store Redis key templates for various management use cases."""
    
    # Configure logging
    logging.basicConfig(level=logging.DEBUG)
    
    # User-Data keys
    user_data = 'user_data:{user_id}'                        # user_data:12345678

    user_profile_key = '{user_data}:profile:{profile_type}'  # user_data:12345678:profile:main
    user_image_key   = '{user_data}:image:{image_type}'      # user_data:12345678:image:main
    user_refund_key  = '{user_data}:refund:{refund_type}'    # user_data:12345678:refund:main
    user_history_key = '{user_data}:history:{history_type}'  # user_data:12345678:history:main
    user_payment_key = '{user_data}:payment:{payment_id}'    # user_data:12345678:payment:payment123

    # Order-Data keys
    order_info_key = 'order_data:info:{order_id}'     # order_data:info:order123:main
    order_current_key  = 'order_data:current:{order_type}'         # order_data:current:main


    admin_dashboard_key = 'admin:dashboard'                  # admin:dashboard
    admin_user_stats_key = 'admin:user_stats:{user_id}'      # admin:user_stats:12345678
    admin_order_stats_key = 'admin:order_stats:{order_id}'   # admin:order_stats:order123

    # Utility keys
    system_cache_key = 'system:cache:{cache_key}'            # system:cache:some_cache_key
    system_lock_key = 'system:lock:{lock_name}'              # system:lock:some_lock_name

    # Utility method to format keys
    @staticmethod
    def format_key(template: str, **kwargs) -> str:
        """Format a Redis key template with provided arguments."""
        try:
            formatted_key = template.format(**kwargs)
            logging.debug(f"Formatted key: {formatted_key}")
            return formatted_key
        except KeyError as e:
            logging.error(f"KeyError: Missing placeholder for {e.args[0]}")
            raise ValueError(f"Missing placeholder for {e.args[0]} in template: {template}")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            raise RuntimeError(f"Error formatting key: {template} with {kwargs}")









    # User-Data methods
    @staticmethod
    def user_profile(user_id: str, profile_type: str = 'main') -> str:
        """Generate the key for user profile."""
        return RedisKeys.format_key(
            RedisKeys.user_profile_key,
            user_data=RedisKeys.user_data.format(user_id=user_id),
            profile_type=profile_type
        )
    
    @staticmethod
    def user_image(user_id: str, image_type: str = 'main') -> str:
        """Generate the key for user profile."""
        return RedisKeys.format_key(
            RedisKeys.user_image_key,
            user_data=RedisKeys.user_data.format(user_id=user_id),
            image_type=image_type
        )


    # Admin methods
    @staticmethod
    def order_info(order_id: str) -> str:
        """Generate the key for order info."""
        return RedisKeys.format_key(
            RedisKeys.order_info_key,
            order_id=order_id
            )



    @staticmethod
    def transaction_lock_key(user_id: int, action: str) -> str:
        return f"transaction:{user_id}:{action}"


