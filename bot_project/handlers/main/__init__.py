# Initialize the main handlers package

from .show_menu import register_handlers as register_start
from .show_wallet import register_handlers as register_wallet
from .top_services import register_handlers as register_top_services
from .inline_query import register_handlers as register_inline
from .message_handler import handle_message, register_handlers as register_message

# List of all handler registration functions
register_functions = [
    register_top_services,
    register_message
]

__all__ = [
    'handle_recharge',
    'handle_message',
    'register_functions'
]