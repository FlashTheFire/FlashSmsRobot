import logging
import traceback
from typing import Optional, Callable, Dict, Any
from functools import wraps
from datetime import datetime
import json

class ErrorHandler:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self._error_counts: Dict[str, int] = {}
        self._last_errors: Dict[str, datetime] = {}
        
    def handle_error(
        self,
        error: Exception,
        context: Optional[dict] = None
    ):
        error_type = type(error).__name__
        error_details = {
            'error_type': error_type,
            'error_message': str(error),
            'traceback': traceback.format_exc(),
            'context': context or {},
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Update error statistics
        self._error_counts[error_type] = self._error_counts.get(error_type, 0) + 1
        self._last_errors[error_type] = datetime.utcnow()
        
        # Log the error with details
        error_msg = f"Error occurred: {error_type} - {str(error)}"
        if context:
            error_msg += f" | Context: {json.dumps(context)}"
        
        self.logger.error(error_msg)
        self.logger.debug(f"Full error details: {json.dumps(error_details, indent=2)}")
            
    def get_error_stats(self) -> Dict[str, Any]:
        """Get statistics about errors that have occurred"""
        return {
            'counts': self._error_counts,
            'last_occurrences': {
                k: v.isoformat() 
                for k, v in self._last_errors.items()
            }
        }
            
    def async_error_handler(self, func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                self.handle_error(e, {
                    'function': func.__name__,
                    'args': [str(arg) for arg in args],
                    'kwargs': {k: str(v) for k, v in kwargs.items()}
                })
                raise
        return wrapper
        
    def format_error_message(self, error: Exception) -> str:
        """Format error message for user display"""
        error_type = type(error).__name__
        if error_type == 'ValidationError':
            return f"Invalid input: {str(error)}"
        elif error_type == 'AuthenticationError':
            return "Authentication failed. Please try again."
        elif error_type == 'RateLimitError':
            return "Too many requests. Please try again later."
        else:
            return "An unexpected error occurred. Please try again later."