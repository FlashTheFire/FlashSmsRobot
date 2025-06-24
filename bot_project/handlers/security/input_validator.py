import re
import html
from typing import Union, List, Dict, Any
import logging
from datetime import datetime

class InputValidator:
    """Input validator with security checks."""
    
    @staticmethod
    def sanitize_text(text: str, max_length: int = 50) -> str:
        """Sanitize text input to prevent injection attacks."""
        if not text:
            return ""
        # Remove any HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        # Escape HTML special characters
        text = html.escape(text)
        # Remove control characters
        text = ''.join(char for char in text if ord(char) >= 32)
        # Limit length
        return text[:max_length].strip()

    @staticmethod
    def validate_user_id(user_id: Union[str, int]) -> bool:
        """Validate user ID format."""
        try:
            str_id = str(user_id)
            return bool(str_id.isdigit() and 5 <= len(str_id) <= 20)
        except:
            return False

    @staticmethod
    def validate_amount(amount: Union[str, float, int], min_val: float = 0.0, max_val: float = 1000000.0) -> bool:
        """Validate numeric amount."""
        try:
            float_amount = float(amount)
            return min_val <= float_amount <= max_val
        except:
            return False

    @staticmethod
    def validate_rank(rank: str, valid_ranks: List[str] = None) -> bool:
        """Validate user rank."""
        if valid_ranks is None:
            valid_ranks = ['bronze', 'silver', 'gold', 'platinum', 'diamond']
        return str(rank).lower() in valid_ranks

    @staticmethod
    def validate_status(status: str, valid_statuses: List[str] = None) -> bool:
        """Validate user status."""
        if valid_statuses is None:
            valid_statuses = ['active', 'banned', 'suspended', 'inactive']
        return str(status).lower() in valid_statuses

    @staticmethod
    def validate_currency(currency: str, valid_currencies: List[str] = None) -> bool:
        """Validate currency code."""
        if valid_currencies is None:
            valid_currencies = ['INR', 'USD', 'EUR', 'GBP']
        return str(currency).upper() in valid_currencies

    @staticmethod
    def validate_callback_data(data: str, pattern: str = None) -> bool:
        """Validate callback query data format."""
        if not data:
            return False
        if pattern:
            return bool(re.match(pattern, data))
        return True

    @classmethod
    def validate_user_data(cls, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and sanitize user data dictionary."""
        try:
            if not isinstance(user_data, dict):
                return {'valid': False, 'error': 'Invalid data format'}

            required_fields = [
                'first_name', 'username', 'currency_code', 'user_status', 'language_code', 'user_id', 'created_at', 'last_updated'
            ]
            
            missing_fields = [field for field in required_fields if field not in user_data]
            if missing_fields:
                return {'valid': False, 'error': f'Missing required fields: {", ".join(missing_fields)}'}

            sanitized_data = {}
            
            sanitized_data['first_name'] = cls.sanitize_text(user_data.get('first_name', ''), 50)
            sanitized_data['username'] = cls.sanitize_text(user_data.get('username', ''), 50)
            sanitized_data['language_code'] = cls.sanitize_text(user_data.get('language_code', ''), 10)

            if not cls.validate_user_id(user_data.get('user_id')):
                return {'valid': False, 'error': 'Invalid user ID'}
            sanitized_data['user_id'] = str(user_data['user_id'])

            if not cls.validate_status(user_data.get('user_status')):
                return {'valid': False, 'error': 'Invalid status'}
            sanitized_data['user_status'] = str(user_data['user_status']).lower()
            sanitized_data['currency_code'] = str(user_data['currency_code']).upper()

            try:
                sanitized_data['created_at'] = datetime.fromisoformat(user_data['created_at']).isoformat()
            except ValueError:
                return {'valid': False, 'error': 'Invalid created_at timestamp'}

            sanitized_data['last_updated'] = float(user_data['last_updated'])

            return {'valid': True, 'data': sanitized_data}

        except Exception as e:
            logging.error(f"Error validating user data: {e}")
            return {'valid': False, 'error': str(e)}