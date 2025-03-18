# utils/exceptions.py
class PaymentGatewayError(Exception):
    """Custom exception for payment gateway failures"""
    def __init__(self, message="Payment gateway error", retryable=False):
        self.message = message
        self.retryable = retryable
        super().__init__(self.message)

class RetryableError(Exception):
    """Exception for temporary failures that can be retried"""
    def __init__(self, message="Temporary service disruption"):
        self.message = message
        super().__init__(self.message)