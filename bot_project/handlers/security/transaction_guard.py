from typing import Any, Dict, Optional, Callable, Awaitable
import logging
from datetime import datetime, timedelta
import json
import asyncio
import redis.asyncio as redis

logger = logging.getLogger(__name__)

class TransactionGuard:
    """Secure transaction handler for database operations."""

    def __init__(self, redis_client: redis.Redis, lock_timeout: int = 30):
        self.redis_client = redis_client
        self.lock_timeout = lock_timeout
        self.current_lock = None
        self._lock = asyncio.Lock()
        self.logger = logging.getLogger(__name__)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.current_lock:
            await self.release_lock(self.current_lock)
        return False

    async def acquire_lock(self, lock_key: str, timeout: int = None) -> bool:
        """Acquire a lock for a transaction."""
        try:
            async with self._lock:
                success = await self.redis_client.set(
                    f"lock:{lock_key}",
                    str(datetime.utcnow().isoformat()),
                    nx=True,
                    ex=timeout or self.lock_timeout
                )
                if success:
                    self.current_lock = lock_key
                    self.logger.debug(f"Lock acquired: {lock_key}")
                    return True
                self.logger.warning(f"Failed to acquire lock: {lock_key}")
                return False
        except Exception as e:
            self.logger.error(f"Failed to acquire lock: {e}")
            return False

    async def release_lock(self, lock_key: str) -> bool:
        """Release a transaction lock."""
        try:
            async with self._lock:
                if await self.redis_client.delete(f"lock:{lock_key}"):
                    if self.current_lock == lock_key:
                        self.current_lock = None
                    self.logger.debug(f"Lock released: {lock_key}")
                    return True
                self.logger.warning(f"Failed to release lock: {lock_key}")
                return False
        except Exception as e:
            self.logger.error(f"Failed to release lock: {e}")
            return False

    async def cleanup_expired_locks(self):
        """Cleanup expired locks."""
        try:
            pattern = "lock:*"
            async for key in self.redis_client.scan_iter(match=pattern):
                lock_time = await self.redis_client.get(key)
                if lock_time:
                    lock_datetime = datetime.fromisoformat(lock_time.decode())
                    if datetime.utcnow() - lock_datetime > timedelta(seconds=self.lock_timeout):
                        await self.redis_client.delete(key)
                        self.logger.info(f"Cleaned up expired lock: {key}")
        except Exception as e:
            self.logger.error(f"Error in cleanup_expired_locks: {e}")

    async def execute_transaction(
        self,
        operation: Callable[..., Awaitable[Any]],
        rollback: Optional[Callable[..., Awaitable[Any]]] = None,
        *args,
        **kwargs
    ) -> Dict[str, Any]:
        """Execute a transaction with rollback capability."""
        transaction_id = f"txn_{datetime.utcnow().timestamp()}"
        
        try:
            # Log transaction start
            await self._log_transaction(transaction_id, 'START', kwargs)
            
            # Execute the operation
            result = await operation(*args, **kwargs)
            
            # Log successful completion
            await self._log_transaction(transaction_id, 'COMPLETE', result)
            
            return {
                'success': True,
                'transaction_id': transaction_id,
                'result': result
            }
            
        except Exception as e:
            error_msg = f"Transaction error: {str(e)}"
            self.logger.error(error_msg)
            
            # Attempt rollback if provided
            if rollback:
                try:
                    await rollback(*args, **kwargs)
                    await self._log_transaction(transaction_id, 'ROLLBACK', {'error': str(e)})
                except Exception as rollback_error:
                    await self._log_transaction(
                        transaction_id,
                        'ROLLBACK_FAILED',
                        {'error': str(e), 'rollback_error': str(rollback_error)}
                    )
            
            return {
                'success': False,
                'transaction_id': transaction_id,
                'error': error_msg
            }

    async def _log_transaction(self, transaction_id: str, status: str, data: Any) -> None:
        """Log transaction details securely."""
        try:
            log_entry = {
                'timestamp': datetime.utcnow().isoformat(),
                'status': status,
                'data': json.dumps(data) if isinstance(data, dict) else str(data)
            }

            await self.redis_client.hset(
                f"transaction_log:{transaction_id}",
                mapping=log_entry
            )
            
            # Set expiry for log entries (30 days)
            await self.redis_client.expire(f"transaction_log:{transaction_id}", 2592000)
            
        except Exception as e:
            self.logger.error(f"Failed to log transaction: {e}")

