import asyncio
from typing import Dict, Set, Optional
from enum import Enum
from collections import defaultdict
import logging
from datetime import datetime, timedelta

class OperationType(Enum):
    DEPOSIT = "deposit"
    WITHDRAWAL = "withdrawal"
    PROFILE_UPDATE = "profile_update"
    SETTINGS_UPDATE = "settings_update"
    BALANCE_UPDATE = "balance_update"
    ASYNC_CHECK = "async_check"
    
class OperationLockManager:
    def __init__(self):
        self.operation_locks: Dict[OperationType, Dict[str, asyncio.Lock]] = defaultdict(lambda: defaultdict(asyncio.Lock))
        self.operation_queues: Dict[OperationType, Dict[str, asyncio.Queue]] = defaultdict(lambda: defaultdict(asyncio.Queue))
        self.active_operations: Dict[OperationType, Dict[str, asyncio.Task]] = defaultdict(dict)
        
    async def acquire_lock(self, operation_type: OperationType, user_id: str) -> bool:
        """
        Acquire a lock for a specific operation type and user.
        Returns True if lock was acquired, False if operation is already in progress.
        """
        # Cancel any existing operation of the same type for this user
        await self.cancel_operation(operation_type, user_id)
        
        lock = self.operation_locks[operation_type][user_id]
        await lock.acquire()
        return True
        
    def release_lock(self, operation_type: OperationType, user_id: str):
        """Release the lock for a specific operation type and user."""
        if user_id in self.operation_locks[operation_type]:
            self.operation_locks[operation_type][user_id].release()
            
    async def cancel_operation(self, operation_type: OperationType, user_id: str):
        """Cancel an active operation for a specific type and user."""
        if user_id in self.active_operations[operation_type]:
            task = self.active_operations[operation_type][user_id]
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            del self.active_operations[operation_type][user_id]
            
    async def start_operation(self, operation_type: OperationType, user_id: str, coro) -> asyncio.Task:
        """Start and track a new operation."""
        task = asyncio.create_task(coro)
        self.active_operations[operation_type][user_id] = task
        
        def cleanup_callback(task):
            if user_id in self.active_operations[operation_type]:
                del self.active_operations[operation_type][user_id]
            if user_id in self.operation_locks[operation_type]:
                try:
                    self.release_lock(operation_type, user_id)
                except RuntimeError:
                    pass  # Lock might already be released
                    
        task.add_done_callback(cleanup_callback)
        return task

class AsyncOperationContext:
    """Context manager for operation locking."""
    def __init__(self, lock_manager: OperationLockManager, operation_type: OperationType, user_id: str):
        self.lock_manager = lock_manager
        self.operation_type = operation_type
        self.user_id = user_id
        
    async def __aenter__(self):
        await self.lock_manager.acquire_lock(self.operation_type, self.user_id)
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.lock_manager.release_lock(self.operation_type, self.user_id)

# Global instance
operation_lock_manager = OperationLockManager()
