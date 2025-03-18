from typing import Dict, Any, Optional, List
import json
import asyncio
from datetime import datetime, timedelta
from redis.asyncio import Redis
from dataclasses import dataclass

from handlers.manager.operation import UserManagement, OrderManagement
from utils.redis_keys import RedisKeys
from utils.functions import format_currency
from .auth import require_admin

@dataclass
class UserActivity:
    last_login: str
    last_order: str
    total_orders: int
    total_spent: float
    average_order_value: float
    success_rate: float

class UserManager:
    """Handle user management and analytics."""
    
    def __init__(self, redis_client: Redis,
                 user_mgr: UserManagement,
                 order_mgr: OrderManagement):
        self.redis = redis_client
        self.user_mgr = user_mgr
        self.order_mgr = order_mgr
        self.redis_keys = RedisKeys()
        self.user_prefix = "admin:user:"
        self.stats_prefix = "admin:user:stats:"
        self.activity_prefix = "admin:user:activity:"
    
    @require_admin(["super_admin", "manager", "support"])
    async def get_user_details(self, user_id: str) -> Dict[str, Any]:
        """Get detailed user information."""
        try:
            # Get user data
            user_data = await self.user_mgr.get_user_info(user_id)
            if not user_data:
                raise ValueError("User not found")
            
            # Get activity data
            activity = await self._get_user_activity(user_id)
            
            # Get recent orders
            recent_orders = await self._get_recent_orders(user_id)
            
            # Combine data
            user_details = {
                "user": user_data,
                "activity": activity.__dict__ if activity else None,
                "recent_orders": recent_orders,
                "forum_topics": await self._get_forum_topics(user_id)
            }
            
            return {"success": True, "data": user_details}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    @require_admin(["super_admin", "manager", "support"])
    async def search_users(self, filters: Optional[Dict] = None,
                         sort_by: str = "registration_date",
                         sort_asc: bool = False,
                         page: int = 1,
                         limit: int = 20) -> Dict[str, Any]:
        """Search users with advanced filtering."""
        try:
            # Apply filters
            search_results = await self.user_mgr.search_users(
                filters=filters,
                sort_field=sort_by,
                sort_ascending=sort_asc,
                page=page,
                page_size=limit
            )
            
            # Enhance results with activity data
            enhanced_users = []
            for user in search_results.get("results", []):
                activity = await self._get_user_activity(user.get("user_id"))
                enhanced_users.append({
                    "user": user,
                    "activity": activity.__dict__ if activity else None
                })
            
            return {
                "success": True,
                "data": enhanced_users,
                "total": search_results.get("total", 0),
                "page": page,
                "total_pages": (search_results.get("total", 0) + limit - 1) // limit
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    @require_admin(["super_admin", "manager"])
    async def update_user_status(self, user_id: str,
                              new_status: str,
                              reason: Optional[str] = None) -> Dict[str, Any]:
        """Update user status with audit trail."""
        try:
            # Get current user data
            user_data = await self.user_mgr.get_user_info(user_id)
            if not user_data:
                raise ValueError("User not found")
            
            # Validate status transition
            current_status = user_data.get("status")
            if not self._is_valid_status_transition(current_status, new_status):
                raise ValueError(f"Invalid status transition: {current_status} -> {new_status}")
            
            # Update status
            update_data = {
                "status": new_status,
                "status_updated_at": datetime.now().isoformat()
            }
            if reason:
                update_data["status_reason"] = reason
            
            success = await self.user_mgr.update_user_info(user_id, update_data)
            if not success:
                raise ValueError("Failed to update user status")
            
            # Log status change
            await self._log_user_action(user_id, "STATUS_CHANGE", {
                "from_status": current_status,
                "to_status": new_status,
                "reason": reason
            })
            
            return {"success": True, "user_id": user_id}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    @require_admin(["super_admin", "manager"])
    async def adjust_user_balance(self, user_id: str,
                               amount: float,
                               reason: str) -> Dict[str, Any]:
        """Adjust user balance with audit trail."""
        try:
            # Get current user data
            user_data = await self.user_mgr.get_user_info(user_id)
            if not user_data:
                raise ValueError("User not found")
            
            # Update balance
            current_balance = float(user_data.get("balance", 0))
            new_balance = current_balance + amount
            
            if new_balance < 0:
                raise ValueError("Balance adjustment would result in negative balance")
            
            update_data = {
                "balance": new_balance,
                "balance_updated_at": datetime.now().isoformat()
            }
            
            success = await self.user_mgr.update_user_info(user_id, update_data)
            if not success:
                raise ValueError("Failed to adjust balance")
            
            # Log balance adjustment
            await self._log_user_action(user_id, "BALANCE_ADJUSTMENT", {
                "amount": amount,
                "previous_balance": current_balance,
                "new_balance": new_balance,
                "reason": reason
            })
            
            return {"success": True, "user_id": user_id}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    @require_admin(["super_admin", "manager"])
    async def manage_forum_topic(self, user_id: str,
                              action: str,
                              topic_id: Optional[str] = None,
                              reason: Optional[str] = None) -> Dict[str, Any]:
        """Manage user's forum topics."""
        try:
            # Get user data
            user_data = await self.user_mgr.get_user_info(user_id)
            if not user_data:
                raise ValueError("User not found")
            
            if action == "archive":
                if not topic_id:
                    raise ValueError("Topic ID required for archive action")
                
                # Archive topic
                success = await self.user_mgr.archive_forum_topic(user_id, topic_id)
                if not success:
                    raise ValueError("Failed to archive forum topic")
                
                action_data = {"topic_id": topic_id, "reason": reason}
                
            elif action == "create":
                # Create new topic
                topic = await self.user_mgr.create_forum_topic(user_id)
                if not topic:
                    raise ValueError("Failed to create forum topic")
                
                action_data = {"topic_id": topic.get("topic_id")}
                
            else:
                raise ValueError(f"Invalid action: {action}")
            
            # Log action
            await self._log_user_action(user_id, f"FORUM_TOPIC_{action.upper()}", action_data)
            
            return {"success": True, "user_id": user_id, "action": action}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    @require_admin(["super_admin", "manager", "support"])
    async def get_user_stats(self, timeframe: str = "daily") -> Dict[str, Any]:
        """Get user statistics for different timeframes."""
        try:
            # Try to get cached stats
            cache_key = f"{self.stats_prefix}{timeframe}"
            cached_stats = await self.redis.get(cache_key)
            
            if cached_stats:
                return json.loads(cached_stats)
            
            # Calculate time range
            now = datetime.now()
            if timeframe == "daily":
                start_time = now - timedelta(days=1)
            elif timeframe == "weekly":
                start_time = now - timedelta(weeks=1)
            elif timeframe == "monthly":
                start_time = now - timedelta(days=30)
            else:
                raise ValueError("Invalid timeframe")
            
            # Get user data
            filters = {
                "registration_date": (start_time.timestamp(), now.timestamp())
            }
            users = await self.user_mgr.search_users(filters=filters)
            
            if not users.get("results"):
                return self._get_default_stats()
            
            user_list = users.get("results", [])
            
            # Calculate statistics
            stats = {
                "total_users": len(user_list),
                "active_users": sum(1 for u in user_list if u.get("status") == "ACTIVE"),
                "average_balance": sum(float(u.get("balance", 0)) for u in user_list) / max(len(user_list), 1),
                "new_registrations": len(user_list),
                "with_orders": sum(1 for u in user_list if float(u.get("total_orders", 0)) > 0)
            }
            
            result = {
                "timeframe": timeframe,
                "stats": stats,
                "calculated_at": now.isoformat()
            }
            
            # Cache for 5 minutes
            await self.redis.setex(cache_key, 300, json.dumps(result))
            
            return result
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def _get_user_activity(self, user_id: str) -> Optional[UserActivity]:
        """Get user activity data."""
        try:
            # Try to get cached activity
            cache_key = f"{self.activity_prefix}{user_id}"
            cached_activity = await self.redis.get(cache_key)
            
            if cached_activity:
                activity_dict = json.loads(cached_activity)
                return UserActivity(**activity_dict)
            
            # Get order data
            filters = {"user_id": user_id}
            orders = await self.order_mgr.search_orders_advanced(filters=filters)
            
            if not orders.get("results"):
                return None
            
            order_list = orders.get("results", [])
            
            # Calculate activity metrics
            total_orders = len(order_list)
            total_spent = sum(float(o.get("amount", 0)) for o in order_list)
            completed_orders = sum(1 for o in order_list if o.get("status") == "COMPLETED")
            
            activity = UserActivity(
                last_login=max((o.get("created_at") for o in order_list), default=""),
                last_order=max((o.get("created_at") for o in order_list), default=""),
                total_orders=total_orders,
                total_spent=total_spent,
                average_order_value=total_spent / max(total_orders, 1),
                success_rate=(completed_orders / max(total_orders, 1)) * 100
            )
            
            # Cache for 5 minutes
            await self.redis.setex(cache_key, 300, json.dumps(activity.__dict__))
            
            return activity
            
        except Exception:
            return None
    
    async def _get_recent_orders(self, user_id: str,
                              limit: int = 5) -> List[Dict[str, Any]]:
        """Get user's recent orders."""
        try:
            filters = {
                "user_id": user_id
            }
            
            orders = await self.order_mgr.search_orders_advanced(
                filters=filters,
                sort_field="created_at",
                sort_ascending=False,
                page=1,
                page_size=limit
            )
            
            return orders.get("results", [])
            
        except Exception:
            return []
    
    async def _get_forum_topics(self, user_id: str) -> List[Dict[str, Any]]:
        """Get user's forum topics."""
        try:
            return await self.user_mgr.get_forum_topics(user_id)
        except Exception:
            return []
    
    async def _log_user_action(self, user_id: str,
                            action_type: str,
                            action_data: Dict[str, Any]) -> None:
        """Log user action for audit."""
        try:
            action = {
                "type": action_type,
                "timestamp": datetime.now().isoformat(),
                "data": action_data
            }
            
            log_key = f"{self.user_prefix}log:{user_id}"
            await self.redis.rpush(log_key, json.dumps(action))
            
        except Exception:
            pass  # Fail silently as this is not critical
    
    def _is_valid_status_transition(self, current_status: str,
                                 new_status: str) -> bool:
        """Check if status transition is valid."""
        valid_transitions = {
            "ACTIVE": ["SUSPENDED", "BANNED"],
            "SUSPENDED": ["ACTIVE", "BANNED"],
            "BANNED": ["ACTIVE"],
            "PENDING": ["ACTIVE", "BANNED"]
        }
        
        return new_status in valid_transitions.get(current_status, [])
    
    def _get_default_stats(self) -> Dict[str, Any]:
        """Return default statistics when calculation fails."""
        return {
            "timeframe": "unknown",
            "stats": {
                "total_users": 0,
                "active_users": 0,
                "average_balance": 0.0,
                "new_registrations": 0,
                "with_orders": 0
            },
            "calculated_at": datetime.now().isoformat()
        }
