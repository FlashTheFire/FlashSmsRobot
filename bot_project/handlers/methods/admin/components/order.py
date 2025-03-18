from typing import Dict, Any, Optional, List
import json
import asyncio
from datetime import datetime, timedelta
from redis.asyncio import Redis
from dataclasses import dataclass

from handlers.manager.operation import OrderManagement, UserManagement
from utils.redis_keys import RedisKeys
from utils.functions import format_currency
from .auth import require_admin

@dataclass
class OrderStats:
    total: int
    completed: int
    pending: int
    failed: int
    success_rate: float
    avg_response_time: float
    total_revenue: float

class OrderManager:
    """Handle order management and analytics."""
    
    def __init__(self, redis_client: Redis,
                 order_mgr: OrderManagement,
                 user_mgr: UserManagement):
        self.redis = redis_client
        self.order_mgr = order_mgr
        self.user_mgr = user_mgr
        self.redis_keys = RedisKeys()
        self.order_prefix = "admin:order:"
        self.stats_prefix = "admin:order:stats:"
    
    @require_admin(["super_admin", "manager", "support"])
    async def get_order_details(self, order_id: str) -> Dict[str, Any]:
        """Get detailed order information."""
        try:
            # Get order data
            order_data = await self.order_mgr.get_order_info(order_id)
            if not order_data:
                raise ValueError("Order not found")
            
            # Get user data
            user_id = order_data.get("user_id")
            user_data = await self.user_mgr.get_user_info(user_id) if user_id else None
            
            # Combine data
            order_details = {
                "order": order_data,
                "user": user_data,
                "timeline": await self._get_order_timeline(order_id),
                "related_orders": await self._get_related_orders(order_id, user_id)
            }
            
            return {"success": True, "data": order_details}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    @require_admin(["super_admin", "manager", "support"])
    async def search_orders(self, filters: Optional[Dict] = None,
                          sort_by: str = "created_at",
                          sort_asc: bool = False,
                          page: int = 1,
                          limit: int = 20) -> Dict[str, Any]:
        """Search orders with advanced filtering."""
        try:
            # Apply filters
            search_results = await self.order_mgr.search_orders_advanced(
                filters=filters,
                sort_field=sort_by,
                sort_ascending=sort_asc,
                page=page,
                page_size=limit
            )
            
            # Enhance results with user data
            enhanced_orders = []
            for order in search_results.get("results", []):
                user_id = order.get("user_id")
                user_data = await self.user_mgr.get_user_info(user_id) if user_id else None
                enhanced_orders.append({
                    "order": order,
                    "user": user_data
                })
            
            return {
                "success": True,
                "data": enhanced_orders,
                "total": search_results.get("total", 0),
                "page": page,
                "total_pages": (search_results.get("total", 0) + limit - 1) // limit
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    @require_admin(["super_admin", "manager"])
    async def update_order_status(self, order_id: str,
                                new_status: str,
                                reason: Optional[str] = None) -> Dict[str, Any]:
        """Update order status with audit trail."""
        try:
            # Get current order data
            order_data = await self.order_mgr.get_order_info(order_id)
            if not order_data:
                raise ValueError("Order not found")
            
            # Validate status transition
            current_status = order_data.get("status")
            if not self._is_valid_status_transition(current_status, new_status):
                raise ValueError(f"Invalid status transition: {current_status} -> {new_status}")
            
            # Update status
            update_data = {
                "status": new_status,
                "status_updated_at": datetime.now().isoformat()
            }
            if reason:
                update_data["status_reason"] = reason
            
            success = await self.order_mgr.update_order_info(order_id, update_data)
            if not success:
                raise ValueError("Failed to update order status")
            
            # Add to timeline
            await self._add_timeline_event(order_id, "STATUS_CHANGE", {
                "from_status": current_status,
                "to_status": new_status,
                "reason": reason
            })
            
            return {"success": True, "order_id": order_id}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    @require_admin(["super_admin", "manager"])
    async def process_refund(self, order_id: str,
                           refund_amount: float,
                           reason: str) -> Dict[str, Any]:
        """Process order refund."""
        try:
            # Get order data
            order_data = await self.order_mgr.get_order_info(order_id)
            if not order_data:
                raise ValueError("Order not found")
            
            # Validate refund amount
            original_amount = float(order_data.get("amount", 0))
            if refund_amount > original_amount:
                raise ValueError("Refund amount cannot exceed original order amount")
            
            # Process refund
            refund_data = {
                "amount": refund_amount,
                "reason": reason,
                "processed_at": datetime.now().isoformat()
            }
            
            # Update order
            update_data = {
                "refund": refund_data,
                "status": "REFUNDED",
                "refunded_at": datetime.now().isoformat()
            }
            
            success = await self.order_mgr.update_order_info(order_id, update_data)
            if not success:
                raise ValueError("Failed to process refund")
            
            # Add to timeline
            await self._add_timeline_event(order_id, "REFUND", refund_data)
            
            # Update user balance if needed
            user_id = order_data.get("user_id")
            if user_id:
                await self.user_mgr.update_user_balance(user_id, refund_amount)
            
            return {"success": True, "order_id": order_id}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    @require_admin(["super_admin", "manager", "support"])
    async def get_order_stats(self, timeframe: str = "daily") -> Dict[str, Any]:
        """Get order statistics for different timeframes."""
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
            
            # Get orders
            filters = {
                "created_at": (start_time.timestamp(), now.timestamp())
            }
            orders = await self.order_mgr.search_orders_advanced(filters=filters)
            
            if not orders.get("results"):
                return self._get_default_stats()
            
            order_list = orders.get("results", [])
            
            # Calculate statistics
            completed = sum(1 for o in order_list if o.get("status") == "COMPLETED")
            pending = sum(1 for o in order_list if o.get("status") == "PENDING")
            failed = sum(1 for o in order_list if o.get("status") in ["FAILED", "CANCELLED"])
            
            stats = OrderStats(
                total=len(order_list),
                completed=completed,
                pending=pending,
                failed=failed,
                success_rate=(completed / max(len(order_list), 1)) * 100,
                avg_response_time=sum(float(o.get("response_time", 0)) for o in order_list) / max(len(order_list), 1),
                total_revenue=sum(float(o.get("amount", 0)) for o in order_list if o.get("status") == "COMPLETED")
            )
            
            result = {
                "timeframe": timeframe,
                "stats": stats.__dict__,
                "calculated_at": now.isoformat()
            }
            
            # Cache for 5 minutes
            await self.redis.setex(cache_key, 300, json.dumps(result))
            
            return result
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def _get_order_timeline(self, order_id: str) -> List[Dict[str, Any]]:
        """Get order timeline events."""
        try:
            timeline_key = f"{self.order_prefix}timeline:{order_id}"
            events = await self.redis.lrange(timeline_key, 0, -1)
            
            return [json.loads(event) for event in events]
            
        except Exception:
            return []
    
    async def _add_timeline_event(self, order_id: str,
                               event_type: str,
                               event_data: Dict[str, Any]) -> None:
        """Add event to order timeline."""
        try:
            timeline_key = f"{self.order_prefix}timeline:{order_id}"
            
            event = {
                "type": event_type,
                "timestamp": datetime.now().isoformat(),
                "data": event_data
            }
            
            await self.redis.rpush(timeline_key, json.dumps(event))
            
        except Exception:
            pass  # Fail silently as this is not critical
    
    async def _get_related_orders(self, order_id: str,
                               user_id: Optional[str]) -> List[Dict[str, Any]]:
        """Get related orders from the same user."""
        try:
            if not user_id:
                return []
            
            filters = {
                "user_id": user_id,
                "order_id": {"$ne": order_id}  # Exclude current order
            }
            
            related = await self.order_mgr.search_orders_advanced(
                filters=filters,
                sort_field="created_at",
                sort_ascending=False,
                page=1,
                page_size=5
            )
            
            return related.get("results", [])
            
        except Exception:
            return []
    
    def _is_valid_status_transition(self, current_status: str,
                                 new_status: str) -> bool:
        """Check if status transition is valid."""
        valid_transitions = {
            "PENDING": ["COMPLETED", "FAILED", "CANCELLED"],
            "COMPLETED": ["REFUNDED"],
            "FAILED": ["PENDING", "CANCELLED"],
            "CANCELLED": ["PENDING"],
            "REFUNDED": []  # No further transitions allowed
        }
        
        return new_status in valid_transitions.get(current_status, [])
    
    def _get_default_stats(self) -> Dict[str, Any]:
        """Return default statistics when calculation fails."""
        stats = OrderStats(
            total=0,
            completed=0,
            pending=0,
            failed=0,
            success_rate=0.0,
            avg_response_time=0.0,
            total_revenue=0.0
        )
        
        return {
            "timeframe": "unknown",
            "stats": stats.__dict__,
            "calculated_at": datetime.now().isoformat()
        }
