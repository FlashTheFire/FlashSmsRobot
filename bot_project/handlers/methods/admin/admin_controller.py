from typing import Dict, Any, Optional
import json
from datetime import datetime
from redis.asyncio import Redis

from handlers.manager.operation import OrderManagement, UserManagement, DepositManagement
from .components.dashboard import DashboardManager
from .components.service import ServiceManager
from .components.order import OrderManager
from .components.user import UserManager
from .components.analytics import AnalyticsManager

class AdminController:
    """Main controller for the admin panel interface."""
    
    def __init__(self, redis_client: Redis,
                 order_mgr: OrderManagement,
                 user_mgr: UserManagement,
                 deposit_mgr: DepositManagement):
        self.redis = redis_client
        self.order_mgr = order_mgr
        self.user_mgr = user_mgr
        self.deposit_mgr = deposit_mgr
        
        # Initialize components
        self.dashboard = DashboardManager(redis_client, order_mgr, user_mgr, deposit_mgr)
        self.service = ServiceManager(redis_client, order_mgr)
        self.order = OrderManager(redis_client, order_mgr, user_mgr)
        self.user = UserManager(redis_client, user_mgr, order_mgr)
        self.analytics = AnalyticsManager(redis_client, order_mgr, user_mgr, deposit_mgr)
        
        # Admin settings
        self.settings_prefix = "admin:settings:"
    
    async def authenticate(self, admin_id: str,
                        password: str,
                        ip_address: str) -> Dict[str, Any]:
        """Authenticate admin user."""
        try:
            # Get admin settings
            admin_settings = await self._get_admin_settings(admin_id)
            if not admin_settings:
                raise ValueError("Admin not found")
            
            # Validate password (in production, use proper password hashing)
            if password != admin_settings.get("password"):
                raise ValueError("Invalid credentials")
            
            return {
                "success": True,
                "admin": {
                    "id": admin_id,
                    "role": admin_settings.get("role"),
                    "name": admin_settings.get("name"),
                    "permissions": admin_settings.get("permissions", [])
                }
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def get_dashboard_data(self, admin_id: str) -> Dict[str, Any]:
        """Get comprehensive dashboard data."""
        try:
            # Get admin settings for customization
            admin_settings = await self._get_admin_settings(admin_id)
            dashboard_settings = admin_settings.get("dashboard_settings", {})
            
            # Get data based on preferences
            tasks = [
                self.dashboard.get_dashboard_stats(),
                self._get_favorite_metrics(admin_id),
                self._get_recent_activity(admin_id)
            ]
            
            if dashboard_settings.get("show_charts", True):
                tasks.extend([
                    self.dashboard.get_chart_data("revenue"),
                    self.dashboard.get_chart_data("orders"),
                    self.dashboard.get_chart_data("users")
                ])
            
            results = await asyncio.gather(*tasks)
            
            # Combine all data
            dashboard_data = {
                "stats": results[0],
                "favorites": results[1],
                "recent_activity": results[2],
                "charts": results[3:] if dashboard_settings.get("show_charts", True) else []
            }
            
            return {"success": True, "data": dashboard_data}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def manage_services(self, action: str,
                           data: Dict[str, Any]) -> Dict[str, Any]:
        """Manage SMS services."""
        try:
            if action == "add":
                return await self.service.add_service(data)
            elif action == "update":
                return await self.service.update_service(data["service_id"], data)
            elif action == "remove":
                return await self.service.remove_service(data["service_id"])
            elif action == "get":
                return await self.service.get_service(data["service_id"])
            elif action == "list":
                return await self.service.list_services(
                    filters=data.get("filters"),
                    sort_by=data.get("sort_by", "created_at"),
                    sort_asc=data.get("sort_asc", False),
                    page=data.get("page", 1),
                    limit=data.get("limit", 20)
                )
            else:
                raise ValueError(f"Invalid action: {action}")
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def manage_orders(self, action: str,
                         data: Dict[str, Any]) -> Dict[str, Any]:
        """Manage orders."""
        try:
            if action == "get":
                return await self.order.get_order_details(data["order_id"])
            elif action == "search":
                return await self.order.search_orders(
                    filters=data.get("filters"),
                    sort_by=data.get("sort_by", "created_at"),
                    sort_asc=data.get("sort_asc", False),
                    page=data.get("page", 1),
                    limit=data.get("limit", 20)
                )
            elif action == "update_status":
                return await self.order.update_order_status(
                    order_id=data["order_id"],
                    new_status=data["status"],
                    reason=data.get("reason")
                )
            elif action == "refund":
                return await self.order.process_refund(
                    order_id=data["order_id"],
                    refund_amount=data["amount"],
                    reason=data["reason"]
                )
            else:
                raise ValueError(f"Invalid action: {action}")
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def manage_users(self, action: str,
                        data: Dict[str, Any]) -> Dict[str, Any]:
        """Manage users."""
        try:
            if action == "get":
                return await self.user.get_user_details(data["user_id"])
            elif action == "search":
                return await self.user.search_users(
                    filters=data.get("filters"),
                    sort_by=data.get("sort_by", "registration_date"),
                    sort_asc=data.get("sort_asc", False),
                    page=data.get("page", 1),
                    limit=data.get("limit", 20)
                )
            elif action == "update_status":
                return await self.user.update_user_status(
                    user_id=data["user_id"],
                    new_status=data["status"],
                    reason=data.get("reason")
                )
            elif action == "adjust_balance":
                return await self.user.adjust_user_balance(
                    user_id=data["user_id"],
                    amount=data["amount"],
                    reason=data["reason"]
                )
            elif action == "manage_forum":
                return await self.user.manage_forum_topic(
                    user_id=data["user_id"],
                    action=data["forum_action"],
                    topic_id=data.get("topic_id"),
                    reason=data.get("reason")
                )
            else:
                raise ValueError(f"Invalid action: {action}")
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def get_analytics(self, report_type: str,
                         params: Dict[str, Any]) -> Dict[str, Any]:
        """Get analytics and reports."""
        try:
            if report_type == "report":
                return await self.analytics.generate_report(
                    report_type=params["type"],
                    start_date=params["start_date"],
                    end_date=params["end_date"],
                    format=params.get("format", "json")
                )
            elif report_type == "real_time":
                return await self.analytics.get_real_time_metrics()
            elif report_type == "trend":
                return await self.analytics.get_trend_analysis(
                    metric=params["metric"],
                    timeframe=params.get("timeframe", "daily"),
                    days=params.get("days", 30)
                )
            else:
                raise ValueError(f"Invalid report type: {report_type}")
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def update_admin_settings(self, admin_id: str,
                                settings: Dict[str, Any]) -> Dict[str, Any]:
        """Update admin user settings."""
        try:
            # Get current settings
            current_settings = await self._get_admin_settings(admin_id)
            if not current_settings:
                raise ValueError("Admin not found")
            
            # Update settings
            current_settings.update(settings)
            current_settings["updated_at"] = datetime.now().isoformat()
            
            # Save settings
            settings_key = f"{self.settings_prefix}{admin_id}"
            await self.redis.set(settings_key, json.dumps(current_settings))
            
            return {"success": True, "admin_id": admin_id}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def _get_admin_settings(self, admin_id: str) -> Optional[Dict[str, Any]]:
        """Get admin settings from Redis."""
        try:
            settings_key = f"{self.settings_prefix}{admin_id}"
            settings_data = await self.redis.get(settings_key)
            
            return json.loads(settings_data) if settings_data else None
            
        except Exception:
            return None
    
    async def _get_favorite_metrics(self, admin_id: str) -> List[Dict[str, Any]]:
        """Get admin's favorite metrics."""
        try:
            admin_settings = await self._get_admin_settings(admin_id)
            favorite_metrics = admin_settings.get("favorite_metrics", [])
            
            metrics = []
            for metric in favorite_metrics:
                if metric["type"] == "order":
                    data = await self.order.get_order_stats(metric.get("timeframe", "daily"))
                elif metric["type"] == "user":
                    data = await self.user.get_user_stats(metric.get("timeframe", "daily"))
                else:
                    continue
                
                metrics.append({
                    "name": metric["name"],
                    "type": metric["type"],
                    "data": data
                })
            
            return metrics
            
        except Exception:
            return []
    
    async def _get_recent_activity(self, admin_id: str) -> List[Dict[str, Any]]:
        """Get recent admin activity."""
        try:
            # Get activity logs
            activity_key = f"{self.settings_prefix}{admin_id}:activity"
            activities = await self.redis.lrange(activity_key, 0, 9)  # Last 10 activities
            
            return [json.loads(activity) for activity in activities]
            
        except Exception:
            return []
