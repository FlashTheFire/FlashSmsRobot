from typing import Dict, Any, Optional, List, Tuple
import json
import asyncio
from datetime import datetime, timedelta
from redis.asyncio import Redis
from dataclasses import dataclass
import pandas as pd
import io

from handlers.manager.operation import OrderManagement, UserManagement, DepositManagement
from utils.redis_keys import RedisKeys
from utils.functions import format_currency
from .auth import require_admin

@dataclass
class AnalyticsReport:
    title: str
    description: str
    metrics: Dict[str, Any]
    charts: List[Dict[str, Any]]
    summary: str
    generated_at: str

class AnalyticsManager:
    """Handle analytics and reporting functionality."""
    
    def __init__(self, redis_client: Redis,
                 order_mgr: OrderManagement,
                 user_mgr: UserManagement,
                 deposit_mgr: DepositManagement):
        self.redis = redis_client
        self.order_mgr = order_mgr
        self.user_mgr = user_mgr
        self.deposit_mgr = deposit_mgr
        self.redis_keys = RedisKeys()
        self.analytics_prefix = "admin:analytics:"
        self.report_prefix = "admin:report:"
    
    @require_admin(["super_admin", "manager"])
    async def generate_report(self, report_type: str,
                           start_date: datetime,
                           end_date: datetime,
                           format: str = "json") -> Dict[str, Any]:
        """Generate analytics report."""
        try:
            # Validate dates
            if start_date >= end_date:
                raise ValueError("Start date must be before end date")
            
            # Generate report based on type
            if report_type == "financial":
                report = await self._generate_financial_report(start_date, end_date)
            elif report_type == "operational":
                report = await self._generate_operational_report(start_date, end_date)
            elif report_type == "user":
                report = await self._generate_user_report(start_date, end_date)
            else:
                raise ValueError(f"Invalid report type: {report_type}")
            
            # Format report
            if format == "json":
                return {"success": True, "data": report.__dict__}
            elif format == "csv":
                return {"success": True, "data": self._convert_to_csv(report)}
            else:
                raise ValueError(f"Invalid format: {format}")
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    @require_admin(["super_admin", "manager", "support"])
    async def get_real_time_metrics(self) -> Dict[str, Any]:
        """Get real-time system metrics."""
        try:
            # Try to get cached metrics
            cache_key = f"{self.analytics_prefix}real_time"
            cached_metrics = await self.redis.get(cache_key)
            
            if cached_metrics:
                return json.loads(cached_metrics)
            
            # Calculate metrics
            now = datetime.now()
            hour_ago = now - timedelta(hours=1)
            
            metrics = {
                "orders": await self._get_hourly_order_metrics(hour_ago, now),
                "users": await self._get_hourly_user_metrics(hour_ago, now),
                "system": await self._get_system_metrics(),
                "calculated_at": now.isoformat()
            }
            
            # Cache for 1 minute
            await self.redis.setex(cache_key, 60, json.dumps(metrics))
            
            return {"success": True, "data": metrics}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    @require_admin(["super_admin", "manager"])
    async def get_trend_analysis(self, metric: str,
                              timeframe: str = "daily",
                              days: int = 30) -> Dict[str, Any]:
        """Get trend analysis for specific metrics."""
        try:
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Get trend data
            if metric == "revenue":
                trend_data = await self._analyze_revenue_trend(start_date, end_date, timeframe)
            elif metric == "orders":
                trend_data = await self._analyze_order_trend(start_date, end_date, timeframe)
            elif metric == "users":
                trend_data = await self._analyze_user_trend(start_date, end_date, timeframe)
            else:
                raise ValueError(f"Invalid metric: {metric}")
            
            return {"success": True, "data": trend_data}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def _generate_financial_report(self, start_date: datetime,
                                     end_date: datetime) -> AnalyticsReport:
        """Generate financial analytics report."""
        try:
            # Get financial data
            filters = {
                "created_at": (start_date.timestamp(), end_date.timestamp())
            }
            
            deposits = await self.deposit_mgr.search_deposits_advanced(filters=filters)
            orders = await self.order_mgr.search_orders_advanced(filters=filters)
            
            deposit_list = deposits.get("results", [])
            order_list = orders.get("results", [])
            
            # Calculate metrics
            total_revenue = sum(float(d.get("deposit_amount", 0)) for d in deposit_list)
            total_orders = len(order_list)
            completed_orders = sum(1 for o in order_list if o.get("status") == "COMPLETED")
            avg_order_value = total_revenue / max(completed_orders, 1)
            
            # Generate charts
            revenue_chart = await self._generate_revenue_chart(start_date, end_date)
            order_value_chart = await self._generate_order_value_chart(start_date, end_date)
            
            # Create report
            return AnalyticsReport(
                title="Financial Performance Report",
                description=f"Financial analysis from {start_date.date()} to {end_date.date()}",
                metrics={
                    "total_revenue": format_currency(total_revenue),
                    "total_orders": total_orders,
                    "completed_orders": completed_orders,
                    "average_order_value": format_currency(avg_order_value),
                    "success_rate": (completed_orders / max(total_orders, 1)) * 100
                },
                charts=[revenue_chart, order_value_chart],
                summary=self._generate_financial_summary(total_revenue, total_orders, avg_order_value),
                generated_at=datetime.now().isoformat()
            )
            
        except Exception:
            return self._get_default_report("Financial Performance Report")
    
    async def _generate_operational_report(self, start_date: datetime,
                                       end_date: datetime) -> AnalyticsReport:
        """Generate operational analytics report."""
        try:
            # Get operational data
            filters = {
                "created_at": (start_date.timestamp(), end_date.timestamp())
            }
            
            orders = await self.order_mgr.search_orders_advanced(filters=filters)
            order_list = orders.get("results", [])
            
            # Calculate metrics
            total_orders = len(order_list)
            completed = sum(1 for o in order_list if o.get("status") == "COMPLETED")
            failed = sum(1 for o in order_list if o.get("status") == "FAILED")
            avg_response_time = sum(float(o.get("response_time", 0)) for o in order_list) / max(total_orders, 1)
            
            # Generate charts
            status_chart = await self._generate_status_chart(start_date, end_date)
            response_time_chart = await self._generate_response_time_chart(start_date, end_date)
            
            # Create report
            return AnalyticsReport(
                title="Operational Performance Report",
                description=f"Operational analysis from {start_date.date()} to {end_date.date()}",
                metrics={
                    "total_orders": total_orders,
                    "completed_orders": completed,
                    "failed_orders": failed,
                    "success_rate": (completed / max(total_orders, 1)) * 100,
                    "average_response_time": f"{avg_response_time:.2f}s"
                },
                charts=[status_chart, response_time_chart],
                summary=self._generate_operational_summary(total_orders, completed, avg_response_time),
                generated_at=datetime.now().isoformat()
            )
            
        except Exception:
            return self._get_default_report("Operational Performance Report")
    
    async def _generate_user_report(self, start_date: datetime,
                                end_date: datetime) -> AnalyticsReport:
        """Generate user analytics report."""
        try:
            # Get user data
            filters = {
                "registration_date": (start_date.timestamp(), end_date.timestamp())
            }
            
            users = await self.user_mgr.search_users(filters=filters)
            user_list = users.get("results", [])
            
            # Calculate metrics
            total_users = len(user_list)
            active_users = sum(1 for u in user_list if u.get("status") == "ACTIVE")
            avg_balance = sum(float(u.get("balance", 0)) for u in user_list) / max(total_users, 1)
            with_orders = sum(1 for u in user_list if float(u.get("total_orders", 0)) > 0)
            
            # Generate charts
            registration_chart = await self._generate_registration_chart(start_date, end_date)
            activity_chart = await self._generate_activity_chart(start_date, end_date)
            
            # Create report
            return AnalyticsReport(
                title="User Analytics Report",
                description=f"User analysis from {start_date.date()} to {end_date.date()}",
                metrics={
                    "total_users": total_users,
                    "active_users": active_users,
                    "average_balance": format_currency(avg_balance),
                    "users_with_orders": with_orders,
                    "activity_rate": (with_orders / max(total_users, 1)) * 100
                },
                charts=[registration_chart, activity_chart],
                summary=self._generate_user_summary(total_users, active_users, with_orders),
                generated_at=datetime.now().isoformat()
            )
            
        except Exception:
            return self._get_default_report("User Analytics Report")
    
    async def _get_hourly_order_metrics(self, start_time: datetime,
                                     end_time: datetime) -> Dict[str, Any]:
        """Get hourly order metrics."""
        try:
            filters = {
                "created_at": (start_time.timestamp(), end_time.timestamp())
            }
            
            orders = await self.order_mgr.search_orders_advanced(filters=filters)
            order_list = orders.get("results", [])
            
            return {
                "total": len(order_list),
                "completed": sum(1 for o in order_list if o.get("status") == "COMPLETED"),
                "failed": sum(1 for o in order_list if o.get("status") == "FAILED"),
                "average_response_time": sum(float(o.get("response_time", 0)) for o in order_list) / max(len(order_list), 1)
            }
            
        except Exception:
            return {
                "total": 0,
                "completed": 0,
                "failed": 0,
                "average_response_time": 0
            }
    
    async def _get_hourly_user_metrics(self, start_time: datetime,
                                    end_time: datetime) -> Dict[str, Any]:
        """Get hourly user metrics."""
        try:
            filters = {
                "last_activity": (start_time.timestamp(), end_time.timestamp())
            }
            
            users = await self.user_mgr.search_users(filters=filters)
            user_list = users.get("results", [])
            
            return {
                "active": len(user_list),
                "new_registrations": sum(1 for u in user_list if u.get("registration_date", 0) >= start_time.timestamp()),
                "with_orders": sum(1 for u in user_list if float(u.get("total_orders", 0)) > 0)
            }
            
        except Exception:
            return {
                "active": 0,
                "new_registrations": 0,
                "with_orders": 0
            }
    
    async def _get_system_metrics(self) -> Dict[str, Any]:
        """Get system performance metrics."""
        try:
            redis_info = await self.redis.info()
            
            return {
                "memory_usage": f"{float(redis_info.get('used_memory_peak_perc', 0)):.1f}%",
                "connected_clients": redis_info.get('connected_clients', 0),
                "total_commands": redis_info.get('total_commands_processed', 0),
                "uptime": redis_info.get('uptime_in_seconds', 0)
            }
            
        except Exception:
            return {
                "memory_usage": "N/A",
                "connected_clients": 0,
                "total_commands": 0,
                "uptime": 0
            }
    
    def _convert_to_csv(self, report: AnalyticsReport) -> str:
        """Convert report to CSV format."""
        try:
            # Create DataFrame from metrics
            df = pd.DataFrame([report.metrics])
            
            # Convert to CSV
            output = io.StringIO()
            df.to_csv(output, index=False)
            return output.getvalue()
            
        except Exception:
            return ""
    
    def _generate_financial_summary(self, total_revenue: float,
                                total_orders: int,
                                avg_order_value: float) -> str:
        """Generate summary for financial report."""
        return (
            f"During this period, the system generated {format_currency(total_revenue)} "
            f"in revenue from {total_orders} orders, with an average order value of "
            f"{format_currency(avg_order_value)}."
        )
    
    def _generate_operational_summary(self, total_orders: int,
                                   completed: int,
                                   avg_response_time: float) -> str:
        """Generate summary for operational report."""
        return (
            f"The system processed {total_orders} orders with {completed} successful "
            f"completions. Average response time was {avg_response_time:.2f} seconds."
        )
    
    def _generate_user_summary(self, total_users: int,
                            active_users: int,
                            with_orders: int) -> str:
        """Generate summary for user report."""
        return (
            f"The platform has {total_users} total users, with {active_users} active "
            f"users and {with_orders} users who have placed orders."
        )
    
    def _get_default_report(self, title: str) -> AnalyticsReport:
        """Return default report when generation fails."""
        return AnalyticsReport(
            title=title,
            description="Report generation failed",
            metrics={},
            charts=[],
            summary="No data available",
            generated_at=datetime.now().isoformat()
        )
    
    async def _generate_revenue_chart(self, start_date: datetime,
                                   end_date: datetime) -> Dict[str, Any]:
        """Generate revenue trend chart."""
        # Implementation details omitted for brevity
        return {
            "type": "line",
            "title": "Revenue Trend",
            "data": {"labels": [], "datasets": []}
        }
    
    async def _generate_order_value_chart(self, start_date: datetime,
                                      end_date: datetime) -> Dict[str, Any]:
        """Generate order value distribution chart."""
        # Implementation details omitted for brevity
        return {
            "type": "bar",
            "title": "Order Value Distribution",
            "data": {"labels": [], "datasets": []}
        }
    
    async def _generate_status_chart(self, start_date: datetime,
                                 end_date: datetime) -> Dict[str, Any]:
        """Generate order status distribution chart."""
        # Implementation details omitted for brevity
        return {
            "type": "pie",
            "title": "Order Status Distribution",
            "data": {"labels": [], "datasets": []}
        }
    
    async def _generate_response_time_chart(self, start_date: datetime,
                                        end_date: datetime) -> Dict[str, Any]:
        """Generate response time trend chart."""
        # Implementation details omitted for brevity
        return {
            "type": "line",
            "title": "Response Time Trend",
            "data": {"labels": [], "datasets": []}
        }
    
    async def _generate_registration_chart(self, start_date: datetime,
                                       end_date: datetime) -> Dict[str, Any]:
        """Generate user registration trend chart."""
        # Implementation details omitted for brevity
        return {
            "type": "line",
            "title": "User Registration Trend",
            "data": {"labels": [], "datasets": []}
        }
    
    async def _generate_activity_chart(self, start_date: datetime,
                                   end_date: datetime) -> Dict[str, Any]:
        """Generate user activity chart."""
        # Implementation details omitted for brevity
        return {
            "type": "bar",
            "title": "User Activity Distribution",
            "data": {"labels": [], "datasets": []}
        }
