from typing import Dict, Any, Optional, List
import asyncio
from datetime import datetime, timedelta
import json
from dataclasses import dataclass
from redis.asyncio import Redis

from handlers.manager.operation import OrderManagement, UserManagement, DepositManagement
from utils.cache_manager import cache_manager, CachePrefix
from utils.functions import format_currency

@dataclass
class ChartData:
    labels: List[str]
    datasets: List[Dict[str, Any]]

class DashboardManager:
    """Handle dashboard statistics and real-time analytics."""
    
    def __init__(self, redis_client: Redis,
                 order_mgr: OrderManagement,
                 user_mgr: UserManagement,
                 deposit_mgr: DepositManagement):
        self.redis = redis_client
        self.order_mgr = order_mgr
        self.user_mgr = user_mgr
        self.deposit_mgr = deposit_mgr
        self.stats_prefix = "admin:stats:"
        self.chart_prefix = "admin:chart:"
        
    async def get_dashboard_stats(self) -> Dict[str, Any]:
        """Get comprehensive dashboard statistics."""
        try:
            # Try to get cached stats
            cache_key = f"{self.stats_prefix}dashboard"
            cached_stats = await self.redis.get(cache_key)
            
            if cached_stats:
                return json.loads(cached_stats)
            
            # Calculate time ranges
            now = datetime.now()
            today_start = datetime.combine(now.date(), datetime.min.time())
            week_start = today_start - timedelta(days=7)
            month_start = today_start - timedelta(days=30)
            
            # Fetch data concurrently
            stats_tasks = [
                self._get_user_stats(today_start, week_start, month_start),
                self._get_order_stats(today_start, week_start, month_start),
                self._get_financial_stats(today_start, week_start, month_start),
                self._get_system_health()
            ]
            
            user_stats, order_stats, financial_stats, system_health = await asyncio.gather(*stats_tasks)
            
            # Combine all stats
            dashboard_stats = {
                "users": user_stats,
                "orders": order_stats,
                "financial": financial_stats,
                "system": system_health,
                "last_updated": now.isoformat()
            }
            
            # Cache for 5 minutes
            await self.redis.setex(cache_key, 300, json.dumps(dashboard_stats))
            
            return dashboard_stats
            
        except Exception as e:
            print(f"Error getting dashboard stats: {str(e)}")
            return self._get_default_stats()
    
    async def get_chart_data(self, chart_type: str, timeframe: str = "daily") -> ChartData:
        """Get data for various dashboard charts."""
        try:
            cache_key = f"{self.chart_prefix}{chart_type}:{timeframe}"
            cached_data = await self.redis.get(cache_key)
            
            if cached_data:
                chart_dict = json.loads(cached_data)
                return ChartData(**chart_dict)
            
            if chart_type == "revenue":
                data = await self._generate_revenue_chart(timeframe)
            elif chart_type == "orders":
                data = await self._generate_orders_chart(timeframe)
            elif chart_type == "users":
                data = await self._generate_users_chart(timeframe)
            else:
                raise ValueError(f"Unknown chart type: {chart_type}")
            
            # Cache for 5 minutes
            await self.redis.setex(cache_key, 300, json.dumps(data.__dict__))
            
            return data
            
        except Exception as e:
            print(f"Error generating chart data: {str(e)}")
            return self._get_default_chart()
    
    async def _get_user_stats(self, today_start: datetime, 
                            week_start: datetime,
                            month_start: datetime) -> Dict[str, Any]:
        """Get user-related statistics."""
        try:
            # Get active users for different time periods
            filters = {
                'last_activity': (today_start.timestamp(), datetime.now().timestamp())
            }
            today_active = await self.user_mgr.search_users(filters=filters)
            
            filters['last_activity'] = (week_start.timestamp(), datetime.now().timestamp())
            week_active = await self.user_mgr.search_users(filters=filters)
            
            filters['last_activity'] = (month_start.timestamp(), datetime.now().timestamp())
            month_active = await self.user_mgr.search_users(filters=filters)
            
            return {
                "total_users": month_active.get('total', 0),
                "active_today": today_active.get('total', 0),
                "active_week": week_active.get('total', 0),
                "active_month": month_active.get('total', 0)
            }
            
        except Exception:
            return {
                "total_users": 0,
                "active_today": 0,
                "active_week": 0,
                "active_month": 0
            }
    
    async def _get_order_stats(self, today_start: datetime,
                             week_start: datetime,
                             month_start: datetime) -> Dict[str, Any]:
        """Get order-related statistics."""
        try:
            # Get orders for different time periods
            filters = {
                'recorded_at': (today_start.timestamp(), datetime.now().timestamp())
            }
            today_orders = await self.order_mgr.search_orders_advanced(filters=filters)
            
            filters['recorded_at'] = (week_start.timestamp(), datetime.now().timestamp())
            week_orders = await self.order_mgr.search_orders_advanced(filters=filters)
            
            filters['recorded_at'] = (month_start.timestamp(), datetime.now().timestamp())
            month_orders = await self.order_mgr.search_orders_advanced(filters=filters)
            
            # Calculate success rates
            today_success = sum(1 for o in today_orders.get('results', []) 
                              if o.get('order_status') == 'COMPLETED')
            week_success = sum(1 for o in week_orders.get('results', [])
                             if o.get('order_status') == 'COMPLETED')
            month_success = sum(1 for o in month_orders.get('results', [])
                              if o.get('order_status') == 'COMPLETED')
            
            return {
                "today": {
                    "total": today_orders.get('total', 0),
                    "success_rate": today_success / max(today_orders.get('total', 1), 1) * 100
                },
                "week": {
                    "total": week_orders.get('total', 0),
                    "success_rate": week_success / max(week_orders.get('total', 1), 1) * 100
                },
                "month": {
                    "total": month_orders.get('total', 0),
                    "success_rate": month_success / max(month_orders.get('total', 1), 1) * 100
                }
            }
            
        except Exception:
            return {
                "today": {"total": 0, "success_rate": 0},
                "week": {"total": 0, "success_rate": 0},
                "month": {"total": 0, "success_rate": 0}
            }
    
    async def _get_financial_stats(self, today_start: datetime,
                                week_start: datetime,
                                month_start: datetime) -> Dict[str, Any]:
        """Get financial statistics."""
        try:
            # Get deposits for different time periods
            filters = {
                'recorded_at': (today_start.timestamp(), datetime.now().timestamp())
            }
            today_deposits = await self.deposit_mgr.search_deposits_advanced(filters=filters)
            
            filters['recorded_at'] = (week_start.timestamp(), datetime.now().timestamp())
            week_deposits = await self.deposit_mgr.search_deposits_advanced(filters=filters)
            
            filters['recorded_at'] = (month_start.timestamp(), datetime.now().timestamp())
            month_deposits = await self.deposit_mgr.search_deposits_advanced(filters=filters)
            
            # Calculate revenues
            today_revenue = sum(float(d.get('deposit_amount', 0)) 
                              for d in today_deposits.get('results', []))
            week_revenue = sum(float(d.get('deposit_amount', 0))
                             for d in week_deposits.get('results', []))
            month_revenue = sum(float(d.get('deposit_amount', 0))
                              for d in month_deposits.get('results', []))
            
            return {
                "revenue": {
                    "today": format_currency(today_revenue),
                    "week": format_currency(week_revenue),
                    "month": format_currency(month_revenue)
                },
                "transactions": {
                    "today": today_deposits.get('total', 0),
                    "week": week_deposits.get('total', 0),
                    "month": month_deposits.get('total', 0)
                }
            }
            
        except Exception:
            return {
                "revenue": {
                    "today": format_currency(0),
                    "week": format_currency(0),
                    "month": format_currency(0)
                },
                "transactions": {
                    "today": 0,
                    "week": 0,
                    "month": 0
                }
            }
    
    async def _get_system_health(self) -> Dict[str, Any]:
        """Get system health metrics."""
        try:
            # Get Redis info
            redis_info = await self.redis.info()
            
            # Calculate memory usage
            used_memory = int(redis_info.get('used_memory', 0))
            total_memory = used_memory + int(redis_info.get('used_memory_rss', 0))
            memory_usage = (used_memory / total_memory) * 100 if total_memory > 0 else 0
            
            return {
                "status": "OPERATIONAL",
                "memory_usage": f"{memory_usage:.1f}%",
                "connected_clients": redis_info.get('connected_clients', 0),
                "uptime_days": redis_info.get('uptime_in_days', 0)
            }
            
        except Exception:
            return {
                "status": "ERROR",
                "memory_usage": "N/A",
                "connected_clients": 0,
                "uptime_days": 0
            }
    
    async def _generate_revenue_chart(self, timeframe: str) -> ChartData:
        """Generate revenue chart data."""
        try:
            now = datetime.now()
            if timeframe == "daily":
                start_time = now - timedelta(days=7)
                interval = timedelta(days=1)
                format_str = "%Y-%m-%d"
            elif timeframe == "weekly":
                start_time = now - timedelta(weeks=12)
                interval = timedelta(weeks=1)
                format_str = "%Y-W%W"
            else:  # monthly
                start_time = now - timedelta(days=365)
                interval = timedelta(days=30)
                format_str = "%Y-%m"
            
            labels = []
            revenue_data = []
            current = start_time
            
            while current <= now:
                next_time = current + interval
                filters = {
                    'recorded_at': (current.timestamp(), next_time.timestamp())
                }
                deposits = await self.deposit_mgr.search_deposits_advanced(filters=filters)
                
                revenue = sum(float(d.get('deposit_amount', 0))
                            for d in deposits.get('results', []))
                
                labels.append(current.strftime(format_str))
                revenue_data.append(revenue)
                current = next_time
            
            return ChartData(
                labels=labels,
                datasets=[{
                    "label": "Revenue",
                    "data": revenue_data,
                    "borderColor": "#4CAF50",
                    "backgroundColor": "rgba(76, 175, 80, 0.1)"
                }]
            )
            
        except Exception:
            return self._get_default_chart()
    
    async def _generate_orders_chart(self, timeframe: str) -> ChartData:
        """Generate orders chart data."""
        try:
            now = datetime.now()
            if timeframe == "daily":
                start_time = now - timedelta(days=7)
                interval = timedelta(days=1)
                format_str = "%Y-%m-%d"
            elif timeframe == "weekly":
                start_time = now - timedelta(weeks=12)
                interval = timedelta(weeks=1)
                format_str = "%Y-W%W"
            else:  # monthly
                start_time = now - timedelta(days=365)
                interval = timedelta(days=30)
                format_str = "%Y-%m"
            
            labels = []
            completed_data = []
            pending_data = []
            current = start_time
            
            while current <= now:
                next_time = current + interval
                filters = {
                    'recorded_at': (current.timestamp(), next_time.timestamp())
                }
                orders = await self.order_mgr.search_orders_advanced(filters=filters)
                
                completed = sum(1 for o in orders.get('results', [])
                              if o.get('order_status') == 'COMPLETED')
                pending = sum(1 for o in orders.get('results', [])
                            if o.get('order_status') == 'PENDING')
                
                labels.append(current.strftime(format_str))
                completed_data.append(completed)
                pending_data.append(pending)
                current = next_time
            
            return ChartData(
                labels=labels,
                datasets=[
                    {
                        "label": "Completed Orders",
                        "data": completed_data,
                        "borderColor": "#4CAF50",
                        "backgroundColor": "rgba(76, 175, 80, 0.1)"
                    },
                    {
                        "label": "Pending Orders",
                        "data": pending_data,
                        "borderColor": "#FFC107",
                        "backgroundColor": "rgba(255, 193, 7, 0.1)"
                    }
                ]
            )
            
        except Exception:
            return self._get_default_chart()
    
    async def _generate_users_chart(self, timeframe: str) -> ChartData:
        """Generate users chart data."""
        try:
            now = datetime.now()
            if timeframe == "daily":
                start_time = now - timedelta(days=7)
                interval = timedelta(days=1)
                format_str = "%Y-%m-%d"
            elif timeframe == "weekly":
                start_time = now - timedelta(weeks=12)
                interval = timedelta(weeks=1)
                format_str = "%Y-W%W"
            else:  # monthly
                start_time = now - timedelta(days=365)
                interval = timedelta(days=30)
                format_str = "%Y-%m"
            
            labels = []
            active_users_data = []
            new_users_data = []
            current = start_time
            
            while current <= now:
                next_time = current + interval
                
                # Get active users
                active_filters = {
                    'last_activity': (current.timestamp(), next_time.timestamp())
                }
                active_users = await self.user_mgr.search_users(filters=active_filters)
                
                # Get new users
                new_filters = {
                    'registration_date': (current.timestamp(), next_time.timestamp())
                }
                new_users = await self.user_mgr.search_users(filters=new_filters)
                
                labels.append(current.strftime(format_str))
                active_users_data.append(active_users.get('total', 0))
                new_users_data.append(new_users.get('total', 0))
                current = next_time
            
            return ChartData(
                labels=labels,
                datasets=[
                    {
                        "label": "Active Users",
                        "data": active_users_data,
                        "borderColor": "#2196F3",
                        "backgroundColor": "rgba(33, 150, 243, 0.1)"
                    },
                    {
                        "label": "New Users",
                        "data": new_users_data,
                        "borderColor": "#9C27B0",
                        "backgroundColor": "rgba(156, 39, 176, 0.1)"
                    }
                ]
            )
            
        except Exception:
            return self._get_default_chart()
    
    def _get_default_stats(self) -> Dict[str, Any]:
        """Return default statistics when data fetching fails."""
        return {
            "users": {
                "total_users": 0,
                "active_today": 0,
                "active_week": 0,
                "active_month": 0
            },
            "orders": {
                "today": {"total": 0, "success_rate": 0},
                "week": {"total": 0, "success_rate": 0},
                "month": {"total": 0, "success_rate": 0}
            },
            "financial": {
                "revenue": {
                    "today": format_currency(0),
                    "week": format_currency(0),
                    "month": format_currency(0)
                },
                "transactions": {
                    "today": 0,
                    "week": 0,
                    "month": 0
                }
            },
            "system": {
                "status": "ERROR",
                "memory_usage": "N/A",
                "connected_clients": 0,
                "uptime_days": 0
            },
            "last_updated": datetime.now().isoformat()
        }
    
    def _get_default_chart(self) -> ChartData:
        """Return default chart data when generation fails."""
        return ChartData(
            labels=["No Data"],
            datasets=[{
                "label": "No Data Available",
                "data": [0],
                "borderColor": "#757575",
                "backgroundColor": "rgba(117, 117, 117, 0.1)"
            }]
        )
