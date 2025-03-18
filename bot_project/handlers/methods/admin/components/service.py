from typing import Dict, Any, Optional, List
import json
import asyncio
from datetime import datetime
from redis.asyncio import Redis

from handlers.manager.operation import OrderManagement
from utils.redis_keys import RedisKeys
from .auth import require_admin

class ServiceManager:
    """Manage SMS services and provider configurations."""
    
    def __init__(self, redis_client: Redis, order_mgr: OrderManagement):
        self.redis = redis_client
        self.order_mgr = order_mgr
        self.redis_keys = RedisKeys()
        self.service_prefix = "service:config:"
        self.provider_prefix = "service:provider:"
        self.stats_prefix = "service:stats:"
    
    @require_admin(["super_admin", "manager"])
    async def add_service(self, service_data: Dict[str, Any]) -> Dict[str, Any]:
        """Add a new SMS service."""
        try:
            service_id = service_data.get("service_id")
            if not service_id:
                raise ValueError("Service ID is required")
            
            # Validate required fields
            required_fields = ["name", "provider_id", "price", "country_code"]
            missing_fields = [f for f in required_fields if f not in service_data]
            if missing_fields:
                raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")
            
            # Add metadata
            service_data.update({
                "created_at": datetime.now().isoformat(),
                "status": "active",
                "search_tags": self._generate_search_tags(service_data)
            })
            
            # Store in Redis
            key = f"{self.service_prefix}{service_id}"
            await self.redis.hset(key, mapping=service_data)
            
            # Update search index
            await self._update_service_index(service_id, service_data)
            
            return {"success": True, "service_id": service_id}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    @require_admin(["super_admin", "manager"])
    async def update_service(self, service_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing SMS service."""
        try:
            key = f"{self.service_prefix}{service_id}"
            
            # Check if service exists
            if not await self.redis.exists(key):
                raise ValueError("Service not found")
            
            # Get current data
            current_data = await self.redis.hgetall(key)
            
            # Update data
            current_data.update(updates)
            current_data["updated_at"] = datetime.now().isoformat()
            current_data["search_tags"] = self._generate_search_tags(current_data)
            
            # Store updates
            await self.redis.hset(key, mapping=current_data)
            
            # Update search index
            await self._update_service_index(service_id, current_data)
            
            return {"success": True, "service_id": service_id}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    @require_admin(["super_admin", "manager"])
    async def remove_service(self, service_id: str) -> Dict[str, Any]:
        """Remove a SMS service."""
        try:
            key = f"{self.service_prefix}{service_id}"
            
            # Check if service exists
            if not await self.redis.exists(key):
                raise ValueError("Service not found")
            
            # Archive instead of delete
            service_data = await self.redis.hgetall(key)
            service_data["status"] = "archived"
            service_data["archived_at"] = datetime.now().isoformat()
            
            # Update in Redis
            await self.redis.hset(key, mapping=service_data)
            
            # Update search index
            await self._update_service_index(service_id, service_data)
            
            return {"success": True, "service_id": service_id}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    @require_admin(["super_admin", "manager", "support"])
    async def get_service(self, service_id: str) -> Dict[str, Any]:
        """Get service details."""
        try:
            key = f"{self.service_prefix}{service_id}"
            service_data = await self.redis.hgetall(key)
            
            if not service_data:
                raise ValueError("Service not found")
            
            # Add usage statistics
            stats = await self._get_service_stats(service_id)
            service_data["statistics"] = stats
            
            return {"success": True, "data": service_data}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    @require_admin(["super_admin", "manager", "support"])
    async def list_services(self, filters: Optional[Dict] = None, 
                          sort_by: str = "created_at",
                          sort_asc: bool = False,
                          page: int = 1,
                          limit: int = 20) -> Dict[str, Any]:
        """List SMS services with filtering and pagination."""
        try:
            # Get all service keys
            pattern = f"{self.service_prefix}*"
            service_keys = await self.redis.keys(pattern)
            
            # Fetch all services
            services = []
            for key in service_keys:
                service_data = await self.redis.hgetall(key)
                if service_data:
                    # Apply filters
                    if filters and not self._matches_filters(service_data, filters):
                        continue
                    services.append(service_data)
            
            # Sort services
            services.sort(
                key=lambda x: x.get(sort_by, ""),
                reverse=not sort_asc
            )
            
            # Paginate
            start_idx = (page - 1) * limit
            end_idx = start_idx + limit
            paginated_services = services[start_idx:end_idx]
            
            return {
                "success": True,
                "data": paginated_services,
                "total": len(services),
                "page": page,
                "total_pages": (len(services) + limit - 1) // limit
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    @require_admin(["super_admin", "manager"])
    async def update_provider_settings(self, provider_id: str,
                                    settings: Dict[str, Any]) -> Dict[str, Any]:
        """Update SMS provider settings."""
        try:
            key = f"{self.provider_prefix}{provider_id}"
            
            # Validate required settings
            required_settings = ["api_key", "api_url", "timeout"]
            missing_settings = [s for s in required_settings if s not in settings]
            if missing_settings:
                raise ValueError(f"Missing required settings: {', '.join(missing_settings)}")
            
            # Add metadata
            settings.update({
                "updated_at": datetime.now().isoformat(),
                "status": "active"
            })
            
            # Store in Redis
            await self.redis.hset(key, mapping=settings)
            
            return {"success": True, "provider_id": provider_id}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    @require_admin(["super_admin", "manager", "support"])
    async def get_provider_settings(self, provider_id: str) -> Dict[str, Any]:
        """Get SMS provider settings."""
        try:
            key = f"{self.provider_prefix}{provider_id}"
            settings = await self.redis.hgetall(key)
            
            if not settings:
                raise ValueError("Provider not found")
            
            # Mask sensitive data
            if "api_key" in settings:
                settings["api_key"] = "****" + settings["api_key"][-4:]
            
            return {"success": True, "data": settings}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def _get_service_stats(self, service_id: str) -> Dict[str, Any]:
        """Get usage statistics for a service."""
        try:
            # Get cached stats
            cache_key = f"{self.stats_prefix}{service_id}"
            cached_stats = await self.redis.get(cache_key)
            
            if cached_stats:
                return json.loads(cached_stats)
            
            # Calculate stats from orders
            now = datetime.now()
            filters = {
                "service_id": service_id,
                "created_at": (now.timestamp() - 30*24*60*60, now.timestamp())
            }
            
            orders = await self.order_mgr.search_orders_advanced(filters=filters)
            
            if not orders.get("response"):
                return self._get_default_stats()
            
            order_list = orders.get("results", [])
            
            stats = {
                "total_orders": len(order_list),
                "success_rate": sum(1 for o in order_list if o.get("status") == "COMPLETED") / max(len(order_list), 1) * 100,
                "average_response_time": sum(float(o.get("response_time", 0)) for o in order_list) / max(len(order_list), 1),
                "total_revenue": sum(float(o.get("amount", 0)) for o in order_list),
                "calculated_at": now.isoformat()
            }
            
            # Cache for 1 hour
            await self.redis.setex(cache_key, 3600, json.dumps(stats))
            
            return stats
            
        except Exception:
            return self._get_default_stats()
    
    def _get_default_stats(self) -> Dict[str, Any]:
        """Return default statistics when calculation fails."""
        return {
            "total_orders": 0,
            "success_rate": 0,
            "average_response_time": 0,
            "total_revenue": 0,
            "calculated_at": datetime.now().isoformat()
        }
    
    def _generate_search_tags(self, service_data: Dict[str, Any]) -> str:
        """Generate search tags for a service."""
        tags = [
            service_data.get("name", ""),
            service_data.get("provider_id", ""),
            service_data.get("country_code", ""),
            service_data.get("service_id", "")
        ]
        return " ".join(filter(None, tags))
    
    def _matches_filters(self, service_data: Dict[str, Any],
                       filters: Dict[str, Any]) -> bool:
        """Check if service data matches the given filters."""
        for key, value in filters.items():
            if key not in service_data:
                return False
            if isinstance(value, (list, tuple)):
                if service_data[key] not in value:
                    return False
            elif service_data[key] != value:
                return False
        return True
    
    async def _update_service_index(self, service_id: str,
                                 service_data: Dict[str, Any]) -> None:
        """Update search index for a service."""
        try:
            index_key = f"{self.service_prefix}index"
            index_data = {
                "id": service_id,
                "name": service_data.get("name", ""),
                "provider_id": service_data.get("provider_id", ""),
                "country_code": service_data.get("country_code", ""),
                "status": service_data.get("status", ""),
                "search_tags": service_data.get("search_tags", "")
            }
            await self.redis.hset(index_key, service_id, json.dumps(index_data))
        except Exception:
            pass  # Fail silently as this is not critical
