from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import jwt
import redis
import json
import asyncio
from pathlib import Path

from ...order_management import OrderManagement
from ...user_management import UserManagement
from ...deposit_management import DepositManagement
from .components.auth import AdminAuth
from .components.dashboard import DashboardManager
from .components.service import ServiceManager
from .components.order import OrderManager
from .components.user import UserManager
from .components.analytics import AnalyticsManager

# Initialize FastAPI app
app = FastAPI(title="SMS Admin API")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files and templates
static_dir = Path(__file__).parent.parent / "static"
templates_dir = Path(__file__).parent.parent / "templates"

app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
templates = Jinja2Templates(directory=str(templates_dir))

# Initialize Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Initialize components
auth_manager = AdminAuth(redis_client)
dashboard_manager = DashboardManager(redis_client)
service_manager = ServiceManager(redis_client)
order_manager = OrderManager(redis_client)
user_manager = UserManager(redis_client)
analytics_manager = AnalyticsManager(redis_client)

# OAuth2 scheme
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# WebSocket connections store
active_connections: List[WebSocket] = []

# Authentication dependency
async def get_current_admin(token: str = Depends(oauth2_scheme)):
    try:
        admin = await auth_manager.validate_token(token)
        if not admin:
            raise HTTPException(
                status_code=401,
                detail="Invalid authentication credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return admin
    except Exception as e:
        raise HTTPException(
            status_code=401,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except WebSocketDisconnect:
                self.disconnect(connection)

manager = ConnectionManager()

# Authentication endpoints
@app.post("/api/admin/auth/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    admin = await auth_manager.authenticate_admin(form_data.username, form_data.password)
    if not admin:
        raise HTTPException(
            status_code=401,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return {"access_token": admin["token"], "token_type": "bearer"}

@app.post("/api/admin/auth/logout")
async def logout(current_admin: dict = Depends(get_current_admin)):
    await auth_manager.logout_admin(current_admin["admin_id"])
    return {"message": "Successfully logged out"}

# Dashboard endpoints
@app.get("/api/admin/dashboard")
async def get_dashboard_data(current_admin: dict = Depends(get_current_admin)):
    try:
        stats = await dashboard_manager.get_dashboard_stats()
        charts = await dashboard_manager.get_dashboard_charts()
        recent_activity = await dashboard_manager.get_recent_activity()
        
        return {
            "success": True,
            "data": {
                "stats": stats,
                "charts": charts,
                "recent_activity": recent_activity
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Service endpoints
@app.get("/api/admin/services")
async def get_services(
    action: str,
    search: Optional[str] = None,
    page: int = 1,
    limit: int = 10,
    current_admin: dict = Depends(get_current_admin)
):
    try:
        if action == "list":
            services = await service_manager.list_services(search, page, limit)
        elif action == "search":
            services = await service_manager.search_services(search, page, limit)
        else:
            raise HTTPException(status_code=400, detail="Invalid action")
            
        return {"success": True, "data": services}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/admin/services")
async def create_service(
    service_data: dict,
    current_admin: dict = Depends(get_current_admin)
):
    try:
        service = await service_manager.create_service(service_data)
        # Broadcast service update
        await manager.broadcast({
            "type": "service_update",
            "action": "create",
            "service": service
        })
        return {"success": True, "data": service}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/admin/services/{service_id}")
async def update_service(
    service_id: str,
    service_data: dict,
    current_admin: dict = Depends(get_current_admin)
):
    try:
        service = await service_manager.update_service(service_id, service_data)
        # Broadcast service update
        await manager.broadcast({
            "type": "service_update",
            "action": "update",
            "service": service
        })
        return {"success": True, "data": service}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Order endpoints
@app.get("/api/admin/orders")
async def get_orders(
    action: str,
    search: Optional[str] = None,
    status: Optional[str] = None,
    page: int = 1,
    limit: int = 10,
    current_admin: dict = Depends(get_current_admin)
):
    try:
        if action == "search":
            orders = await order_manager.search_orders(search, status, page, limit)
        elif action == "pending":
            orders = await order_manager.get_pending_orders(page, limit)
        else:
            raise HTTPException(status_code=400, detail="Invalid action")
            
        return {"success": True, "data": orders}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/admin/orders/{order_id}")
async def update_order_status(
    order_id: str,
    status_data: dict,
    current_admin: dict = Depends(get_current_admin)
):
    try:
        order = await order_manager.update_order_status(order_id, status_data["status"])
        # Broadcast order update
        await manager.broadcast({
            "type": "order_update",
            "action": "status_update",
            "order": order
        })
        return {"success": True, "data": order}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# User endpoints
@app.get("/api/admin/users")
async def get_users(
    action: str,
    search: Optional[str] = None,
    status: Optional[str] = None,
    page: int = 1,
    limit: int = 10,
    current_admin: dict = Depends(get_current_admin)
):
    try:
        if action == "search":
            users = await user_manager.search_users(search, status, page, limit)
        elif action == "active":
            users = await user_manager.get_active_users(page, limit)
        else:
            raise HTTPException(status_code=400, detail="Invalid action")
            
        return {"success": True, "data": users}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/admin/users/{user_id}/balance")
async def adjust_user_balance(
    user_id: str,
    balance_data: dict,
    current_admin: dict = Depends(get_current_admin)
):
    try:
        user = await user_manager.adjust_balance(user_id, balance_data["amount"])
        # Broadcast user update
        await manager.broadcast({
            "type": "user_update",
            "action": "balance_update",
            "user": user
        })
        return {"success": True, "data": user}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Analytics endpoints
@app.get("/api/admin/analytics")
async def get_analytics(
    type: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    current_admin: dict = Depends(get_current_admin)
):
    try:
        if type == "dashboard":
            data = await analytics_manager.get_dashboard_analytics()
        elif type == "financial":
            data = await analytics_manager.get_financial_analytics(start_date, end_date)
        elif type == "user":
            data = await analytics_manager.get_user_analytics(start_date, end_date)
        else:
            raise HTTPException(status_code=400, detail="Invalid analytics type")
            
        return {"success": True, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# WebSocket endpoint
@app.websocket("/ws/admin")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                if message.get("type") == "ping":
                    await websocket.send_json({"type": "pong"})
            except json.JSONDecodeError:
                pass
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# Background task for real-time updates
async def broadcast_updates():
    while True:
        try:
            # Get latest stats
            stats = await dashboard_manager.get_dashboard_stats()
            await manager.broadcast({
                "type": "stats_update",
                "stats": stats
            })
            
            # Check for new notifications
            notifications = await dashboard_manager.get_new_notifications()
            for notification in notifications:
                await manager.broadcast({
                    "type": "notification",
                    "message": notification
                })
                
        except Exception as e:
            print(f"Error in broadcast_updates: {e}")
            
        await asyncio.sleep(30)  # Update every 30 seconds

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(broadcast_updates())
