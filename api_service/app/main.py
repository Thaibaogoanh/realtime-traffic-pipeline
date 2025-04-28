import os
import json
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as redis
import clickhouse_connect # Sử dụng clickhouse-connect
from datetime import datetime
from typing import List, Optional, Dict, Any

# --- Import các thành phần khác nếu tách file ---
# from .schemas import TrafficHistoryRecord, TrafficState # Ví dụ
# from .db_queries import get_traffic_history # Ví dụ
# from .config import settings # Ví dụ

# --- Cấu hình ---
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 8123)) # Port HTTP cho client này
CLICKHOUSE_NATIVE_PORT = int(os.environ.get("CLICKHOUSE_NATIVE_PORT", 9000))
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "traffic_db")
CLICKHOUSE_TABLE = os.environ.get("CLICKHOUSE_TABLE", "traffic_events")
# Thêm user/password nếu ClickHouse yêu cầu
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD")


# --- Khởi tạo FastAPI App ---
app = FastAPI(title="Realtime Traffic API - DE Focused")

# --- CORS Middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Cho phép tất cả các nguồn gốc (thay đổi trong production)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Quản lý Kết nối và Redis ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.redis_pool = None
        self.clickhouse_client = None

    async def connect_redis(self):
        try:
            self.redis_pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
            # Thử ping để chắc chắn kết nối
            r = redis.Redis(connection_pool=self.redis_pool)
            await r.ping()
            print(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
        except Exception as e:
            print(f"Error connecting to Redis: {e}")
            self.redis_pool = None # Đặt lại là None nếu lỗi

    async def disconnect_redis(self):
        if self.redis_pool:
            await self.redis_pool.disconnect()
            print("Disconnected from Redis")

    def connect_clickhouse(self):
        try:
            self.clickhouse_client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT, # Dùng port HTTP
                database=CLICKHOUSE_DB,
                user=CLICKHOUSE_USER,     # Sẽ là None nếu không set env var
                password=CLICKHOUSE_PASSWORD # Sẽ là None nếu không set env var
            )
            # Thử ping để chắc chắn
            self.clickhouse_client.ping()
            print(f"Connected to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}, DB: {CLICKHOUSE_DB}")
        except Exception as e:
            print(f"Error connecting to ClickHouse: {e}")
            self.clickhouse_client = None

    def disconnect_clickhouse(self):
         if self.clickhouse_client:
             self.clickhouse_client.close()
             print("Disconnected from ClickHouse")

    async def connect_websocket(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print(f"New WebSocket connection: {websocket.client}. Total: {len(self.active_connections)}")

    def disconnect_websocket(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            print(f"WebSocket disconnected: {websocket.client}. Total: {len(self.active_connections)}")

    async def broadcast(self, message: str):
        disconnected_clients = []
        active_connections_copy = self.active_connections[:] # Tạo bản sao để duyệt an toàn
        for connection in active_connections_copy:
            try:
                await connection.send_text(message)
            except Exception as e: # Bắt lỗi rộng hơn (vd: ConnectionClosedOK)
                print(f"Error sending to client or client disconnected: {connection.client} - {e}")
                disconnected_clients.append(connection)

        # Xóa các client đã ngắt kết nối khỏi danh sách gốc
        for client in disconnected_clients:
             if client in self.active_connections:
                 self.active_connections.remove(client)


manager = ConnectionManager()

# --- Lifecycle events ---
@app.on_event("startup")
async def startup_event():
    await manager.connect_redis()
    manager.connect_clickhouse() # Kết nối đồng bộ là đủ cho client này
    # Chỉ khởi động polling nếu kết nối Redis thành công
    if manager.redis_pool:
        asyncio.create_task(poll_redis_and_broadcast())
    else:
        print("Cannot start Redis polling due to connection failure.")


@app.on_event("shutdown")
async def shutdown_event():
    await manager.disconnect_redis()
    manager.disconnect_clickhouse()

# --- Background Task: Polling Redis ---
async def poll_redis_and_broadcast():
    if not manager.redis_pool: return # Thoát nếu không có pool

    redis_client = redis.Redis(connection_pool=manager.redis_pool)
    last_state = {}
    segment_keys_pattern = "segment:*"

    print("Starting Redis polling task...")
    while True:
        try:
            if not manager.active_connections: # Nếu không có ai kết nối thì đợi lâu hơn
                 await asyncio.sleep(5)
                 continue

            current_state = {}
            segment_keys = await redis_client.keys(segment_keys_pattern)
            if segment_keys:
                segment_values = await redis_client.mget(segment_keys)
                for key, value_json in zip(segment_keys, segment_values):
                    if value_json:
                        try:
                            segment_id = key.split(":")[-1]
                            current_state[segment_id] = json.loads(value_json)
                        except (json.JSONDecodeError, IndexError, TypeError) as e:
                            print(f"Error parsing Redis data for key {key}: {e}, Value: {value_json}")

            # Chỉ broadcast nếu state thay đổi
            if current_state and current_state != last_state:
                # print(f"State changed. Broadcasting {len(current_state)} segment states.") # Log nhiều quá
                await manager.broadcast(json.dumps({"type": "traffic_update", "data": current_state}))
                last_state = current_state.copy()

        except redis.exceptions.ConnectionError as r_err:
            print(f"Redis connection error during polling: {r_err}. Attempting to reconnect...")
            await manager.disconnect_redis()
            await asyncio.sleep(5) # Đợi trước khi thử kết nối lại ở lần lặp sau
            await manager.connect_redis()
            redis_client = redis.Redis(connection_pool=manager.redis_pool) if manager.redis_pool else None
            if not redis_client:
                print("Failed to re-establish Redis connection for polling. Stopping poll task.")
                break # Dừng task nếu không kết nối lại được
        except Exception as e:
            print(f"Error during Redis polling: {e}")

        await asyncio.sleep(2) # Đợi 2 giây

# --- API Endpoints ---

@app.get("/api/traffic/state")
async def get_initial_traffic_state() -> Dict[str, Any]:
    """
    Lấy trạng thái giao thông tức thời của tất cả các đoạn đường từ Redis.
    """
    if not manager.redis_pool:
         raise HTTPException(status_code=503, detail="Redis service unavailable")

    redis_client = redis.Redis(connection_pool=manager.redis_pool)
    initial_state = {}
    segment_keys_pattern = "segment:*"
    try:
        segment_keys = await redis_client.keys(segment_keys_pattern)
        if segment_keys:
            segment_values = await redis_client.mget(segment_keys)
            for key, value_json in zip(segment_keys, segment_values):
                 if value_json:
                        try:
                            segment_id = key.split(":")[-1]
                            initial_state[segment_id] = json.loads(value_json)
                        except (json.JSONDecodeError, IndexError, TypeError):
                             pass # Bỏ qua nếu parse lỗi
        return {"type": "initial_state", "data": initial_state}
    except Exception as e:
        print(f"Error fetching initial state from Redis: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch initial state from Redis")


# --- Endpoint Lịch sử ---
@app.get("/api/traffic/historical/summary")
async def get_traffic_history(
    segment_id: Optional[str] = Query(None, description="Filter by segment ID (e.g., segment_1)"),
    start_time: datetime = Query(..., description="Start time in ISO format (e.g., 2025-04-18T02:00:00Z)"),
    end_time: datetime = Query(..., description="End time in ISO format (e.g., 2025-04-18T03:00:00Z)"),
    agg_level: str = Query("minute", enum=["minute", "hour", "day"], description="Aggregation level")
) -> List[Dict[str, Any]]:
    """
    Truy vấn dữ liệu lịch sử giao thông tổng hợp từ ClickHouse.
    """
    if not manager.clickhouse_client or not manager.clickhouse_client.ping():
         # Thử kết nối lại một lần
         print("ClickHouse client unavailable, attempting to reconnect...")
         manager.connect_clickhouse()
         if not manager.clickhouse_client or not manager.clickhouse_client.ping():
              raise HTTPException(status_code=503, detail="ClickHouse service unavailable")

    # --- Xây dựng câu truy vấn SQL ---
    # Chọn các cột cần lấy
    select_columns = [
        f"toStartOf{agg_level}(window_start) AS agg_time", # Hàm CH group theo giờ/phút/ngày
        "segment_id",
        "status", # Có thể lấy status phổ biến nhất hoặc avg speed
        "round(avg(avg_speed), 2) AS avg_speed_agg",
        "sum(vehicle_count) AS total_vehicles"
    ]
    # Bảng nguồn
    from_table = f"{CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}"
    # Điều kiện WHERE
    where_conditions = ["window_start >= %(start_time)s", "window_end <= %(end_time)s"]
    params = {'start_time': start_time, 'end_time': end_time}
    if segment_id:
        where_conditions.append("segment_id = %(segment_id)s")
        params['segment_id'] = segment_id
    # Group By
    group_by_columns = [f"toStartOf{agg_level}(window_start)", "segment_id", "status"]
    # Order By
    order_by_columns = ["agg_time", "segment_id"]

    query = f"""
    SELECT {', '.join(select_columns)}
    FROM {from_table}
    WHERE {' AND '.join(where_conditions)}
    GROUP BY {', '.join(group_by_columns)}
    ORDER BY {', '.join(order_by_columns)}
    """

    print(f"Executing ClickHouse Query:\n{query}\nParams: {params}")

    try:
        result = manager.clickhouse_client.query(query, parameters=params)
        # Chuyển kết quả thành list of dicts
        history_data = [dict(zip(result.column_names, row)) for row in result.result_rows]
        print(f"Query returned {len(history_data)} rows.")
        return history_data
    except Exception as e:
        print(f"Error querying ClickHouse: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to query traffic history: {e}")


# --- WebSocket Endpoint ---
@app.websocket("/ws/traffic")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect_websocket(websocket)
    try:
        # Giữ kết nối để nhận broadcast
        while True:
            # Đợi message từ client (không cần xử lý gì ở đây)
            # Hoặc đợi cho đến khi client ngắt kết nối
            await websocket.receive_text()
    except WebSocketDisconnect:
        print(f"WebSocket client {websocket.client} disconnected (gracefully).")
        manager.disconnect_websocket(websocket)
    except Exception as e:
        # Bắt các lỗi khác như ConnectionClosedError
        print(f"WebSocket Error or client {websocket.client} disconnected abruptly: {e}")
        manager.disconnect_websocket(websocket)