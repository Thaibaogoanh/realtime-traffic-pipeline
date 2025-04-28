# api_service/app/schemas.py
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

class TrafficState(BaseModel):
    avg_speed: Optional[float]
    vehicle_count: Optional[int]
    status: Optional[str]
    window_end: Optional[datetime]

class InitialTrafficStateResponse(BaseModel):
    type: str = "initial_state"
    data: Dict[str, TrafficState]

class WebSocketTrafficUpdate(BaseModel):
     type: str = "traffic_update"
     data: Dict[str, TrafficState]

class TrafficHistoryRecord(BaseModel):
    agg_time: datetime
    segment_id: str
    status: str
    avg_speed_agg: float
    total_vehicles: int

    class Config:
        orm_mode = True # Giúp tương thích khi trả về từ ORM hoặc dict

# Có thể thêm các model khác cho request parameters nếu cần validation phức tạp hơn