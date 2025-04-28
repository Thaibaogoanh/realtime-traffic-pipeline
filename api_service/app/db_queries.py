# api_service/app/db_queries.py
import os
from datetime import datetime
from typing import List, Optional, Dict, Any
import clickhouse_connect

# --- Lấy thông tin kết nối (có thể dùng Pydantic Settings) ---
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "traffic_db")
CLICKHOUSE_TABLE = os.environ.get("CLICKHOUSE_TABLE", "traffic_events")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD")

# --- Client connection (nên quản lý trong main.py hoặc dùng dependency injection) ---
# Ví dụ này tạo client mới mỗi lần gọi, không tối ưu cho production
def get_clickhouse_client():
     try:
         client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                database=CLICKHOUSE_DB,
                user=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD
            )
         client.ping()
         return client
     except Exception as e:
          print(f"Failed to connect to ClickHouse in db_queries: {e}")
          return None

def get_traffic_history_from_db(
    segment_id: Optional[str],
    start_time: datetime,
    end_time: datetime,
    agg_level: str
) -> List[Dict[str, Any]]:
    """
    Thực hiện truy vấn lấy dữ liệu lịch sử từ ClickHouse.
    """
    client = get_clickhouse_client()
    if not client:
        # Có thể raise exception ở đây để main.py xử lý
        return {"error": "ClickHouse connection failed"}

    # --- Xây dựng câu truy vấn (giống như trong main.py) ---
    select_columns = [
        f"toStartOf{agg_level}(window_start) AS agg_time",
        "segment_id",
        "status",
        "round(avg(avg_speed), 2) AS avg_speed_agg",
        "sum(vehicle_count) AS total_vehicles"
    ]
    from_table = f"{CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}"
    where_conditions = ["window_start >= %(start_time)s", "window_end <= %(end_time)s"]
    params = {'start_time': start_time, 'end_time': end_time}
    if segment_id:
        where_conditions.append("segment_id = %(segment_id)s")
        params['segment_id'] = segment_id
    group_by_columns = [f"toStartOf{agg_level}(window_start)", "segment_id", "status"]
    order_by_columns = ["agg_time", "segment_id"]

    query = f"""
    SELECT {', '.join(select_columns)}
    FROM {from_table}
    WHERE {' AND '.join(where_conditions)}
    GROUP BY {', '.join(group_by_columns)}
    ORDER BY {', '.join(order_by_columns)}
    """

    try:
        result = client.query(query, parameters=params)
        history_data = [dict(zip(result.column_names, row)) for row in result.result_rows]
        return history_data
    except Exception as e:
        print(f"Error querying ClickHouse in db_queries: {e}")
        # Raise exception hoặc trả về lỗi
        raise ConnectionError(f"Failed to query traffic history: {e}") # Ví dụ
    finally:
        if client:
            client.close()