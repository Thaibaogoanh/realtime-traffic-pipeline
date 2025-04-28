# Cell 1: Import thư viện và lấy biến môi trường
import os
import json
import math
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count, udf, lit, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, FloatType, IntegerType

# --- Lấy cấu hình từ Biến Môi trường ---
# Lưu ý: Các biến này được set trong docker-compose.yml cho service spark-jupyter
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafk-1:9092,kafk-2:9092,kafk-3:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw_traffic_data")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse-server")
CLICKHOUSE_NATIVE_PORT = int(os.environ.get("CLICKHOUSE_NATIVE_PORT", 9000))
CLICKHOUSE_DB = "traffic_db"
CLICKHOUSE_TABLE = "traffic_events"

print(f"Kafka Brokers: {KAFKA_BROKERS}")
print(f"Kafka Topic: {KAFKA_TOPIC}")
print(f"Redis Host: {REDIS_HOST}:{REDIS_PORT}")
print(f"ClickHouse Host: {CLICKHOUSE_HOST}:{CLICKHOUSE_NATIVE_PORT}")

# --- Schema cho dữ liệu JSON từ Kafka ---
schema = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("speed", DoubleType(), True),
    StructField("segment_id_actual", StringType(), True) # Giữ lại để tham khảo/debug
])

# --- Dữ liệu đoạn đường tĩnh (Ví dụ - Nên tải từ file hoặc nguồn khác) ---
SEGMENTS_COORDS = {
    "segment_1": (10.98, 106.75, "Ngã tư 550 Area"), # lat, lon, name
    "segment_2": (10.94, 106.81, "KCN Sóng Thần Area"),
    "segment_3": (10.96, 106.78, "Trung tâm Dĩ An Area")
}
# Có thể tạo Spark DataFrame từ đây để join nếu cần làm giàu dữ liệu
# segments_sdf = spark.createDataFrame([(k, v[0], v[1], v[2]) for k, v in SEGMENTS_COORDS.items()], ["segment_id", "lat", "lon", "name"])
# segments_sdf.show()

# --- Các hàm tiện ích (Có thể tách ra utils.py) ---
def haversine(lon1, lat1, lon2, lat2):
    # (Giữ nguyên hàm haversine từ trước)
    R = 6371 # km
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    a = math.sin(dLat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dLon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance

def find_nearest_segment(lat, lon):
    # (Giữ nguyên hàm map matching đơn giản)
    min_dist = float('inf')
    nearest_segment = "unknown"
    if lat is None or lon is None:
        return nearest_segment

    for segment_id, (seg_lat, seg_lon, _) in SEGMENTS_COORDS.items(): # Lấy tọa độ từ dict
        dist = haversine(lon, lat, seg_lon, seg_lat)
        # Ngưỡng khoảng cách tối đa để map (ví dụ: 1km)
        if dist < min_dist and dist < 1.0:
            min_dist = dist
            nearest_segment = segment_id
    return nearest_segment

find_nearest_segment_udf = udf(find_nearest_segment, StringType())

def classify_status(avg_speed):
    # (Giữ nguyên hàm phân loại)
    if avg_speed is None:
        return "unknown"
    elif avg_speed > 45: # Điều chỉnh ngưỡng tốc độ
        return "clear"
    elif avg_speed > 20:
        return "slow"
    else:
        return "congested"

classify_status_udf = udf(classify_status, StringType())

# Cell 2: Khởi tạo Spark Session
# Lưu ý: Trong môi trường jupyter/pyspark-notebook, thường đã có sẵn SparkSession tên là 'spark'
# Nếu không có hoặc muốn tùy chỉnh, bạn có thể tạo mới:

# from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#     .appName("JupyterTrafficProcessor") \
#     .master("local[*]") \ # Sử dụng biến môi trường SPARK_MASTER nếu có
#     # Thêm các cấu hình Spark nếu cần
#     # Ví dụ: cấu hình Kafka connector, S3 connector (nếu dùng MinIO)
#     # Đã cấu hình packages trong docker-compose command, không cần lặp lại ở đây thường lệ
#     # .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4,org.apache.hadoop:hadoop-aws:3.3.4") \
#     # Cấu hình S3A cho MinIO nếu ghi Parquet
#     # .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT")) \
#     # .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ACCESS_KEY")) \
#     # .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_SECRET_KEY")) \
#     # .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .getOrCreate()

# Kiểm tra xem biến 'spark' đã tồn tại chưa
if 'spark' not in locals():
    print("Creating new SparkSession...")
    spark = SparkSession.builder.appName("JupyterTrafficProcessor").getOrCreate()
else:
    print("Using existing SparkSession.")

# Set log level để giảm output không cần thiết
spark.sparkContext.setLogLevel("WARN")
print(f"Spark Session ready. Spark version: {spark.version}")

# Cell 3: Đọc dữ liệu từ Kafka
print(f"Reading stream from Kafka topic '{KAFKA_TOPIC}' at brokers '{KAFKA_BROKERS}'")

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \ # Bắt đầu từ dữ liệu mới nhất
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON và chọn các cột cần thiết, chuyển đổi kiểu dữ liệu
value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")) \
                   .select("data.*") \
                   .withColumn("event_time", col("timestamp").cast(TimestampType())) \
                   .withColumn("latitude", col("latitude").cast(DoubleType())) \
                   .withColumn("longitude", col("longitude").cast(DoubleType())) \
                   .withColumn("speed", col("speed").cast(FloatType())) \
                   .filter(col("latitude").isNotNull() & col("longitude").isNotNull() & col("speed").isNotNull()) # Lọc dữ liệu null cơ bản

print("Kafka stream schema:")
value_df.printSchema()

# Cell 4: Xử lý dữ liệu (Map Matching, Aggregation, Classification)

# 1. Map Matching
mapped_df = value_df.withColumn("segment_id", find_nearest_segment_udf(col("latitude"), col("longitude"))) \
                    .filter(col("segment_id") != "unknown") # Chỉ giữ lại các điểm map được

# (Tùy chọn) Thêm Data Quality Check (ví dụ lọc tốc độ bất thường)
# mapped_df = mapped_df.filter((col("speed") > 0) & (col("speed") < 150))

# (Tùy chọn) Enrichment - Join với dữ liệu tĩnh về segment (nếu có segments_sdf)
# enriched_df = mapped_df.join(segments_sdf, "segment_id", "left_outer")

# 2. Windowing & Aggregation
# Sử dụng df đã map (hoặc enriched nếu có)
agg_df = mapped_df \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute", "30 seconds").alias("time_window"), # Cửa sổ 1 phút, trượt 30s
        col("segment_id")
    ) \
    .agg(
        avg("speed").alias("avg_speed"),
        count("*").alias("vehicle_count")
    )

# 3. Phân loại trạng thái
status_df = agg_df.withColumn("status", classify_status_udf(col("avg_speed"))) \
                  .select(
                      col("segment_id"),
                      col("time_window.start").alias("window_start"),
                      col("time_window.end").alias("window_end"),
                      col("avg_speed").cast(FloatType()), # Đảm bảo đúng kiểu dữ liệu
                      col("vehicle_count").cast(IntegerType()), # Đảm bảo đúng kiểu dữ liệu
                      col("status")
                   )


print("Aggregated stream schema:")
status_df.printSchema()

# Cell 5: Ghi dữ liệu vào Redis và ClickHouse (Dùng foreachBatch)

# --- Hàm ghi vào Redis ---
def write_to_redis(df, epoch_id):
    # Sử dụng df đã collect(), không nên collect trong production với dữ liệu lớn
    # Nên dùng df.foreachPartition thay thế để xử lý song song
    # Ví dụ này dùng collect() cho đơn giản trong notebook
    if df.isEmpty():
        return

    import redis # import bên trong để tránh lỗi serialization

    print(f"--- Redis Write (Epoch: {epoch_id}) ---")
    r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    pipe = r.pipeline()
    try:
        # Chỉ lấy bản ghi mới nhất cho mỗi segment trong batch này
        # Dùng RDD để xử lý linh hoạt hơn
        latest_updates = df.rdd \
            .map(lambda row: (row.segment_id, row)) \
            .reduceByKey(lambda row1, row2: row1 if row1.window_end > row2.window_end else row2) \
            .map(lambda item: item[1]) \
            .collect()

        for row in latest_updates:
            segment_id = row.segment_id
            state = {
                "avg_speed": round(row.avg_speed, 2) if row.avg_speed else None,
                "vehicle_count": row.vehicle_count,
                "status": row.status,
                "window_end": row.window_end.isoformat()
            }
            key = f"segment:{segment_id}"
            value = json.dumps(state)
            print(f"  Updating Redis: {key} -> {value}")
            pipe.set(key, value)
        pipe.execute()
    except Exception as e:
        print(f"  Error writing to Redis: {e}")

# --- Hàm ghi vào ClickHouse ---
def write_to_clickhouse(df, epoch_id):
    if df.isEmpty():
        return

    # Sử dụng thư viện clickhouse-connect (hoặc clickhouse-driver)
    import clickhouse_connect # import bên trong

    print(f"--- ClickHouse Write (Epoch: {epoch_id}) ---")
    try:
        # Lấy dữ liệu dưới dạng list of tuples/dicts
        # Cần đảm bảo tên cột và thứ tự khớp với bảng ClickHouse
        data_to_insert = df.select(
            "segment_id",
            "window_start",
            "window_end",
            "avg_speed",
            "vehicle_count",
            "status"
            # processing_time sẽ được ClickHouse tự thêm DEFAULT now()
        ).collect() # Collect() không tốt cho production lớn!

        if not data_to_insert:
            print("  No data to insert.")
            return

        # Chuyển đổi Row thành list/tuple nếu cần bởi client
        # clickhouse-connect có thể nhận list of lists hoặc list of dicts
        data_list = [list(row) for row in data_to_insert]
        # Hoặc data_dict = [row.asDict() for row in data_to_insert]


        # Kết nối và insert
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_NATIVE_PORT, # Dùng port Native TCP
            database=CLICKHOUSE_DB
            # Thêm username/password nếu ClickHouse có yêu cầu
        )
        print(f"  Inserting {len(data_list)} rows into ClickHouse table '{CLICKHOUSE_TABLE}'...")
        # Tên cột cần khớp chính xác thứ tự trong data_list
        client.insert(CLICKHOUSE_TABLE, data_list,
                      column_names=['segment_id', 'window_start', 'window_end', 'avg_speed', 'vehicle_count', 'status'])
        print(f"  Successfully inserted {len(data_list)} rows.")
        client.close()

    except Exception as e:
        print(f"  Error writing to ClickHouse: {e}")
        # Có thể thêm logic retry hoặc ghi log chi tiết hơn


# --- Khởi chạy Streaming Queries ---
# Output Mode 'update' thường dùng cho foreachBatch khi không có aggregation hoàn chỉnh
# Hoặc 'complete' nếu aggregation cho phép (ví dụ: count toàn bộ)
# 'append' nếu chỉ ghi dữ liệu mới không cập nhật state cũ

# Query ghi vào Redis và ClickHouse
combined_write_query = status_df.writeStream \
    .outputMode("update") # Hoặc "complete" nếu aggregation trả về state đầy đủ
    .option("checkpointLocation", "/tmp/spark-checkpoints/combined_write") # Rất quan trọng!
    .foreachBatch(lambda df, epoch_id: [write_to_redis(df, epoch_id), write_to_clickhouse(df, epoch_id)]) \
    .start()


# (Tùy chọn) Query ghi ra console để debug
# console_query = status_df.writeStream \
#     .outputMode("update") \
#     .format("console") \
#     .option("truncate", "false") \
#     .option("numRows", 50) \
#     .start()

print("Streaming queries started. Check Spark UI (if accessible) and output logs.")
# Trong Jupyter, query sẽ chạy ngầm. Để dừng, bạn cần dùng:
# combined_write_query.stop()
# console_query.stop()