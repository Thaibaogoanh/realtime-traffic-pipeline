import os
import json
import math
from datetime import datetime
import traceback
from typing import Optional

# Import thư viện cần thiết cho Pandas UDF
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, count, udf, lit, expr, pandas_udf, PandasUDFType, year, month, dayofmonth, hour
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, FloatType, IntegerType
from pyspark.sql.avro.functions import from_avro

# Import các thư viện cho kết nối bên ngoài (sẽ dùng trong foreachBatch)
# Mặc dù import ở đây, chúng sẽ được import lại bên trong hàm foreachBatch để tránh lỗi serialization
# import redis
# import clickhouse_connect

print("--- PySpark Script Dependencies Imported ---")

# --- Lấy cấu hình từ Biến Môi trường ---
# Các biến này sẽ được Airflow truyền vào khi chạy spark-submit
# Có giá trị mặc định phòng trường hợp chạy thử nghiệm cục bộ không qua Airflow
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS_INTERNAL", "kafka:9092") # Sửa lại nếu mặc định khác
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw_traffic_data")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse-server")
# CLICKHOUSE_NATIVE_PORT = int(os.environ.get("CLICKHOUSE_NATIVE_PORT", 9000)) # Không dùng port native nữa
CLICKHOUSE_HTTP_PORT = int(os.environ.get("CLICKHOUSE_PORT", 8123)) # Dùng port HTTP
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER") # Sẽ là None nếu không set
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD") # Sẽ là None nếu không set
CLICKHOUSE_DB = "traffic_db"
CLICKHOUSE_TABLE = "traffic_events"
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "traffic-data")
# Đường dẫn checkpoint trên S3/MinIO là bắt buộc cho production và fault tolerance
CHECKPOINT_LOCATION_BASE = os.environ.get("CHECKPOINT_LOCATION", f"s3a://{MINIO_BUCKET}/checkpoints")
# Đường dẫn file schema được mount vào container Airflow/Spark
AVRO_SCHEMA_FILE_PATH = os.environ.get("AVRO_SCHEMA_PATH_CONTAINER", "/opt/spark/schemas/raw_traffic_event.avsc")


# --- Dữ liệu đoạn đường tĩnh ---
SEGMENTS_COORDS = {
    "segment_1": (10.98, 106.75, "Ngã tư 550 Area"),
    "segment_2": (10.94, 106.81, "KCN Sóng Thần Area"),
    "segment_3": (10.96, 106.78, "Trung tâm Dĩ An Area")
}

# --- Hàm tiện ích ---
def haversine(lon1, lat1, lon2, lat2):
    """Tính khoảng cách Haversine giữa 2 điểm (kinh/vĩ độ)."""
    R = 6371 # km
    try:
        dLat = math.radians(lat2 - lat1)
        dLon = math.radians(lon2 - lon1)
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        a = math.sin(dLat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dLon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = R * c
    except Exception:
        # Trả về giá trị lớn nếu có lỗi tính toán (vd: input không hợp lệ)
        return float('inf')
    return distance

# --- Hàm phân loại trạng thái (UDF thường) ---
@udf(StringType()) # Dùng decorator cho gọn
def classify_status_udf(avg_speed: Optional[float]) -> Optional[str]:
    """Phân loại trạng thái giao thông dựa trên tốc độ trung bình."""
    if avg_speed is None or pd.isna(avg_speed): return None  # Trả về None nếu không xác định (khớp với Optional[str])
    elif avg_speed > 45: return "clear"
    elif avg_speed > 20: return "slow"
    else: return "congested"


# --- Các hàm ghi dữ liệu cho foreachBatch ---

def write_to_redis(df, epoch_id):
    """Ghi trạng thái mới nhất của từng segment vào Redis."""
    print(f">>> ENTERING write_to_redis (Epoch: {epoch_id})")
    if df.isEmpty():
        print(f"    Redis (Epoch: {epoch_id}): DataFrame is empty. Skipping.")
        return
    import redis # Import bên trong hàm
    import pandas # Import bên trong nếu cần (thường không cần cho đoạn này)

    print(f"--- Redis Write Start (Epoch: {epoch_id}) ---")
    start_time = time.time()
    processed_count = 0
    try:
        # Tối ưu: Chỉ lấy bản ghi có window_end mới nhất cho mỗi segment_id trong batch này
        # Chuyển sang Pandas để dùng last() hiệu quả hơn RDD reduceByKey/collect
        pandas_df = df.toPandas()
        latest_updates = pandas_df.loc[pandas_df.groupby('segment_id')['window_end'].idxmax()]

        if latest_updates.empty:
            print(f"  No latest updates found in batch {epoch_id} for Redis.")
            return

        r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        r.ping() # Check connection
        pipe = r.pipeline()

        for _, row in latest_updates.iterrows():
            segment_id = row["segment_id"]
            state = {
                # Xử lý giá trị NaN có thể có từ aggregation
                "avg_speed": round(row["avg_speed"], 2) if pd.notna(row["avg_speed"]) else None,
                "vehicle_count": int(row["vehicle_count"]) if pd.notna(row["vehicle_count"]) else 0,
                "status": row["status"] if pd.notna(row["status"]) else "unknown",
                "window_end": row["window_end"].isoformat() if pd.notna(row["window_end"]) else None
            }
            key = f"segment:{segment_id}"
            value = json.dumps(state)
            pipe.set(key, value)
            processed_count += 1

        pipe.execute()
        end_time = time.time()
        print(f"  Finished Redis write for Epoch {epoch_id}. Updated {processed_count} segments in {end_time - start_time:.2f}s.")
    except ImportError:
        print("!!! ERROR: Redis library not found. Please install 'redis'.")
    except redis.exceptions.ConnectionError as ce:
        print(f"!!! REDIS CONNECTION ERROR (Epoch {epoch_id}): {ce}")
    except Exception as e:
        print(f"!!! ERROR writing Epoch {epoch_id} to Redis: {e}")
        traceback.print_exc()


def write_to_clickhouse(df, epoch_id):
    """Ghi dữ liệu tổng hợp của batch vào ClickHouse."""
    print(f">>> ENTERING write_to_clickhouse (Epoch: {epoch_id})")
    if df.isEmpty():
        print(f"    clickhouse (Epoch: {epoch_id}): DataFrame is empty. Skipping.")
        return
    import clickhouse_connect # Import bên trong
    import pandas # Import bên trong

    print(f"--- ClickHouse Write Start (Epoch: {epoch_id}) ---")
    start_time = time.time()
    # Chuyển đổi Spark DataFrame sang Pandas DataFrame
    pandas_df_to_insert = df.select(
        "segment_id", "window_start", "window_end", "avg_speed", "vehicle_count", "status"
    ).toPandas()

    if pandas_df_to_insert.empty:
        print(f"  No data to insert into ClickHouse for Epoch {epoch_id}.")
        return

    rows_to_insert = len(pandas_df_to_insert)
    print(f"  Attempting to insert {rows_to_insert} rows into ClickHouse table '{CLICKHOUSE_TABLE}'...")

    client = None
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_HTTP_PORT, # Luôn dùng port HTTP cho client này
            database=CLICKHOUSE_DB,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD
            # Thêm các cài đặt timeout nếu cần
            # settings={'connect_timeout': 10, 'send_receive_timeout': 300}
        )
        client.ping() # Check connection

        # Insert dữ liệu từ Pandas DataFrame
        # clickhouse-connect hỗ trợ insert trực tiếp từ pandas
        client.insert_df(CLICKHOUSE_TABLE, pandas_df_to_insert)

        end_time = time.time()
        print(f"  Successfully inserted {rows_to_insert} rows into ClickHouse for Epoch {epoch_id} in {end_time - start_time:.2f}s.")

    except ImportError:
        print("!!! ERROR: ClickHouse client library not found. Please install 'clickhouse-connect'.")
    except Exception as e:
        print(f"!!! ERROR writing Epoch {epoch_id} to ClickHouse: {e}")
        traceback.print_exc()
    finally:
        if client:
            client.close() # Đảm bảo đóng kết nối


def write_to_minio(df, epoch_id):
    """Ghi dữ liệu tổng hợp của batch ra MinIO dưới dạng Parquet."""
    print(f">>> ENTERING write_to_minio (Epoch: {epoch_id})")
    if df.isEmpty():
        print(f"    minio (Epoch: {epoch_id}): DataFrame is empty. Skipping.")
        return

    # Thêm cột year, month, day, hour để partition theo kiểu Hive
    # Cần import các hàm này từ pyspark.sql.functions
    try:
        df_to_write = df.withColumn("proc_dt", expr("date_trunc('hour', window_start)")) \
                        .withColumn("year", year(col("proc_dt"))) \
                        .withColumn("month", month(col("proc_dt"))) \
                        .withColumn("day", dayofmonth(col("proc_dt"))) \
                        .withColumn("hour", hour(col("proc_dt"))) \
                        .drop("proc_dt") # Bỏ cột tạm đi sau khi tạo cột phân vùng
    except Exception as e:
         # Nếu có lỗi khi thêm cột (ví dụ window_start bị null?), ghi vào thư mục lỗi
         print(f"  Warning: Could not add partition columns for MinIO write (Epoch {epoch_id}): {e}. Writing to fallback path.")
         output_path = f"s3a://{MINIO_BUCKET}/processed_traffic_errors/epoch={epoch_id}"
         df_to_write = df # Ghi dữ liệu gốc
         write_mode = "overwrite"
         partition_cols = [] # Không phân vùng
    else:
         # Đường dẫn gốc cho dữ liệu đã phân vùng
         output_path = f"s3a://{MINIO_BUCKET}/processed_traffic_partitioned"
         write_mode = "append" # Append vào các partition đã có
         partition_cols = ["year", "month", "day", "hour"]

    print(f"--- MinIO Write Start (Epoch: {epoch_id}) ---")
    start_time = time.time()
    # row_count = df_to_write.count() # .count() là action, không nên dùng trong foreachBatch
    print(f"  Attempting to write batch data to MinIO path: {output_path} (PartitionBy: {partition_cols or 'None'})")

    try:
        write_options = df_to_write.write \
            .format("parquet") \
            .mode(write_mode)

        if partition_cols:
            write_options = write_options.partitionBy(*partition_cols)

        write_options.save(output_path)

        end_time = time.time()
        # Không thể lấy count() dễ dàng, chỉ log thành công
        print(f"  Successfully wrote batch {epoch_id} to MinIO in {end_time - start_time:.2f}s.")

    except ImportError:
         print("!!! ERROR: boto3 library not found. Please install 'boto3'.")
    except Exception as e:
        print(f"!!! ERROR writing batch {epoch_id} to MinIO: {e}")
        traceback.print_exc()
        if hasattr(e, 'java_exception'):
             print(f"  Java Exception: {e.java_exception.toString()}")


# --- Hàm Chính ---
def main():
    """Hàm thực thi chính của ứng dụng Spark Streaming."""
    print("--- Starting Spark Streaming Traffic Processor ---")
    print(f"Reading from Kafka: {KAFKA_BROKERS} / Topic: {KAFKA_TOPIC}")
    # print(f"Schema Registry URL: {SCHEMA_REGISTRY_URL}") # Không dùng SR nữa
    print(f"Avro Schema File: {AVRO_SCHEMA_FILE_PATH}")
    print(f"Writing state to Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"Writing history to ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT} (DB: {CLICKHOUSE_DB})")
    print(f"Writing Parquet to MinIO Bucket: s3a://{MINIO_BUCKET}/")
    print(f"Checkpoint Location Base: {CHECKPOINT_LOCATION_BASE}")

    # --- Khởi tạo Spark Session ---
    # Cấu hình S3A và các cấu hình khác nên được truyền qua spark-submit
    # hoặc đặt trong spark-defaults.conf của image Spark/Airflow
    spark = SparkSession.builder \
        .appName("TrafficProcessorDE") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") # Giảm bớt log của Spark
    print(f"Spark Session created. Spark version: {spark.version}")
    print(f"Using PySpark version: {spark.sparkContext.version}") # In version PySpark thực tế

    # --- Broadcast dữ liệu SEGMENTS_COORDS ---
    print("Broadcasting segments data...")
    try:
        segments_broadcast = spark.sparkContext.broadcast(SEGMENTS_COORDS)
        print("Segments data broadcasted successfully.")
    except Exception as e:
        print(f"!!! FATAL ERROR: Failed to broadcast segment data: {e}")
        spark.stop()
        exit(1)

    # --- Định nghĩa Pandas UDF Map Matching bên trong main ---
    @pandas_udf(StringType(), PandasUDFType.SCALAR)
    def find_nearest_segment_pandas_udf(lat_series: pd.Series, lon_series: pd.Series) -> pd.Series:
        """Pandas UDF tìm segment gần nhất cho từng batch."""
        local_segments_data = segments_broadcast.value # Lấy dữ liệu broadcast
        results = []
        for lat, lon in zip(lat_series, lon_series):
            min_dist = float('inf')
            nearest_segment = "unknown"
            if pd.notna(lat) and pd.notna(lon): # Kiểm tra NaN bằng pandas
                for segment_id, (seg_lat, seg_lon, _) in local_segments_data.items():
                    dist = haversine(lon, lat, seg_lon, seg_lat) # Gọi hàm haversine (đã import)
                    if dist < min_dist and dist < 1.0: # Ngưỡng 1km
                        min_dist = dist
                        nearest_segment = segment_id
            results.append(nearest_segment)
        return pd.Series(results)
    print("Pandas UDF for map matching defined.")

    # --- Đọc Stream từ Kafka ---
    print("Setting up Kafka stream reader...")
    try:
        kafka_options = {
            "kafka.bootstrap.servers": KAFKA_BROKERS,
            "subscribe": KAFKA_TOPIC,
            "startingOffsets": "earliest",
            "failOnDataLoss": "false",
            # Thêm các option Kafka quan trọng khác nếu cần
            # "kafka.group.id": "spark-streaming-traffic-group", # Đặt group id
            # "maxOffsetsPerTrigger": "10000" # Giới hạn số lượng message mỗi batch
        }
        print(f"Kafka Read Options: {kafka_options}")
        kafka_df = spark.readStream \
            .format("kafka") \
            .options(**kafka_options) \
            .load()
        print("Kafka stream reader loaded.")
    except Exception as e:
        print(f"!!! FATAL ERROR: Failed to load Kafka stream reader: {e}")
        traceback.print_exc()
        spark.stop()
        exit(1)

    # --- Đọc schema Avro từ file ---
    avro_schema_str = None
    try:
        print(f"Reading Avro schema from file: {AVRO_SCHEMA_FILE_PATH}")
        with open(AVRO_SCHEMA_FILE_PATH, 'r') as f:
            avro_schema_str = f.read()
        print(f"Successfully read Avro schema string from file.")
    except Exception as e:
         print(f"!!! FATAL ERROR: Could not read Avro schema file {AVRO_SCHEMA_FILE_PATH}: {e}")
         raise e # Ném lỗi để Airflow biết

    # --- Deserialize Avro dùng schema string từ file ---
    print("Setting up Avro deserialization...")
    try:
        deserialized_df = kafka_df.select(
            # Thử dùng PERMISSIVE để xem có message lỗi không, nếu ổn định thì chuyển lại FAILFAST
            from_avro(col("value"), avro_schema_str, options={"mode": "PERMISSIVE"}).alias("data")
            # Tạo cột _corrupt_record nếu dùng PERMISSIVE
            # from_avro(col("value"), avro_schema_str, options={"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}).alias("data")
        ).select("data.*") #, "_corrupt_record") # Lấy cả cột lỗi nếu có
        print("Avro deserialization step defined.")
    except Exception as e:
         print(f"!!! FATAL ERROR: Failed to set up Avro deserialization: {e}")
         traceback.print_exc()
         spark.stop()
         exit(1)

    # --- Áp dụng các bước xử lý ---
    print("Defining data processing steps...")
    # Filter bản ghi lỗi nếu dùng PERMISSIVE
    # parsed_df = deserialized_df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
    parsed_df = deserialized_df # Giả sử FAILFAST hoặc không cần xử lý lỗi phức tạp

    value_df = parsed_df \
                   .withColumn("event_time", col("timestamp").cast(TimestampType())) \
                   .withColumn("latitude", col("latitude").cast(DoubleType())) \
                   .withColumn("longitude", col("longitude").cast(DoubleType())) \
                   .withColumn("speed", col("speed").cast(FloatType())) \
                   .filter(col("event_time").isNotNull() & # Thêm kiểm tra event_time
                           col("latitude").isNotNull() & col("longitude").isNotNull() & col("speed").isNotNull())

    # Sử dụng Pandas UDF
    mapped_df = value_df.withColumn(
                        "segment_id",
                        find_nearest_segment_pandas_udf(col("latitude"), col("longitude"))
                   ) \
                   .filter(col("segment_id") != "unknown")

    # Aggregation & Classification
    agg_df = mapped_df \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(window(col("event_time"), "1 minute", "30 seconds").alias("time_window"), col("segment_id")) \
        .agg(avg("speed").alias("avg_speed"), count("*").alias("vehicle_count"))

    status_df = agg_df.withColumn("status", classify_status_udf(col("avg_speed"))) \
                      .select(
                          col("segment_id"),
                          col("time_window.start").alias("window_start"),
                          col("time_window.end").alias("window_end"),
                          col("avg_speed").cast(FloatType()),
                          col("vehicle_count").cast(IntegerType()),
                          col("status")
                       )
    print("Data processing steps defined.")

    # --- Ghi kết quả ---
    query_name = "traffic_processing_pipeline_debug2" # Đổi tên để dùng checkpoint mới
    checkpoint_location = f"{CHECKPOINT_LOCATION_BASE}/{query_name}"
    print(f"Using checkpoint location: {checkpoint_location}")

    print("Starting streaming query...")
    streaming_query = status_df.writeStream \
        .queryName(query_name) \
        .outputMode("update") \
        .option("checkpointLocation", checkpoint_location) \
        .foreachBatch(lambda df, epoch_id: [
            # Gọi các hàm ghi theo thứ tự mong muốn
            # Có thể thêm kiểm tra lỗi và log chi tiết hơn trong các hàm này
            print(f"--- foreachBatch START (Epoch: {epoch_id}), Batch size: {df.count()} ---"),
            write_to_redis(df, epoch_id),
            write_to_clickhouse(df, epoch_id),
            write_to_minio(df, epoch_id),
            print(f"--- foreachBatch END (Epoch: {epoch_id}) ---")
         ]) \
        .trigger(processingTime='90 seconds') \
        .start()

    print(f"Streaming query '{query_name}' started successfully.")
    # Giữ cho ứng dụng chạy mãi mãi cho đến khi bị dừng từ bên ngoài (ví dụ: Airflow hủy task)
    streaming_query.awaitTermination()
    print(f"Streaming query '{query_name}' terminated.")


if __name__ == "__main__":
    # Hàm haversine được import từ utils
    # Các hàm ghi được định nghĩa ở global scope
    main()