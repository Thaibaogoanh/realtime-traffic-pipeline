from __future__ import annotations

import pendulum
import os # Import os nếu muốn dùng os.environ.get cho giá trị mặc định

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# --- Có thể lấy giá trị mặc định ở đây nếu cần, ví dụ cho checkpoint ---
# Biến môi trường MINIO_BUCKET sẽ được đọc trực tiếp trong bash_command
# CHECKPOINT_LOCATION = f"s3a://{os.environ.get('MINIO_BUCKET', 'traffic-data')}/checkpoints_airflow/traffic_processing_pipeline"


# --- Định nghĩa DAG ---
with DAG(
    dag_id='traffic_data_pipeline',
    schedule=None, # Chạy thủ công
    start_date=pendulum.datetime(2025, 4, 25, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=['traffic', 'streaming', 'spark', 'kafka', 'avro'],
    doc_md="""
    ### Traffic Data Processing Pipeline v2 (Avro, Airflow, MinIO, ClickHouse)

    Orchestrates the Spark Streaming job for processing real-time traffic data.
    Reads configurations directly from environment variables set in Docker Compose.
    - Reads Avro data from Kafka.
    - Performs map matching using Pandas UDF.
    - Writes latest state to Redis.
    - Writes historical aggregates to ClickHouse.
    - Writes processed data as Parquet to MinIO.
    """,
) as dag:

    # Task dùng BashOperator để gọi spark-submit
    submit_spark_job = BashOperator(
        task_id='submit_traffic_processor_spark_job',
        # --- SỬA LẠI bash_command ---
        bash_command="""
            echo "--- Checking Line Endings for processor.py ---"
            cat -e /opt/spark/app/processor.py | head -n 10
            echo "--- Finished Checking Line Endings ---"
            echo "Setting PYTHONPATH..."
            PYTHON_USER_SITE="/home/airflow/.local/lib/python3.10/site-packages"
            export PYTHONPATH="${PYTHON_USER_SITE}:${PYTHONPATH:-}"
            echo "Using PYTHONPATH=$PYTHONPATH"
            echo "Exporting other environment variables..."
            export KAFKA_BROKERS_INTERNAL="$KAFKA_BROKERS_INTERNAL" && \\
            export KAFKA_TOPIC="$KAFKA_TOPIC" && \\
            export SCHEMA_REGISTRY_URL="$SCHEMA_REGISTRY_URL" && \\
            export REDIS_HOST="$REDIS_HOST" && \\
            export CLICKHOUSE_HOST="$CLICKHOUSE_HOST" && \\
            export CLICKHOUSE_NATIVE_PORT="$CLICKHOUSE_NATIVE_PORT" && \\
            export MINIO_ENDPOINT="$MINIO_ENDPOINT" && \\
            export MINIO_ACCESS_KEY="$MINIO_ACCESS_KEY" && \\
            export MINIO_SECRET_KEY="$MINIO_SECRET_KEY" && \\
            export MINIO_BUCKET="$MINIO_BUCKET" && \\
            CHECKPOINT_LOCATION_VAR="s3a://${MINIO_BUCKET:-traffic-data}/checkpoints_airflow/traffic_processing_pipeline" && \\
            export CHECKPOINT_LOCATION="$CHECKPOINT_LOCATION_VAR" && \\
            export PYSPARK_PYTHON=/usr/local/bin/python3.10 && \\
            echo "--- Running Spark Submit ---" && \\
            spark-submit \\
            --master local[*] \\
            --deploy-mode client \\
            --conf spark.sql.streaming.schemaInference=true \\
            --conf spark.hadoop.fs.s3a.endpoint="$MINIO_ENDPOINT" \\
            --conf spark.hadoop.fs.s3a.access.key="$MINIO_ACCESS_KEY" \\
            --conf spark.hadoop.fs.s3a.secret.key="$MINIO_SECRET_KEY" \\
            --conf spark.hadoop.fs.s3a.path.style.access=true \\
            --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \\
            --conf spark.sql.avro.schemaRegistryUrl="$SCHEMA_REGISTRY_URL" \\
            /opt/spark/app/processor.py
            """,)

# DAG này chỉ có 1 task là submit_spark_job