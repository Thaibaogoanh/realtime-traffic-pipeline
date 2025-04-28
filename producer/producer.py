import time
import json
import random
# from kafka import KafkaProducer, KafkaAdminClient # Bỏ thư viện cũ
# from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError
from confluent_kafka.admin import AdminClient, NewTopic # Dùng confluent_kafka
from confluent_kafka.error import KafkaException
from confluent_kafka import SerializingProducer # Dùng SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer # Key thường là String
from datetime import datetime
import os
import socket # Để lấy hostname làm client.id

# --- Cấu hình ---
KAFKA_BROKERS_LIST_HOST = os.environ.get('KAFKA_BROKERS_HOST', 'localhost:29092,localhost:39092,localhost:49092')
SCHEMA_REGISTRY_URL = os.environ.get('SCHEMA_REGISTRY_URL_HOST', 'http://localhost:8081') # URL Schema Registry từ host
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'raw_traffic_data')
AVRO_SCHEMA_PATH = os.environ.get('AVRO_SCHEMA_PATH', '../schemas/raw_traffic_event.avsc') # Đường dẫn tương đối đến schema
RECONNECT_DELAY_SECONDS = 10

# --- Dữ liệu mô phỏng (Giữ nguyên) ---
SEGMENTS = { # ... (Giữ nguyên) ...
    "segment_1": {"lat": 10.98, "lon": 106.75},
    "segment_2": {"lat": 10.94, "lon": 106.81},
    "segment_3": {"lat": 10.96, "lon": 106.78}
}
VEHICLE_IDS = [f"xe_{i:03d}" for i in range(20)]

def load_avro_schema(schema_path):
    """Đọc schema Avro từ file."""
    try:
        with open(schema_path, 'r') as f:
            schema_str = f.read()
        print(f"Avro schema loaded successfully from {schema_path}")
        return schema_str
    except FileNotFoundError:
        print(f"ERROR: Avro schema file not found at {schema_path}")
        exit(1)
    except Exception as e:
        print(f"ERROR: Failed to read Avro schema: {e}")
        exit(1)

def generate_mock_data():
    # (Hàm generate_mock_data giữ nguyên)
    vehicle_id = random.choice(VEHICLE_IDS)
    segment_id = random.choice(list(SEGMENTS.keys()))
    segment_coords = SEGMENTS[segment_id]
    lat = segment_coords["lat"] + random.uniform(-0.005, 0.005)
    lon = segment_coords["lon"] + random.uniform(-0.005, 0.005)
    speed = random.uniform(5, 70)
    timestamp = datetime.now().isoformat() # Giữ ISO format cho đơn giản, Spark sẽ parse sau

    # Dữ liệu trả về phải khớp với schema Avro
    return {
        "vehicle_id": vehicle_id,
        "timestamp": timestamp,
        "latitude": lat,
        "longitude": lon,
        "speed": speed,
        "segment_id_actual": segment_id
    }

def delivery_report(err, msg):
    """ Callback được gọi khi message được gửi thành công hoặc thất bại."""
    if err is not None:
        print(f"Message delivery failed for key {msg.key()}: {err}")
    else:
        # In log ít hơn để tránh spam console
        # print(f'Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')
        pass

def create_avro_producer(brokers, schema_registry_url, avro_schema_str):
    """Tạo Avro Producer kết nối Schema Registry."""
    producer = None
    while producer is None:
        try:
            schema_registry_conf = {'url': schema_registry_url}
            schema_registry_client = SchemaRegistryClient(schema_registry_conf)

            # Value serializer dùng Avro
            value_serializer = AvroSerializer(schema_registry_client, avro_schema_str)
            # Key serializer thường là String
            key_serializer = StringSerializer('utf_8')

            producer_conf = {
                'bootstrap.servers': brokers,
                'client.id': socket.gethostname(),
                'key.serializer': key_serializer,
                'value.serializer': value_serializer,
                # 'acks': 'all', # Đảm bảo an toàn hơn
                # 'enable.idempotence': 'true' # Chống trùng lặp message
            }
            producer = SerializingProducer(producer_conf)
            print(f"Avro Producer connected successfully to: {brokers}")
            print(f"Schema Registry URL: {schema_registry_url}")
        except KafkaException as e:
            print(f"KafkaException creating Avro Producer: {e}. Retrying in {RECONNECT_DELAY_SECONDS} seconds...")
            time.sleep(RECONNECT_DELAY_SECONDS)
        except Exception as e:
            print(f"Error creating Avro Producer: {e}. Retrying in {RECONNECT_DELAY_SECONDS} seconds...")
            time.sleep(RECONNECT_DELAY_SECONDS)
    return producer

def create_topic_if_not_exists(brokers, topic_name, num_partitions=3, replication_factor=3, retries=5, delay=5):
    """Tạo topic dùng confluent_kafka AdminClient."""
    print(f"Checking/Creating topic '{topic_name}'...")
    conf = {'bootstrap.servers': brokers}
    admin_client = AdminClient(conf)

    for attempt in range(retries):
        try:
            # Kiểm tra topic tồn tại
            metadata = admin_client.list_topics(timeout=10)
            if topic_name in metadata.topics:
                print(f"Topic '{topic_name}' already exists.")
                return True

            # Tạo topic mới
            new_topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            # Create topics operation is asynchronous, initiating it returns Futures.
            fs = admin_client.create_topics([new_topic])

            # Wait for each operation to finish.
            for topic, f in fs.items():
                f.result()  # The result itself is None, but raises on error
                print(f"Topic '{topic}' created.")
            return True

        except KafkaException as e:
            # Kiểm tra xem lỗi có phải là TOPIC_ALREADY_EXISTS không
            if e.args[0].code() == KafkaException.TOPIC_ALREADY_EXISTS:
                 print(f"Topic '{topic_name}' already exists (detected during creation).")
                 return True
            else:
                 print(f"Attempt {attempt+1}/{retries}: KafkaException: {e}. Retrying in {delay} seconds...")
                 time.sleep(delay)
        except Exception as e:
            print(f"Attempt {attempt+1}/{retries}: Error interacting with Kafka Admin client: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)

    print(f"Failed to create/verify topic '{topic_name}' after {retries} attempts.")
    return False


if __name__ == "__main__":
    print(f"--- Traffic Data Avro Producer ---")
    print(f"Target Kafka Brokers (Host): {KAFKA_BROKERS_LIST_HOST}")
    print(f"Schema Registry URL (Host): {SCHEMA_REGISTRY_URL}")
    print(f"Target Kafka Topic: {KAFKA_TOPIC}")
    print(f"Avro Schema Path: {AVRO_SCHEMA_PATH}")

    # Load Avro schema
    avro_schema_str = load_avro_schema(AVRO_SCHEMA_PATH)

    # Tạo topic nếu chưa có
    if not create_topic_if_not_exists(KAFKA_BROKERS_LIST_HOST, KAFKA_TOPIC):
         print("Exiting producer as topic could not be verified/created.")
         exit(1)

    # Tạo producer
    producer = create_avro_producer(KAFKA_BROKERS_LIST_HOST, SCHEMA_REGISTRY_URL, avro_schema_str)

    print(f"\nStarting data generation. Press Ctrl+C to stop.")
    message_count = 0
    try:
        while True:
            data = generate_mock_data()
            # Key cho message, ví dụ dùng vehicle_id
            message_key = data["vehicle_id"]

            # Gửi message (value là dict khớp schema, key là string)
            # Callback delivery_report sẽ được gọi sau
            producer.produce(topic=KAFKA_TOPIC, key=message_key, value=data, on_delivery=delivery_report)

            message_count += 1
            # Poll để trigger callback và xử lý lỗi (quan trọng!)
            producer.poll(0)

            if message_count % 500 == 0: # Log và flush thường xuyên hơn
                 print(f"Produced {message_count} messages...")
                 producer.flush(timeout=5) # Đợi tối đa 5s để gửi hết

            time.sleep(random.uniform(0.05, 0.2)) # Gửi nhanh hơn nữa

    except KeyboardInterrupt:
        print("\nStopping producer...")
    except Exception as e:
        print(f"\nAn error occurred during production: {e}")
    finally:
        if producer:
            print(f"Flushing remaining messages ({len(producer)})...")
            # Đợi cho tất cả message được gửi đi hoặc timeout
            remaining = producer.flush(timeout=30)
            if remaining > 0:
                 print(f"WARNING: {remaining} messages may not have been sent.")
            print("Producer closed.")