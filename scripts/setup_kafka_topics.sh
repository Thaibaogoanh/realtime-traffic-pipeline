#!/bin/bash

# Địa chỉ một trong các Kafka broker (listener nội bộ)
BOOTSTRAP_SERVER="kafka:9092"
# Container Kafka để chạy lệnh (ví dụ: kafk-1)
KAFKA_CONTAINER="kafk-1"

echo "Waiting for Kafka broker ($KAFKA_CONTAINER) to be available..."
# Đợi Kafka sẵn sàng (cần cài đặt kafka-topics trong image hoặc dùng cách kiểm tra khác)
# Cách đơn giản là đợi một khoảng thời gian cố định
sleep 15

echo "Creating Kafka topics..."

# Hàm tạo topic
create_topic() {
  TOPIC_NAME=$1
  PARTITIONS=$2
  REPLICATION_FACTOR=$3
  EXTRA_CONFIG=$4

  echo "Attempting to create topic: $TOPIC_NAME"
  docker exec $KAFKA_CONTAINER kafka-topics --bootstrap-server $BOOTSTRAP_SERVER --create \
    --topic $TOPIC_NAME \
    --partitions $PARTITIONS \
    --replication-factor $REPLICATION_FACTOR \
    $EXTRA_CONFIG

  if [ $? -eq 0 ]; then
    echo "Topic $TOPIC_NAME created successfully or already exists."
  else
    echo "Failed to create topic $TOPIC_NAME."
    # Có thể thêm kiểm tra topic tồn tại trước khi tạo
  fi
}

# Tạo các topic cần thiết
create_topic "raw_traffic_data" 3 3 ""
create_topic "connect_configs" 1 3 "--config cleanup.policy=compact"
create_topic "connect_offsets" 1 3 "--config cleanup.policy=compact"
create_topic "connect_status" 1 3 "--config cleanup.policy=compact"


echo "Topic creation process finished."