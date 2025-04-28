# Chọn phiên bản Airflow phù hợp
ARG AIRFLOW_VERSION=2.8.1
ARG PYTHON_VERSION=3.10
#ARG SPARK_VERSION=3.3.3 # Phiên bản Spark muốn cài vào image Airflow
#ARG HADOOP_VERSION=3.3 # Phiên bản Hadoop

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root

# Cài các gói hệ thống cần thiết (ví dụ: default-jre cho Spark client)
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    curl \
    # Thêm các gói khác nếu cần
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Tải và cài đặt Spark binaries (để có spark-submit)
# Thay đổi version nếu cần để khớp với package Spark/Kafka
ARG SPARK_VERSION=3.3.4
ARG HADOOP_PROFILE=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
RUN curl -o /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_PROFILE}.tgz \
    && tar -xvzf /tmp/spark.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_PROFILE} ${SPARK_HOME} \
    && rm /tmp/spark.tgz

# (Quan trọng) Tải các JARs cần thiết cho Spark Submit vào thư mục jars của Spark
# Điều này đảm bảo Airflow worker có sẵn JARs khi submit job, không cần --packages nữa
# Thay đổi version JARs cho khớp với Spark và Kafka
ARG KAFKA_PKG_VERSION=3.3.4         
ARG AWS_PKG_VERSION=3.3.4           
ARG AVRO_PKG_VERSION=3.3.4          
ARG KAFKA_CLIENTS_VERSION=3.7.0  
ARG TOKEN_PROVIDER_VERSION=3.3.4   
ARG SCALA_VERSION=2.12              

RUN mkdir -p ${SPARK_HOME}/jars && cd ${SPARK_HOME}/jars && \
    echo "Downloading spark-sql-kafka..." && \
    curl -f -o spark-sql-kafka-0-10_${SCALA_VERSION}-${KAFKA_PKG_VERSION}.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${KAFKA_PKG_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${KAFKA_PKG_VERSION}.jar && \
    echo "Downloading spark-avro..." && \
    curl -f -o spark-avro_${SCALA_VERSION}-${AVRO_PKG_VERSION}.jar https://repo1.maven.org/maven2/org/apache/spark/spark-avro_${SCALA_VERSION}/${AVRO_PKG_VERSION}/spark-avro_${SCALA_VERSION}-${AVRO_PKG_VERSION}.jar && \
    echo "Downloading aws-java-sdk-bundle..." && \
    curl -f -o aws-java-sdk-bundle-1.11.1026.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar && \
    echo "Downloading hadoop-aws..." && \
    curl -f -o hadoop-aws-${AWS_PKG_VERSION}.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${AWS_PKG_VERSION}/hadoop-aws-${AWS_PKG_VERSION}.jar && \
    echo "Downloading commons-pool2..." && \
    curl -f -o commons-pool2-2.11.1.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    echo "Downloading kafka-clients..." && \
    curl -f -o kafka-clients-${KAFKA_CLIENTS_VERSION}.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar && \
    # +++ THÊM DÒNG NÀY VÀO +++
    echo "Downloading spark-token-provider-kafka..." && \
    curl -f -o spark-token-provider-kafka-0-10_${SCALA_VERSION}-${TOKEN_PROVIDER_VERSION}.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${TOKEN_PROVIDER_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${TOKEN_PROVIDER_VERSION}.jar && \
    # +++ KẾT THÚC THÊM +++
    echo "Finished downloading JARs." && \
    cd /

USER airflow

# Cài đặt các thư viện Python cho Airflow và các thư viện mà DAG cần
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

# (Tùy chọn) Copy các script hoặc file cấu hình khác vào image nếu cần