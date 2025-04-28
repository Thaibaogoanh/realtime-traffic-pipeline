# Sử dụng base image jupyter/pyspark-notebook
# Chọn tag tương ứng với phiên bản Spark bạn muốn (vd: spark-3.3.4)
# Hoặc dùng latest nếu không quá quan trọng version Spark chính xác
FROM jupyter/pyspark-notebook:spark-3.5.0

# User mặc định là jovyan (UID 1000) và đã có sudo (nếu cấu hình)
# Chuyển sang root để cài đặt thư viện hệ thống nếu cần
# USER root
# RUN apt-get update && apt-get install -y --no-install-recommends <tên-package> && rm -rf /var/lib/apt/lists/*
# USER ${NB_UID} # Quay lại user mặc định (jovyan)

# Cài đặt các thư viện Python bổ sung cần cho project từ requirements.txt
# Copy file requirements vào thư mục tạm
COPY --chown=${NB_UID}:${NB_GID} ./requirements.txt /tmp/requirements.txt

# Cài đặt bằng pip với user jovyan
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --user -r /tmp/requirements.txt

# WORKDIR đã được đặt là /home/jovyan/work trong base image
# Chúng ta sẽ mount code vào đây từ docker-compose.yml

# Base image đã có CMD để khởi động Jupyter Lab/Notebook server
# Không cần định nghĩa lại CMD/ENTRYPOINT trừ khi muốn tùy biến cao