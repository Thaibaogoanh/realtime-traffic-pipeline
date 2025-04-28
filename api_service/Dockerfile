# Sử dụng base image Python slim
FROM python:3.10-slim

# Đặt thư mục làm việc
WORKDIR /app

# Copy file requirements
COPY ./requirements.txt /app/requirements.txt

# Cài đặt dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy mã nguồn ứng dụng FastAPI
COPY ./app /app

# Mở port 8000
EXPOSE 8000

# Lệnh chạy ứng dụng
# Đảm bảo file main.py nằm trong thư mục app/
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
# Thêm --reload để tự động load lại code khi phát triển (xóa khi deploy production)