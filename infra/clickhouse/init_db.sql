-- Tạo database nếu chưa tồn tại
CREATE DATABASE IF NOT EXISTS traffic_db;

-- Tạo bảng để lưu trữ sự kiện giao thông tổng hợp
-- Sử dụng database vừa tạo hoặc database mặc định nếu không tạo mới
CREATE TABLE IF NOT EXISTS traffic_db.traffic_events (
    -- ID của đoạn đường
    segment_id String,

    -- Thời gian bắt đầu của cửa sổ tổng hợp
    window_start DateTime CODEC(Delta, ZSTD(1)), -- Nén tốt cho timestamp

    -- Thời gian kết thúc của cửa sổ tổng hợp
    window_end DateTime CODEC(Delta, ZSTD(1)),

    -- Tốc độ trung bình trong cửa sổ
    avg_speed Float32 CODEC(Gorilla, ZSTD(1)), -- Nén tốt cho float thay đổi ít

    -- Số lượng xe đếm được trong cửa sổ
    vehicle_count UInt32 CODEC(T64, ZSTD(1)), -- Nén tốt cho integer không âm

    -- Trạng thái phân loại (clear, slow, congested)
    status LowCardinality(String) CODEC(ZSTD(1)), -- Nén tốt cho string có ít giá trị khác nhau

    -- Thời gian bản ghi được xử lý và ghi vào DB (tự động)
    processing_time DateTime DEFAULT now() CODEC(Delta, ZSTD(1))

) ENGINE = MergeTree() -- Engine phổ biến và mạnh mẽ của ClickHouse
PARTITION BY toYYYYMM(window_start) -- Phân vùng dữ liệu theo tháng (tối ưu truy vấn theo thời gian)
ORDER BY (segment_id, window_start); -- Khóa sắp xếp (quan trọng cho hiệu năng MergeTree)
-- TTL toDate(window_start) + INTERVAL 3 MONTH DELETE; -- (Tùy chọn) Tự động xóa dữ liệu cũ hơn 3 tháng