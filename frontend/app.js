// Map Initialization ( giữ nguyên phần khởi tạo bản đồ Leaflet)
const map = L.map('map').setView([10.96, 106.78], 13); // Center around Di An

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
}).addTo(map);

// --- Dữ liệu đoạn đường (cập nhật nếu cần) ---
const segments = {
    "segment_1": { coords: [[10.985, 106.75], [10.975, 106.75]], name: "Ngã tư 550 Area", layer: null, status: 'unknown' },
    "segment_2": { coords: [[10.945, 106.81], [10.935, 106.81]], name: "KCN Sóng Thần Area", layer: null, status: 'unknown' },
    "segment_3": { coords: [[10.965, 106.78], [10.955, 106.78]], name: "Trung tâm Dĩ An Area", layer: null, status: 'unknown' }
    // Thêm các đoạn đường khác nếu có
};

// --- Màu sắc trạng thái ---
const statusColors = {
    'clear': 'green',
    'slow': 'orange',
    'congested': 'red',
    'unknown': 'grey'
};

// --- Lấy phần tử DOM ---
const segmentInfoPanelsContainer = document.getElementById('segment-info-panels');
const connectionStatusDiv = document.getElementById('connection-status');
const connectionStatusText = connectionStatusDiv.querySelector('.status-text');

// --- Khởi tạo các panel thông tin và layer bản đồ ---
function initializeUI() {
    segmentInfoPanelsContainer.innerHTML = ''; // Xóa placeholder
    for (const segmentId in segments) {
        const segment = segments[segmentId];

        // Tạo layer đường trên bản đồ
        segment.layer = L.polyline(segment.coords, { color: statusColors[segment.status], weight: 5 }).addTo(map)
            .bindPopup(`<b>${segment.name} (${segmentId})</b><br>Status: ${segment.status}`);

        // Tạo panel thông tin
        const panel = document.createElement('div');
        panel.className = `segment-panel status-${segment.status}`; // Ban đầu là unknown
        panel.id = `panel-${segmentId}`;
        panel.innerHTML = `
            <h3>${segment.name} (${segmentId})</h3>
            <p><strong>Avg Speed:</strong> <span class="avg-speed">N/A</span> km/h</p>
            <p><strong>Vehicles:</strong> <span class="vehicle-count">N/A</span></p>
            <p><strong>Status:</strong> <span class="status">${segment.status}</span></p>
            <p><small><strong>Last Update:</strong> <span class="last-update">N/A</span></small></p>
        `;
        segmentInfoPanelsContainer.appendChild(panel);
    }
}

// --- Cập nhật trạng thái segment (cả map và panel) ---
function updateSegmentStatus(segmentId, segmentData) {
    if (segments[segmentId]) {
        const segment = segments[segmentId];
        const newStatus = segmentData.status || 'unknown'; // Dùng unknown nếu không có status
        const newColor = statusColors[newStatus];

        // 1. Cập nhật màu đường trên bản đồ
        if (segment.layer) {
            segment.layer.setStyle({ color: newColor });
            // Cập nhật nội dung popup (tùy chọn)
            // segment.layer.setPopupContent(`<b>${segment.name} (${segmentId})</b><br>Status: ${newStatus}`);
        }

        // 2. Cập nhật panel thông tin
        const panel = document.getElementById(`panel-${segmentId}`);
        if (panel) {
            panel.className = `segment-panel status-${newStatus}`; // Cập nhật class để đổi màu nền status
            panel.querySelector('.avg-speed').textContent = segmentData.avg_speed !== null ? segmentData.avg_speed.toFixed(1) : 'N/A';
            panel.querySelector('.vehicle-count').textContent = segmentData.vehicle_count !== null ? segmentData.vehicle_count : 'N/A';
            panel.querySelector('.status').textContent = newStatus;
             panel.querySelector('.last-update').textContent = segmentData.window_end ? new Date(segmentData.window_end).toLocaleTimeString() : 'N/A';
        }
         segments[segmentId].status = newStatus; // Lưu lại trạng thái mới
    } else {
        console.warn(`Segment ID "${segmentId}" not found in frontend config.`);
    }
}

// --- Thiết lập WebSocket ---
const wsUrl = `ws://${window.location.hostname}:8888/ws/traffic`; // Địa chỉ WebSocket server
let ws;

function connectWebSocket() {
    console.log(`Attempting to connect to WebSocket at: ${wsUrl}`);
    ws = new WebSocket(wsUrl);

    ws.onopen = function(event) {
        console.log("WebSocket connection opened.");
        connectionStatusDiv.className = 'status-connected';
        connectionStatusText.textContent = 'Connected';
        // Yêu cầu dữ liệu khởi tạo khi kết nối thành công
        ws.send(JSON.stringify({ action: "get_initial_data" }));
    };

    ws.onmessage = function(event) {
        try {
            const message = JSON.parse(event.data);
            console.log("Message received:", message);

            if (message.type === 'initial_data' || message.type === 'update') {
                 if (message.payload && typeof message.payload === 'object') {
                    for (const segmentId in message.payload) {
                        updateSegmentStatus(segmentId, message.payload[segmentId]);
                    }
                } else {
                     console.warn("Received initial_data/update message with invalid payload:", message.payload);
                 }
            } else if (message.type === 'error') {
                console.error("Error message from server:", message.error);
            }
        } catch (e) {
            console.error("Failed to parse WebSocket message or update UI:", e);
            console.error("Received data:", event.data);
        }
    };

    ws.onerror = function(event) {
        console.error("WebSocket error observed:", event);
        connectionStatusDiv.className = 'status-disconnected';
        connectionStatusText.textContent = 'Error';
    };

    ws.onclose = function(event) {
        console.log("WebSocket connection closed. Attempting to reconnect...", event.reason);
        connectionStatusDiv.className = 'status-disconnected';
        connectionStatusText.textContent = 'Disconnected';
        // Thử kết nối lại sau 5 giây
        setTimeout(connectWebSocket, 5000);
    };
}

// --- Khởi tạo UI và bắt đầu kết nối ---
document.addEventListener('DOMContentLoaded', (event) => {
    initializeUI();
    connectWebSocket();
});