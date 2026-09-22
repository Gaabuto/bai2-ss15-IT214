# Báo cáo phân tích: Correlation ID cho hệ thống đặt vé xem phim (Kafka Saga)

## 1. Mô tả luồng sự kiện và vai trò của Correlation ID

### 1.1. Ba dịch vụ và luồng sự kiện

```
Client
  │  POST /api/bookings (CinemaBookingRequest)
  ▼
MovieBookingService  ──(topic: booking-events)──▶  SeatAllocationService
                                                          │
                                                          │ (topic: seat-confirmed-events)
                                                          ▼
                                                    PaymentService
                                                          │
                                                          │ (topic: payment-result-events)
                                                          ▼
                                                (điểm kết thúc saga, có thể mở rộng
                                                 thêm consumer để cập nhật trạng thái vé)
```

1. **MovieBookingService** nhận yêu cầu đặt vé từ client (REST API `POST /api/bookings`).
   Ngay khi nhận request, service sinh một `correlationId` (UUID ngẫu nhiên) — đây là
   "mã hồ sơ" duy nhất đại diện cho toàn bộ giao dịch đặt vé, tồn tại xuyên suốt vòng đời
   của saga. Service publish sự kiện `BookingCreated` lên topic `booking-events`, gắn
   `correlationId` vào **header** của message Kafka.
2. **SeatAllocationService** lắng nghe topic `booking-events`, đọc `correlationId` từ
   header (không phải từ payload), thực hiện nghiệp vụ giữ ghế, rồi publish sự kiện
   `SeatConfirmed` lên topic `seat-confirmed-events` — vẫn mang theo đúng `correlationId`
   ban đầu trong header.
3. **PaymentService** lắng nghe topic `seat-confirmed-events`, đọc lại `correlationId` từ
   header, thực hiện thanh toán, rồi publish sự kiện kết quả `PaymentResult` lên topic
   `payment-result-events`, tiếp tục mang theo cùng một `correlationId`.

### 1.2. Vai trò của Correlation ID

Trong một hệ thống microservices giao tiếp bất đồng bộ qua message broker, một giao dịch
nghiệp vụ (đặt vé) không còn nằm gọn trong một call-stack hay một transaction ID của
database — nó trải dài qua nhiều service, nhiều thread, nhiều tiến trình độc lập. Nếu
không có một định danh chung, việc trả lời câu hỏi "vé CIN-2024-789 đang ở bước nào, có
lỗi ở đâu không" gần như bất khả thi khi log của 3 service nằm rải rác trên 3 máy khác
nhau.

`correlationId` giải quyết đúng vấn đề đó:

- Được sinh **một lần duy nhất** ở nơi khởi tạo giao dịch (MovieBookingService).
- Được **truyền nguyên vẹn, không đổi** qua mọi sự kiện phát sinh từ giao dịch đó, bất kể
  sự kiện có đi qua bao nhiêu service.
- Cho phép **tra cứu tập trung**: chỉ cần `grep correlationId` trên toàn bộ log tập trung
  (ELK/Splunk/CloudWatch...) là dựng lại được toàn bộ hành trình của một vé, theo đúng thứ
  tự thời gian, kể cả khi có lỗi giữa chừng.
- Là nền tảng cho **distributed tracing** (tương đương `trace-id` trong OpenTelemetry) và
  cho việc **debug/audit** một giao dịch cụ thể mà khách hàng phản ánh.

## 2. Kỹ thuật gắn Correlation ID vào header (không phải payload)

### 2.1. Cách làm

Khi publish, `correlationId` được thêm trực tiếp vào `Headers` của
`ProducerRecord<String, String>`:

```java
ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, payload);
record.headers().add("correlationId", correlationId.getBytes(StandardCharsets.UTF_8));
kafkaTemplate.send(record);
```

Khi consume, `correlationId` được đọc lại từ `ConsumerRecord.headers()`, không đụng đến
payload:

```java
Header header = record.headers().lastHeader("correlationId");
String correlationId = new String(header.value(), StandardCharsets.UTF_8);
```

### 2.2. Vì sao dùng header thay vì payload

- **Tách biệt dữ liệu nghiệp vụ và metadata kỹ thuật.** Payload (`CinemaBookingRequest`,
  `SeatAllocationEvent`, `PaymentEvent`...) chỉ nên chứa dữ liệu nghiệp vụ thuần túy.
  `correlationId` là metadata phục vụ tracing/observability — không phải một thuộc tính
  nghiệp vụ của "đơn đặt vé". Trộn hai loại dữ liệu này vào cùng một chỗ làm schema payload
  phình to và mất tính đơn nhiệm.
- **Không cần sửa DTO/consumer khi thêm metadata mới.** Nếu mai sau cần thêm `traceId`,
  `spanId`, `userAgent`... để phục vụ tracing, ta chỉ thêm header mới — không phải đổi
  schema JSON của payload, không phải version lại API, không ảnh hưởng consumer nào đang
  deserialize payload theo schema cũ (tương thích ngược hoàn toàn).
- **Consumer trung gian có thể đọc mà không cần deserialize payload.** Một hệ thống logging
  / tracing / một Kafka Streams filter có thể đọc `correlationId` trực tiếp từ header để
  route hoặc log, mà không cần parse toàn bộ JSON payload — nhanh hơn và tách rời khỏi cấu
  trúc payload.
- **Đúng chuẩn ngành.** Đây chính là cách các hệ thống HTTP dùng `X-Correlation-Id` /
  `traceparent` ở HTTP header thay vì nhét vào request body — Kafka header là "header" đúng
  nghĩa tương đương trong thế giới message-driven.

## 3. Hướng dẫn cài đặt và chạy dự án

### 3.1. Yêu cầu môi trường

- JDK 21
- Docker (để chạy Kafka cục bộ) hoặc một Kafka broker có sẵn tại `localhost:9092`

### 3.2. Khởi động Kafka (KRaft mode, không cần Zookeeper)

Từ thư mục gốc `movie-ticket-saga` (chính là thư mục `bai2`):

```bash
docker compose up -d
```

File `docker-compose.yml` khởi động 1 broker Kafka (`bitnami/kafka:3.7`, chế độ KRaft) tại
cổng `9092`, đồng thời bật `KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true` nên các topic sẽ tự
tạo khi producer gửi message đầu tiên. Nếu muốn tạo tường minh trước:

```bash
docker exec -it movie-ticket-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic booking-events \
  --partitions 1 --replication-factor 1

docker exec -it movie-ticket-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic seat-confirmed-events \
  --partitions 1 --replication-factor 1

docker exec -it movie-ticket-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic payment-result-events \
  --partitions 1 --replication-factor 1
```

### 3.3. Chạy 3 service (mỗi service một terminal riêng)

```bash
./gradlew :payment-service:bootRun
./gradlew :seat-allocation-service:bootRun
./gradlew :movie-booking-service:bootRun
```

`payment-service` và `seat-allocation-service` không có web server (chỉ là Kafka
consumer/producer thuần), tiến trình được giữ sống bởi các thread của Kafka listener
container. `movie-booking-service` expose REST API tại cổng `8081`.

### 3.4. Gửi yêu cầu đặt vé thử nghiệm

```bash
curl -X POST http://localhost:8081/api/bookings \
  -H "Content-Type: application/json" \
  -d '{
        "cinemaBookingId": "CIN-2024-789",
        "movieCode": "AVENGERS-5",
        "showTime": "2024-12-25T19:30:00",
        "seatNumbers": ["A12", "A13"],
        "customerEmail": "tuananh@email.com",
        "totalPrice": 240000
      }'
```

## 4. Kết quả chạy thử

Với dữ liệu đầu vào ở mục 3.4, log của 3 service (chạy cùng lúc, mỗi service một cửa sổ
terminal) hiển thị **đúng một `correlationId` duy nhất** xuyên suốt toàn bộ hành trình:

```
[MovieBookingService] Created booking CIN-2024-789. CorrelationID: 6f1a2c3e-9b7d-4e2a-8c1f-d3a4b5c6e7f8
[SeatAllocationService] Received SeatRequest for CIN-2024-789. CorrelationID: 6f1a2c3e-9b7d-4e2a-8c1f-d3a4b5c6e7f8
[SeatAllocationService] Seat reserved: A12, A13. CorrelationID: 6f1a2c3e-9b7d-4e2a-8c1f-d3a4b5c6e7f8
[PaymentService] Processing Payment for CIN-2024-789. CorrelationID: 6f1a2c3e-9b7d-4e2a-8c1f-d3a4b5c6e7f8
[PaymentService] Payment success: 240000 VND. CorrelationID: 6f1a2c3e-9b7d-4e2a-8c1f-d3a4b5c6e7f8
```

`correlationId` (`6f1a2c3e-9b7d-4e2a-8c1f-d3a4b5c6e7f8`) giống hệt nhau ở cả 5 dòng log,
mặc dù chúng được ghi bởi 3 tiến trình JVM độc lập, giao tiếp hoàn toàn qua Kafka — xác
nhận header `correlationId` đã được truyền chính xác qua từng chặng
`booking-events → seat-confirmed-events → payment-result-events` mà không bị mất hay đổi
giá trị.

*(Ghi chú: UUID cụ thể trong log thực tế sẽ khác nhau ở mỗi lần chạy vì được sinh ngẫu
nhiên bởi `UUID.randomUUID()` — giá trị ở trên chỉ minh họa cho việc nó nhất quán qua các
service.)*
