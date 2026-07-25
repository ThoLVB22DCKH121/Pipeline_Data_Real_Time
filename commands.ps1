# ===== Kafka Commands (PowerShell) =====

# Tạo topic trades.raw
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --create `
  --topic trades.raw `
  --partitions 3 `
  --replication-factor 1

# Liệt kê tất cả topics
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --list

# Consumer đọc trades.raw (real-time)
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic trades.raw

# Consumer đọc trades.raw (từ đầu)
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic trades.raw `
  --from-beginning

# Producer gửi message thủ công
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh `
  --bootstrap-server localhost:9092 `
  --topic trades.raw

# Consumer với consumer group
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic trades.raw `
  --group my-shared-group

# Xem trạng thái consumer group
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server localhost:9092 `
  --describe `
  --group my-shared-group

# Consumer đọc trades.enriched (từ đầu)
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic trades.enriched `
  --from-beginning

# ===== ClickHouse Commands =====

# Đếm số records trong bảng trades
Invoke-WebRequest -UseBasicParsing "http://localhost:8123/?user=default&password=123456&query=SELECT+count()+FROM+trades"
