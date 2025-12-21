from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "first-topic"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="test-consumer-group",
    value_deserializer=lambda v: v.decode("utf-8", errors="replace")
)

print(f"Listening on topic: {TOPIC_NAME} ... (Ctrl+C to stop)")
for msg in consumer:
    print(msg.value)
