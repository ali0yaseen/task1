from kafka import KafkaProducer
import csv
import json
import time
import random
from datetime import datetime, timezone
from pathlib import Path

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "first-topic"


CSV_FILE_PATH = Path(__file__).resolve().parent.parent / "data" / "Items-bigdata.csv"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    acks="all",
    retries=5,
    linger_ms=50
)

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def safe_int(v, default=0) -> int:
    try:
        s = str(v).strip()
        if not s:
            return default
        return int(float(s))
    except Exception:
        return default

def clean_row(row: dict) -> dict:

    cleaned = {}
    for k, v in (row or {}).items():
        kk = (k or "").replace("\ufeff", "").strip()
        vv = ("" if v is None else str(v)).replace("\ufeff", "").strip()
        cleaned[kk] = vv
    return cleaned

def read_csv_rows(csv_path: Path) -> list[dict]:

    encodings_to_try = ["utf-8-sig", "cp1252", "latin-1"]

    for enc in encodings_to_try:
        try:
            with open(csv_path, mode="r", encoding=enc, newline="") as f:
                reader = csv.DictReader(f)
                rows = [clean_row(r) for r in reader]

            return rows
        except UnicodeDecodeError:
            continue


    with open(csv_path, mode="r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        return [clean_row(r) for r in reader]

def pick_item_id(row: dict) -> str:
    for k in ["ItemId", "itemId", "item_id", "ID", "Id", "id", "SKU", "sku"]:
        if k in row and str(row[k]).strip():
            return str(row[k]).strip()
    if row:
        first_key = list(row.keys())[0]
        return str(row.get(first_key, "unknown")).strip() or "unknown"
    return "unknown"

def build_event(row: dict) -> dict:
    row = clean_row(row)
    item_id = pick_item_id(row)


    current_stock = safe_int(row.get("Stock Balance"), default=0)

    item_name = row.get("Iteam-Name", "Unknown Item")

    event_type = random.choices(["SALE", "RESTOCK"], weights=[0.75, 0.25])[0]
    qty = random.randint(1, 5)
    delta = -qty if event_type == "SALE" else qty

    event = {
        "event_time": now_iso(),
        "event_type": event_type,
        "item_id": item_id,
        "item_name": item_name,
        "delta_qty": delta,
        "source": "csv-simulator",
        "reported_stock": current_stock
    }

    return event
def main():
    if not CSV_FILE_PATH.exists():
        raise FileNotFoundError(f"CSV file not found: {CSV_FILE_PATH}")

    print(f"Starting Kafka Producer -> topic: {TOPIC_NAME}")
    print(f"Reading CSV from: {CSV_FILE_PATH}")

    rows = read_csv_rows(CSV_FILE_PATH)

    if not rows:
        print("CSV is empty. Nothing to stream.")
        return

    i = 0
    try:
        while True:
            row = rows[i % len(rows)]
            event = build_event(row)

            future = producer.send(TOPIC_NAME, value=event)
            metadata = future.get(timeout=10)

            print(
                f"Sent -> partition={metadata.partition}, offset={metadata.offset}, "
                f"event={event['event_type']}, item_id={event['item_id']}, delta={event['delta_qty']}"
            )

            i += 1
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopped by user (Ctrl+C).")
    finally:
        producer.flush()
        producer.close()
        print("Producer closed.")

if __name__ == "__main__":
    main()
