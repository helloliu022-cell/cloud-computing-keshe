import json
import os
import random
import socket
import time
from datetime import datetime, timezone

from paho.mqtt import client as mqtt_client

BROKER_HOST = os.getenv("BROKER_HOST", "127.0.0.1")
BROKER_PORT = int(os.getenv("BROKER_PORT", "1883"))
SENSOR_ID = os.getenv("SENSOR_ID", "k3s-edge-01")
TOPIC = os.getenv("MQTT_TOPIC", f"edge/sensor/{SENSOR_ID}")
INTERVAL = float(os.getenv("PUBLISH_INTERVAL", "2"))

CLIENT_ID = f"publisher-{SENSOR_ID}-{socket.gethostname()}"


def build_payload(seq):
    return {
        "sensor_id": SENSOR_ID,
        "seq": seq,
        "temperature": round(random.uniform(20.0, 35.0), 2),
        "humidity": round(random.uniform(35.0, 80.0), 2),
        "voltage": round(random.uniform(3.1, 4.2), 2),
        "edge_node": socket.gethostname(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    client = mqtt_client.Client(
        client_id=CLIENT_ID,
        callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2,
    )

    while True:
        try:
            client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
            print(f"[Publisher] Connected to broker {BROKER_HOST}:{BROKER_PORT}", flush=True)
            break
        except Exception as e:
            print(f"[Publisher] Connect failed: {e}, retrying...", flush=True)
            time.sleep(3)

    seq = 0
    while True:
        seq += 1
        payload = build_payload(seq)
        payload_text = json.dumps(payload, ensure_ascii=False)
        result = client.publish(TOPIC, payload_text, qos=1)
        print(f"[Publisher] topic={TOPIC}, result={result.rc}, payload={payload_text}", flush=True)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
