import json
import os
import time
from datetime import datetime, timezone

import redis
from paho.mqtt import client as mqtt_client

BROKER_HOST = os.getenv("BROKER_HOST", "mosquitto.edge-mqtt.svc.cluster.local")
BROKER_PORT = int(os.getenv("BROKER_PORT", "1883"))
TOPIC = os.getenv("MQTT_TOPIC", "edge/sensor/#")

REDIS_HOST = os.getenv("REDIS_HOST", "redis-svc.keshe.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")

CLIENT_ID = os.getenv("CLIENT_ID", "cloud-subscriber")


def get_redis():
    if REDIS_PASSWORD:
        return redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            decode_responses=True,
        )
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )


r = get_redis()


def on_connect(client, userdata, flags, reason_code, properties=None):
    print(f"[Subscriber] Connected to MQTT broker {BROKER_HOST}:{BROKER_PORT}, reason={reason_code}", flush=True)
    client.subscribe(TOPIC)
    print(f"[Subscriber] Subscribed topic: {TOPIC}", flush=True)


def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8", errors="ignore")
    now = datetime.now(timezone.utc).isoformat()

    record = {
        "received_at": now,
        "topic": msg.topic,
        "payload": payload,
    }

    try:
        parsed = json.loads(payload)
        sensor_id = parsed.get("sensor_id", "unknown")
    except Exception:
        sensor_id = "unknown"

    key_latest = f"mqtt:last:{sensor_id}"

    r.set(key_latest, json.dumps(record, ensure_ascii=False))
    r.lpush("mqtt:messages", json.dumps(record, ensure_ascii=False))
    r.ltrim("mqtt:messages", 0, 99)

    print(f"[Subscriber] topic={msg.topic}, payload={payload}, saved_to_redis={key_latest}", flush=True)


def main():
    while True:
        try:
            r.ping()
            print(f"[Subscriber] Redis connected: {REDIS_HOST}:{REDIS_PORT}", flush=True)
            break
        except Exception as e:
            print(f"[Subscriber] Waiting for Redis: {e}", flush=True)
            time.sleep(3)

    client = mqtt_client.Client(
        client_id=CLIENT_ID,
        callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2,
    )
    client.on_connect = on_connect
    client.on_message = on_message

    while True:
        try:
            client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
            client.loop_forever()
        except Exception as e:
            print(f"[Subscriber] MQTT connection failed: {e}, retrying...", flush=True)
            time.sleep(5)


if __name__ == "__main__":
    main()
