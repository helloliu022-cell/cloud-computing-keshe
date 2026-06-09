import os
import socket
import time
from datetime import datetime

import redis
import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")
REDIS_KEY = "cloud_course_counter"


def get_redis_client():
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD if REDIS_PASSWORD else None,
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=2,
    )


@app.route("/")
def index():
    return jsonify(
        {
            "message": "Cloud Computing Course Design Backend",
            "students": [
                {"name": "刘晓慧", "student_id": "2023112512"},
                {"name": "陆香凝", "student_id": "2023112519"},
            ],
            "class": "计算机2023-04班",
            "hostname": socket.gethostname(),
            "time": datetime.now().isoformat(timespec="seconds"),
        }
    )


@app.route("/api/ping")
def ping():
    return jsonify(
        {
            "status": "ok",
            "service": "flask-backend",
            "hostname": socket.gethostname(),
            "time": datetime.now().isoformat(timespec="seconds"),
        }
    )


@app.route("/api/visit")
def visit():
    client_ip = request.headers.get("X-Forwarded-For", request.remote_addr)
    try:
        r = get_redis_client()
        count = r.incr(REDIS_KEY)
        r.set("last_client_ip", client_ip)
        return jsonify(
            {
                "status": "ok",
                "visit_count": count,
                "last_client_ip": client_ip,
                "redis_host": REDIS_HOST,
                "hostname": socket.gethostname(),
            }
        )
    except Exception as exc:
        return jsonify({"status": "redis_error", "error": str(exc)}), 500


@app.route("/api/compute")
def compute():
    start = time.time()
    total = 0
    for i in range(800000):
        total += i * i % 97
    elapsed = round(time.time() - start, 4)
    return jsonify(
        {
            "status": "ok",
            "result": total,
            "elapsed_seconds": elapsed,
            "hostname": socket.gethostname(),
        }
    )


@app.route("/api/external")
def external():
    resp = requests.get("https://www.huaweicloud.com", timeout=3)
    return jsonify({"status": "ok", "http_status": resp.status_code})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
