import  json
import time
import os
import threading
import datetime

import pika
import requests
import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


def log(message: str):
    print(f"[{datetime.datetime.now().isoformat()}] {message}")


INVENTORY_URL = os.environ.get("INVENTORY_URL", "http://localhost:8001")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
RABBITMQ_URL = os.environ.get("RABBITMQ_URL")


def get_connection_params():
    if RABBITMQ_URL:
        return pika.URLParameters(RABBITMQ_URL)
    return pika.ConnectionParameters("localhost", heartbeat=600)


app = FastAPI()
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# Reuse one HTTP connection pool for calls to Inventory Service
http_session = requests.Session()

# pika connections aren't thread-safe, so each thread gets its own
_thread_local = threading.local()


def reset_channel():
    """Drop the cached connection so the next get_channel() reconnects."""
    conn = getattr(_thread_local, "connection", None)
    try:
        if conn and not conn.is_closed:
            conn.close()
    except Exception:
        pass
    for attr in ("connection", "channel"):
        if hasattr(_thread_local, attr):
            delattr(_thread_local, attr)


def get_channel():
    if not hasattr(_thread_local, "channel") or _thread_local.connection.is_closed:
        _thread_local.connection = pika.BlockingConnection(get_connection_params())
        _thread_local.channel = _thread_local.connection.channel()
        _thread_local.channel.queue_declare(queue="payment_requests", durable=True)
    return _thread_local.channel


class CreateOrderRequest(BaseModel):
    order_id: str
    product_id: str
    quantity: int
    amount: float


def save_order_status(order_id: str, status_data: dict):
    redis_client.setex(f"order_result:{order_id}", 86400, json.dumps(status_data))


def publish_payment_request(order_id: str, amount: float, product_id: str, quantity: int):
    """Publish a charge request. If the cached connection has gone stale
    (idle timeout, network reset), reconnect and retry once."""
    body = json.dumps({
        "order_id": order_id,
        "amount": amount,
        "product_id": product_id,
        "quantity": quantity,
    })
    for attempt in (1, 2):
        try:
            channel = get_channel()
            channel.basic_publish(exchange="", routing_key="payment_requests", body=body)
            return
        except Exception as e:
            log(f"[Publish] Attempt {attempt} failed for {order_id}: {e}")
            reset_channel()
            if attempt == 2:
                raise


def release_inventory_with_retry(product_id: str, quantity: int, order_id: str, max_attempts: int = 3):
    for attempt in range(1, max_attempts + 1):
        try:
            resp = http_session.post(
                f"{INVENTORY_URL}/inventory/release",
                json={"product_id": product_id, "quantity": quantity, "order_id": order_id},
                timeout=5,
            )
            if resp.status_code == 200:
                log(f"[Rollback] Released inventory for {order_id} on attempt {attempt}")
                return True
        except requests.RequestException as e:
            log(f"[Rollback] Attempt {attempt} failed for {order_id}: {e}")

        if attempt < max_attempts:
            time.sleep(1)

    redis_client.rpush("failed_rollbacks", json.dumps({
        "order_id": order_id,
        "product_id": product_id,
        "quantity": quantity,
        "reason": "release_failed_after_retries",
    }))
    log(f"[Rollback] FAILED after {max_attempts} attempts for {order_id} -- logged for manual review")
    return False


def handle_payment_result(ch, method, properties, body):
    result = json.loads(body)
    order_id = result["order_id"]
    log(f"[Order listener] Received payment result: {result}")

    if result.get("status") == "success":
        save_order_status(order_id, {
            "order_id": order_id,
            "status": "PAID",
            "transaction_id": result.get("transaction_id"),
        })
    else:
        release_inventory_with_retry(result["product_id"], result["quantity"], order_id)
        save_order_status(order_id, {
            "order_id": order_id,
            "status": "FAILED",
            "reason": result.get("reason"),
            "note": "Inventory reservation was rolled back",
        })

    ch.basic_ack(delivery_tag=method.delivery_tag)


def start_result_listener():
    while True:
        try:
            connection = pika.BlockingConnection(get_connection_params())
            channel = connection.channel()
            channel.queue_declare(queue="payment_results", durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue="payment_results", on_message_callback=handle_payment_result)
            log("[Order listener] Waiting for payment results...")
            channel.start_consuming()
        except Exception as e:
            log(f"[Order listener] Connection lost, reconnecting in 5s... ({e})")
            time.sleep(5)


@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=start_result_listener, daemon=True)
    thread.start()


@app.post("/orders")
def create_order(req: CreateOrderRequest):
    cached_result = redis_client.get(f"order_result:{req.order_id}")
    if cached_result:
        log(f"[Idempotency] Duplicate request detected for {req.order_id}, returning cached result")
        return json.loads(cached_result)

    try:
        reserve_resp = http_session.post(
            f"{INVENTORY_URL}/inventory/reserve",
            json={"product_id": req.product_id, "quantity": req.quantity, "order_id": req.order_id},
            timeout=5,
        )
    except requests.RequestException as e:
        raise HTTPException(status_code=503, detail=f"Inventory service unreachable: {e}")

    if reserve_resp.status_code != 200:
        raise HTTPException(status_code=400, detail="Could not reserve inventory")

    save_order_status(req.order_id, {"order_id": req.order_id, "status": "PENDING"})

    try:
        publish_payment_request(req.order_id, req.amount, req.product_id, req.quantity)
    except Exception:
        # Couldn't hand the order to the payment queue: undo the reservation
        # instead of leaving the order stuck in PENDING forever.
        release_inventory_with_retry(req.product_id, req.quantity, req.order_id)
        save_order_status(req.order_id, {
            "order_id": req.order_id,
            "status": "FAILED",
            "reason": "payment_queue_unavailable",
            "note": "Inventory reservation was rolled back",
        })
        raise HTTPException(status_code=503, detail="Payment queue unavailable, please retry")

    return {"order_id": req.order_id, "status": "PENDING"}


@app.get("/orders/{order_id}")
def get_order(order_id: str):
    cached = redis_client.get(f"order_result:{order_id}")
    if not cached:
        raise HTTPException(status_code=404, detail="Order not found")
    return json.loads(cached)