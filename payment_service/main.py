import json
import random
import threading
import time
import os
import pika
import datetime
from fastapi import FastAPI
from pydantic import BaseModel

def log(message: str):
    print(f"[{datetime.datetime.now().isoformat()}] {message}")

RABBITMQ_URL = os.environ.get("RABBITMQ_URL")

def get_connection_params():
    if RABBITMQ_URL:
        return pika.URLParameters(RABBITMQ_URL)
    return pika.ConnectionParameters(
        "localhost",
        heartbeat=600,
        blocked_connection_timeout=300,
    )

app = FastAPI()


class ChargeRequest(BaseModel):
    order_id: str
    amount: float


def process_charge(order_id: str, amount: float):
    success = random.random() < 0.8
    if success:
        return {
            "status": "success",
            "order_id": order_id,
            "amount_charged": amount,
            "transaction_id": f"txn_{order_id}",
        }
    else:
        return {"status": "failed", "order_id": order_id, "reason": "card_declined"}


@app.post("/payments/charge")
def charge(req: ChargeRequest):
    return process_charge(req.order_id, req.amount)


def on_request(ch, method, properties, body):
    data = json.loads(body)
    log(f"[Payment listener] Received charge request: {data}")

    result = process_charge(data["order_id"], data["amount"])
    result["product_id"] = data["product_id"]
    result["quantity"] = data["quantity"]

    ch.basic_publish(
        exchange="",
        routing_key="payment_results",
        body=json.dumps(result),
    )
    ch.basic_ack(delivery_tag=method.delivery_tag)
    log(f"[Payment listener] Published result: {result}")


def start_consumer():
    while True:
        try:
            connection = pika.BlockingConnection(get_connection_params())
            channel = connection.channel()
            channel.queue_declare(queue="payment_requests", durable=True)
            channel.queue_declare(queue="payment_results", durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue="payment_requests", on_message_callback=on_request)
            log("[Payment listener] Waiting for charge requests...")
            channel.start_consuming()
        except Exception as e:
            log(f"[Payment listener] Connection lost, reconnecting in 5s... ({e})")
            time.sleep(5)


@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=start_consumer, daemon=True)
    thread.start()