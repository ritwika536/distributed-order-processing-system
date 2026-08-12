import json
import random
import threading
import pika
from fastapi import FastAPI
from pydantic import BaseModel
import datetime

def log(message: str):
    print(f"[{datetime.datetime.now().isoformat()}] {message}")
app = FastAPI()


class ChargeRequest(BaseModel):
    order_id: str
    amount: float


def process_charge(order_id: str, amount: float):
    """The actual 'charging' logic -- same random success/failure as before,
    just pulled into its own function so both the API and the queue listener
    can use it."""
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
    """Keeping the direct API endpoint too, so you can still test manually."""
    return process_charge(req.order_id, req.amount)


def on_request(ch, method, properties, body):
    data = json.loads(body)
    log(f"[Payment listener] Received charge request: {data}")

    result = process_charge(data["order_id"], data["amount"])
    result["product_id"] = data["product_id"]
    result["quantity"] = data["quantity"]

    # Publish the result to a fixed queue instead of replying directly --
    # Order Service's background listener will pick it up whenever it's free.
    ch.basic_publish(
        exchange="",
        routing_key="payment_results",
        body=json.dumps(result),
    )
    ch.basic_ack(delivery_tag=method.delivery_tag)
    log(f"[Payment listener] Published result: {result}")


def start_consumer():
    """Connects to RabbitMQ and listens forever. If the connection drops
    (e.g., heartbeat timeout after being idle), automatically reconnects
    instead of crashing."""
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    "localhost",
                    heartbeat=600,  # allow longer idle periods before timing out
                    blocked_connection_timeout=300,
                )
            )
            channel = connection.channel()
            channel.queue_declare(queue="payment_requests")
            channel.queue_declare(queue="payment_results")
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue="payment_requests", on_message_callback=on_request)
            log("[Payment listener] Waiting for charge requests...")
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            log("[Payment listener] Connection lost, reconnecting...")
            continue


@app.on_event("startup")
def startup_event():
    """When FastAPI starts up, also start the queue listener -- but in a
    separate background thread, so it doesn't block the API from working."""
    thread = threading.Thread(target=start_consumer, daemon=True)
    thread.start()