import json
import random
import threading
import pika
from fastapi import FastAPI
from pydantic import BaseModel

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
    """This function runs automatically every time a message arrives on the
    'payment_requests' queue."""
    data = json.loads(body)
    print(f"[Payment listener] Received charge request: {data}")

    result = process_charge(data["order_id"], data["amount"])

    # Send the result back to whichever queue the sender told us to reply to
    ch.basic_publish(
        exchange="",
        routing_key=properties.reply_to,
        properties=pika.BasicProperties(correlation_id=properties.correlation_id),
        body=json.dumps(result),
    )
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(f"[Payment listener] Sent result: {result}")


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
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue="payment_requests", on_message_callback=on_request)
            print("[Payment listener] Waiting for charge requests...")
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            print("[Payment listener] Connection lost, reconnecting...")
            continue


@app.on_event("startup")
def startup_event():
    """When FastAPI starts up, also start the queue listener -- but in a
    separate background thread, so it doesn't block the API from working."""
    thread = threading.Thread(target=start_consumer, daemon=True)
    thread.start()