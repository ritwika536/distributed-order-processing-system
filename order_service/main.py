import json
import uuid
import pika
import requests
import time
import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

INVENTORY_URL = "http://localhost:8001"

app = FastAPI()
redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)


class CreateOrderRequest(BaseModel):
    order_id: str
    product_id: str
    quantity: int
    amount: float


def request_payment(order_id: str, amount: float, timeout: int = 10):
    """Sends a 'charge this order' message to Payment Service via RabbitMQ,
    and waits for the result to come back on a temporary reply queue."""
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()

    # Create a temporary, auto-named queue just for this one reply
    result = channel.queue_declare(queue="", exclusive=True)
    callback_queue = result.method.queue

    correlation_id = str(uuid.uuid4())
    response_holder = {}

    def on_response(ch, method, properties, body):
        if properties.correlation_id == correlation_id:
            response_holder["data"] = json.loads(body)
            ch.stop_consuming()

    channel.basic_consume(queue=callback_queue, on_message_callback=on_response, auto_ack=True)

    channel.queue_declare(queue="payment_requests")
    channel.basic_publish(
        exchange="",
        routing_key="payment_requests",
        properties=pika.BasicProperties(reply_to=callback_queue, correlation_id=correlation_id),
        body=json.dumps({"order_id": order_id, "amount": amount}),
    )

    connection.process_data_events(time_limit=timeout)
    connection.close()

    if "data" not in response_holder:
        return {"status": "failed", "reason": "payment_service_timeout"}

    return response_holder["data"]

def release_inventory_with_retry(product_id: str, quantity: int, order_id: str, max_attempts: int = 3):
    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.post(
                f"{INVENTORY_URL}/inventory/release",
                json={"product_id": product_id, "quantity": quantity, "order_id": order_id},
                timeout=5,
            )
            if resp.status_code == 200:
                print(f"[Rollback] Released inventory for {order_id} on attempt {attempt}")
                return True
        except requests.RequestException as e:
            print(f"[Rollback] Attempt {attempt} failed for {order_id}: {e}")

        if attempt < max_attempts:
            time.sleep(1)

    failed_rollback = {
        "order_id": order_id,
        "product_id": product_id,
        "quantity": quantity,
        "reason": "release_failed_after_retries",
    }
    redis_client.rpush("failed_rollbacks", json.dumps(failed_rollback))
    print(f"[Rollback] FAILED after {max_attempts} attempts for {order_id} -- logged for manual review")
    return False
@app.post("/orders")
def create_order(req: CreateOrderRequest):
    # --- Idempotency check ---
    cached_result = redis_client.get(f"order_result:{req.order_id}")
    if cached_result:
        print(f"[Idempotency] Duplicate request detected for {req.order_id}, returning cached result")
        return json.loads(cached_result)

    # Step 1: Reserve inventory
    reserve_resp = requests.post(
    f"{INVENTORY_URL}/inventory/reserve",
    json={"product_id": req.product_id, "quantity": req.quantity, "order_id": req.order_id},
)

    if reserve_resp.status_code != 200:
        raise HTTPException(status_code=400, detail="Could not reserve inventory")

    # Step 2: Charge payment via the message queue
    payment_result = request_payment(req.order_id, req.amount)

    # Step 3: If payment failed, roll back the inventory reservation
    if payment_result.get("status") != "success":
        release_inventory_with_retry(req.product_id, req.quantity, req.order_id)
        result = {
            "order_id": req.order_id,
            "status": "FAILED",
            "reason": payment_result.get("reason"),
            "note": "Inventory reservation was rolled back",
        }
    else:
        result = {
            "order_id": req.order_id,
            "status": "PAID",
            "transaction_id": payment_result.get("transaction_id"),
        }

    # --- Save result for idempotency, expires after 24 hours ---
    redis_client.setex(f"order_result:{req.order_id}", 86400, json.dumps(result))

    return result