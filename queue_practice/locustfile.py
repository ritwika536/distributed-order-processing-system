from locust import HttpUser, task, between
import random
import uuid

class OrderUser(HttpUser):
    wait_time = between(0.1, 0.5)  # small pause between requests per simulated user

    @task
    def place_order(self):
        order_id = str(uuid.uuid4())
        self.client.post("/orders", json={
            "order_id": order_id,
            "product_id": "sku-1",
            "quantity": 1,
            "amount": 10.00
        })