import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

INVENTORY_URL = "http://localhost:8001"
PAYMENT_URL = "http://localhost:8002"

app = FastAPI()

class CreateOrderRequest(BaseModel):
    order_id: str
    product_id: str
    quantity: int
    amount: float

@app.post("/orders")
def create_order(req: CreateOrderRequest):
    # Step 1: Reserve inventory
    reserve_resp = requests.post(
        f"{INVENTORY_URL}/inventory/reserve",
        json={"product_id": req.product_id, "quantity": req.quantity}
    )

    if reserve_resp.status_code != 200:
        raise HTTPException(status_code=400, detail="Could not reserve inventory")

    # Step 2: Charge payment
    payment_resp = requests.post(
        f"{PAYMENT_URL}/payments/charge",
        json={"order_id": req.order_id, "amount": req.amount}
    )
    payment_result = payment_resp.json()

    # Step 3: If payment failed, roll back the inventory reservation
    if payment_result["status"] != "success":
        requests.post(
            f"{INVENTORY_URL}/inventory/release",
            json={"product_id": req.product_id, "quantity": req.quantity}
        )
        return {
            "order_id": req.order_id,
            "status": "FAILED",
            "reason": payment_result.get("reason"),
            "note": "Inventory reservation was rolled back"
        }

    return {"order_id": req.order_id, "status": "PAID", "transaction_id": payment_result["transaction_id"]}