import random
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class ChargeRequest(BaseModel):
    order_id: str
    amount: float

@app.post("/payments/charge")
def charge(req: ChargeRequest):
    success = random.random() < 0.8  # 80% chance of success

    if success:
        return {
            "status": "success",
            "order_id": req.order_id,
            "amount_charged": req.amount,
            "transaction_id": f"txn_{req.order_id}"
        }
    else:
        return {
            "status": "failed",
            "order_id": req.order_id,
            "reason": "card_declined"
        }