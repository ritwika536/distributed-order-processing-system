import requests

resp = requests.post(
    "http://localhost:8001/seed",
    json={"product_id": "race-test", "name": "Limited Item", "stock": 1}
)
print(resp.status_code, resp.json())