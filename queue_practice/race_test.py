import threading
import requests

URL = "http://localhost:8001/inventory/reserve"

results = []

def try_reserve(order_id):
    try:
        resp = requests.post(URL, json={
            "product_id": "race-test",
            "quantity": 1,
            "order_id": order_id
        })
        try:
            body = resp.json()
        except ValueError:
            body = {"raw_response": resp.text}
        results.append((order_id, resp.status_code, body))
    except Exception as e:
        results.append((order_id, "ERROR", str(e)))
threads = []
for i in range(10):
    t = threading.Thread(target=try_reserve, args=(f"race-order-{i}",))
    threads.append(t)

for t in threads:
    t.start()

for t in threads:
    t.join()

successes = [r for r in results if r[1] == 200]
failures = [r for r in results if r[1] != 200]

print(f"\nTotal requests: {len(results)}")
print(f"Successful reservations: {len(successes)}")
print(f"Failed: {len(failures)}")

print("\n--- Full details ---")
for order_id, status_code, body in results:
    print(f"{order_id}: status={status_code}, body={body}")

if len(successes) == 1:
    print("\n✅ CORRECT: exactly 1 request succeeded, no overselling")
else:
    print(f"\n❌ ISSUE: {len(successes)} requests succeeded")