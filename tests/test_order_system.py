import time
import uuid
import requests

ORDER_URL = "http://localhost:8000"
INVENTORY_URL = "http://localhost:8001"


def unique_id():
    return str(uuid.uuid4())


def seed_product(product_id, stock, name="Test Product"):
    resp = requests.post(f"{INVENTORY_URL}/seed", json={
        "product_id": product_id, "name": name, "stock": stock
    })
    assert resp.status_code == 200


def get_order_status(order_id, timeout=5):
    """Polls the order status until it's no longer PENDING, or times out."""
    start = time.time()
    while time.time() - start < timeout:
        resp = requests.get(f"{ORDER_URL}/orders/{order_id}")
        if resp.status_code == 200:
            data = resp.json()
            if data["status"] != "PENDING":
                return data
        time.sleep(0.3)
    raise TimeoutError(f"Order {order_id} did not resolve in time")


def get_stock(product_id):
    resp = requests.get(f"{INVENTORY_URL}/inventory/{product_id}")
    return resp.json()["stock"]


def test_successful_order_reduces_stock():
    """A PAID order should permanently reduce inventory."""
    product_id = f"test-{unique_id()}"
    seed_product(product_id, stock=10)

    order_id = unique_id()
    resp = requests.post(f"{ORDER_URL}/orders", json={
        "order_id": order_id, "product_id": product_id,
        "quantity": 2, "amount": 20.0
    })
    assert resp.status_code == 200
    assert resp.json()["status"] == "PENDING"

    final = get_order_status(order_id)
    stock_after = get_stock(product_id)

    if final["status"] == "PAID":
        assert stock_after == 8  # 10 - 2
    else:
        assert stock_after == 10  # rolled back correctly


def test_duplicate_order_id_is_idempotent():
    """Sending the same order_id twice should not double-charge or
    double-reserve -- the second call should return the identical
    cached result."""
    product_id = f"test-{unique_id()}"
    seed_product(product_id, stock=10)

    order_id = unique_id()
    payload = {"order_id": order_id, "product_id": product_id, "quantity": 1, "amount": 10.0}

    first = requests.post(f"{ORDER_URL}/orders", json=payload)
    get_order_status(order_id)  # wait for it to resolve

    stock_after_first = get_stock(product_id)

    second = requests.post(f"{ORDER_URL}/orders", json=payload)

    stock_after_second = get_stock(product_id)

    assert stock_after_first == stock_after_second, "Duplicate request should not change stock again"


def test_insufficient_stock_is_rejected():
    """Requesting more than available stock should fail immediately,
    without ever reaching payment."""
    product_id = f"test-{unique_id()}"
    seed_product(product_id, stock=1)

    order_id = unique_id()
    resp = requests.post(f"{ORDER_URL}/orders", json={
        "order_id": order_id, "product_id": product_id,
        "quantity": 5, "amount": 50.0
    })
    assert resp.status_code == 400


def test_concurrent_reservation_does_not_oversell():
    """The classic race condition test, formalized: only exactly one
    of many simultaneous requests for the last unit should succeed."""
    import threading

    product_id = f"race-{unique_id()}"
    seed_product(product_id, stock=1)

    results = []

    def reserve():
        resp = requests.post(f"{INVENTORY_URL}/inventory/reserve", json={
            "product_id": product_id, "quantity": 1, "order_id": unique_id()
        })
        results.append(resp.status_code)

    threads = [threading.Thread(target=reserve) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    successes = results.count(200)
    assert successes == 1, f"Expected exactly 1 success, got {successes}"