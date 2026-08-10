import requests
resp = requests.get("http://localhost:8001/inventory/race-test")
print(resp.status_code, resp.json())